//! Mint a short-lived reporter credential on the executor and accept measurements
//! only for the named local slot. Remote visibility follows authenticated peer sync.
use super::*;
use axum::{
    extract::{Path as Route, State},
    http::HeaderMap,
};
use hmac::{Hmac, Mac};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use tarla_scheduler::SlotHealth;
fn mac(s: &Service, id: &str, expiry: i64) -> Result<String> {
    let mut m = Hmac::<Sha256>::new_from_slice(s.ws.attach_key.as_bytes())?;
    m.update(
        format!(
            "tarla/slot-telemetry/v1\0{}\0{}\0{id}\0{expiry}",
            s.id(),
            s.node()
        )
        .as_bytes(),
    );
    Ok(format!("{expiry}.{:x}", m.finalize().into_bytes()))
}
fn local(s: &Service, slot: &str) -> Result<()> {
    if !s.can_execute() || !s.slots.iter().any(|x| x.snapshot.slot_id == slot) {
        bail!("unknown local executor slot")
    }
    Ok(())
}
pub async fn issue(State(s): State<S>, Route(slot): Route<String>) -> Api {
    local(&s, &slot)?;
    let expires = now() + 3600;
    Ok(Json(
        json!({"token":mac(&s,&slot,expires)?,"expires_at":expires,"slot_id":slot}),
    ))
}
pub async fn ingest(
    State(s): State<S>,
    Route(slot): Route<String>,
    headers: HeaderMap,
    Json(v): Json<Value>,
) -> Api {
    let forbidden = || {
        ApiError(
            StatusCode::UNAUTHORIZED,
            "invalid slot telemetry credential".into(),
        )
    };
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|t| t.strip_prefix("Bearer "))
        .ok_or_else(forbidden)?;
    let (expiry, _) = token.split_once('.').ok_or_else(forbidden)?;
    let ts = expiry.parse::<i64>().map_err(|_| forbidden())?;
    if ts <= now() || ts > now() + 3600 || !secret_equal(token, &mac(&s, &slot, ts)?) {
        return Err(forbidden());
    }
    store(&s, &slot, &v)?;
    Ok(Json(json!({"stored":true})))
}
pub fn store(s: &Service, slot: &str, raw: &Value) -> Result<()> {
    local(s, slot)?;
    let report = raw.as_object().context("expected slot report")?;
    for (key, expected) in [
        ("workspace_id", s.id()),
        ("node_id", s.node()),
        ("slot_id", slot),
    ] {
        if report.get(key).is_some_and(|v| v != expected) {
            bail!("telemetry scope mismatch")
        }
    }
    let time = now();
    let ts = match raw.get("sampled_at") {
        None => time,
        Some(v) => v
            .as_i64()
            .filter(|n| *n <= time && *n >= time - 300)
            .context("invalid telemetry sample time")?,
    };
    let mut c = s.db()?;
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let previous = tx
        .query_row(
            "SELECT report,updated_at FROM service_telemetry WHERE slot_id=?",
            [slot],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)),
        )
        .optional()?;
    if previous.as_ref().is_some_and(|(_, t)| *t > ts) {
        bail!("stale slot report")
    }
    let mut out = if let Some((old, _)) = previous {
        serde_json::from_str(&old)?
    } else {
        json!({})
    };
    let mut changed = false;
    for (k, v) in report {
        match k.as_str() {
            "workspace_id" | "node_id" | "slot_id" | "sampled_at" => continue,
            "health" => match serde_json::from_value::<SlotHealth>(v.clone())? {
                SlotHealth::NodeOffline => bail!("node presence is measured by peer heartbeat"),
                SlotHealth::AuthExpired { message } if message.len() > 1024 => {
                    bail!("reason is too long")
                }
                _ => {}
            },
            "window_used_pct" | "weekly_used_pct" => {
                if !v.as_f64().is_some_and(|n| (0.0..=100.0).contains(&n)) {
                    bail!("invalid quota reading")
                }
            }
            "window_resets_at" | "weekly_resets_at" => {
                if !v.is_null() && !v.as_i64().is_some_and(|n| n >= 0) {
                    bail!("invalid quota reset time")
                }
            }
            _ => bail!("unsupported telemetry field"),
        }
        out[k] = v.clone();
        changed = true;
    }
    if !changed {
        bail!("empty slot report")
    }
    tx.execute("INSERT INTO service_telemetry VALUES(?,?,?) ON CONFLICT(slot_id) DO UPDATE SET report=excluded.report,updated_at=excluded.updated_at",params![slot,out.to_string(),ts])?;
    tx.commit()?;
    s.notify.notify_waiters();
    Ok(())
}
