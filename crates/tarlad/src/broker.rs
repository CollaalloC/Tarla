//! Zero-plaintext credential broker with `--op-id` SQLite idempotency lock (Spec §6.5).
//! Sensitive secrets remain inside `tarlad` and are never injected into `hc` terminal environments.
use super::*;
use hmac::{Hmac, Mac};
use rusqlite::{params, OptionalExtension, TransactionBehavior};

fn validate_secret_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 80
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        bail!("invalid credential reference `{name}`");
    }
    Ok(())
}

/// Scrub `tarlad`-private broker, pairing, and workspace secrets before spawning any Agent terminal.
pub fn scrub_agent_env(env: &mut BTreeMap<String, String>) {
    env.retain(|k, _| {
        !k.starts_with("TARLA_SECRET_")
            && !k.starts_with("TARLA_BROKER_")
            && !k.starts_with("TARLA_PAIR_")
    });
}

fn resolve_broker_secret(home: &Path, cred_ref: &str) -> Result<String> {
    validate_secret_name(cred_ref)?;
    let cred_file = home.join("credentials").join(cred_ref);
    if cred_file.is_file() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::symlink_metadata(&cred_file)?.permissions().mode() & 0o077;
            if mode != 0 {
                bail!(
                    "credential file `{}` must not be group/world accessible",
                    cred_file.display()
                );
            }
        }
        let secret = std::fs::read_to_string(&cred_file)?.trim().to_owned();
        anyhow::ensure!(!secret.is_empty(), "credential `{cred_ref}` is empty");
        return Ok(secret);
    }
    let env_key = format!(
        "TARLA_SECRET_{}",
        cred_ref
            .chars()
            .map(|c| if c == '-' {
                '_'
            } else {
                c.to_ascii_uppercase()
            })
            .collect::<String>()
    );
    let val = std::env::var(&env_key)
        .with_context(|| format!("missing broker credential `{cred_ref}` (file or `{env_key}`)"))?;
    let trimmed = val.trim().to_owned();
    anyhow::ensure!(!trimmed.is_empty(), "credential `{cred_ref}` is empty");
    Ok(trimmed)
}

fn redact_secret(text: &str, secret: &str) -> String {
    if secret.is_empty() {
        text.to_owned()
    } else {
        text.replace(secret, "[REDACTED]")
    }
}

/// Execute an external side-effecting operation (`http_request` or `hmac_sha256`) under an
/// atomic SQLite `idempotent_ops` lock keyed by `op_id`. Retries with the same `op_id`
/// return the cached result without re-running the side effect.
pub async fn execute_brokered_op(
    s: &Service,
    op_id: &str,
    run_id: &str,
    credential_ref: &str,
    action: &str,
    payload: &Value,
) -> Result<Value> {
    validate_id(op_id)?;
    validate_id(run_id)?;
    validate_secret_name(credential_ref)?;

    {
        let mut db = s.db()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let existing: Option<(String, Option<String>)> = tx
            .query_row(
                "SELECT status, result_json FROM idempotent_ops WHERE op_id = ?1",
                [op_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        match existing {
            Some((status, Some(saved))) if status == "succeeded" => {
                let mut parsed: Value = serde_json::from_str(&saved)?;
                parsed["replayed"] = json!(true);
                return Ok(parsed);
            }
            Some((status, _)) if status == "inflight" => {
                bail!("operation `{op_id}` is already in flight");
            }
            Some(_) => {
                tx.execute(
                    "UPDATE idempotent_ops SET status = 'inflight', result_json = NULL WHERE op_id = ?1",
                    [op_id],
                )?;
            }
            None => {
                tx.execute(
                    "INSERT INTO idempotent_ops(op_id, workspace_id, run_id, status, result_json, created_at)
                     VALUES (?1, ?2, ?3, 'inflight', NULL, ?4)",
                    params![op_id, s.id(), run_id, now()],
                )?;
            }
        }
        tx.commit()?;
    }

    let secret = match resolve_broker_secret(&s.home, credential_ref) {
        Ok(sec) => sec,
        Err(err) => {
            let _ = s.db().and_then(|db| {
                db.execute(
                    "UPDATE idempotent_ops SET status = 'failed' WHERE op_id = ?1",
                    [op_id],
                )
                .map_err(anyhow::Error::from)
            });
            return Err(err);
        }
    };

    let outcome: Result<Value> = async {
        match action {
            "hmac_sha256" => {
                let message = payload["message"]
                    .as_str()
                    .context("hmac_sha256 requires payload.message")?;
                let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
                    .map_err(|_| anyhow!("invalid HMAC key"))?;
                mac.update(message.as_bytes());
                let digest = format!("{:x}", mac.finalize().into_bytes());
                Ok(json!({
                    "op_id": op_id,
                    "action": "hmac_sha256",
                    "signature": digest,
                    "replayed": false,
                }))
            }
            "http_request" => {
                let url = string(payload, "url")?;
                let method_str = payload["method"].as_str().unwrap_or("POST");
                let method = reqwest::Method::from_bytes(method_str.as_bytes())
                    .context("invalid HTTP method")?;
                let header_name = payload["auth_header"].as_str().unwrap_or("Authorization");
                let header_prefix = payload["auth_prefix"].as_str().unwrap_or("Bearer ");
                let mut req = s
                    .http
                    .request(method, url)
                    .header(header_name, format!("{header_prefix}{secret}"));
                if let Some(body) = payload.get("body") {
                    req = req.json(body);
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| anyhow!("{}", redact_secret(&e.to_string(), &secret)))?;
                let code = resp.status().as_u16();
                let raw_body = resp.text().await.unwrap_or_default();
                let scrubbed = redact_secret(&raw_body, &secret);
                let gated = crate::guardrails::gate_output(
                    &s.ws.root.join("runtime"),
                    op_id,
                    &scrubbed,
                    crate::guardrails::DEFAULT_RESULT_GATE_CHARS,
                )?;
                if !(200..300).contains(&code) {
                    bail!("brokered HTTP {code}: {}", gated.text);
                }
                Ok(json!({
                    "op_id": op_id,
                    "action": "http_request",
                    "status_code": code,
                    "body": gated.text,
                    "replayed": false,
                }))
            }
            other => bail!("unsupported broker action `{other}`"),
        }
    }
    .await;

    let db = s.db()?;
    match outcome {
        Ok(val) => {
            let encoded = serde_json::to_string(&val)?;
            db.execute(
                "UPDATE idempotent_ops SET status = 'succeeded', result_json = ?1 WHERE op_id = ?2",
                params![encoded, op_id],
            )?;
            tarla_memory::record_sync_event(
                &db,
                s.id(),
                "broker_op",
                op_id,
                &json!({"op_id": op_id, "run_id": run_id, "action": action, "status": "succeeded"}),
                now(),
            )?;
            s.rev_wake.notify_waiters();
            Ok(val)
        }
        Err(err) => {
            let safe_msg = redact_secret(&err.to_string(), &secret);
            db.execute(
                "UPDATE idempotent_ops SET status = 'failed', result_json = ?1 WHERE op_id = ?2",
                params![serde_json::to_string(&json!({"error": safe_msg}))?, op_id],
            )?;
            Err(anyhow!(safe_msg))
        }
    }
}
