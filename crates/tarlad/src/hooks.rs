//! Scoped executor callbacks. An agent can report facts about its own run and poll
//! a human decision. It cannot approve requests, dispatch, read a different run,
//! or modify the supervisor's memory ledger directly.
use super::*;
use axum::{
    extract::{Path as Route, State},
    http::HeaderMap,
};
use hmac::{Hmac, Mac};
use rusqlite::{params, OptionalExtension};
use tarla_memory::{MemoryBackend, Mutation, SqliteMemory};
use tarla_protocol::RunReceipt;

pub fn token(s: &Service, id: &str) -> Result<String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(s.ws.attach_key.as_bytes())?;
    mac.update(b"tarla/run-hook/v1\0");
    mac.update(id.as_bytes());
    Ok(format!("{:x}", mac.finalize().into_bytes()))
}
pub(super) fn active(c: &Connection, id: &str, node: &str) -> Result<bool> {
    Ok(c.query_row(
        "SELECT EXISTS(SELECT 1 FROM runs r JOIN service_claims c USING(run_id)
         LEFT JOIN service_launch_permits p USING(run_id)
         WHERE r.run_id=? AND r.target_node_id=? AND r.desired_state='running'
         AND r.observed_state NOT IN ('failed','succeeded','cancelled')
         AND c.phase IN ('launching','active') AND (p.state IS NULL OR p.state='admitted'))",
        params![id, node],
        |r| r.get(0),
    )?)
}

fn require_active(c: &Connection, id: &str, node: &str) -> Api<()> {
    if !active(c, id, node)? {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "run is not active on this executor".into(),
        ));
    }
    Ok(())
}
pub async fn callback(
    State(s): State<S>,
    Route((id, op)): Route<(String, String)>,
    headers: HeaderMap,
    Json(v): Json<Value>,
) -> Api {
    let supplied = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    let expected = token(&s, &id)?;
    if !secret_equal(supplied, &expected) {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "invalid run hook credential".into(),
        ));
    }
    require_active(&s.db()?, &id, s.node())?;
    let out = match op.as_str() {
        "approval" => {
            let request = string(&v, "request_id")?;
            validate_id(request)?;
            let aid = format!("{:x}", Sha256::digest(format!("{id}\n{request}")));
            let mut c = s.db()?;
            let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_active(&tx, &id, s.node())?;
            let tool = string(&v, "tool_or_cmd")?;
            let reason = string(&v, "risk_reason")?;
            if tool.len() > 2048 || reason.len() > 2048 {
                return Err(anyhow!("approval exceeds size limit").into());
            }
            let new=tx.execute("INSERT OR IGNORE INTO approvals(approval_id,workspace_id,run_id,node_id,tool_or_cmd,risk_reason,created_at) VALUES(?,?,?,?,?,?,?)",params![aid,s.id(),id,s.node(),tool,reason,now()])?;
            let (old_tool, old_reason, resolution): (String, String, String) = tx.query_row(
                "SELECT tool_or_cmd,risk_reason,resolution FROM approvals WHERE approval_id=?",
                [&aid],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            if old_tool != tool || old_reason != reason {
                return Err(ApiError(
                    StatusCode::CONFLICT,
                    "request id already refers to another action".into(),
                ));
            }
            if new != 0 {
                let payload = json!({"run_id":id,"state":"blocked","approval":{"approval_id":aid,"tool_or_cmd":tool,"risk_reason":reason}});
                tx.execute(
                    "UPDATE runs SET observed_state='blocked' WHERE run_id=?",
                    [&id],
                )?;
                tx.execute("INSERT INTO outbox_events(event_id,run_id,event_type,payload_json,created_at) VALUES(?,?,'state_changed',?,?)",params![uid(),id,payload.to_string(),now()])?;
            }
            tx.commit()?;
            s.notify.notify_waiters();
            json!({"approval_id":aid,"resolution":resolution})
        }
        "memory" => queue_proposal(&s, &v, Some(&id))?,
        "receipt" => {
            let receipt = tarla_context::bound_receipt(
                RunReceipt {
                    run_id: id.clone(),
                    final_state: "succeeded".into(),
                    executive_summary: string(&v, "executive_summary")?.into(),
                    artifact_refs: serde_json::from_value(
                        v.get("artifact_refs").cloned().unwrap_or(json!([])),
                    )
                    .map_err(anyhow::Error::from)?,
                    s0_transcript_pointer: format!("node:{}/run:{id}/transcript.jsonl", s.node()),
                },
                s.ws.config.context_policy.run_receipt_max_tokens,
            )?;
            let mut db = s.db()?;
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_active(&tx, &id, s.node())?;
            tx.execute("INSERT INTO service_receipt_drafts VALUES(?,?) ON CONFLICT(run_id) DO UPDATE SET receipt=excluded.receipt",params![id,json!(receipt).to_string()])?;
            tx.commit()?;
            json!({"stored":true})
        }
        "rate-limit" => {
            let mut db = s.db()?;
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_active(&tx, &id, s.node())?;
            handoff::record_limit(&tx, &id, &v)?;
            tx.commit()?;
            json!({"stored":true})
        }
        "telemetry" => {
            let mut db = s.db()?;
            let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            require_active(&tx, &id, s.node())?;
            let slot: String = tx.query_row(
                "SELECT target_slot_id FROM runs WHERE run_id=?",
                [&id],
                |r| r.get(0),
            )?;
            slot_reports::store(&tx, &s, &slot, &v)?;
            tx.commit()?;
            s.notify.notify_waiters();
            json!({"stored":true})
        }
        _ => return Err(ApiError(StatusCode::NOT_FOUND, "unknown run hook".into())),
    };
    Ok(Json(out))
}
pub fn queue_proposal(s: &Service, v: &Value, run: Option<&str>) -> Result<Value> {
    let operation = v["operation"].as_str().unwrap_or("update");
    anyhow::ensure!(
        matches!(operation, "create" | "update" | "archive"),
        "unsupported memory operation"
    );
    let key = v["proposal_id"]
        .as_str()
        .map(String::from)
        .unwrap_or_else(uid);
    validate_id(&key)?;
    let mut proposal = Mutation {
        proposal_id: format!("{:x}", Sha256::digest(format!("{}:{key}", s.node()))),
        memory_id: string(v, "memory_id")?.into(),
        scope: v["scope"].as_str().unwrap_or("global").into(),
        content: v["content"].as_str().unwrap_or_default().into(),
        base_version: v["base_version"].as_i64().context("missing base_version")?,
        source_node_id: s.node().into(),
        source_run_id: run.map(String::from),
        is_deleted: v["operation"] == "archive",
        hlc_timestamp: None,
    };
    tarla_memory::validate_mutation(&proposal)?;
    let mut c = s.db()?;
    let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    if let Some(id) = run {
        anyhow::ensure!(
            active(&tx, id, s.node())?,
            "run is not active on this executor"
        );
    }
    let existing: Option<(String, Option<String>)> = tx
        .query_row(
            "SELECT proposal,result FROM service_proposals WHERE id=?",
            [&proposal.proposal_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((p, result)) = existing {
        let mut orig: Mutation = serde_json::from_str(&p)?;
        orig.hlc_timestamp = proposal.hlc_timestamp.clone();
        if serde_json::to_value(orig)? != serde_json::to_value(&proposal)? {
            bail!("conflicting proposal id")
        }
        if let Some(result) = result {
            return Ok(serde_json::from_str(&result)?);
        }
    } else {
        proposal.hlc_timestamp = Some(tarla_memory::next_local_hlc(&tx, s.node())?);
        tx.execute(
            "INSERT INTO service_proposals VALUES(?,?,NULL)",
            params![proposal.proposal_id, serde_json::to_string(&proposal)?],
        )?;
    }
    tx.commit()?;
    s.sync_wake.notify_one();
    s.notify.notify_waiters();
    Ok(json!({"status":"queued","proposal_id":key}))
}
// An automatic lesson is checked against the supervisor's accepted receipt.
// Select it only after the very same event is in this round's confirmed S1
// prefix. An older supervisor's acked_at can survive a snapshot rollback.
fn ready_proposals(
    c: &Connection,
    confirmed_through: Option<i64>,
    time: i64,
) -> Result<Vec<Value>> {
    list(
        c,
        "SELECT p.id,p.proposal FROM service_proposals p LEFT JOIN service_proposal_retries r USING(id)
         WHERE p.result IS NULL AND (r.retry_after IS NULL OR r.retry_after<=?1)
         AND (p.id NOT GLOB 'distill-*' OR EXISTS (
            SELECT 1 FROM service_distillations d
            JOIN outbox_events e ON e.event_id=d.receipt_event AND e.run_id=d.run_id
            JOIN service_s1_sequence seq ON seq.event_id=e.event_id
            WHERE d.run_id=json_extract(p.proposal,'$.source_run_id') AND e.event_type='run_receipt'
              AND e.acked_at IS NOT NULL AND seq.seq<=?2
         ))
         ORDER BY (r.retry_after IS NOT NULL), r.retry_after, p.rowid LIMIT 16",
        &[&time, &confirmed_through],
    )
}

pub async fn flush_memory(s: &Service, confirmed_through: Option<i64>) -> Result<()> {
    let rows = ready_proposals(&s.db()?, confirmed_through, now())?;
    for row in rows {
        let p: Mutation = serde_json::from_str(string(&row, "proposal")?)?;
        let result = if s.is_supervisor() {
            apply(s, s.node(), &p)
        } else {
            peers::call(s, s.supervisor(), "propose", &json!({"proposal":p})).await
        };
        let c = s.db()?;
        let result = match result {
            Ok(value) => value,
            Err(error) => {
                // A stale or not-yet-recovered base must not block unrelated offline writes.
                c.execute("INSERT INTO service_proposal_retries VALUES(?,?) ON CONFLICT(id) DO UPDATE SET retry_after=excluded.retry_after",params![p.proposal_id,now()+5])?;
                return Err(error);
            }
        };
        let tx = c.unchecked_transaction()?;
        tx.execute(
            "UPDATE service_proposals SET result=? WHERE id=?",
            params![result.to_string(), p.proposal_id],
        )?;
        tx.execute(
            "DELETE FROM service_proposal_retries WHERE id=?",
            [&p.proposal_id],
        )?;
        tx.commit()?;
    }
    Ok(())
}
pub fn apply(s: &Service, node: &str, p: &Mutation) -> Result<Value> {
    if node != p.source_node_id {
        bail!("proposal source mismatch")
    }
    if let Some(id) = &p.source_run_id {
        if api::one(s, id)?["target_node_id"] != node {
            bail!("proposal does not own this run")
        }
    }
    let mut c = s.db()?;
    distillation::verify(&c, node, p)?;
    let result = SqliteMemory::new(&mut c, s.id(), s.node()).propose_mutation(p)?;
    peers::mutation_committed(s, &result);
    Ok(json!(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn automatic_proposals_wait_for_their_own_receipt_ack_without_delaying_manual_work() {
        let db = tarla_memory::open(std::path::Path::new(":memory:"), "work").unwrap();
        db.execute_batch(crate::MIGRATIONS).unwrap();
        db.execute_batch(crate::s1::SCHEMA).unwrap();
        // The queries use durable rows in the migrated schema. The fixture
        // supplies run identities solely to satisfy its local foreign keys.
        db.execute_batch("INSERT INTO conversations(conversation_id,workspace_id,created_at,updated_at)
            VALUES('conversation','work',1,1);
            INSERT INTO runs(run_id,workspace_id,conversation_id,supervisor_term,target_node_id,target_slot_id,
              desired_state,observed_state,mission_packet_json,created_at,updated_at)
              VALUES('one','work','conversation',1,'worker','slot','running','succeeded','{}',1,1),
                    ('two','work','conversation',1,'worker','slot','running','succeeded','{}',1,1);
            INSERT INTO outbox_events(event_id,run_id,event_type,payload_json,created_at,acked_at)
              VALUES('first','one','run_receipt','{\"final_state\":\"succeeded\"}',1,1),
                    ('second','two','run_receipt','{\"final_state\":\"succeeded\"}',1,NULL);
            INSERT INTO service_proposals(id,proposal,result)
              VALUES('distill-one','{\"source_run_id\":\"one\"}',NULL),
                    ('distill-two','{\"source_run_id\":\"two\"}',NULL),
                    ('manual','{\"source_run_id\":\"two\"}',NULL),
                    ('manual-retry','{}',NULL);
            INSERT INTO service_proposal_retries VALUES('manual-retry',100);").unwrap();
        let ready = |cursor, time| {
            ready_proposals(&db, cursor, time)
                .unwrap()
                .into_iter()
                .map(|row| row["id"].as_str().unwrap().to_owned())
                .collect::<Vec<_>>()
        };
        assert_eq!(ready(None, 99), ["manual"]);
        assert_eq!(ready(Some(0), 99), ["manual"]);
        assert_eq!(ready(Some(2), 99), ["distill-one", "manual"]);
        db.execute(
            "UPDATE outbox_events SET acked_at=2 WHERE event_id='second'",
            [],
        )
        .unwrap();
        // Rewind to a receiver whose restored cursor predates the second receipt.
        assert_eq!(
            ready(Some(1), 100),
            ["distill-one", "manual", "manual-retry"]
        );
        assert_eq!(
            ready(Some(2), 100),
            ["distill-one", "distill-two", "manual", "manual-retry"]
        );
    }
}
