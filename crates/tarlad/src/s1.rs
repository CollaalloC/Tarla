//! Executor event ordering, durable claim receipts and supervisor-issued evidence
//! of an immutable assignment. Terminal streams are never part of this journal.
use super::*;
use anyhow::ensure;
use hmac::{Hmac, Mac};
use rusqlite::{params, OptionalExtension};

const BATCH: usize = 24;
pub const SCHEMA: &str = include_str!("s1.sql");

pub fn cursor(db: &Connection, node: &str) -> Result<i64> {
    Ok(db
        .query_row(
            "SELECT seq FROM service_s1_frontier WHERE node=?",
            [node],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0))
}
pub fn sent(db: &Connection, supervisor: &str) -> Result<i64> {
    Ok(db
        .query_row(
            "SELECT seq FROM service_s1_delivery WHERE supervisor=?",
            [supervisor],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0))
}

pub fn batch(db: &Connection, after: i64) -> Result<Value> {
    ensure!(after >= 0, "invalid S1 cursor");
    let rows = list(
        db,
        "SELECT x.seq,o.event_id,o.run_id,o.event_type,o.payload_json
        FROM service_s1_sequence x JOIN outbox_events o USING(event_id)
        WHERE x.seq>? ORDER BY x.seq LIMIT 24",
        &[&after],
    )?;
    let mut claims = serde_json::Map::new();
    let mut events = vec![];
    let mut bytes = 0;
    for ev in rows {
        let run = string(&ev, "run_id")?;
        let proof: Option<String> = if claims.contains_key(run) {
            None
        } else {
            db.query_row(
                "SELECT certificate FROM service_s1_proofs WHERE run_id=?",
                [run],
                |r| r.get(0),
            )
            .optional()?
        };
        let added = serde_json::to_vec(&ev)?.len() + proof.as_ref().map_or(0, String::len);
        if !events.is_empty() && bytes + added > 64 * 1024 {
            break;
        }
        bytes += added;
        if let Some(proof) = proof {
            claims.insert(run.into(), serde_json::from_str(&proof)?);
        }
        events.push(ev);
    }
    Ok(json!({"s1_cursor":after,"events":events,"claims":claims}))
}

fn assignment_mac(db: &Connection, metadata: &Value) -> Result<Hmac<Sha256>> {
    let key: Vec<u8> = db.query_row(
        "SELECT key FROM service_s1_signer WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    let mut mac = Hmac::<Sha256>::new_from_slice(&key)?;
    mac.update(b"tarla/s1/assignment/v1\0");
    mac.update(&serde_json::to_vec(metadata)?);
    Ok(mac)
}

/// Leave raw model tool arguments and private authorization material on the
/// supervisor. The digest still lets a restored ledger reject conflicting retries.
pub fn request_digest(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}

pub fn certify(db: &Connection, s: &Service, node: &str, row: &mut Value) -> Result<()> {
    let id = string(row, "run_id")?;
    let requirements: Option<(String, String, String)> = db
        .query_row(
            "SELECT role,model_family,request_json FROM service_requirements WHERE run_id=?",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    let proof: Option<String> = db
        .query_row(
            "SELECT certificate FROM service_s1_proofs WHERE run_id=?",
            [id],
            |r| r.get(0),
        )
        .optional()?;
    let certificate = if let Some(original) = proof {
        serde_json::from_str(&original)?
    } else {
        let mut metadata = row.as_object().context("invalid assignment row")?.clone();
        metadata.remove("desired_state");
        metadata.remove("observed_state");
        metadata.insert("workspace_id".into(), json!(s.id()));
        metadata.insert("target_node_id".into(), json!(node));
        if let Some((role, family, request)) = requirements {
            metadata.insert("role".into(), json!(role));
            metadata.insert("model_family".into(), json!(family));
            metadata.insert("request_hash".into(), json!(request_digest(&request)));
        }
        let metadata = Value::Object(metadata);
        let tag = format!(
            "{:x}",
            assignment_mac(db, &metadata)?.finalize().into_bytes()
        );
        json!({"assignment":metadata,"tag":tag})
    };
    let claim: Option<String> = db
        .query_row(
            "SELECT claim_id FROM service_s1_claims WHERE run_id=?",
            [id],
            |r| r.get(0),
        )
        .optional()?;
    row["certificate"] = certificate;
    row["authorized_claim"] = json!(claim);
    Ok(())
}

pub fn apply_metadata(db: &Connection, id: &str, item: &Value) -> Result<()> {
    if let Some(path) = item["pinned_workdir"].as_str() {
        db.execute(
            "INSERT INTO service_pin_epochs VALUES(?,?)",
            params![id, item["pin_epoch"].as_str().unwrap_or("legacy")],
        )?;
        db.execute(
            "INSERT INTO service_run_paths VALUES(?,?)",
            params![id, path],
        )?;
    }
    if let Some(predecessor) = item["resume_from"].as_str() {
        validate_id(predecessor)?;
        db.execute(
            "INSERT INTO service_resume_origins(run_id,predecessor) VALUES(?,?)",
            params![id, predecessor],
        )?;
    }
    distillation::accept_baseline(db, id, item)?;
    Ok(())
}

/// Restore only a supervisor-signed, workspace- and executor-bound assignment.
/// An executor cannot turn an invented event into a mission on the supervisor.
fn recover(db: &Connection, s: &Service, node: &str, id: &str, proof: &Value) -> Result<()> {
    let data = &proof["assignment"];
    ensure!(
        data["run_id"] == id && data["workspace_id"] == s.id() && data["target_node_id"] == node,
        "recovery certificate scope mismatch"
    );
    let tag = format!("{:x}", assignment_mac(db, data)?.finalize().into_bytes());
    ensure!(
        secret_equal(&tag, string(proof, "tag")?),
        "invalid supervisor assignment certificate"
    );
    let term = data["supervisor_term"]
        .as_i64()
        .filter(|t| *t > 0 && *t <= s.term())
        .context("invalid original assignment term")?;
    let conv = string(data, "conversation_id")?;
    let slot = string(data, "target_slot_id")?;
    let packet: tarla_protocol::MissionPacket =
        serde_json::from_str(string(data, "mission_packet_json")?)?;
    ensure!(
        packet.run_id == id && packet.workspace_id == s.id(),
        "invalid recovery mission"
    );
    control::ensure_conversation(db, s, conv)?;
    fleet::insert_run(db, s, conv, node, slot, &packet)?;
    db.execute(
        "UPDATE runs SET supervisor_term=?,observed_state='unconfirmed' WHERE run_id=?",
        params![term, id],
    )?;
    apply_metadata(db, id, data)?;
    if let Some(digest) = data["request_hash"].as_str() {
        db.execute(
            "INSERT INTO service_requirements VALUES(?,?,?,?)",
            params![
                id,
                string(data, "role")?,
                string(data, "model_family")?,
                format!("sha256:{digest}")
            ],
        )?;
    }
    db.execute(
        "INSERT INTO service_s1_proofs VALUES(?,?)",
        params![id, proof.to_string()],
    )?;
    Ok(())
}

pub fn accept_claim(db: &Connection, id: &str, claim: &str) -> Result<()> {
    validate_id(claim)?;
    db.execute(
        "INSERT OR IGNORE INTO service_s1_claims VALUES(?,?)",
        params![id, claim],
    )?;
    let original: String = db.query_row(
        "SELECT claim_id FROM service_s1_claims WHERE run_id=?",
        [id],
        |r| r.get(0),
    )?;
    ensure!(
        original == claim,
        "a different executor claim already owns this run"
    );
    Ok(())
}

/// Applies only a contiguous prefix. A delayed packet can acknowledge its
/// identical prefix; a gap returns the exact cursor the executor must replay.
pub fn accept(
    db: &Connection,
    s: &Service,
    node: &str,
    request: &Value,
) -> Result<(i64, Vec<String>)> {
    let mut frontier = cursor(db, node)?;
    let start = request["s1_cursor"]
        .as_i64()
        .filter(|v| *v >= 0)
        .context("bad S1 cursor")?;
    let events = request["events"].as_array().context("missing S1 events")?;
    ensure!(events.len() <= BATCH, "S1 batch too large");
    let mut ack = vec![];
    if start > frontier {
        return Ok((frontier, ack));
    }
    for event in events {
        let seq = event["seq"]
            .as_i64()
            .filter(|v| *v > 0)
            .context("invalid executor sequence")?;
        if seq > frontier + 1 {
            break;
        }
        let eid = string(event, "event_id")?;
        let id = string(event, "run_id")?;
        let digest = request_digest(
            &json!([eid, id, event["event_type"], event["payload_json"]]).to_string(),
        );
        if seq <= frontier {
            let prior: Option<(String, String)> = db
                .query_row(
                    "SELECT event_id,digest FROM service_s1_received WHERE node=? AND seq=?",
                    params![node, seq],
                    |r| Ok((r.get(0)?, r.get(1)?)),
                )
                .optional()?;
            ensure!(
                prior == Some((eid.into(), digest)),
                "conflicting S1 history"
            );
        } else {
            let exists: bool = db.query_row(
                "SELECT EXISTS(SELECT 1 FROM runs WHERE run_id=?)",
                [id],
                |r| r.get(0),
            )?;
            if !exists {
                recover(db, s, node, id, &request["claims"][id])?;
            }
            peers::accept_events(db, s, node, std::slice::from_ref(event))?;
            db.execute(
                "INSERT INTO service_s1_received VALUES(?,?,?,?)",
                params![node, seq, eid, digest],
            )?;
            frontier = seq;
        }
        ack.push(eid.to_owned());
    }
    db.execute("INSERT INTO service_s1_frontier VALUES(?,?) ON CONFLICT(node) DO UPDATE SET seq=excluded.seq",params![node,frontier])?;
    Ok((frontier, ack))
}

pub fn save_offer(db: &Connection, s: &Service, id: &str, item: &Value, fresh: bool) -> Result<()> {
    let cert = &item["certificate"];
    if cert.is_null() {
        if fresh {
            fleet::claim(db, s, id)?;
        }
        return Ok(());
    }
    ensure!(s.can_execute(), "node cannot accept an assignment");
    let data = &cert["assignment"];
    ensure!(
        data["workspace_id"] == s.id() && data["target_node_id"] == s.node(),
        "misdirected supervisor offer"
    );
    for field in [
        "run_id",
        "conversation_id",
        "target_slot_id",
        "supervisor_term",
        "mission_packet_json",
        "pinned_workdir",
        "pin_epoch",
        "resume_from",
        "lesson_scope",
        "lesson_base",
    ] {
        ensure!(
            item[field] == data[field],
            "offer changed signed field {field}"
        );
    }
    let previous: Option<String> = db
        .query_row(
            "SELECT certificate FROM service_s1_proofs WHERE run_id=?",
            [id],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(saved) = previous {
        ensure!(
            serde_json::from_str::<Value>(&saved)? == *cert,
            "conflicting original offer"
        );
    } else {
        db.execute(
            "INSERT INTO service_s1_proofs VALUES(?,?)",
            params![id, cert.to_string()],
        )?;
    }
    if fresh {
        db.execute(
            "INSERT INTO service_claims(run_id,phase,updated_at) VALUES(?,'offered',?)",
            params![id, now()],
        )?;
    }
    let minted = uid();
    if db.execute(
        "INSERT OR IGNORE INTO service_s1_claims VALUES(?,?)",
        params![id, minted],
    )? != 0
    {
        let payload = json!({"run_id":id,"claim_id":minted});
        db.execute("INSERT INTO outbox_events(event_id,run_id,event_type,payload_json,created_at) VALUES(?,?,'run_claimed',?,?)",params![uid(),id,payload.to_string(),now()])?;
        s.sync_wake.notify_one();
    }
    let local: String = db.query_row(
        "SELECT claim_id FROM service_s1_claims WHERE run_id=?",
        [id],
        |r| r.get(0),
    )?;
    if item["desired_state"] == "cancelled" || item["authorized_claim"].as_str() == Some(&local) {
        if db.execute("UPDATE service_claims SET phase='queued',updated_at=? WHERE run_id=? AND phase='offered'",params![now(),id])?!=0 {
            s.notify.notify_one();
        }
    }
    Ok(())
}

pub fn startup(s: &S) {
    if !s.is_supervisor() {
        return;
    }
    for node in s.peers.keys() {
        let s = s.clone();
        let node = node.clone();
        tokio::spawn(async move {
            if let Err(err) = pull(&s, &node).await {
                eprintln!("startup S1 reconciliation with {node}: {err}");
            }
        });
    }
}
async fn pull(s: &Service, node: &str) -> Result<()> {
    loop {
        let seq = cursor(&s.db()?, node)?;
        let batch = peers::call(s, node, "s1_pull", &json!({"cursor":seq})).await?;
        ensure!(
            batch["workspace_id"] == s.id() && batch["term"] == s.term(),
            "S1 response scope mismatch"
        );
        let mut db = s.db()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let (next, _) = accept(&tx, s, node, &batch)?;
        tx.commit()?;
        s.receipt_wake.notify_one();
        if next <= seq || batch["events"].as_array().is_none_or(Vec::is_empty) {
            break;
        }
    }
    Ok(())
}
