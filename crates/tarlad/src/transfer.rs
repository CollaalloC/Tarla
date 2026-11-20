//! A confirmed attempt stays immutable. A safe continuation receives a new run ID,
//! with the predecessor recorded on both the supervisor and the executing node.
use super::*;

fn clipped(text: &str, limit: usize) -> &str {
    let mut n = text.len().min(limit);
    while !text.is_char_boundary(n) {
        n -= 1;
    }
    &text[..n]
}

pub fn record_limit(db: &Connection, run: &str, raw: &Value) -> Result<()> {
    let until = raw["resets_at"]
        .as_i64()
        .filter(|v| *v > 0)
        .context("a quota checkpoint requires a positive resets_at timestamp")?;
    let progress = string(raw, "checkpoint")?;
    if progress.trim().is_empty() || progress.len() > 1600 {
        bail!("checkpoint must contain 1 to 1600 UTF-8 bytes")
    }
    db.execute(
        "INSERT INTO service_rate_limits VALUES(?,?,?) ON CONFLICT(run_id)
        DO UPDATE SET resets_at=excluded.resets_at,checkpoint=excluded.checkpoint",
        params![run, until, progress],
    )?;
    Ok(())
}

pub fn accept_quota(db: &Connection, _s: &Service, run: &str, raw: &Value) -> Result<()> {
    record_limit(db, run, raw)?;
    db.execute("INSERT INTO service_turn_transfers(run_id,quota,checkpoint) VALUES(?,1,?)
        ON CONFLICT(run_id) DO UPDATE SET quota=1,checkpoint=excluded.checkpoint WHERE state='pending'",
        params![run,raw["checkpoint"].as_str()])?;
    Ok(())
}

fn boundary(db: &Connection, parent: &str) -> Result<Value> {
    list(
        db,
        "SELECT r.*,t.target,t.prompt,t.quota,t.checkpoint,t.manual_source,t.state AS transfer_state,
        p.workdir AS old_workdir,q.request_json,e.epoch AS original_epoch
        FROM service_turn_transfers t JOIN runs r USING(run_id)
        JOIN service_requirements q USING(run_id)
        LEFT JOIN service_run_paths p USING(run_id) LEFT JOIN service_pin_epochs e USING(run_id)
        WHERE r.run_id=? AND r.observed_state IN ('failed','succeeded','cancelled')
        AND EXISTS(SELECT 1 FROM service_wakes w WHERE w.conversation_id=r.conversation_id
            AND json_extract(w.receipt,'$.run_id')=r.run_id)",
        &[&parent],
    )?
    .pop()
    .context("awaiting an acknowledged turn boundary")
}

fn owns_binding(db: &Connection, row: &Value) -> Result<bool> {
    let Some(path) = row["old_workdir"].as_str() else {
        return Ok(true);
    };
    let conv = string(row, "conversation_id")?;
    Ok(current(db, conv)?
        == Some((
            string(row, "target_node_id")?.into(),
            string(row, "target_slot_id")?.into(),
            path.into(),
        ))
        && epoch(db, conv)?.0 == row["original_epoch"].as_str().unwrap_or("legacy"))
}

pub fn commit_continuation(
    tx: &rusqlite::Transaction<'_>,
    conv: &str,
    parent: &str,
    child: &str,
    destination: &AgentSlotSnapshot,
    path: Option<&str>,
) -> Result<()> {
    let row = boundary(tx, parent)?;
    if row["transfer_state"] != "pending"
        || row["conversation_id"] != conv
        || row["desired_state"] == "cancelled"
        || !owns_binding(tx, &row)?
    {
        bail!("continuation superseded")
    }
    tx.execute(
        "UPDATE service_turn_transfers SET state='applying' WHERE run_id=? AND state='pending'",
        [parent],
    )?;
    if let Some(path) = path {
        if row["old_workdir"].is_null() {
            bail!("unpinned continuation cannot change binding");
        }
        change(tx, conv, &destination.node_id, &destination.slot_id, path)?;
        let (ep, _) = epoch(tx, conv)?;
        register(
            tx,
            conv,
            child,
            &destination.node_id,
            &destination.slot_id,
            path,
            &json!({"pin_epoch":ep}),
        )?;
    }
    tx.execute(
        "INSERT INTO service_resume_origins VALUES(?,?)",
        params![child, parent],
    )?;
    tx.execute("UPDATE service_turn_transfers SET state='applied',successor=?,last_error=NULL WHERE run_id=?",params![child,parent])?;
    let report = format!(
        "已在回合结束后转交给 {}/{}，后续任务为 {child}。",
        destination.node_id, destination.slot_id
    );
    control::record_run_reply(
        tx,
        &destination.workspace_id,
        conv,
        &format!("handoff:{parent}"),
        child,
        &report,
    )?;
    Ok(())
}

fn process(s: &Service, run: &str) -> Result<()> {
    let mut db = s.db()?;
    let row = boundary(&db, run)?;
    if row["transfer_state"] != "pending" {
        return Ok(());
    }
    let conv = string(&row, "conversation_id")?;
    let unconfirmed_quota =
        row["quota"] == 1 && row["observed_state"] != "failed" && row["manual_source"].is_null();
    if unconfirmed_quota || !owns_binding(&db, &row)? || row["desired_state"] == "cancelled" {
        db.execute("UPDATE service_turn_transfers SET state='cancelled',last_error=NULL WHERE run_id=? AND state='pending'",[run])?;
        return Ok(());
    }
    let original: Value = serde_json::from_str(string(&row, "request_json")?)?;
    let mut req = TaskRequirement {
        workspace_id: s.id().into(),
        role: serde_json::from_value(
            original
                .get("role")
                .cloned()
                .unwrap_or(json!("implementer")),
        )?,
        required_caps: fleet::strings(&original, "required_caps")?,
        exclude_model_family: original["exclude_model_family"].as_str().map(String::from),
    };
    let mut slots = fleet::snapshots(s)?;
    if req.role == AgentRole::Reviewer {
        let family = fleet::author_family(&db, conv, original["author_run_id"].as_str())?;
        slots.retain(|slot| slot.model_family != family);
    }
    if row["quota"] == 1 {
        let mut stmt=db.prepare("WITH RECURSIVE history(run_id) AS (
            SELECT ?1 UNION SELECT o.predecessor FROM service_resume_origins o JOIN history h ON h.run_id=o.run_id)
            SELECT r.target_node_id,r.target_slot_id FROM history h JOIN runs r USING(run_id)")?;
        let tried = stmt
            .query_map([run], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
            })?
            .collect::<rusqlite::Result<std::collections::HashSet<_>>>()?;
        slots.retain(|slot| {
            !tried.contains(&(slot.node_id.clone(), slot.slot_id.clone()))
                || (slot.node_id == row["target_node_id"] && slot.slot_id == row["target_slot_id"])
        });
    }
    let parent_node = string(&row, "target_node_id")?;
    let parent_slot = string(&row, "target_slot_id")?;
    let selected = destination(
        &req,
        &slots,
        parent_node,
        parent_slot,
        row["target"].as_str().context("invalid handoff target")?,
        row["quota"] != 1,
    )
    .context("waiting for an eligible destination slot")?;
    let pinned = row["old_workdir"].is_string();
    let path = pinned.then(|| path_for(s, selected)).transpose()?;
    let resume_objective = row["quota"] == 1 && row["observed_state"] == "failed";
    if !resume_objective && row["prompt"].is_null() {
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let current = boundary(&tx, run)?;
        if current["transfer_state"] != "pending"
            || current["desired_state"] == "cancelled"
            || !owns_binding(&tx, &current)?
        {
            return Ok(());
        }
        if pinned {
            change(
                &tx,
                conv,
                &selected.node_id,
                &selected.slot_id,
                path.as_deref().unwrap(),
            )?;
        }
        tx.execute("UPDATE service_turn_transfers SET state='applied',last_error=NULL WHERE run_id=? AND state='pending'",[run])?;
        let report = format!(
            "已在回合结束后切换到 {}/{}。",
            selected.node_id, selected.slot_id
        );
        control::record_run_reply(&tx, s.id(), conv, &format!("handoff:{run}"), run, &report)?;
        tx.commit()?;
        return Ok(());
    }
    let packet: tarla_protocol::MissionPacket =
        serde_json::from_str(string(&row, "mission_packet_json")?)?;
    let old_summary = row["checkpoint"]
        .as_str()
        .filter(|v| !v.trim().is_empty())
        .or(row["receipt_summary"].as_str())
        .unwrap_or("Inspect the previous attempt before repeating a change.");
    let check = format!(
        "Continue after {run} ({}). Verify completed effects before repeating work. Checkpoint: {}",
        string(&row, "observed_state")?,
        clipped(old_summary, 360)
    );
    let mut context = vec![check];
    context.extend(packet.distilled_context.into_iter().take(4));
    let mut request = json!({"objective":row["prompt"].as_str().unwrap_or(&packet.objective),
        "acceptance_criteria":packet.acceptance_criteria, "distilled_context":context, "required_fact_keys":packet.required_fact_keys,
        "role":req.role,"required_caps":std::mem::take(&mut req.required_caps),
        "node_id":selected.node_id,"slot_id":selected.slot_id});
    for field in ["scope", "author_run_id", "exclude_model_family"] {
        if let Some(value) = original.get(field) {
            request[field] = value.clone();
        }
    }
    if let Some(path) = path {
        request["pinned_workdir"] = json!(path);
    }
    let id = format!("{:x}", Sha256::digest(format!("turn-continuation/{run}")));
    fleet::dispatch_selected(
        s,
        conv,
        &request,
        Some(id),
        None,
        Some(fleet::Acknowledgement::Continuation(run)),
    )?;
    Ok(())
}

async fn reconcile(s: &Service) -> Result<()> {
    let ready = list(
        &s.db()?,
        "SELECT t.run_id,r.conversation_id FROM service_turn_transfers t JOIN runs r USING(run_id)
         WHERE t.state='pending' AND r.observed_state IN ('failed','succeeded','cancelled')
           AND EXISTS(SELECT 1 FROM service_wakes w WHERE w.conversation_id=r.conversation_id
                      AND json_extract(w.receipt,'$.run_id')=r.run_id) ORDER BY t.rowid",
        &[],
    )?;
    for item in ready {
        let run = string(&item, "run_id")?;
        let conversation = string(&item, "conversation_id")?;
        // A chat or report in one conversation must not block other ready transfers.
        let Ok(_guard) = tokio::time::timeout(Duration::from_millis(5), s.lock(conversation)).await
        else {
            continue;
        };
        if let Err(error) = process(s, run) {
            let detail = error.to_string();
            s.db()?.execute(
                "UPDATE service_turn_transfers SET last_error=?1
                WHERE run_id=?2 AND state='pending' AND last_error IS NOT ?1",
                params![detail, run],
            )?;
        }
    }
    Ok(())
}

pub async fn worker(s: S) {
    if !s.is_supervisor() {
        return;
    }
    loop {
        if let Err(error) = reconcile(&s).await {
            eprintln!("turn handoff deferred: {error}");
        }
        tokio::select! {
            _ = s.notify.notified() => (),
            _ = tokio::time::sleep(Duration::from_millis(750)) => (),
        }
    }
}
