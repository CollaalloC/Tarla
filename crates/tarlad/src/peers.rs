use super::*;
use axum::{
    extract::{Path as Route, State},
    http::HeaderMap,
};
use rusqlite::{params, OptionalExtension};
use tarla_memory::{Memory, MemoryBackend, SqliteMemory};
use tarla_protocol::{MissionPacket, RunReceipt};
use tarla_scheduler::AgentSlotSnapshot;
pub struct Peer {
    pub url: String,
    token: String,
}
pub fn load(conf: &toml::Table, ws: &Workspace) -> Result<BTreeMap<String, Peer>> {
    let mut peers = BTreeMap::<String, Peer>::new();
    if let Some(rows) = conf.get("peers") {
        for row in rows.as_array().context("expected [[peers]]")? {
            let v = serde_json::to_value(row)?;
            let id = string(&v, "node_id")?;
            tarla_workspace::validate_name(id)?;
            let url = string(&v, "url")?.trim_end_matches('/');
            let parsed = reqwest::Url::parse(url)?;
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.host().is_none()
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.query().is_some()
                || parsed.fragment().is_some()
                || parsed.path() != "/"
            {
                bail!("peer URL must be a bare HTTP(S) origin on the paired network")
            }
            let token = std::env::var(string(&v, "token_env")?)
                .context("missing configured peer secret")?;
            if token.len() < 32
                || token == ws.token
                || token == ws.attach_key
                || peers.values().any(|p| p.token == token)
            {
                bail!("use a separate 32+ character token per paired peer")
            }
            if id == ws.config.workspace.node_id
                || peers
                    .insert(
                        id.into(),
                        Peer {
                            url: url.into(),
                            token,
                        },
                    )
                    .is_some()
            {
                bail!("duplicate or local peer id")
            }
        }
    };
    Ok(peers)
}
pub fn check_term(db: &mut Connection, ws: &Workspace) -> Result<()> {
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let authorized = &ws.config.workspace;
    let previous: Option<(String, i64)> = tx
        .query_row(
            "SELECT supervisor,term FROM service_epoch WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if previous.is_some_and(|(node, term)| {
        term > authorized.supervisor_term
            || (term == authorized.supervisor_term && node != authorized.supervisor_node_id)
    }) {
        bail!("workspace configuration conflicts with the persisted supervisor term")
    }
    tx.execute(
        "INSERT OR REPLACE INTO service_epoch VALUES(1,?,?)",
        params![authorized.supervisor_node_id, authorized.supervisor_term],
    )?;
    tx.commit()?;
    Ok(())
}
/// The authenticated hint carries only a cursor. Durable revisions still travel
/// through the usual pull/validation/ack path, which also handles missed hints.
pub fn memory_changed(s: &Service, seq: i64) {
    s.s2_changed.send_if_modified(|last| {
        if seq <= *last {
            false
        } else {
            *last = seq;
            true
        }
    });
}
pub fn mutation_committed(s: &Service, result: &tarla_memory::MutationResult) {
    if let tarla_memory::MutationResult::Applied { memory } = result {
        memory_changed(s, memory.sync_seq);
    }
}
pub fn start_s2_notifications(s: &S) {
    if !s.is_supervisor() {
        return;
    }
    for node in s.peers.keys() {
        let s = s.clone();
        let node = node.clone();
        let mut changes = s.s2_changed.subscribe();
        tokio::spawn(async move {
            // One in-flight hint per peer. Independent peers cannot block each other;
            // watch coalesces bursts while a peer is unreachable or slow.
            while changes.changed().await.is_ok() {
                let cursor = *changes.borrow_and_update();
                let _ = call(&s, &node, "memory_changed", &json!({"cursor":cursor})).await;
            }
        });
    }
}

pub async fn call(s: &Service, node: &str, op: &str, payload: &Value) -> Result<Value> {
    let peer = s.peers.get(node).context("node is not paired")?;
    let mut p = payload.clone();
    p["workspace_id"] = json!(s.id());
    p["term"] = json!(s.term());
    let res = s
        .http
        .post(format!("{}/peer/v1/{op}", peer.url))
        .header("x-tarla-peer", s.node())
        .bearer_auth(&peer.token)
        .json(&p)
        .send()
        .await?;
    if !res.status().is_success() {
        bail!("paired node rejected {op}: {}", res.status())
    }
    Ok(res.json().await?)
}
pub async fn receive(
    State(s): State<S>,
    Route(op): Route<String>,
    h: HeaderMap,
    Json(v): Json<Value>,
) -> Api {
    let node = h
        .get("x-tarla-peer")
        .and_then(|n| n.to_str().ok())
        .unwrap_or("");
    let auth = h
        .get("authorization")
        .and_then(|n| n.to_str().ok())
        .and_then(|n| n.strip_prefix("Bearer "))
        .unwrap_or("");
    if !s
        .peers
        .get(node)
        .is_some_and(|p| secret_equal(&p.token, auth))
    {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "invalid peer authentication".into(),
        ));
    }
    if v["workspace_id"] != s.id() || v["term"] != s.term() {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "workspace or supervisor epoch mismatch".into(),
        ));
    }
    let ans = match op.as_str() {
        "propose" => {
            ensure_supervisor(&s)?;
            let m = serde_json::from_value(v["proposal"].clone()).map_err(anyhow::Error::from)?;
            hooks::apply(&s, node, &m)?
        }
        "sync" => {
            ensure_supervisor(&s)?;
            sync(&s, node, &v)?
        }
        "memory_changed" => {
            if s.is_supervisor() || node != s.supervisor() {
                return Err(ApiError(
                    StatusCode::FORBIDDEN,
                    "only the paired supervisor can signal a memory change".into(),
                ));
            }
            let cursor = v["cursor"]
                .as_i64()
                .filter(|n| *n > 0)
                .context("invalid memory cursor hint")?;
            let local = SqliteMemory::new(&mut s.db()?, s.id(), s.node()).sync_cursor()?;
            if cursor > local {
                s.sync_wake.notify_one();
            }
            json!({"workspace_id":s.id(),"term":s.term()})
        }
        "ticket" | "log" | "resolve" | "forward" => {
            if node != s.supervisor() {
                return Err(ApiError(
                    StatusCode::FORBIDDEN,
                    "only the designated supervisor may request this operation".into(),
                ));
            }
            match op.as_str() {
                "ticket" => terminal::local_ticket(&s, string(&v, "run_id")?, &v)?,
                "log" => {
                    api::local_log(
                        &s,
                        string(&v, "run_id")?,
                        v["offset"].as_u64().unwrap_or(0),
                        v["limit"].as_u64().unwrap_or(40),
                        v["query"].as_str().unwrap_or(""),
                    )
                    .await?
                }
                "forward" => api::forward(&s, string(&v, "run_id")?, string(&v, "text")?).await?,
                _ => api::resolve_local_approval(&s, string(&v, "id")?, string(&v, "resolution")?)?,
            }
        }
        _ => {
            return Err(ApiError(
                StatusCode::NOT_FOUND,
                "unknown peer operation".into(),
            ))
        }
    };
    Ok(Json(ans))
}
fn pending(c: &Connection) -> Result<Vec<Value>> {
    list(c,"SELECT event_id,run_id,event_type,payload_json FROM outbox_events WHERE acked_at IS NULL ORDER BY rowid LIMIT 24",&[])
}
fn sync(s: &Service, node: &str, v: &Value) -> Result<Value> {
    let slots: Vec<AgentSlotSnapshot> = serde_json::from_value(v["slots"].clone())?;
    if slots.len() > 128
        || slots
            .iter()
            .any(|x| x.node_id != node || x.workspace_id != s.id())
    {
        bail!("bad slot advertisement")
    }
    let mut db = s.db()?;
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    handoff::peer_paths(&tx, node, &slots, v)?;
    tx.execute("INSERT INTO service_peers VALUES(?,?,?) ON CONFLICT(node_id) DO UPDATE SET seen=excluded.seen,slots=excluded.slots",params![node,now(),serde_json::to_string(&slots)?])?;
    if let Some(items) = v["facts"].as_array() {
        facts::replace(&tx, node, items)?;
    }
    let mut advertised = facts::local(&tx, s.node())?;
    advertised.extend(
        facts::mirror(&tx)?
            .into_iter()
            .filter(|r| r["node_id"] != node),
    );
    let ack = accept_events(
        &tx,
        s,
        node,
        v["events"].as_array().context("expected event batch")?,
    )?;
    let assignments=list(&tx,"SELECT r.run_id,r.conversation_id,r.target_slot_id,r.desired_state,r.observed_state,r.mission_packet_json,p.workdir AS pinned_workdir,e.epoch AS pin_epoch FROM runs r LEFT JOIN service_run_paths p USING(run_id) LEFT JOIN service_pin_epochs e USING(run_id) WHERE target_node_id=? AND observed_state NOT IN ('failed','succeeded','cancelled') ORDER BY created_at,run_id",&[&node])?;
    tx.commit()?;
    if v["events"]
        .as_array()
        .is_some_and(|events| events.iter().any(|e| e["event_type"] == "run_receipt"))
    {
        s.receipt_wake.notify_one();
    }
    let cursor = v["cursor"].as_i64().context("missing memory cursor")?;
    let delta = SqliteMemory::new(&mut db, s.id(), s.node()).pull_delta_since(cursor, 32)?;
    Ok(
        json!({"workspace_id":s.id(),"term":s.term(),"ack":ack,"assignments":assignments,"delta":delta,"facts":advertised}),
    )
}
fn accept_events(c: &Connection, s: &Service, node: &str, events: &[Value]) -> Result<Vec<String>> {
    if events.len() > 24 {
        bail!("too many events")
    }
    let mut ack = vec![];
    for e in events {
        let eid = string(e, "event_id")?;
        validate_id(eid)?;
        let id = string(e, "run_id")?;
        let (conv, owner, state): (String, String, String) = c.query_row(
            "SELECT conversation_id,target_node_id,observed_state FROM runs WHERE run_id=?",
            [id],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )?;
        if owner != node {
            bail!("event does not own that run")
        }
        if c.execute(
            "INSERT OR IGNORE INTO service_inbox VALUES(?,?)",
            params![eid, id],
        )? != 0
        {
            let p: Value = serde_json::from_str(string(e, "payload_json")?)?;
            if p["run_id"] != id {
                bail!("event payload run mismatch")
            }
            let terminal = matches!(state.as_str(), "succeeded" | "failed" | "cancelled");
            match string(e, "event_type")? {
                "state_changed" if !terminal => {
                    let next = string(&p, "state")?;
                    if !matches!(next, "running" | "unconfirmed" | "blocked") {
                        bail!("unknown executor state")
                    }
                    c.execute("UPDATE runs SET observed_state=?,hc_terminal_id=coalesce(?,hc_terminal_id),updated_at=? WHERE run_id=?",params![next,p["terminal"].as_str(),now(),id])?;
                    if let Some(a) = p.get("approval").filter(|v| v.is_object()) {
                        if a["kind"] == "terminal_input" {
                            c.execute("INSERT INTO service_osc VALUES(?,?,?) ON CONFLICT(run_id) DO UPDATE SET approval_id=excluded.approval_id,fingerprint=excluded.fingerprint",params![id,string(a,"approval_id")?,string(a,"fingerprint")?])?;
                        }
                        c.execute("INSERT OR IGNORE INTO approvals(approval_id,workspace_id,run_id,node_id,tool_or_cmd,risk_reason,created_at) VALUES(?,?,?,?,?,?,?)",params![string(a,"approval_id")?,s.id(),id,node,string(a,"tool_or_cmd")?,string(a,"risk_reason")?,now()])?;
                    }
                }
                "terminal_input_closed" => {
                    let aid = string(&p, "approval_id")?;
                    c.execute("UPDATE approvals SET resolution='handled_at_terminal',resolved_at=? WHERE run_id=? AND approval_id=? AND resolution='pending'",params![now(),id,aid])?;
                    c.execute(
                        "DELETE FROM service_osc WHERE run_id=? AND approval_id=?",
                        params![id, aid],
                    )?;
                }
                "state_changed" => {}
                "run_receipt" => {
                    let r = tarla_context::bound_receipt(
                        serde_json::from_value::<RunReceipt>(p)?,
                        s.ws.config.context_policy.run_receipt_max_tokens,
                    )?;
                    if terminal && state != r.final_state {
                        bail!("conflicting terminal state")
                    }
                    // A peer may reissue its final receipt under a different outbox ID.
                    // The first accepted completion remains the single conversation wake.
                    if c.query_row("SELECT exists(SELECT 1 FROM service_wakes WHERE conversation_id=? AND json_extract(receipt,'$.run_id')=?)",
                        params![conv, id], |row| row.get::<_,bool>(0))? {
                        ack.push(eid.to_owned());
                        continue;
                    }
                    c.execute("UPDATE approvals SET resolution='ended',resolved_at=? WHERE run_id=? AND resolution='pending'",params![now(),id])?;
                    c.execute("UPDATE runs SET observed_state=?,receipt_summary=?,artifacts_json=?,updated_at=? WHERE run_id=?",params![r.final_state,r.executive_summary,serde_json::to_string(&r.artifact_refs)?,now(),id])?;
                    c.execute("INSERT OR IGNORE INTO service_wakes(event_id,conversation_id,receipt) VALUES(?,?,?)",params![eid,conv,serde_json::to_string(&r)?])?;
                }
                _ => bail!("invalid outbox event"),
            }
        }
        ack.push(eid.to_owned());
    }
    Ok(ack)
}
pub async fn replicate(s: S) {
    loop {
        if let Err(e) = hooks::flush_memory(&s).await {
            eprintln!("memory proposal deferred: {e}")
        }
        if let Err(e) = round(&s).await {
            eprintln!("replication deferred: {e}")
        }
        tokio::select! {_=s.sync_wake.notified()=>(),_=s.notify.notified()=>(),_=tokio::time::sleep(Duration::from_millis(900))=>()}
    }
}
async fn round(s: &Service) -> Result<()> {
    if s.is_supervisor() {
        let mut c = s.db()?;
        let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        let batch = pending(&tx)?;
        let ack = accept_events(&tx, s, s.node(), &batch)?;
        for eid in ack {
            tx.execute(
                "UPDATE outbox_events SET acked_at=? WHERE event_id=?",
                params![now(), eid],
            )?;
        }
        tx.execute("UPDATE runs SET observed_state='unconfirmed',updated_at=? WHERE observed_state IN ('running','blocked','dispatch_pending') AND target_node_id IN (SELECT node_id FROM service_peers WHERE seen<?)",params![now(),now()-15])?;
        tx.commit()?;
        if batch.iter().any(|e| e["event_type"] == "run_receipt") {
            s.receipt_wake.notify_one();
        }
        return Ok(());
    }
    if !s.peers.contains_key(s.supervisor()) {
        return Ok(());
    }
    let p = {
        let mut c = s.db()?;
        json!({"cursor":SqliteMemory::new(&mut c,s.id(),s.node()).sync_cursor()?,"slots":fleet::local_slots(s)?,"slot_paths":s.slots.iter().filter(|_|s.can_execute()).map(|p|(&p.snapshot.slot_id,p.workdir.to_string_lossy())).collect::<BTreeMap<_,_>>(),"events":pending(&c)?,"facts":facts::local(&c,s.node())?})
    };
    let response = call(s, s.supervisor(), "sync", &p).await?;
    if response["workspace_id"] != s.id() || response["term"] != s.term() {
        bail!("supervisor response scope mismatch")
    }
    let mut c = s.db()?;
    let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    if let Some(items) = response["facts"].as_array() {
        let mut by_owner = BTreeMap::<String, Vec<Value>>::new();
        for item in items {
            let owner = string(item, "node_id")?;
            if owner == s.node() {
                bail!("remote facts cannot overwrite local facts")
            }
            by_owner
                .entry(owner.to_owned())
                .or_default()
                .push(item.clone());
        }
        tx.execute("DELETE FROM service_peer_facts", [])?;
        for (owner, rows) in by_owner {
            facts::replace(&tx, &owner, &rows)?;
        }
    }
    tx.execute("INSERT INTO service_peers(node_id,seen,slots) VALUES(?,?,'[]') ON CONFLICT(node_id) DO UPDATE SET seen=excluded.seen",params![s.supervisor(),now()])?;
    for item in response["assignments"]
        .as_array()
        .context("missing assignments")?
    {
        let id = string(item, "run_id")?;
        let conv = string(item, "conversation_id")?;
        let slot = string(item, "target_slot_id")?;
        let packet = tarla_context::bound_mission(
            serde_json::from_str::<MissionPacket>(string(item, "mission_packet_json")?)?,
            s.ws.config.context_policy.mission_packet_max_tokens,
        )?;
        validate_id(id)?;
        if packet.run_id != id || packet.workspace_id != s.id() {
            bail!("assignment scope mismatch")
        }
        control::ensure_conversation(&tx, s, conv)?;
        let current = list(
            &tx,
            "SELECT r.target_slot_id,r.mission_packet_json,r.conversation_id,p.workdir AS pinned_workdir,e.epoch AS pin_epoch FROM runs r LEFT JOIN service_run_paths p USING(run_id) LEFT JOIN service_pin_epochs e USING(run_id) WHERE r.run_id=?",
            &[&id],
        )?;
        if let Some(old) = current.first() {
            if old["target_slot_id"] != slot
                || old["conversation_id"] != conv
                || old["pinned_workdir"] != item["pinned_workdir"]
                || old["pin_epoch"].as_str().unwrap_or("legacy")
                    != item["pin_epoch"].as_str().unwrap_or("legacy")
                || old["mission_packet_json"] != serde_json::to_string(&packet)?
            {
                bail!("conflicting immutable run claim")
            }
        } else {
            fleet::insert_run(&tx, s, conv, s.node(), slot, &packet)?;
            if let Some(path) = item["pinned_workdir"].as_str() {
                tx.execute(
                    "INSERT INTO service_pin_epochs VALUES(?,?)",
                    params![id, item["pin_epoch"].as_str().unwrap_or("legacy")],
                )?;
                tx.execute(
                    "INSERT INTO service_run_paths VALUES(?,?)",
                    params![id, path],
                )?;
            }
            fleet::claim(&tx, s, id)?;
        }
        if item["desired_state"] == "cancelled" {
            tx.execute(
                "UPDATE runs SET desired_state='cancelled' WHERE run_id=?",
                [id],
            )?;
        }
        // An acknowledged state may have been marked unconfirmed on the supervisor
        // during a partition. Reassert this same run's current state on reconnection.
        if item["observed_state"] == "unconfirmed" {
            let old = list(
                &tx,
                "SELECT observed_state,hc_terminal_id FROM runs WHERE run_id=?",
                &[&id],
            )?;
            if let Some(row) = old
                .first()
                .filter(|r| matches!(r["observed_state"].as_str(), Some("running" | "blocked")))
            {
                let payload = json!({"run_id":id,"state":row["observed_state"],"terminal":row["hc_terminal_id"]});
                tx.execute("INSERT INTO outbox_events(event_id,run_id,event_type,payload_json,created_at) VALUES(?,?,'state_changed',?,?)",params![uid(),id,payload.to_string(),now()])?;
            }
        }
    }
    for ack in response["ack"]
        .as_array()
        .context("missing acknowledgement")?
    {
        let eid = ack.as_str().context("invalid acknowledgement")?;
        if !p["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["event_id"] == eid)
        {
            bail!("unexpected acknowledgement")
        }
        tx.execute(
            "UPDATE outbox_events SET acked_at=? WHERE event_id=?",
            params![now(), eid],
        )?;
    }
    tx.commit()?;
    let delta: Vec<Memory> = serde_json::from_value(response["delta"].clone())?;
    SqliteMemory::new(&mut c, s.id(), s.node()).apply_delta(&delta)?;
    if delta.len() == 32 {
        s.sync_wake.notify_one();
    }
    Ok(())
}
