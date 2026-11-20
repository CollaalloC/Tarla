use super::*;
use axum::{
    extract::{Path as Route, Query, Request, State},
    middleware::Next,
};
use rusqlite::{params, OptionalExtension};
use serde::Deserialize;
use tarla_memory::SqliteMemory;
use tarla_scheduler::SlotHealth;

pub async fn client_auth(State(s): State<S>, req: Request, next: Next) -> Api<Response> {
    let offered = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if !secret_equal(offered, &s.ws.token) {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "invalid workspace credential".into(),
        ));
    }
    if let Some(expected) = req.headers().get("x-tarla-workspace") {
        if expected.as_bytes() != s.id().as_bytes() {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "workspace identity changed".into(),
            ));
        }
    }
    Ok(next.run(req).await)
}
pub async fn health(State(s): State<S>) -> Json<Value> {
    Json(
        json!({"workspace_id":s.id(),"node_id":s.node(),"role_mode":s.ws.config.workspace.role_mode,"supervisor_node_id":s.supervisor(),"supervisor_term":s.term()}),
    )
}
pub async fn conversations(State(s): State<S>) -> Api {
    Ok(Json(json!(list(
        &s.db()?,
        "SELECT * FROM conversations ORDER BY updated_at DESC,conversation_id",
        &[]
    )?)))
}
#[derive(Deserialize, Default)]
pub struct Page {
    limit: Option<usize>,
    latest: Option<bool>,
    after_seq: Option<i64>,
    before_seq: Option<i64>,
    offset: Option<u64>,
    query: Option<String>,
}
pub async fn messages(State(s): State<S>, Route(id): Route<String>, Query(p): Query<Page>) -> Api {
    let c = s.db()?;
    let limit = p.limit.unwrap_or(100).clamp(1, 500) as i64;
    let order = if p.latest.unwrap_or(false) {
        "DESC"
    } else {
        "ASC"
    };
    let mut rows=list(&c,&format!("SELECT seq_no,channel_type,source_msg_id,sender_role,content,run_id,created_at FROM messages WHERE conversation_id=? AND seq_no>? AND seq_no<? ORDER BY seq_no {order} LIMIT ?"),&[&id,&p.after_seq.unwrap_or(0),&p.before_seq.unwrap_or(i64::MAX),&limit])?;
    if order == "DESC" {
        rows.reverse()
    }
    Ok(Json(json!(rows)))
}
pub async fn post_message(
    State(s): State<S>,
    Route(id): Route<String>,
    Json(v): Json<Value>,
) -> Api {
    ensure_supervisor(&s)?;
    Ok(Json(control::message(&s, &id, &v).await?))
}
pub async fn binding(State(s): State<S>, Route(id): Route<String>, Json(v): Json<Value>) -> Api {
    ensure_supervisor(&s)?;
    Ok(Json(control::set_binding(&s, &id, &v)?))
}
pub fn run_list(s: &Service) -> Result<Vec<Value>> {
    list(&s.db()?,"SELECT run_id,workspace_id,conversation_id,target_node_id,target_slot_id,desired_state,observed_state,receipt_summary,json_extract(mission_packet_json,'$.objective') AS objective,created_at,updated_at FROM runs ORDER BY updated_at DESC LIMIT 200",&[])
}
pub async fn runs(State(s): State<S>) -> Api {
    Ok(Json(json!(run_list(&s)?)))
}
pub async fn create_run(State(s): State<S>, Json(v): Json<Value>) -> Api {
    ensure_supervisor(&s)?;
    let cid = string(&v, "conversation_id")?;
    let rid = v["request_id"]
        .as_str()
        .map(|k| format!("{:x}", Sha256::digest(format!("api-run:{cid}:{k}"))));
    Ok(Json(fleet::dispatch(&s, cid, &v, rid)?))
}
pub fn one(s: &Service, id: &str) -> Result<Value> {
    list(&s.db()?,"SELECT r.run_id,r.workspace_id,r.conversation_id,r.supervisor_term,r.target_node_id,r.target_slot_id,
        r.desired_state,r.observed_state,json_extract(r.mission_packet_json,'$.objective') AS objective,r.hc_terminal_id,
        r.receipt_summary,r.artifacts_json,r.created_at,r.updated_at,t.state AS handoff_state,t.successor AS successor_run_id,
        t.last_error AS handoff_detail,o.predecessor AS predecessor_run_id FROM runs r
        LEFT JOIN service_turn_transfers t USING(run_id) LEFT JOIN service_resume_origins o USING(run_id) WHERE r.run_id=?",&[&id])?.pop().context("unknown run")
}
pub async fn get_run(State(s): State<S>, Route(id): Route<String>) -> Api {
    let mut run = one(&s, &id).map_err(|e| ApiError(StatusCode::NOT_FOUND, e.to_string()))?;
    terminal::describe(&s, &mut run).await?;
    Ok(Json(run))
}
pub async fn cancel(State(s): State<S>, Route(id): Route<String>) -> Api {
    ensure_supervisor(&s)?;
    let mut db = s.db()?;
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let pending=tx.execute("UPDATE service_turn_transfers SET state='cancelled',last_error=NULL WHERE run_id=? AND state='pending'",[&id])?;
    let running=tx.execute("UPDATE runs SET desired_state='cancelled' WHERE run_id=? AND observed_state NOT IN ('succeeded','failed','cancelled')",[&id])?;
    if running + pending == 0 {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "run is absent or already terminal".into(),
        ));
    }
    tx.commit()?;
    s.notify.notify_waiters();
    Ok(Json(one(&s, &id)?))
}
pub async fn slots(State(s): State<S>) -> Api {
    Ok(Json(json!(fleet::snapshots(&s)?)))
}
pub async fn nodes(State(s): State<S>) -> Api {
    let c = s.db()?;
    let mut arr =
        vec![json!({"node_id":s.node(),"online":true,"role_mode":s.ws.config.workspace.role_mode})];
    for node in s.peers.keys() {
        let seen = c
            .query_row(
                "SELECT seen FROM service_peers WHERE node_id=?",
                [node],
                |r| r.get::<_, i64>(0),
            )
            .optional()?;
        arr.push(json!({"node_id":node,"online":seen.is_some_and(|t|t>=now()-15),"last_seen":seen}))
    }
    Ok(Json(json!(arr)))
}
pub async fn memories(State(s): State<S>, Query(p): Query<Page>) -> Api {
    Ok(Json(json!(
        control::search_memories(&s, p.query.as_deref().unwrap_or(""), p.limit.unwrap_or(200))
            .await?
    )))
}
pub async fn put_memory(State(s): State<S>, Json(v): Json<Value>) -> Api {
    if s.is_supervisor() {
        Ok(Json(control::memories(&s, &v).await?))
    } else {
        Ok(Json(hooks::queue_proposal(&s, &v, None)?))
    }
}
pub async fn facts(State(s): State<S>) -> Api {
    let db = s.db()?;
    let mut rows = list(
        &db,
        "SELECT * FROM workspace_facts ORDER BY node_id,fact_key",
        &[],
    )?;
    rows.extend(facts::mirror(&db)?);
    Ok(Json(json!(rows)))
}
pub async fn actions(State(s): State<S>) -> Api {
    let c = s.db()?;
    let mut a=list(&c,"SELECT a.approval_id AS id,CASE WHEN o.approval_id IS NULL THEN 'approval' ELSE 'terminal_input' END AS kind,a.tool_or_cmd AS title,a.run_id,a.node_id,a.risk_reason,a.created_at FROM approvals a LEFT JOIN service_osc o ON a.approval_id=o.approval_id WHERE a.resolution='pending'",&[])?;
    a.extend(list(&c,"SELECT c.candidate_id AS id,'memory_conflict' AS kind,json_extract(d.request_json,'$.memory_id') AS title,json_extract(d.request_json,'$.memory_id') AS memory_id,c.base_version,coalesce(m.version,0) AS current_version,coalesce(m.content,'') AS content,c.proposed_text,c.created_at FROM memory_candidates c JOIN memory_candidate_details d USING(candidate_id) LEFT JOIN memories m ON m.memory_id=json_extract(d.request_json,'$.memory_id') WHERE c.status='pending'",&[])?);
    for slot in fleet::snapshots(&s)? {
        if let SlotHealth::AuthExpired { message } = slot.health {
            a.push(json!({"id":format!("auth:{}:{}",slot.node_id,slot.slot_id),"kind":"auth_expired","title":message,"slot_id":slot.slot_id,"node_id":slot.node_id}))
        }
    }
    Ok(Json(json!(a)))
}
pub async fn resolve(State(s): State<S>, Route(id): Route<String>, Json(v): Json<Value>) -> Api {
    let resolution = string(&v, "resolution")?;
    if !matches!(resolution, "approved" | "rejected") {
        return Err(anyhow!("expected approved or rejected").into());
    }
    let node = s
        .db()?
        .query_row(
            "SELECT node_id FROM approvals WHERE approval_id=?",
            [&id],
            |r| r.get::<_, String>(0),
        )
        .optional()?;
    if let Some(node) = node {
        if node != s.node() {
            ensure_supervisor(&s)?;
            peers::call(
                &s,
                &node,
                "resolve",
                &json!({"id":id,"resolution":resolution}),
            )
            .await?;
        }
        return Ok(Json(resolve_local_approval(&s, &id, resolution)?));
    }
    ensure_supervisor(&s)?;
    let mut c = s.db()?;
    let applied = SqliteMemory::new(&mut c, s.id(), s.node()).resolve_candidate(
        &id,
        resolution == "approved",
        v["expected_version"]
            .as_i64()
            .context("expected_version required for memory decision")?,
    )?;
    if let Some(m) = &applied {
        peers::memory_changed(&s, m.sync_seq);
    }
    Ok(Json(json!({"resolution":resolution,"memory":applied})))
}
pub fn resolve_local_approval(s: &Service, id: &str, v: &str) -> Result<Value> {
    if !matches!(v, "approved" | "rejected") {
        bail!("invalid approval outcome")
    }
    let mut db = s.db()?;
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    if tx.query_row(
        "SELECT exists(SELECT 1 FROM service_osc WHERE approval_id=?)",
        [id],
        |r| r.get::<_, bool>(0),
    )? {
        bail!("use the run's interactive terminal to answer this prompt")
    }
    let n = tx.execute(
        "UPDATE approvals SET resolution=?,resolved_by='paired_user',resolved_at=?
         WHERE approval_id=? AND resolution='pending' AND
         (node_id!=? OR EXISTS(SELECT 1 FROM runs r WHERE r.run_id=approvals.run_id AND
          r.target_node_id=approvals.node_id AND r.desired_state='running' AND
          r.observed_state NOT IN ('succeeded','failed','cancelled')))",
        params![v, now(), id, s.node()],
    )?;
    if n == 0 {
        let prior: Option<String> = tx
            .query_row(
                "SELECT resolution FROM approvals WHERE approval_id=?",
                [id],
                |r| r.get(0),
            )
            .optional()?;
        if prior.as_deref() != Some(v) {
            bail!("approval is no longer pending for an active run")
        }
    }
    if n == 1 {
        let run: Option<String> = tx
            .query_row(
                "SELECT run_id FROM approvals WHERE approval_id=? AND node_id=?",
                params![id, s.node()],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(run) = run {
            let event = json!({"run_id":run,"approval_id":id,"resolution":v});
            tx.execute("INSERT INTO outbox_events(event_id,run_id,event_type,payload_json,created_at) VALUES(?,?,'approval_resolved',?,?)",params![uid(),run,event.to_string(),now()])?;
        }
    }
    tx.commit()?;
    s.notify.notify_waiters();
    Ok(json!({"id":id,"resolution":v}))
}
pub async fn logs(State(s): State<S>, Route(id): Route<String>, Query(p): Query<Page>) -> Api {
    Ok(Json(
        read_log(
            &s,
            &id,
            p.offset.unwrap_or(0),
            p.limit.unwrap_or(40) as u64,
            p.query.as_deref().unwrap_or(""),
        )
        .await?,
    ))
}
pub async fn read_log(s: &Service, id: &str, offset: u64, limit: u64, q: &str) -> Result<Value> {
    let r = one(s, id)?;
    let n = string(&r, "target_node_id")?;
    if n == s.node() {
        local_log(s, id, offset, limit, q).await
    } else {
        peers::call(
            s,
            n,
            "log",
            &json!({"run_id":id,"offset":offset,"limit":limit,"query":q}),
        )
        .await
    }
}
pub async fn local_log(
    s: &Service,
    id: &str,
    offset: u64,
    limit: u64,
    query: &str,
) -> Result<Value> {
    validate_id(id)?;
    let run = one(s, id)?;
    if run["target_node_id"] != s.node() {
        bail!("this node does not own the run")
    }
    transcript::page(&s.ws.root, id, offset, limit, query)
}
pub async fn forward(s: &Service, id: &str, text: &str) -> Result<Value> {
    let r = one(s, id)?;
    let terminal = string(&r, "hc_terminal_id")?;
    if r["target_node_id"] != s.node() || r["observed_state"] != "running" {
        bail!("pinned run unavailable")
    }
    fleet::herdr(s)?.send(id, terminal, text).await?;
    Ok(json!({"run_id":id,"forwarded":true}))
}
