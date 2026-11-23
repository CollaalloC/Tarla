//! V2.0 `tarla ctl` HTTP control surface with cryptographic `TARLA_CTL_ROLE=master|worker`
//! bearer-token scoping, zero-MCP JSON responses, and cross-node forwarding.
use super::*;
use axum::{
    extract::{Path as Route, Query, State},
    http::HeaderMap,
};
use hmac::{Hmac, Mac};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::Deserialize;
use tarla_memory::{MemoryBackend, Mutation, SqliteMemory};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum CtlRole {
    Master,
    Worker,
}

impl CtlRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Master => "master",
            Self::Worker => "worker",
        }
    }
}

#[derive(Debug, Clone)]
pub struct CtlPrincipal {
    pub role: CtlRole,
    pub bot_id: String,
    pub run_id: Option<String>,
}

fn compute_ctl_mac(
    master_secret: &str,
    workspace: &str,
    role: &str,
    bot_id: &str,
    run_part: &str,
) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(master_secret.as_bytes())
        .expect("HMAC accepts arbitrary key length");
    mac.update(b"tarla/ctl/v2\0");
    mac.update(workspace.as_bytes());
    mac.update(b"\0");
    mac.update(role.as_bytes());
    mac.update(b"\0");
    mac.update(bot_id.as_bytes());
    mac.update(b"\0");
    mac.update(run_part.as_bytes());
    format!("{:x}", mac.finalize().into_bytes())
}

pub fn issue_scoped_token(
    master_secret: &str,
    workspace: &str,
    role: CtlRole,
    bot_id: &str,
    run_id: Option<&str>,
) -> String {
    let r_part = run_id.filter(|s| !s.is_empty()).unwrap_or("-");
    let sig = compute_ctl_mac(master_secret, workspace, role.as_str(), bot_id, r_part);
    format!("ctl2.{}.{}.{}.{}", role.as_str(), bot_id, r_part, sig)
}

pub fn authenticate(s: &Service, headers: &HeaderMap) -> Api<CtlPrincipal> {
    if let Some(ws_hdr) = headers.get("x-tarla-workspace") {
        if ws_hdr.as_bytes() != s.id().as_bytes() {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "workspace identity changed".into(),
            ));
        }
    }
    let offered = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");

    let requested_role = headers
        .get("x-tarla-ctl-role")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("master");

    // Owner token (`api.token`) defaults to Master, unless the caller explicitly downgraded via `TARLA_CTL_ROLE=worker`.
    if secret_equal(offered, &s.ws.token) {
        let role = if requested_role == "worker" {
            CtlRole::Worker
        } else {
            CtlRole::Master
        };
        let bot_id = headers
            .get("x-tarla-bot-id")
            .and_then(|v| v.to_str().ok())
            .filter(|b| !b.is_empty())
            .unwrap_or("chief")
            .to_owned();
        let run_id = headers
            .get("x-tarla-run-id")
            .and_then(|v| v.to_str().ok())
            .filter(|r| !r.is_empty())
            .map(str::to_owned);
        return Ok(CtlPrincipal {
            role,
            bot_id,
            run_id,
        });
    }

    if let Some(rest) = offered.strip_prefix("ctl2.") {
        let parts: Vec<&str> = rest.splitn(4, '.').collect();
        if parts.len() == 4 {
            let (role_str, bot_id, run_part, sig) = (parts[0], parts[1], parts[2], parts[3]);
            let expected = compute_ctl_mac(&s.ws.token, s.id(), role_str, bot_id, run_part);
            if secret_equal(sig, &expected) {
                let role = match role_str {
                    "master" => CtlRole::Master,
                    "worker" => CtlRole::Worker,
                    _ => {
                        return Err(ApiError(
                            StatusCode::UNAUTHORIZED,
                            "invalid ctl role".into(),
                        ))
                    }
                };
                if role == CtlRole::Worker && requested_role == "master" {
                    return Err(ApiError(
                        StatusCode::FORBIDDEN,
                        "worker token cannot escalate to TARLA_CTL_ROLE=master".into(),
                    ));
                }
                return Ok(CtlPrincipal {
                    role,
                    bot_id: bot_id.to_owned(),
                    run_id: (run_part != "-").then(|| run_part.to_owned()),
                });
            }
        }
    }

    Err(ApiError(
        StatusCode::UNAUTHORIZED,
        "invalid workspace or ctl credential".into(),
    ))
}

fn require_master(p: &CtlPrincipal) -> Api<()> {
    if p.role != CtlRole::Master {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "command requires TARLA_CTL_ROLE=master".into(),
        ));
    }
    Ok(())
}

#[derive(Deserialize, Default)]
pub struct BotsQuery {
    pub include_offline: Option<bool>,
}

/// `GET /v2/ctl/bots` — Master-only fleet, multi-core, quota headroom, and Bot queue overview.
pub async fn list_bots(State(s): State<S>, headers: HeaderMap, Query(q): Query<BotsQuery>) -> Api {
    let principal = authenticate(&s, &headers)?;
    require_master(&principal)?;
    let include_offline = q.include_offline.unwrap_or(false);

    let db = s.db()?;
    let mut nodes = vec![json!({
        "node_id": s.node(),
        "online": true,
        "role_mode": s.ws.config.workspace.role_mode,
    })];
    for peer_id in s.peers.keys() {
        let seen: Option<i64> = db
            .query_row(
                "SELECT seen FROM service_peers WHERE node_id = ?1",
                [peer_id],
                |r| r.get(0),
            )
            .optional()?;
        let online = seen.is_some_and(|t| t >= now() - 15);
        if online || include_offline {
            nodes.push(json!({
                "node_id": peer_id,
                "online": online,
                "last_seen": seen,
            }));
        }
    }

    let mut cores = Vec::new();
    for (core_name, hc) in &s.herdr_cores {
        let live = hc.available().await.is_ok();
        cores.push(json!({
            "core_group": core_name,
            "session": hc.session_name(),
            "socket": hc.socket_path().to_string_lossy(),
            "daemon_live": live,
        }));
    }

    let bindings = bot_session::list_bindings(&db)?;
    let mut bots = Vec::new();
    let ts = now();
    for b in bindings {
        let queued: i64 = db.query_row(
            "SELECT COUNT(*) FROM botflow_inbox WHERE target_bot_id = ?1 AND status = 'queued'",
            [&b.bot_id],
            |r| r.get(0),
        )?;
        let active_runs: i64 = db.query_row(
            "SELECT COUNT(*) FROM runs
             WHERE conversation_id = ?1
               AND observed_state NOT IN ('succeeded','failed','aborted','cancelled','canceled')",
            [&b.conversation_id],
            |r| r.get(0),
        )?;
        let quota = bot_session::read_quota_headroom(&db, &b.active_template);
        bots.push(json!({
            "bot_id": b.bot_id,
            "conversation_id": b.conversation_id,
            "active_node_id": b.active_node_id,
            "active_core_group": b.active_core_group,
            "active_template": b.active_template,
            "hc_terminal_addr": b.hc_terminal_addr,
            "compaction_epoch": b.compaction_epoch,
            "human_lease_active": b.human_lease_active(ts),
            "quota": quota,
            "inbox_queue_depth": queued,
            "active_runs": active_runs,
        }));
    }

    Ok(Json(json!({
        "workspace_id": s.id(),
        "nodes": nodes,
        "herdr_cores": cores,
        "bots": bots,
    })))
}

/// `POST /v2/ctl/goals` — Master-only persistent ROS 2 Action dispatch.
pub async fn post_goal(State(s): State<S>, headers: HeaderMap, Json(v): Json<Value>) -> Api {
    let principal = authenticate(&s, &headers)?;
    require_master(&principal)?;
    ensure_supervisor(&s)?;

    let bot_id = string(&v, "bot_id")?;
    let objective = string(&v, "objective")?;
    let accept: Vec<String> = v
        .get("acceptance_criteria")
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(Value::as_str)
                .map(str::to_owned)
                .collect()
        })
        .unwrap_or_default();
    let op_id = v["op_id"].as_str();
    let corr = v["correlation_id"].as_str();
    let out = bot_session::send_goal(&s, bot_id, objective, accept, op_id, None, corr, 0).await?;
    Ok(Json(out))
}

/// `POST /v2/ctl/goals/{id}/cancel` — Master-only safe transition to `canceling` / `canceled`.
pub async fn post_cancel_goal(
    State(s): State<S>,
    headers: HeaderMap,
    Route(id): Route<String>,
    Json(v): Json<Value>,
) -> Api {
    let principal = authenticate(&s, &headers)?;
    require_master(&principal)?;
    ensure_supervisor(&s)?;
    let reason = v["reason"].as_str();
    Ok(Json(bot_session::cancel_goal(&s, &id, reason).await?))
}

/// `POST /v2/ctl/emit` — Master & Worker shared port emission (`feedback`, `blocked_approval`, or `<port>`).
pub async fn post_emit(State(s): State<S>, headers: HeaderMap, Json(v): Json<Value>) -> Api {
    let principal = authenticate(&s, &headers)?;
    let raw_port = string(&v, "port")?;
    let payload = v.get("payload").cloned().unwrap_or_else(|| json!({}));

    let (bot_id, port) = if let Some((explicit_bot, sub_port)) = raw_port.split_once('/') {
        if principal.role == CtlRole::Worker && explicit_bot != principal.bot_id {
            return Err(ApiError(
                StatusCode::FORBIDDEN,
                format!(
                    "worker `{}` cannot spoof events from `{explicit_bot}`",
                    principal.bot_id
                ),
            ));
        }
        (explicit_bot.to_owned(), sub_port.to_owned())
    } else {
        let b = v["bot_id"]
            .as_str()
            .filter(|s| !s.is_empty())
            .unwrap_or(&principal.bot_id);
        if principal.role == CtlRole::Worker && b != principal.bot_id {
            return Err(ApiError(
                StatusCode::FORBIDDEN,
                "worker cannot emit on behalf of another bot".into(),
            ));
        }
        (b.to_owned(), raw_port.to_owned())
    };

    let run_id = v["run_id"]
        .as_str()
        .map(str::to_owned)
        .or_else(|| principal.run_id.clone());

    if port == "feedback" {
        let db = s.db()?;
        let rev = tarla_memory::record_sync_event(
            &db,
            s.id(),
            "action_feedback",
            run_id.as_deref().unwrap_or(&bot_id),
            &json!({"bot_id": bot_id, "run_id": run_id, "payload": payload}),
            now(),
        )?;
        s.rev_wake.notify_waiters();
        return Ok(Json(
            json!({"status": "recorded", "kind": "feedback", "rev": rev}),
        ));
    }

    if port == "blocked_approval" {
        let rid = run_id.context("blocked_approval requires an active run_id")?;
        let approval_id = uid();
        let reason = payload["reason"]
            .as_str()
            .unwrap_or("Agent requested mobile Master Control approval");
        let tool = payload["tool"].as_str().unwrap_or("bot_approval_gate");
        let ts = now();
        let mut db = s.db()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        tx.execute(
            "UPDATE runs SET observed_state = 'blocked_approval', updated_at = ?1 WHERE run_id = ?2",
            params![ts, rid],
        )?;
        tx.execute(
            "INSERT INTO approvals(approval_id, workspace_id, run_id, node_id, tool_or_cmd, risk_reason, resolution, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7)",
            params![approval_id, s.id(), rid, s.node(), tool, reason, ts],
        )?;
        tx.commit()?;
        s.rev_wake.notify_waiters();
        return Ok(Json(json!({
            "run_id": rid,
            "state": "blocked_approval",
            "approval_id": approval_id,
        })));
    }

    let corr = v["correlation_id"].as_str();
    let hop = v["hop_count"].as_u64().unwrap_or(0) as u8;

    if s.is_supervisor() {
        let (env, outcomes) =
            bot_session::emit_botflow_port_event(&s, &bot_id, &port, payload, corr, hop)?;
        Ok(Json(json!({
            "event_id": env.event_id,
            "rev": env.rev,
            "source_topic": env.source_topic,
            "hop_count": env.hop_count,
            "outcomes": outcomes,
        })))
    } else {
        let forwarded = peers::call(
            &s,
            s.supervisor(),
            "v2_emit",
            &json!({
                "bot_id": bot_id,
                "port": port,
                "payload": payload,
                "correlation_id": corr,
                "hop_count": hop,
            }),
        )
        .await?;
        Ok(Json(forwarded))
    }
}

/// `POST /v2/ctl/memory` — `search`, `propose`, or `recall` for Master and Worker roles.
pub async fn post_memory(State(s): State<S>, headers: HeaderMap, Json(v): Json<Value>) -> Api {
    let principal = authenticate(&s, &headers)?;
    let action = string(&v, "action")?;
    match action {
        "search" => {
            let query = v["query"].as_str().unwrap_or("");
            let limit = v["limit"].as_u64().unwrap_or(20).clamp(1, 200) as usize;
            let scope_filter = v["scope"].as_str();
            let mut items = control::search_memories(&s, query, limit).await?;
            if let Some(sc) = scope_filter {
                items.retain(|m| m.scope == sc);
            }
            Ok(Json(json!({"memories": items})))
        }
        "propose" => {
            let mut body = v.clone();
            if body.get("operation").is_none() {
                body["operation"] = json!("update");
            }
            if principal.role == CtlRole::Master && s.is_supervisor() {
                Ok(Json(control::memories(&s, &body).await?))
            } else {
                let run_scope = principal.run_id.as_deref();
                if s.is_supervisor() {
                    let mut db = s.db()?;
                    let mut store = SqliteMemory::new(&mut db, s.id(), s.node());
                    let req = Mutation {
                        proposal_id: body["proposal_id"]
                            .as_str()
                            .map(str::to_owned)
                            .unwrap_or_else(uid),
                        memory_id: string(&body, "memory_id")?.to_owned(),
                        scope: body["scope"]
                            .as_str()
                            .unwrap_or(&format!("bot:{}", principal.bot_id))
                            .to_owned(),
                        content: string(&body, "content")?.to_owned(),
                        base_version: body["base_version"].as_i64().unwrap_or(0),
                        source_node_id: s.node().to_owned(),
                        source_run_id: run_scope.map(str::to_owned),
                        is_deleted: false,
                        hlc_timestamp: None,
                    };
                    let reply = store.propose_mutation(&req)?;
                    peers::mutation_committed(&s, &reply);
                    s.rev_wake.notify_waiters();
                    Ok(Json(json!(reply)))
                } else {
                    Ok(Json(hooks::queue_proposal(&s, &body, run_scope)?))
                }
            }
        }
        "recall" => {
            let conv = v["conversation_id"]
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| format!("bot:{}", principal.bot_id));
            let res = tarla_context::recall_tool(&s.db()?, &conv, &v)?;
            Ok(Json(json!({"conversation_id": conv, "results": res})))
        }
        other => Err(ApiError(
            StatusCode::BAD_REQUEST,
            format!("unsupported ctl memory action `{other}`"),
        )),
    }
}

/// `POST /v2/ctl/bots/{id}/switch` — Hot-swap Bot's underlying CLI template while keeping `conversation_id`.
pub async fn post_switch_engine(
    State(s): State<S>,
    headers: HeaderMap,
    Route(bot_id): Route<String>,
    Json(v): Json<Value>,
) -> Api {
    let principal = authenticate(&s, &headers)?;
    require_master(&principal)?;
    let target = string(&v, "target")?;
    let (binding, packet) = bot_session::switch_bot_engine(&s, &bot_id, target).await?;
    Ok(Json(json!({
        "binding": binding,
        "compaction_epoch": packet.compaction_epoch,
        "dag_nodes": packet.dag_summaries.len(),
        "recent_messages": packet.recent_messages.len(),
        "cold_start_preview_bytes": packet.rendered.len(),
    })))
}

/// `POST /v2/ctl/bots/{id}/human-lease` — Acquire or release `human_lease_active` (Issue #75 guard).
pub async fn post_human_lease(
    State(s): State<S>,
    headers: HeaderMap,
    Route(bot_id): Route<String>,
    Json(v): Json<Value>,
) -> Api {
    let principal = authenticate(&s, &headers)?;
    require_master(&principal)?;
    let secs = v["duration_secs"].as_i64();
    Ok(Json(bot_session::set_human_lease(&s, &bot_id, secs)?))
}

/// `POST /v2/ctl/runs/{id}/rollback` — SHA-256 conflict-aware checkpoint rollback.
pub async fn post_rollback(
    State(s): State<S>,
    headers: HeaderMap,
    Route(run_id): Route<String>,
) -> Api {
    let principal = authenticate(&s, &headers)?;
    require_master(&principal)?;
    let row = api::one(&s, &run_id)?;
    let target_node = string(&row, "target_node_id")?.to_owned();
    if target_node != s.node() {
        ensure_supervisor(&s)?;
        let res = peers::call(&s, &target_node, "v2_rollback", &json!({"run_id": run_id})).await?;
        return Ok(Json(res));
    }
    Ok(Json(local_rollback(&s, &run_id)?))
}

pub fn local_rollback(s: &Service, run_id: &str) -> Result<Value> {
    let spec = s
        .botflow
        .as_ref()
        .context("botflow.yml required for checkpoint rollback")?;
    let row = api::one(s, run_id)?;
    let slot_tpl = string(&row, "target_slot_id")?;
    let target_ref = format!("{}/{slot_tpl}", s.node());
    let resolved = spec.resolve_target(&target_ref)?;
    let report = checkpoints::rollback_run(
        &mut s.db()?,
        s.id(),
        run_id,
        Path::new(&resolved.workdir),
        now(),
    )?;
    s.rev_wake.notify_waiters();
    Ok(json!(report))
}

/// `POST /v2/ctl/ops` — `--op-id` idempotent zero-plaintext credential broker.
pub async fn post_broker_op(State(s): State<S>, headers: HeaderMap, Json(v): Json<Value>) -> Api {
    let principal = authenticate(&s, &headers)?;
    let op_id = string(&v, "op_id")?;
    let cred_ref = string(&v, "credential_ref")?;
    let action = string(&v, "action")?;
    let run_id = v["run_id"]
        .as_str()
        .or(principal.run_id.as_deref())
        .unwrap_or("manual-op");
    let payload = v.get("payload").cloned().unwrap_or_else(|| json!({}));
    let out = broker::execute_brokered_op(&s, op_id, run_id, cred_ref, action, &payload).await?;
    Ok(Json(out))
}

#[derive(Deserialize, Default)]
pub struct SpillQuery {
    pub offset_line: Option<usize>,
    pub limit: Option<usize>,
    pub query: Option<String>,
}

/// `GET /v2/spills/{id}` — Paginated reader for `>8,000`-char spilled outputs.
pub async fn get_spill(
    State(s): State<S>,
    headers: HeaderMap,
    Route(spill_id): Route<String>,
    Query(q): Query<SpillQuery>,
) -> Api {
    let _ = authenticate(&s, &headers)?;
    let page = guardrails::read_spill_page(
        &s.ws.root.join("runtime"),
        &spill_id,
        q.offset_line.unwrap_or(1),
        q.limit.unwrap_or(100),
        q.query.as_deref().unwrap_or(""),
    )?;
    Ok(Json(page))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_worker_and_master_tokens_verify_cryptographically() {
        let master = "secret-token-0123456789abcdef0123456789";
        let worker_tok =
            issue_scoped_token(master, "work", CtlRole::Worker, "implementer", Some("r-9"));
        assert!(worker_tok.starts_with("ctl2.worker.implementer.r-9."));
        let master_tok = issue_scoped_token(master, "work", CtlRole::Master, "chief", None);
        assert!(master_tok.starts_with("ctl2.master.chief.-."));
        assert_ne!(worker_tok, master_tok);
    }
}
