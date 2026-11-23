//! V2.0 `Conversation ≠ Terminal` Bot runtime bindings (`BotRuntimeBinding`),
//! multi-core `herdr-core` template provisioning, hot-swap engine continuity,
//! 30-minute idle terminal reaper, `human_lease_active` (Issue #75) gate,
//! zero-token local quota failover, and ROS 2 Action execution loop.
use super::*;
use crate::{
    broker::scrub_agent_env,
    checkpoints::{capture_worktree, diff_worktree_paths, finalize_run_checkpoints},
    guardrails::{
        evaluate_failover_gate, gate_output, run_check_before_done, FailoverDecision,
        MAX_VERIFY_RETRIES,
    },
};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use tarla_botflow::{
    claim_next_inbox, publish_event, ActionState, BotDef, BotflowSpec, PublishOutcome,
    ResolvedTarget,
};
use tarla_context::{bound_mission, bound_receipt};
use tarla_protocol::{MissionPacket, RunReceipt};
use tarla_sidecar::{sync_core_templates, AgentTemplateSpec, Herdr, TerminalSpawnSpec};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BotRuntimeBinding {
    pub bot_id: String,
    pub conversation_id: String,
    pub active_node_id: String,
    pub active_core_group: String,
    pub active_template: String,
    pub hc_terminal_addr: Option<String>,
    pub compaction_epoch: u64,
    pub idle_reap_after_secs: u64,
    pub human_lease_until: Option<i64>,
    pub last_active_at: i64,
}

impl BotRuntimeBinding {
    pub fn human_lease_active(&self, now_secs: i64) -> bool {
        self.human_lease_until.is_some_and(|until| until > now_secs)
    }

    pub fn active_target_ref(&self) -> String {
        format!("{}/{}", self.active_node_id, self.active_template)
    }
}

/// Load `<workspace_root>/botflow.yml` if present, materialize local `herdr-core` group configs
/// and `.md` agent templates (`notes_quiet = 3, draft_wait = 60`), and initialize `bot_bindings`.
pub fn load_and_provision(
    ws: &Workspace,
    db: &mut Connection,
) -> Result<(Option<BotflowSpec>, BTreeMap<String, Herdr>)> {
    let path = ws.root.join("botflow.yml");
    if !path.is_file() {
        return Ok((None, BTreeMap::new()));
    }
    let raw = std::fs::read_to_string(&path)?;
    let spec = BotflowSpec::from_yaml_str(&raw)?;
    anyhow::ensure!(
        spec.workspace == ws.config.workspace.id,
        "botflow.yml workspace `{}` does not match `{}`",
        spec.workspace,
        ws.config.workspace.id
    );

    let runtime_root = ws.root.join("runtime");
    let mut cores = BTreeMap::new();
    let local_node = &ws.config.workspace.node_id;
    if let Some(node_def) = spec.nodes.get(local_node) {
        if node_def.role != "master_control" {
            for (core_name, core_def) in &node_def.herdr_cores {
                let herdr = Herdr::for_core_group(
                    &runtime_root,
                    &spec.workspace,
                    core_name,
                    core_def.session.as_deref(),
                )?;
                let mut core_templates = Vec::new();
                for (tpl_name, tpl_def) in &node_def.templates {
                    if &tpl_def.core != core_name {
                        continue;
                    }
                    let is_master = spec.bots.iter().any(|b| {
                        b.id == "chief"
                            && b.targets
                                .iter()
                                .any(|t| t == &format!("{local_node}/{tpl_name}"))
                    });
                    let mut agent_tpl = if is_master {
                        AgentTemplateSpec::master_chief(
                            tpl_name,
                            &tpl_def.command,
                            tpl_def.ready.as_deref() == Some("quiet"),
                        )
                    } else {
                        AgentTemplateSpec::new(
                            tpl_name,
                            format!("Managed Tarla V2 Bot template ({tpl_name})"),
                            &tpl_def.command,
                            concat!(
                                "你是受 Tarla V2 微内核托管的专业协作 Bot。\n",
                                "使用 `tarla ctl --json emit <port> --payload '<json>'` 发布阶段事件或审批请求，",
                                "使用 `tarla ctl --json memory <search|propose|recall>` 检索与沉淀长期记忆。"
                            ),
                        )
                    };
                    if let Some(r) = &tpl_def.ready {
                        agent_tpl.ready = Some(r.clone());
                    }
                    core_templates.push(agent_tpl);
                }
                sync_core_templates(&herdr, 3, 60, &core_templates)?;
                cores.insert(core_name.clone(), herdr);
            }
        }
    }

    let ts = now();
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    for bot in &spec.bots {
        let primary = spec.resolve_target(&bot.targets[0])?;
        let conv_id = format!("bot:{}", bot.id);
        tx.execute(
            "INSERT INTO conversations(conversation_id, workspace_id, title, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?4)
             ON CONFLICT(conversation_id) DO NOTHING",
            params![conv_id, spec.workspace, bot.name, ts],
        )?;
        tx.execute(
            "INSERT INTO bot_bindings(
                bot_id, workspace_id, conversation_id, active_node_id, active_core_group,
                active_template, hc_terminal_addr, compaction_epoch, idle_reap_secs,
                human_lease_until, last_active_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, 1, ?7, NULL, ?8)
             ON CONFLICT(bot_id) DO UPDATE SET
                idle_reap_secs = excluded.idle_reap_secs",
            params![
                bot.id,
                spec.workspace,
                conv_id,
                primary.node_id,
                primary.core_group,
                primary.template_name,
                bot.idle_reap_secs as i64,
                ts
            ],
        )?;
    }
    tx.commit()?;
    Ok((Some(spec), cores))
}

pub fn read_binding(db: &Connection, bot_id: &str) -> Result<Option<BotRuntimeBinding>> {
    Ok(db
        .query_row(
            "SELECT bot_id, conversation_id, active_node_id, active_core_group, active_template,
                    hc_terminal_addr, compaction_epoch, idle_reap_secs, human_lease_until, last_active_at
             FROM bot_bindings WHERE bot_id = ?1",
            [bot_id],
            |r| {
                Ok(BotRuntimeBinding {
                    bot_id: r.get(0)?,
                    conversation_id: r.get(1)?,
                    active_node_id: r.get(2)?,
                    active_core_group: r.get(3)?,
                    active_template: r.get(4)?,
                    hc_terminal_addr: r.get(5)?,
                    compaction_epoch: r.get::<_, i64>(6)? as u64,
                    idle_reap_after_secs: r.get::<_, i64>(7)? as u64,
                    human_lease_until: r.get(8)?,
                    last_active_at: r.get(9)?,
                })
            },
        )
        .optional()?)
}

pub fn list_bindings(db: &Connection) -> Result<Vec<BotRuntimeBinding>> {
    let mut stmt = db.prepare(
        "SELECT bot_id, conversation_id, active_node_id, active_core_group, active_template,
                hc_terminal_addr, compaction_epoch, idle_reap_secs, human_lease_until, last_active_at
         FROM bot_bindings ORDER BY bot_id ASC",
    )?;
    let out = stmt
        .query_map([], |r| {
            Ok(BotRuntimeBinding {
                bot_id: r.get(0)?,
                conversation_id: r.get(1)?,
                active_node_id: r.get(2)?,
                active_core_group: r.get(3)?,
                active_template: r.get(4)?,
                hc_terminal_addr: r.get(5)?,
                compaction_epoch: r.get::<_, i64>(6)? as u64,
                idle_reap_after_secs: r.get::<_, i64>(7)? as u64,
                human_lease_until: r.get(8)?,
                last_active_at: r.get(9)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(out)
}

/// Read local zero-token quota telemetry for a template/slot; returns `true` when >= 95% exhausted.
pub fn quota_exhausted_95pct(db: &Connection, template_name: &str) -> bool {
    let raw: Option<String> = db
        .query_row(
            "SELECT report FROM service_telemetry WHERE slot_id = ?1",
            [template_name],
            |r| r.get(0),
        )
        .optional()
        .ok()
        .flatten();
    let Some(raw) = raw else {
        return false;
    };
    let Ok(v) = serde_json::from_str::<Value>(&raw) else {
        return false;
    };
    let win = v["window_used_pct"].as_f64().unwrap_or(0.0);
    let week = v["weekly_used_pct"].as_f64().unwrap_or(0.0);
    win >= 95.0 || week >= 95.0
}

pub fn read_quota_headroom(db: &Connection, template_name: &str) -> Value {
    let raw: Option<String> = db
        .query_row(
            "SELECT report FROM service_telemetry WHERE slot_id = ?1",
            [template_name],
            |r| r.get(0),
        )
        .optional()
        .ok()
        .flatten();
    if let Some(raw) = raw {
        if let Ok(v) = serde_json::from_str::<Value>(&raw) {
            return json!({
                "window_used_pct": v["window_used_pct"].as_f64().unwrap_or(0.0),
                "weekly_used_pct": v["weekly_used_pct"].as_f64().unwrap_or(0.0),
            });
        }
    }
    json!({"window_used_pct": 0.0, "weekly_used_pct": 0.0})
}

/// Apply a Bot engine switch locally on this node (closing any live local `hc` terminal).
pub async fn local_switch_bot_engine(
    s: &Service,
    bot_id: &str,
    target_ref: &str,
) -> Result<(BotRuntimeBinding, tarla_context::ColdStartPacket)> {
    let spec = s
        .botflow
        .as_ref()
        .context("botflow.yml is not configured in this workspace")?;
    let bot = spec
        .bot(bot_id)
        .with_context(|| format!("unknown bot `{bot_id}`"))?;
    let resolved = spec.resolve_target(target_ref)?;

    let prev = {
        let db = s.db()?;
        read_binding(&db, bot_id)?.with_context(|| format!("missing bot binding `{bot_id}`"))?
    };

    if prev.active_node_id == s.node() {
        if let (Some(addr), Some(hc)) = (
            prev.hc_terminal_addr.as_deref(),
            s.herdr_cores.get(&prev.active_core_group),
        ) {
            let _ = hc.cancel(&format!("bot:{bot_id}"), addr).await;
        }
    }

    let ts = now();
    let mut db = s.db()?;
    tarla_memory::invalidate_bot_epoch_prefix(&db, bot_id)?;
    db.execute(
        "UPDATE bot_bindings
         SET active_node_id = ?1,
             active_core_group = ?2,
             active_template = ?3,
             hc_terminal_addr = NULL,
             last_active_at = ?4
         WHERE bot_id = ?5",
        params![
            resolved.node_id,
            resolved.core_group,
            resolved.template_name,
            ts,
            bot_id
        ],
    )?;

    let role_prompt = format!(
        "Bot `{}` ({}) running on template `{}`.",
        bot.id, bot.name, resolved.target_ref
    );
    let packet = tarla_context::build_cold_start_packet(
        &mut db,
        s.id(),
        s.node(),
        bot_id,
        &prev.conversation_id,
        None,
        prev.compaction_epoch as i64,
        &role_prompt,
        8,
        s.ws.config.context_policy.supervisor_active_budget_tokens,
    )?;
    db.execute(
        "UPDATE bot_bindings SET compaction_epoch = ?1 WHERE bot_id = ?2",
        params![packet.compaction_epoch, bot_id],
    )?;
    tarla_memory::record_sync_event(
        &db,
        s.id(),
        "bot_binding",
        bot_id,
        &json!({
            "bot_id": bot_id,
            "conversation_id": prev.conversation_id,
            "previous_target": prev.active_target_ref(),
            "active_target": resolved.target_ref,
            "compaction_epoch": packet.compaction_epoch,
        }),
        ts,
    )?;
    s.rev_wake.notify_waiters();

    let updated = read_binding(&db, bot_id)?.context("missing updated binding")?;
    Ok((updated, packet))
}

/// Hot-swap a Bot's active engine (`claude` -> `codex` / `gemini`), closing any live `hc` terminal
/// (locally and on paired executor nodes) while preserving `conversation_id` and preparing the
/// cold-start continuation packet.
pub async fn switch_bot_engine(
    s: &Service,
    bot_id: &str,
    target_ref: &str,
) -> Result<(BotRuntimeBinding, tarla_context::ColdStartPacket)> {
    let prev_node = read_binding(&s.db()?, bot_id)?
        .with_context(|| format!("missing bot binding `{bot_id}`"))?
        .active_node_id;
    let (updated, packet) = local_switch_bot_engine(s, bot_id, target_ref).await?;
    if s.is_supervisor() {
        let mut peer_nodes = BTreeSet::new();
        if prev_node != s.node() && s.peers.contains_key(&prev_node) {
            peer_nodes.insert(prev_node);
        }
        if updated.active_node_id != s.node() && s.peers.contains_key(&updated.active_node_id) {
            peer_nodes.insert(updated.active_node_id.clone());
        }
        for peer_node in peer_nodes {
            let _ = peers::call(
                s,
                &peer_node,
                "v2_switch",
                &json!({"bot_id": bot_id, "target_ref": target_ref}),
            )
            .await;
        }
    }
    Ok((updated, packet))
}

/// Set or clear `human_lease_until` on a Bot binding (Issue #75 draft protection).
pub fn set_human_lease(s: &Service, bot_id: &str, duration_secs: Option<i64>) -> Result<Value> {
    let db = s.db()?;
    let binding = read_binding(&db, bot_id)?.with_context(|| format!("unknown bot `{bot_id}`"))?;
    let until = duration_secs.map(|d| now().saturating_add(d.clamp(5, 3600)));
    db.execute(
        "UPDATE bot_bindings SET human_lease_until = ?1, last_active_at = ?2 WHERE bot_id = ?3",
        params![until, now(), bot_id],
    )?;
    tarla_memory::record_sync_event(
        &db,
        s.id(),
        "bot_binding",
        bot_id,
        &json!({
            "bot_id": bot_id,
            "conversation_id": binding.conversation_id,
            "human_lease_until": until,
        }),
        now(),
    )?;
    s.rev_wake.notify_waiters();
    if until.is_none() {
        s.botflow_wake.notify_waiters();
    }
    Ok(json!({
        "bot_id": bot_id,
        "human_lease_until": until,
        "human_lease_active": until.is_some(),
    }))
}

pub fn mark_human_lease_for_terminal(db: &Connection, terminal_id: &str, expires_at: i64) {
    let _ = db.execute(
        "UPDATE bot_bindings SET human_lease_until = MAX(COALESCE(human_lease_until, 0), ?1)
         WHERE hc_terminal_addr = ?2",
        params![expires_at, terminal_id],
    );
}

fn bot_has_active_run(db: &Connection, conversation_id: &str) -> Result<bool> {
    let count: i64 = db.query_row(
        "SELECT COUNT(*) FROM runs
         WHERE conversation_id = ?1
           AND observed_state NOT IN ('succeeded','failed','aborted','cancelled','canceled')",
        [conversation_id],
        |r| r.get(0),
    )?;
    Ok(count > 0)
}

/// Dispatch a persistent V2 ROS 2 Action to `bot_id` (local or remote executor node).
#[allow(clippy::too_many_arguments)]
pub async fn send_goal(
    s: &S,
    bot_id: &str,
    objective: &str,
    acceptance_criteria: Vec<String>,
    op_id: Option<&str>,
    upstream_command: Option<&str>,
    correlation_id: Option<&str>,
    hop_count: u8,
) -> Result<Value> {
    let spec = s
        .botflow
        .as_ref()
        .context("botflow.yml is not configured in this workspace")?;
    let bot = spec
        .bot(bot_id)
        .with_context(|| format!("unknown bot `{bot_id}`"))?
        .clone();

    if let Some(op_key) = op_id.filter(|k| !k.trim().is_empty()) {
        validate_id(op_key)?;
        let db = s.db()?;
        if let Some(saved) = db
            .query_row(
                "SELECT result_json FROM idempotent_ops WHERE op_id = ?1 AND status = 'succeeded'",
                [op_key],
                |r| r.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten()
        {
            let mut val: Value = serde_json::from_str(&saved)?;
            val["replayed"] = json!(true);
            return Ok(val);
        }
    }

    // Check 95% local zero-token quota telemetry and pick a healthy target.
    let target = {
        let db = s.db()?;
        let binding =
            read_binding(&db, bot_id)?.with_context(|| format!("missing binding `{bot_id}`"))?;
        let mut excluded = BTreeSet::new();
        for t in &bot.targets {
            if let Ok(resolved) = spec.resolve_target(t) {
                if resolved.node_id == s.node()
                    && quota_exhausted_95pct(&db, &resolved.template_name)
                {
                    excluded.insert(t.clone());
                }
            }
        }
        let current_ref = binding.active_target_ref();
        if !excluded.contains(&current_ref)
            && (!bot.constraints.different_command_from_upstream
                || upstream_command.is_none_or(|up| {
                    spec.resolve_target(&current_ref)
                        .map(|r| {
                            tarla_botflow::command_basename(&r.command)
                                != tarla_botflow::command_basename(up)
                        })
                        .unwrap_or(false)
                }))
        {
            spec.resolve_target(&current_ref)?
        } else {
            spec.select_target_for_bot(bot_id, upstream_command, &excluded)?
                .with_context(|| {
                    format!("no healthy heterogeneous target available for `{bot_id}`")
                })?
        }
    };

    let run_id = uid();
    let conv_id = format!("bot:{bot_id}");
    let max_mission_tokens = (spec.policy.mission_packet_max_bytes / 4).max(128);
    let packet = bound_mission(
        MissionPacket {
            run_id: run_id.clone(),
            workspace_id: s.id().to_owned(),
            objective: objective.to_owned(),
            acceptance_criteria: acceptance_criteria.clone(),
            distilled_context: correlation_id
                .map(|c| vec![format!("correlation_id={c} hop={hop_count}")])
                .unwrap_or_default(),
            project_memory_snapshot: String::new(),
            required_fact_keys: Vec::new(),
        },
        max_mission_tokens,
    )?;
    let packet_json = serde_json::to_string(&packet)?;
    let ts = now();

    {
        let mut db = s.db()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        control::ensure_conversation(&tx, s, &conv_id)?;
        tx.execute(
            "INSERT INTO messages(conversation_id, channel_type, source_msg_id, sender_role, content, run_id, created_at)
             VALUES (?1, 'v2_goal', ?2, 'user', ?3, ?4, ?5)
             ON CONFLICT(channel_type, source_msg_id) DO NOTHING",
            params![conv_id, format!("goal-{run_id}"), objective, run_id, ts],
        )?;
        tx.execute(
            "INSERT INTO runs(
                run_id, workspace_id, conversation_id, supervisor_term, target_node_id,
                target_slot_id, desired_state, observed_state, mission_packet_json,
                created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running', 'accepted', ?7, ?8, ?8)",
            params![
                run_id,
                s.id(),
                conv_id,
                s.term(),
                target.node_id,
                target.template_name,
                packet_json,
                ts
            ],
        )?;
        tx.execute(
            "UPDATE bot_bindings
             SET active_node_id = ?1, active_core_group = ?2, active_template = ?3, last_active_at = ?4
             WHERE bot_id = ?5",
            params![
                target.node_id,
                target.core_group,
                target.template_name,
                ts,
                bot_id
            ],
        )?;
        let response = json!({
            "run_id": run_id,
            "bot_id": bot_id,
            "target_node_id": target.node_id,
            "target_template": target.template_name,
            "state": ActionState::Accepted.as_str(),
        });
        if let Some(op_key) = op_id.filter(|k| !k.trim().is_empty()) {
            tx.execute(
                "INSERT INTO idempotent_ops(op_id, workspace_id, run_id, status, result_json, created_at)
                 VALUES (?1, ?2, ?3, 'succeeded', ?4, ?5)
                 ON CONFLICT(op_id) DO UPDATE SET status = 'succeeded', result_json = excluded.result_json",
                params![op_key, s.id(), run_id, serde_json::to_string(&response)?, ts],
            )?;
        }
        tx.commit()?;
    }
    s.rev_wake.notify_waiters();

    let result = json!({
        "run_id": run_id,
        "bot_id": bot_id,
        "target_node_id": target.node_id,
        "target_template": target.template_name,
        "state": ActionState::Accepted.as_str(),
    });

    if target.node_id == s.node() {
        let svc = s.clone();
        let rid = run_id.clone();
        let corr = correlation_id.map(str::to_owned);
        tokio::spawn(async move {
            if let Err(err) =
                execute_local_bot_action(&svc, &bot, &target, &rid, corr, hop_count).await
            {
                eprintln!("v2 action `{rid}` failed: {err}");
            }
        });
    } else {
        let payload = json!({
            "run_id": run_id,
            "bot_id": bot_id,
            "target_ref": target.target_ref,
            "mission_packet": packet,
            "correlation_id": correlation_id,
            "hop_count": hop_count,
        });
        let svc = s.clone();
        let node_name = target.node_id.clone();
        tokio::spawn(async move {
            if let Err(err) = peers::call(&svc, &node_name, "v2_goal", &payload).await {
                eprintln!("remote v2_goal dispatch to `{node_name}` deferred/failed: {err}");
            }
        });
    }

    Ok(result)
}

/// Accept and spawn a V2 Action sent from the Supervisor onto a paired Executor node.
pub fn accept_remote_v2_goal(s: &S, v: &Value) -> Result<Value> {
    let spec = s.botflow.as_ref().context("executor lacks botflow.yml")?;
    let run_id = string(v, "run_id")?.to_owned();
    let bot_id = string(v, "bot_id")?.to_owned();
    let target_ref = string(v, "target_ref")?;
    let bot = spec
        .bot(&bot_id)
        .with_context(|| format!("unknown bot `{bot_id}`"))?
        .clone();
    let target = spec.resolve_target(target_ref)?;
    anyhow::ensure!(
        target.node_id == s.node(),
        "v2_goal routed to wrong node `{}`",
        s.node()
    );
    let packet: MissionPacket = serde_json::from_value(v["mission_packet"].clone())?;
    let conv_id = format!("bot:{bot_id}");
    let ts = now();
    {
        let mut db = s.db()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        control::ensure_conversation(&tx, s, &conv_id)?;
        tx.execute(
            "INSERT INTO messages(conversation_id, channel_type, source_msg_id, sender_role, content, run_id, created_at)
             VALUES (?1, 'v2_goal', ?2, 'user', ?3, ?4, ?5)
             ON CONFLICT(channel_type, source_msg_id) DO NOTHING",
            params![conv_id, format!("goal-{run_id}"), packet.objective, run_id, ts],
        )?;
        tx.execute(
            "INSERT INTO runs(
                run_id, workspace_id, conversation_id, supervisor_term, target_node_id,
                target_slot_id, desired_state, observed_state, mission_packet_json,
                created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'running', 'accepted', ?7, ?8, ?8)
             ON CONFLICT(run_id) DO NOTHING",
            params![
                run_id,
                s.id(),
                conv_id,
                s.term(),
                s.node(),
                target.template_name,
                serde_json::to_string(&packet)?,
                ts
            ],
        )?;
        tx.commit()?;
    }
    let svc = s.clone();
    let rid = run_id.clone();
    let corr = v["correlation_id"].as_str().map(str::to_owned);
    let hop = v["hop_count"].as_u64().unwrap_or(0) as u8;
    tokio::spawn(async move {
        if let Err(err) = execute_local_bot_action(&svc, &bot, &target, &rid, corr, hop).await {
            eprintln!("remote executor v2 action `{rid}` error: {err}");
        }
    });
    Ok(json!({"run_id": run_id, "state": "accepted"}))
}

/// Transition an active V2 Action to `canceling` (and `canceled` if still `accepted`).
pub async fn cancel_goal(s: &Service, run_id: &str, reason: Option<&str>) -> Result<Value> {
    let row = api::one(s, run_id)?;
    let target_node = string(&row, "target_node_id")?.to_owned();
    let current = row["observed_state"].as_str().unwrap_or("");
    if tarla_botflow::is_terminal_run_state(current) {
        bail!("run `{run_id}` is already in terminal state `{current}`");
    }
    let next_state = if current == "accepted" {
        ActionState::Canceled
    } else {
        ActionState::Canceling
    };
    let ts = now();
    {
        let db = s.db()?;
        db.execute(
            "UPDATE runs
             SET desired_state = 'cancelled',
                 observed_state = ?1,
                 receipt_summary = COALESCE(?2, receipt_summary),
                 updated_at = ?3
             WHERE run_id = ?4",
            params![next_state.as_str(), reason, ts, run_id],
        )?;
    }
    s.rev_wake.notify_waiters();
    s.notify.notify_waiters();

    if target_node == s.node() {
        if let Some(term_addr) = row["hc_terminal_id"].as_str() {
            for hc in s.herdr_cores.values() {
                let _ = hc.interrupt(run_id, term_addr).await;
            }
        }
    } else if s.is_supervisor() {
        let _ = peers::call(
            s,
            &target_node,
            "v2_cancel",
            &json!({"run_id": run_id, "reason": reason}),
        )
        .await;
    }
    Ok(json!({
        "run_id": run_id,
        "state": next_state.as_str(),
        "reason": reason,
    }))
}

async fn execute_local_bot_action(
    s: &S,
    bot: &BotDef,
    initial_target: &ResolvedTarget,
    run_id: &str,
    correlation_id: Option<String>,
    hop_count: u8,
) -> Result<()> {
    let spec = s.botflow.as_ref().context("missing botflow spec")?;
    let conv_id = format!("bot:{}", bot.id);
    let packet: MissionPacket = {
        let db = s.db()?;
        let (desired, raw_pkt): (String, String) = db.query_row(
            "SELECT desired_state, mission_packet_json FROM runs WHERE run_id = ?1",
            [run_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if desired == "cancelled" {
            db.execute(
                "UPDATE runs SET observed_state = 'canceled', updated_at = ?1 WHERE run_id = ?2",
                params![now(), run_id],
            )?;
            s.rev_wake.notify_waiters();
            return Ok(());
        }
        serde_json::from_str(&raw_pkt)?
    };

    let mut current_target = initial_target.clone();
    let mut excluded_targets = BTreeSet::new();
    let mut verify_attempt: u8 = 0;
    let mut verifier_feedback: Option<String> = None;
    let mut pre_snapshots: BTreeMap<PathBuf, crate::checkpoints::WorktreeSnapshot> =
        BTreeMap::new();

    loop {
        let workdir = PathBuf::from(&current_target.workdir);
        std::fs::create_dir_all(&workdir)?;
        if !pre_snapshots.contains_key(&workdir) {
            pre_snapshots.insert(workdir.clone(), capture_worktree(&workdir)?);
        }
        let pre_snapshot = pre_snapshots
            .get(&workdir)
            .expect("worktree snapshot inserted")
            .clone();

        // Build cold-start continuation packet (Role + Epoch-Pinned Dual-Tier Memory + DAG + Recent 8 Turns).
        let cold_start = {
            let mut db = s.db()?;
            let binding = read_binding(&db, &bot.id)?.context("missing binding")?;
            let role_prompt = format!(
                "Bot `{}` ({}) — Objective: {}",
                bot.id, bot.name, packet.objective
            );
            let pkt = tarla_context::build_cold_start_packet(
                &mut db,
                s.id(),
                s.node(),
                &bot.id,
                &conv_id,
                binding.hc_terminal_addr.as_deref(),
                binding.compaction_epoch as i64,
                &role_prompt,
                8,
                s.ws.config.context_policy.supervisor_active_budget_tokens,
            )?;
            db.execute(
                "UPDATE runs SET observed_state = 'executing', target_slot_id = ?1, updated_at = ?2
                 WHERE run_id = ?3 AND desired_state != 'cancelled'",
                params![current_target.template_name, now(), run_id],
            )?;
            db.execute(
                "UPDATE bot_bindings SET active_template = ?1, active_core_group = ?2, compaction_epoch = ?3, last_active_at = ?4
                 WHERE bot_id = ?5",
                params![
                    current_target.template_name,
                    current_target.core_group,
                    pkt.compaction_epoch,
                    now(),
                    bot.id
                ],
            )?;
            pkt
        };
        s.rev_wake.notify_waiters();

        // Persist cold-start packet to private runtime file for CLI / template consumption.
        let packets_dir = s.ws.root.join("runtime/packets");
        std::fs::create_dir_all(&packets_dir)?;
        let packet_file = packets_dir.join(format!("{run_id}.md"));
        let mut prompt_body = cold_start.rendered.clone();
        prompt_body.push_str(&format!(
            "\n\n[Current Action Goal `{run_id}`]\nObjective: {}\n",
            packet.objective
        ));
        if !packet.acceptance_criteria.is_empty() {
            prompt_body.push_str(&format!(
                "Acceptance Criteria: {}\n",
                packet.acceptance_criteria.join("; ")
            ));
        }
        if let Some(fb) = &verifier_feedback {
            prompt_body.push_str(&format!(
                "\n[Check-Before-Done Rework Feedback (attempt {verify_attempt}/{MAX_VERIFY_RETRIES})]\n{fb}\n"
            ));
        }
        std::fs::write(&packet_file, &prompt_body)?;

        let worker_token = crate::v2_ctl::issue_scoped_token(
            &s.ws.token,
            s.id(),
            crate::v2_ctl::CtlRole::Worker,
            &bot.id,
            Some(run_id),
        );
        let mut child_env: BTreeMap<String, String> = std::env::vars().collect();
        scrub_agent_env(&mut child_env);
        if let Some(profile) = &current_target.env_profile {
            let driver = tarla_botflow::command_basename(&current_target.command);
            for (k, v) in tarla_sidecar::credential_environment(&s.home, profile, driver)? {
                child_env.insert(k, v);
            }
        }
        child_env.insert("TARLA_URL".into(), s.local_url.clone());
        child_env.insert("TARLA_WORKSPACE".into(), s.id().to_owned());
        child_env.insert("TARLA_TOKEN".into(), worker_token);
        child_env.insert("TARLA_CTL_ROLE".into(), "worker".into());
        child_env.insert("TARLA_BOT_ID".into(), bot.id.clone());
        child_env.insert("TARLA_RUN_ID".into(), run_id.to_owned());
        child_env.insert("TARLA_MISSION_JSON".into(), serde_json::to_string(&packet)?);
        child_env.insert(
            "TARLA_CONTEXT_FILE".into(),
            packet_file.to_string_lossy().into_owned(),
        );
        if let Some(corr) = &correlation_id {
            child_env.insert("TARLA_CORRELATION_ID".into(), corr.clone());
        }
        child_env.insert("TARLA_HOP_COUNT".into(), hop_count.to_string());

        let exec_res = run_target_process(
            s,
            &current_target,
            run_id,
            &workdir,
            &prompt_body,
            &child_env,
        )
        .await;

        // Check if canceled while running.
        let desired_now: String = s.db()?.query_row(
            "SELECT desired_state FROM runs WHERE run_id = ?1",
            [run_id],
            |r| r.get(0),
        )?;
        if desired_now == "cancelled" {
            finalize_terminal_state(
                s,
                &bot.id,
                &conv_id,
                run_id,
                ActionState::Canceled,
                "Action canceled by Master Control",
                Vec::new(),
            )?;
            return Ok(());
        }

        // Wait while paused in `blocked_approval` (270s mobile approval window pauses execution timeout!).
        wait_if_blocked_approval(s, run_id).await?;
        let state_after_approval: String = s.db()?.query_row(
            "SELECT observed_state FROM runs WHERE run_id = ?1",
            [run_id],
            |r| r.get(0),
        )?;
        if state_after_approval == "canceled" {
            finalize_terminal_state(
                s,
                &bot.id,
                &conv_id,
                run_id,
                ActionState::Canceled,
                "Canceled during approval hold",
                Vec::new(),
            )?;
            return Ok(());
        }
        if state_after_approval == "aborted" {
            finalize_terminal_state(
                s,
                &bot.id,
                &conv_id,
                run_id,
                ActionState::Aborted,
                "Rejected during approval hold",
                Vec::new(),
            )?;
            return Ok(());
        }

        match exec_res {
            Ok((0, raw_output)) => {
                let gated = gate_output(
                    &s.ws.root.join("runtime"),
                    run_id,
                    &raw_output,
                    spec.policy.result_gate_max_chars,
                )?;
                let changed_files = if bot.checkpoint_files {
                    finalize_run_checkpoints(&mut s.db()?, run_id, &workdir, &pre_snapshot)?
                } else {
                    diff_worktree_paths(&workdir, &pre_snapshot)?
                };

                // Guardrail #2: Check-Before-Done heterogeneous verification.
                if let Some(verifier_ref) = &bot.verify_before_done {
                    let verifier = spec.resolve_target(verifier_ref)?;
                    let verdict = run_check_before_done(
                        &verifier,
                        run_id,
                        &packet.objective,
                        &packet.acceptance_criteria,
                        &changed_files,
                        &s.ws.root.join("runtime"),
                    )?;
                    if !verdict.passed {
                        if verify_attempt < MAX_VERIFY_RETRIES {
                            verify_attempt += 1;
                            verifier_feedback = Some(verdict.feedback);
                            continue;
                        }
                        finalize_terminal_state(
                            s,
                            &bot.id,
                            &conv_id,
                            run_id,
                            ActionState::Aborted,
                            &format!(
                                "Check-Before-Done rejected after {} retries: {}",
                                MAX_VERIFY_RETRIES, verdict.feedback
                            ),
                            changed_files,
                        )?;
                        return Ok(());
                    }
                }

                let summary = if gated.text.trim().is_empty() {
                    format!(
                        "Completed `{}` on `{}`",
                        packet.objective, current_target.target_ref
                    )
                } else {
                    gated.text
                };
                finalize_terminal_state(
                    s,
                    &bot.id,
                    &conv_id,
                    run_id,
                    ActionState::Succeeded,
                    &summary,
                    changed_files,
                )?;
                return Ok(());
            }
            Ok((exit_code, raw_output)) => {
                // Non-zero exit or 429 rate-limit -> evaluate Guardrail #1 (!acted vs acted).
                excluded_targets.insert(current_target.target_ref.clone());
                let reason = format!(
                    "exit_code={exit_code}: {}",
                    raw_output.chars().take(240).collect::<String>()
                );
                let decision = evaluate_failover_gate(
                    &mut s.db()?,
                    s.id(),
                    run_id,
                    s.node(),
                    &bot.id,
                    &workdir,
                    &pre_snapshot,
                    spec,
                    None,
                    &excluded_targets,
                    &reason,
                    now(),
                )?;
                s.rev_wake.notify_waiters();
                match decision {
                    FailoverDecision::SilentReplay { next_target } => {
                        current_target = next_target;
                        continue;
                    }
                    FailoverDecision::BlockedApproval {
                        approval_id,
                        changed_paths,
                        external_ops,
                    } => {
                        let _ = tarla_memory::record_sync_event(
                            &s.db()?,
                            s.id(),
                            "acted_failover_blocked",
                            run_id,
                            &json!({
                                "approval_id": approval_id,
                                "changed_paths": changed_paths,
                                "external_ops": external_ops,
                            }),
                            now(),
                        );
                        if !s.is_supervisor() {
                            let svc = s.clone();
                            let msg = json!({
                                "run_id": run_id,
                                "bot_id": bot.id,
                                "state": "blocked_approval",
                                "approval": {
                                    "approval_id": approval_id,
                                    "tool_or_cmd": format!("failover:{}", bot.id),
                                    "risk_reason": reason,
                                }
                            });
                            tokio::spawn(async move {
                                let _ = peers::call(&svc, svc.supervisor(), "v2_state", &msg).await;
                            });
                        }
                        if bot.checkpoint_files {
                            let _ = finalize_run_checkpoints(
                                &mut s.db()?,
                                run_id,
                                &workdir,
                                &pre_snapshot,
                            );
                        }
                        wait_if_blocked_approval(s, run_id).await?;
                        let resolved_state: String = s.db()?.query_row(
                            "SELECT observed_state FROM runs WHERE run_id = ?1",
                            [run_id],
                            |r| r.get(0),
                        )?;
                        if resolved_state == "executing" {
                            if let Some(next_target) =
                                spec.select_target_for_bot(&bot.id, None, &excluded_targets)?
                            {
                                current_target = next_target;
                                continue;
                            }
                        }
                        finalize_terminal_state(
                            s,
                            &bot.id,
                            &conv_id,
                            run_id,
                            ActionState::Aborted,
                            &reason,
                            Vec::new(),
                        )?;
                        return Ok(());
                    }
                    FailoverDecision::NoFallbackAvailable => {
                        if bot.checkpoint_files {
                            let _ = finalize_run_checkpoints(
                                &mut s.db()?,
                                run_id,
                                &workdir,
                                &pre_snapshot,
                            );
                        }
                        finalize_terminal_state(
                            s,
                            &bot.id,
                            &conv_id,
                            run_id,
                            ActionState::Aborted,
                            &reason,
                            Vec::new(),
                        )?;
                        return Ok(());
                    }
                }
            }
            Err(err) => {
                finalize_terminal_state(
                    s,
                    &bot.id,
                    &conv_id,
                    run_id,
                    ActionState::Aborted,
                    &err.to_string(),
                    Vec::new(),
                )?;
                return Err(err);
            }
        }
    }
}

async fn run_target_process(
    s: &Service,
    target: &ResolvedTarget,
    run_id: &str,
    workdir: &Path,
    prompt_body: &str,
    env: &BTreeMap<String, String>,
) -> Result<(i32, String)> {
    // If the target's `herdr-core` group daemon is live, launch in the private `hc` core first.
    if let Some(hc) = s.herdr_cores.get(&target.core_group) {
        if hc.available().await.is_ok() {
            let term_addr = hc
                .spawn_terminal(&TerminalSpawnSpec {
                    run_id: run_id.to_owned(),
                    name: None,
                    template: None,
                    argv: vec![target.command.clone(), prompt_body.to_owned()],
                    prompt: None,
                    cwd: workdir.canonicalize()?,
                    env: env.clone(),
                    labels: BTreeMap::from([(
                        "tarla_template".into(),
                        target.template_name.clone(),
                    )]),
                    parent: None,
                    outlive_parent: false,
                    notify: Vec::new(),
                })
                .await?;
            {
                let db = s.db()?;
                db.execute(
                    "UPDATE runs SET hc_terminal_id = ?1, updated_at = ?2 WHERE run_id = ?3",
                    params![term_addr, now(), run_id],
                )?;
                db.execute(
                    "UPDATE bot_bindings SET hc_terminal_addr = ?1, last_active_at = ?2
                     WHERE active_node_id = ?3 AND active_template = ?4",
                    params![term_addr, now(), s.node(), target.template_name],
                )?;
            }
            loop {
                let st = hc.status(run_id, &term_addr).await?;
                if st.state == "exited" || st.exit_code.is_some() {
                    let text = hc.read(run_id, &term_addr, 400).await.unwrap_or_default();
                    return Ok((st.exit_code.unwrap_or(0), text));
                }
                if st.blocked_prompt().is_some() {
                    let db = s.db()?;
                    db.execute(
                        "UPDATE runs SET observed_state = 'blocked_approval', updated_at = ?1 WHERE run_id = ?2",
                        params![now(), run_id],
                    )?;
                    s.rev_wake.notify_waiters();
                }
                tokio::time::sleep(Duration::from_millis(80)).await;
            }
        }
    }

    // Direct supervised subprocess execution (works identically when `herdr-core` is absent).
    let mut cmd = tokio::process::Command::new(&target.command);
    cmd.current_dir(workdir)
        .arg(prompt_body)
        .envs(env)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());
    let child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn target command `{}`", target.command))?;
    let output = child.wait_with_output().await?;
    let code = output.status.code().unwrap_or(1);
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    let combined = if stderr.trim().is_empty() {
        stdout.into_owned()
    } else if stdout.trim().is_empty() {
        stderr.into_owned()
    } else {
        format!("{stdout}\n{stderr}")
    };
    Ok((code, combined))
}

async fn wait_if_blocked_approval(s: &Service, run_id: &str) -> Result<()> {
    let hold_deadline = std::time::Instant::now()
        + Duration::from_secs(tarla_botflow::APPROVAL_HOLD_TIMEOUT_SECS as u64);
    loop {
        let (state, pending_res): (String, Option<String>) = {
            let db = s.db()?;
            let st: String = db.query_row(
                "SELECT observed_state FROM runs WHERE run_id = ?1",
                [run_id],
                |r| r.get(0),
            )?;
            let res: Option<String> = db
                .query_row(
                    "SELECT resolution FROM approvals WHERE run_id = ?1 ORDER BY created_at DESC LIMIT 1",
                    [run_id],
                    |r| r.get(0),
                )
                .optional()?;
            (st, res)
        };
        if state != "blocked_approval" {
            return Ok(());
        }
        match pending_res.as_deref() {
            Some("approved") => {
                s.db()?.execute(
                    "UPDATE runs SET observed_state = 'executing', updated_at = ?1 WHERE run_id = ?2",
                    params![now(), run_id],
                )?;
                s.rev_wake.notify_waiters();
                return Ok(());
            }
            Some("rejected") => {
                s.db()?.execute(
                    "UPDATE runs SET observed_state = 'aborted', updated_at = ?1 WHERE run_id = ?2",
                    params![now(), run_id],
                )?;
                s.rev_wake.notify_waiters();
                return Ok(());
            }
            _ => {}
        }
        if std::time::Instant::now() >= hold_deadline {
            s.db()?.execute(
                "UPDATE runs SET observed_state = 'aborted', receipt_summary = 'approval timed out after 270s', updated_at = ?1 WHERE run_id = ?2",
                params![now(), run_id],
            )?;
            s.rev_wake.notify_waiters();
            return Ok(());
        }
        tokio::select! {
            _ = s.notify.notified() => {}
            _ = s.rev_wake.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(80)) => {}
        }
    }
}

fn finalize_terminal_state(
    s: &S,
    bot_id: &str,
    conv_id: &str,
    run_id: &str,
    final_state: ActionState,
    summary: &str,
    artifacts: Vec<String>,
) -> Result<()> {
    let max_receipt_tokens = s
        .botflow
        .as_ref()
        .map(|b| (b.policy.run_receipt_max_bytes / 4).max(64))
        .unwrap_or(500);
    let receipt = bound_receipt(
        RunReceipt {
            run_id: run_id.to_owned(),
            final_state: final_state.as_str().to_owned(),
            executive_summary: if summary.trim().is_empty() {
                final_state.as_str().to_owned()
            } else {
                summary.to_owned()
            },
            artifact_refs: artifacts,
            s0_transcript_pointer: format!("node:{}/run:{run_id}/transcript.jsonl", s.node()),
        },
        max_receipt_tokens,
    )?;
    let ts = now();
    let mut db = s.db()?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute(
        "UPDATE runs
         SET observed_state = ?1,
             receipt_summary = ?2,
             artifacts_json = ?3,
             updated_at = ?4
         WHERE run_id = ?5",
        params![
            final_state.as_str(),
            receipt.executive_summary,
            serde_json::to_string(&receipt.artifact_refs)?,
            ts,
            run_id
        ],
    )?;
    tx.execute(
        "INSERT INTO messages(conversation_id, channel_type, source_msg_id, sender_role, content, run_id, created_at)
         VALUES (?1, ?2, ?3, 'assistant', ?4, ?5, ?6)
         ON CONFLICT(channel_type, source_msg_id) DO NOTHING",
        params![
            conv_id,
            format!("bot:{bot_id}"),
            format!("reply-{run_id}"),
            receipt.executive_summary,
            run_id,
            ts
        ],
    )?;
    tx.commit()?;

    if !s.is_supervisor() {
        let svc = s.clone();
        let msg = json!({
            "run_id": run_id,
            "bot_id": bot_id,
            "state": final_state.as_str(),
            "receipt": receipt,
        });
        tokio::spawn(async move {
            let _ = peers::call(&svc, svc.supervisor(), "v2_state", &msg).await;
        });
    }

    s.rev_wake.notify_waiters();
    s.botflow_wake.notify_waiters();
    Ok(())
}

/// Mirror a paired Executor's V2 ROS 2 Action state / receipt update onto the Supervisor.
pub fn accept_remote_v2_state(s: &Service, node: &str, v: &Value) -> Result<Value> {
    let run_id = string(v, "run_id")?;
    let bot_id = string(v, "bot_id")?;
    let state = string(v, "state")?;
    let conv_id = format!("bot:{bot_id}");
    let ts = now();
    let mut db = s.db()?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some(app) = v.get("approval").filter(|a| a.is_object()) {
        tx.execute(
            "INSERT OR IGNORE INTO approvals(approval_id, workspace_id, run_id, node_id, tool_or_cmd, risk_reason, resolution, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7)",
            params![
                string(app, "approval_id")?,
                s.id(),
                run_id,
                node,
                string(app, "tool_or_cmd")?,
                string(app, "risk_reason")?,
                ts
            ],
        )?;
    }
    if let Some(rcpt_val) = v.get("receipt").filter(|r| r.is_object()) {
        let rcpt: RunReceipt = serde_json::from_value(rcpt_val.clone())?;
        tx.execute(
            "UPDATE runs
             SET observed_state = ?1,
                 receipt_summary = ?2,
                 artifacts_json = ?3,
                 updated_at = ?4
             WHERE run_id = ?5 AND target_node_id = ?6",
            params![
                state,
                rcpt.executive_summary,
                serde_json::to_string(&rcpt.artifact_refs)?,
                ts,
                run_id,
                node
            ],
        )?;
        tx.execute(
            "INSERT INTO messages(conversation_id, channel_type, source_msg_id, sender_role, content, run_id, created_at)
             VALUES (?1, ?2, ?3, 'assistant', ?4, ?5, ?6)
             ON CONFLICT(channel_type, source_msg_id) DO NOTHING",
            params![
                conv_id,
                format!("bot:{bot_id}"),
                format!("reply-{run_id}"),
                rcpt.executive_summary,
                run_id,
                ts
            ],
        )?;
    } else {
        tx.execute(
            "UPDATE runs SET observed_state = ?1, updated_at = ?2
             WHERE run_id = ?3 AND target_node_id = ?4",
            params![state, ts, run_id, node],
        )?;
    }
    tx.commit()?;
    s.rev_wake.notify_waiters();
    s.botflow_wake.notify_waiters();
    Ok(json!({"run_id": run_id, "state": state}))
}

/// Background worker: delivers ready `botflow_inbox` items (respecting `debounce_ms`,
/// `human_lease_active`, and single-flight per Bot) and reaps idle `hc` terminals after 30 minutes.
pub async fn worker(s: S) {
    if s.botflow.is_none() {
        return;
    }
    loop {
        if let Err(err) = tick_botflow_and_reap(&s).await {
            eprintln!("botflow worker tick error: {err}");
        }
        tokio::select! {
            _ = s.botflow_wake.notified() => {}
            _ = tokio::time::sleep(Duration::from_millis(120)) => {}
        }
    }
}

async fn tick_botflow_and_reap(s: &S) -> Result<()> {
    let Some(spec) = s.botflow.as_ref() else {
        return Ok(());
    };
    let now_secs = now();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;

    // 1. Reap idle `hc` terminals past `idle_reap_after_secs` (default 1800s) while keeping `conversation_id`.
    let bindings = list_bindings(&s.db()?)?;
    for b in &bindings {
        if b.active_node_id == s.node() && b.hc_terminal_addr.is_some() {
            let idle_for = now_secs.saturating_sub(b.last_active_at) as u64;
            let busy = bot_has_active_run(&s.db()?, &b.conversation_id)?;
            if !busy && !b.human_lease_active(now_secs) && idle_for >= b.idle_reap_after_secs {
                if let (Some(addr), Some(hc)) = (
                    b.hc_terminal_addr.as_deref(),
                    s.herdr_cores.get(&b.active_core_group),
                ) {
                    let _ = hc.cancel(&format!("bot:{}", b.bot_id), addr).await;
                }
                s.db()?.execute(
                    "UPDATE bot_bindings SET hc_terminal_addr = NULL WHERE bot_id = ?1",
                    [&b.bot_id],
                )?;
            }
        }
    }

    // 2. Deliver ready `botflow_inbox` events on the designated supervisor.
    if !s.is_supervisor() {
        return Ok(());
    }
    for bot in &spec.bots {
        let (lease_active, busy) = {
            let db = s.db()?;
            let Some(binding) = read_binding(&db, &bot.id)? else {
                continue;
            };
            let b_busy = bot_has_active_run(&db, &binding.conversation_id)?;
            (binding.human_lease_active(now_secs), b_busy)
        };
        let delivery = {
            let mut db = s.db()?;
            claim_next_inbox(&mut db, &bot.id, now_ms, lease_active, busy)?
        };
        let Some(item) = delivery else {
            continue;
        };
        let upstream_cmd = item.payload["source_topic"]
            .as_str()
            .and_then(|topic| topic.split_once('/'))
            .and_then(|(up_bot, _)| read_binding(&s.db().ok()?, up_bot).ok().flatten())
            .and_then(|up_bind| {
                spec.resolve_target(&up_bind.active_target_ref())
                    .ok()
                    .map(|r| r.command)
            });

        let obj = format!(
            "[Botflow Event `{}` on port `{}` (coalesced={}, hop={})] {}",
            item.payload["source_topic"].as_str().unwrap_or("event"),
            item.input_port,
            item.coalesced_count,
            item.hop_count,
            serde_json::to_string(&item.payload["payload"])?
        );
        send_goal(
            s,
            &bot.id,
            &obj,
            Vec::new(),
            None,
            upstream_cmd.as_deref(),
            Some(&item.correlation_id),
            item.hop_count,
        )
        .await?;
    }
    Ok(())
}

/// Emit a structured event from `<bot_id>/<port>` onto the workspace `botflow.yml` bus.
pub fn emit_botflow_port_event(
    s: &Service,
    bot_id: &str,
    port: &str,
    payload: Value,
    correlation_id: Option<&str>,
    current_hop: u8,
) -> Result<(tarla_botflow::EventEnvelope, Vec<PublishOutcome>)> {
    let spec = s
        .botflow
        .as_ref()
        .context("botflow.yml is not configured in this workspace")?;
    if bot_id != "sys" {
        let bot = spec
            .bot(bot_id)
            .with_context(|| format!("unknown bot `{bot_id}`"))?;
        anyhow::ensure!(
            bot.outputs.iter().any(|p| p == port),
            "bot `{bot_id}` does not declare output port `{port}`"
        );
    }
    let event_key = uid();
    let gated_payload = crate::guardrails::gate_json_payload(
        &s.ws.root.join("runtime"),
        &event_key,
        payload,
        spec.policy.result_gate_max_chars,
    )?;
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64;
    let source_topic = format!("{bot_id}/{port}");
    let next_hop = current_hop.saturating_add(1);

    let mut db = s.db()?;
    let hlc = {
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let stamp = tarla_memory::next_local_hlc(&tx, s.node())?;
        tx.commit()?;
        serde_json::to_string(&stamp)?
    };
    let res = publish_event(
        &mut db,
        spec,
        &source_topic,
        correlation_id,
        next_hop,
        &hlc,
        gated_payload,
        now_ms,
    )?;
    s.rev_wake.notify_waiters();
    s.botflow_wake.notify_waiters();
    Ok(res)
}
