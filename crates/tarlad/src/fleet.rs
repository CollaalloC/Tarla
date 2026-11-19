use super::*;
use rusqlite::{params, OptionalExtension};
use tarla_memory::{MemoryBackend, SqliteMemory};
use tarla_protocol::{MissionPacket, RunReceipt};
use tarla_scheduler::{select_best_slot, AgentSlotSnapshot, SlotHealth, TaskRequirement};
use tarla_sidecar::Herdr;

pub struct Slot {
    pub snapshot: AgentSlotSnapshot,
    pub argv: Vec<String>,
    pub workdir: PathBuf,
}
pub fn load(conf: &toml::Table, ws: &Workspace, _home: &Path) -> Result<Vec<Slot>> {
    let mut slots = Vec::new();
    if let Some(items) = conf.get("slots") {
        for item in items.as_array().context("expected [[slots]]")? {
            let v = serde_json::to_value(item)?;
            let id = string(&v, "slot_id")?;
            tarla_workspace::validate_name(id)?;
            if slots.iter().any(|s: &Slot| s.snapshot.slot_id == id) {
                bail!("duplicate slot")
            }
            let path = PathBuf::from(string(&v, "workdir")?);
            if !path.is_absolute() || !path.is_dir() {
                bail!("slot workdir must be an existing absolute directory")
            }
            let argv: Vec<String> = serde_json::from_value(v["argv"].clone())?;
            if argv.first().is_none_or(String::is_empty) {
                bail!("slot argv cannot be empty")
            }
            let mut sv = json!({"workspace_id":ws.config.workspace.id,"node_id":ws.config.workspace.node_id,"supported_roles":["implementer"],"billing_tier":"prepaid_subscription","window_used_pct":0.0,"health":"healthy"});
            for (k, val) in v.as_object().context("invalid slot")? {
                if !matches!(k.as_str(), "workspace_id" | "node_id" | "active_runs") {
                    sv[k] = val.clone();
                }
            }
            slots.push(Slot {
                snapshot: serde_json::from_value(sv)?,
                argv,
                workdir: path.canonicalize()?,
            });
        }
    };
    Ok(slots)
}
pub fn local_slots(s: &Service) -> Result<Vec<AgentSlotSnapshot>> {
    if !s.can_execute() {
        return Ok(vec![]);
    }
    let c = s.db()?;
    let mut result = vec![];
    for slot in &s.slots {
        let mut snap = slot.snapshot.clone();
        snap.active_runs=c.query_row("SELECT count(*) FROM runs WHERE target_node_id=? AND target_slot_id=? AND observed_state NOT IN ('succeeded','failed','cancelled')",params![s.node(),snap.slot_id],|r|r.get(0))?;
        if let Some(raw) = c
            .query_row(
                "SELECT report FROM service_telemetry WHERE slot_id=?",
                [&snap.slot_id],
                |r| r.get::<_, String>(0),
            )
            .optional()?
        {
            let v: Value = serde_json::from_str(&raw)?;
            if let Some(health) = v.get("health") {
                snap.health = serde_json::from_value(health.clone())?;
            }
            if let Some(n) = v["window_used_pct"].as_f64() {
                snap.window_used_pct = n as f32;
            }
            if let Some(n) = v["weekly_used_pct"].as_f64() {
                snap.weekly_used_pct = n as f32;
            }
            if let Some(n) = v.get("window_resets_at") {
                snap.window_resets_at = n.as_i64();
            }
            if let Some(n) = v.get("weekly_resets_at") {
                snap.weekly_resets_at = n.as_i64();
            }
        }
        result.push(snap);
    }
    Ok(result)
}
pub fn snapshots(s: &Service) -> Result<Vec<AgentSlotSnapshot>> {
    let mut slots = local_slots(s)?;
    let c = s.db()?;
    for p in list(&c, "SELECT node_id,slots,seen FROM service_peers", &[])? {
        if !s.peers.contains_key(string(&p, "node_id")?) {
            continue;
        }
        let mut remote: Vec<AgentSlotSnapshot> = serde_json::from_str(string(&p, "slots")?)?;
        if p["seen"].as_i64().unwrap_or(0) < now() - 15 {
            for r in &mut remote {
                r.health = SlotHealth::NodeOffline;
            }
        }
        slots.extend(remote);
    }
    Ok(slots)
}
pub fn dispatch(s: &Service, conv: &str, arg: &Value, run_id: Option<String>) -> Result<Value> {
    if !s.is_supervisor() {
        bail!("only the designated supervisor can dispatch")
    }
    let mut c = s.db()?;
    control::ensure_conversation(&c, s, conv)?;
    let rid = run_id.unwrap_or_else(uid);
    if let Some(existing) = list(
        &c,
        "SELECT r.run_id,r.conversation_id,q.request_json FROM runs r LEFT JOIN service_requirements q USING(run_id) WHERE run_id=?",
        &[&rid],
    )?
    .first()
    {
        if existing["conversation_id"] != conv || existing["request_json"].as_str().is_some_and(|old|old!=arg.to_string()) {
            bail!("run identity conflict")
        }
        return Ok(json!({"run_id":rid}));
    }
    let mut slots = snapshots(s)?;
    if let Some(n) = arg["node_id"].as_str() {
        slots.retain(|x| x.node_id == n)
    }
    if let Some(n) = arg["slot_id"].as_str() {
        slots.retain(|x| x.slot_id == n)
    }
    let mut require = TaskRequirement {
        workspace_id: s.id().into(),
        role: serde_json::from_value(arg.get("role").cloned().unwrap_or(json!("implementer")))?,
        required_caps: strings(arg, "required_caps")?,
        exclude_model_family: arg["exclude_model_family"].as_str().map(String::from),
    };
    if require.role == tarla_scheduler::AgentRole::Reviewer
        && require.exclude_model_family.is_none()
    {
        require.exclude_model_family=c.query_row("SELECT q.model_family FROM service_requirements q JOIN runs r USING(run_id) WHERE r.conversation_id=? AND q.role IN ('architect','implementer') ORDER BY r.created_at DESC,r.rowid DESC LIMIT 1",[conv],|r|r.get(0)).optional()?;
        if require.exclude_model_family.is_none() {
            bail!("review requires the model family of the original author")
        }
    }
    let role = serde_json::to_value(require.role)?
        .as_str()
        .context("unknown role")?
        .to_owned();
    let preferred =
        s.ws.config
            .extra
            .get("scheduler")
            .and_then(|v| v.get("roles"))
            .and_then(|v| v.get(&role))
            .and_then(toml::Value::as_array);
    let eligible = preferred
        .into_iter()
        .flatten()
        .filter_map(|v| v.as_str())
        .find_map(|id| {
            let matching: Vec<_> = slots.iter().filter(|s| s.slot_id == id).cloned().collect();
            select_best_slot(&require, &matching, now()).cloned()
        });
    let candidate = eligible
        .as_ref()
        .or_else(|| select_best_slot(&require, &slots, now()))
        .context("no eligible agent slot")?;
    // Reject an impossible return budget before the remote program can change its workspace.
    tarla_context::bound_receipt(
        RunReceipt {
            run_id: rid.clone(),
            final_state: "succeeded".into(),
            executive_summary: "Done.".into(),
            artifact_refs: vec![],
            s0_transcript_pointer: format!("node:{}/run:{rid}/transcript.jsonl", candidate.node_id),
        },
        s.ws.config.context_policy.run_receipt_max_tokens,
    )?;
    let goal = arg["objective"]
        .as_str()
        .or_else(|| arg["goal"].as_str())
        .filter(|s| !s.trim().is_empty())
        .context("objective required")?;
    let mut decisions = strings(arg, "distilled_context")?;
    for item in tarla_context::related_decisions(&c, conv, goal, 5)? {
        if !decisions.contains(&item) {
            decisions.push(item);
        }
    }
    let m = SqliteMemory::new(&mut c, s.id(), s.node());
    let mut acceptance = strings(arg, "acceptance_criteria")?;
    if acceptance.is_empty() {
        acceptance = strings(arg, "acceptance")?
    }
    if acceptance.is_empty() {
        acceptance.push("Report the outcome of the requested objective.".into())
    }
    let packet = tarla_context::bound_mission(
        MissionPacket {
            run_id: rid.clone(),
            workspace_id: s.id().into(),
            objective: goal.into(),
            acceptance_criteria: acceptance,
            distilled_context: decisions,
            project_memory_snapshot: m.prefetch_frozen_snapshot(
                arg["scope"].as_str().unwrap_or("global"),
                s.ws.config.memory.prefetch_token_budget,
            )?,
            required_fact_keys: strings(arg, "required_fact_keys")?,
        },
        s.ws.config.context_policy.mission_packet_max_tokens,
    )?;
    let dbtx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    insert_run(
        &dbtx,
        s,
        conv,
        &candidate.node_id,
        &candidate.slot_id,
        &packet,
    )?;
    dbtx.execute(
        "INSERT INTO service_requirements VALUES(?,?,?,?)",
        params![rid, role, candidate.model_family, arg.to_string()],
    )?;
    if let Some(path) = arg["pinned_workdir"].as_str() {
        handoff::register(
            &dbtx,
            conv,
            &rid,
            &candidate.node_id,
            &candidate.slot_id,
            path,
            arg,
        )?;
    }
    if candidate.node_id == s.node() {
        claim(&dbtx, s, &rid)?;
    }
    dbtx.commit()?;
    s.notify.notify_waiters();
    Ok(json!({"run_id":rid,"target_node_id":candidate.node_id,"target_slot_id":candidate.slot_id}))
}
fn strings(arg: &Value, k: &str) -> Result<Vec<String>> {
    match arg.get(k) {
        None | Some(Value::Null) => Ok(vec![]),
        Some(Value::String(s)) => Ok(vec![s.clone()]),
        Some(v) => Ok(serde_json::from_value(v.clone())?),
    }
}
pub fn insert_run(
    c: &Connection,
    s: &Service,
    conv: &str,
    node: &str,
    slot: &str,
    packet: &MissionPacket,
) -> Result<()> {
    c.execute("INSERT INTO runs(run_id,workspace_id,conversation_id,supervisor_term,target_node_id,target_slot_id,desired_state,observed_state,mission_packet_json,created_at,updated_at) VALUES(?,?,?,?,?,?,'running','dispatch_pending',?,?,?)",params![packet.run_id,s.id(),conv,s.term(),node,slot,serde_json::to_string(packet)?,now(),now()])?;
    Ok(())
}
pub fn claim(c: &Connection, s: &Service, id: &str) -> Result<()> {
    if !s.can_execute() {
        bail!("node is configured as supervisor only")
    }
    c.execute(
        "INSERT OR IGNORE INTO service_claims(run_id,phase,updated_at) VALUES(?,'queued',?)",
        params![id, now()],
    )?;
    Ok(())
}
pub fn herdr(s: &Service) -> Result<Herdr> {
    Ok(Herdr::new(&s.ws.root.join("runtime"))?)
}
pub async fn worker(s: S) {
    loop {
        if let Err(e) = tick(&s).await {
            eprintln!("executor reconciliation: {e}")
        }
        tokio::select! {_=s.notify.notified()=>(),_=tokio::time::sleep(Duration::from_millis(750))=>()}
    }
}
async fn tick(s: &Service) -> Result<()> {
    if !s.can_execute() {
        return Ok(());
    }
    let jobs=list(&s.db()?,"SELECT r.*,c.phase,c.terminal,p.workdir AS pinned_workdir FROM service_claims c JOIN runs r USING(run_id) LEFT JOIN service_run_paths p USING(run_id) WHERE c.phase!='finished' ORDER BY c.updated_at LIMIT 128",&[])?;
    for job in jobs {
        if let Err(e) = step(s, &job).await {
            eprintln!("run {}: {e}", job["run_id"]);
        }
    }
    Ok(())
}
async fn step(s: &Service, job: &Value) -> Result<()> {
    let rid = string(job, "run_id")?;
    let hc = herdr(s)?;
    let mut phase = string(job, "phase")?.to_string();
    let goal = job["desired_state"] == "cancelled";
    let mut terminal = job["terminal"].as_str().map(String::from);
    let slot_id = string(job, "target_slot_id")?;
    {
        let mut locks = s
            .account_locks
            .lock()
            .map_err(|_| anyhow!("slot lock registry unavailable"))?;
        if !locks.contains_key(rid) {
            match tarla_sidecar::try_lock_slot(&s.home, slot_id) {
                Ok(guard) => {
                    locks.insert(rid.into(), guard);
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
                Err(e) => return Err(e.into()),
            }
        }
    }
    if phase == "queued" {
        if goal {
            return finish(s, rid, "cancelled", "Cancelled before launch.", "");
        }
        let Some(slot) = s
            .slots
            .iter()
            .find(|p| p.snapshot.slot_id == job["target_slot_id"].as_str().unwrap_or(""))
        else {
            return finish(s, rid, "failed", "The configured slot is unavailable.", "");
        };
        if let Some(p) = job["pinned_workdir"].as_str() {
            if p != slot.workdir.to_string_lossy() {
                return finish(
                    s,
                    rid,
                    "failed",
                    "Pinned workdir does not match this slot.",
                    "",
                );
            }
        }
        let packet: MissionPacket = serde_json::from_str(string(job, "mission_packet_json")?)?;
        for k in &packet.required_fact_keys {
            let path: Option<String> = s
                .db()?
                .query_row(
                    "SELECT local_path FROM workspace_facts WHERE node_id=? AND fact_key=?",
                    params![s.node(), k],
                    |r| r.get(0),
                )
                .optional()?;
            if !path.as_deref().is_some_and(|p| Path::new(p).exists()) {
                return finish(
                    s,
                    rid,
                    "failed",
                    "A required environment fact could not be verified.",
                    "",
                );
            }
        }
        if hc.available().await.is_err() {
            let setup = async {
                let t = tarla_sidecar::Toolchain::load(&s.home.join("bin"))?;
                t.ensure_herdr(&hc).await
            }
            .await;
            if let Err(e) = setup {
                return finish(
                    s,
                    rid,
                    "failed",
                    &format!("Private herdr-core unavailable: {e}"),
                    "",
                );
            }
        }
        let argv = match handoff::launch(s, job, slot, serde_json::to_string(&packet)?) {
            Ok(v) => v,
            Err(e) => return finish(s, rid, "failed", &format!("Pinned driver: {e}"), ""),
        };
        let mut env = tarla_sidecar::credential_environment(
            &s.home,
            &slot.snapshot.slot_id,
            &slot.snapshot.driver,
        )?;
        env.insert(
            "TARLA_HOOK_URL".into(),
            format!("{}/hooks/v1/{rid}", s.local_url),
        );
        env.insert("TARLA_RUN_TOKEN".into(), hooks::token(s, rid)?);
        env.insert("TARLA_RUN_ID".into(), rid.into());
        if s.db()?.execute("UPDATE service_claims SET phase='launching',updated_at=? WHERE run_id=? AND phase='queued'",params![now(),rid])?!=1{return Ok(())}
        match hc.start_run(rid, &argv, &slot.workdir, &env).await {
            Ok(t) => terminal = Some(t),
            Err(e) => {
                state(s, rid, "unconfirmed", None)?;
                return Err(e.into());
            }
        };
        phase = "launching".into();
    }
    if phase == "launching" {
        if terminal.is_none() {
            terminal = hc.find_run(rid).await?;
        }
        let Some(t) = terminal.as_deref() else {
            state(s, rid, "unconfirmed", None)?;
            return Ok(());
        };
        let mut db = s.db()?;
        let tx = db.transaction()?;
        tx.execute(
            "UPDATE service_claims SET phase='active',terminal=?,updated_at=? WHERE run_id=?",
            params![t, now(), rid],
        )?;
        update(&tx, s, rid, "running", Some(t), None)?;
        tx.commit()?;
    }
    if let Some(t) = terminal {
        s.transcripts.ensure(&s.ws.root, rid, &t, &hc).await?;
        let st = match hc.status(&t).await {
            Ok(st) => st,
            Err(e) => {
                state(s, rid, "unconfirmed", Some(&t))?;
                return Err(e.into());
            }
        };
        if st.exit_code.is_some()
            || matches!(st.state.as_str(), "exited" | "dead" | "closed" | "finished")
        {
            s.transcripts.drain(rid).await;
            let out = hc.read(&t, 1000).await.unwrap_or_default();
            let final_state = if goal {
                "cancelled"
            } else if st.exit_code == Some(0) {
                "succeeded"
            } else {
                "failed"
            };
            let summary = st
                .reason
                .as_deref()
                .unwrap_or("The configured agent process exited.");
            return finish(s, rid, final_state, summary, &out);
        }
        if goal {
            hc.cancel(&t).await?;
            return Ok(());
        }
        let status = st.status.unwrap_or(Value::Null);
        if status["state"] == "blocked" || st.state == "blocked" {
            let reason = status["msg"]
                .as_str()
                .or(status["title"].as_str())
                .unwrap_or("The agent requires human input.");
            let aid = format!("{:x}", Sha256::digest(format!("{rid}\n{status}")));
            let mut c = s.db()?;
            let tx = c.transaction()?;
            tx.execute("INSERT OR IGNORE INTO approvals(approval_id,workspace_id,run_id,node_id,tool_or_cmd,risk_reason,created_at) VALUES(?,?,?,?,?,?,?)",params![aid,s.id(),rid,s.node(),status["tool"].as_str().unwrap_or("agent request"),reason,now()])?;
            update(
                &tx,
                s,
                rid,
                "blocked",
                Some(&t),
                Some(
                    json!({"approval_id":aid,"risk_reason":reason,"tool_or_cmd":status["tool"].as_str().unwrap_or("agent request")}),
                ),
            )?;
            tx.commit()?;
        } else {
            let pending = s.db()?.query_row(
                "SELECT exists(SELECT 1 FROM approvals WHERE run_id=? AND resolution='pending')",
                [rid],
                |r| r.get::<_, bool>(0),
            )?;
            state(
                s,
                rid,
                if pending { "blocked" } else { "running" },
                Some(&t),
            )?;
        }
    };
    Ok(())
}
fn state(s: &Service, id: &str, next: &str, t: Option<&str>) -> Result<()> {
    let mut c = s.db()?;
    let tx = c.transaction()?;
    update(&tx, s, id, next, t, None)?;
    tx.commit()?;
    Ok(())
}
fn update(
    c: &Connection,
    s: &Service,
    id: &str,
    next: &str,
    t: Option<&str>,
    approval: Option<Value>,
) -> Result<()> {
    if c.execute("UPDATE runs SET observed_state=?,hc_terminal_id=coalesce(?,hc_terminal_id),updated_at=? WHERE run_id=? AND observed_state!=?",params![next,t,now(),id,next])?==0{return Ok(())}
    push(
        c,
        id,
        "state_changed",
        &json!({"run_id":id,"node_id":s.node(),"state":next,"terminal":t,"approval":approval}),
    )?;
    Ok(())
}
fn push(c: &Connection, id: &str, kind: &str, payload: &Value) -> Result<()> {
    c.execute("INSERT INTO outbox_events(event_id,run_id,event_type,payload_json,created_at) VALUES(?,?,?,?,?)",params![uid(),id,kind,serde_json::to_string(payload)?,now()])?;
    Ok(())
}
fn finish(s: &Service, id: &str, status: &str, summary: &str, transcript: &str) -> Result<()> {
    let path = crate::transcript::finish(&s.ws.root, id, status, transcript)?;
    let native = handoff::outcome(&s.ws.root, id)?;
    let summary = native
        .as_ref()
        .filter(|n| status == "succeeded" && !n.answer.is_empty())
        .map_or(summary, |n| n.answer.as_str());
    let mut receipt = RunReceipt {
        run_id: id.to_owned(),
        final_state: status.to_owned(),
        executive_summary: summary.chars().take(400).collect(),
        artifact_refs: vec![],
        s0_transcript_pointer: format!("node:{}/run:{id}/transcript.jsonl", s.node()),
    };
    if let Some(raw) = s
        .db()?
        .query_row(
            "SELECT receipt FROM service_receipt_drafts WHERE run_id=?",
            [id],
            |r| r.get::<_, String>(0),
        )
        .optional()?
    {
        let draft: RunReceipt = serde_json::from_str(&raw)?;
        receipt.executive_summary = draft.executive_summary;
        receipt.artifact_refs = draft.artifact_refs;
    }
    let receipt =
        tarla_context::bound_receipt(receipt, s.ws.config.context_policy.run_receipt_max_tokens)?;
    let mut c = s.db()?;
    let tx = c.transaction()?;
    tx.execute("UPDATE runs SET observed_state=?,receipt_summary=?,s0_log_local_path=?,updated_at=? WHERE run_id=?",params![status,receipt.executive_summary,path.to_string_lossy(),now(),id])?;
    tx.execute(
        "UPDATE service_claims SET phase='finished',updated_at=? WHERE run_id=?",
        params![now(), id],
    )?;
    if let Some(n) = native {
        tx.execute(
            "UPDATE runs SET native_session_ref=? WHERE run_id=?",
            params![n.session, id],
        )?;
    }
    push(&tx, id, "run_receipt", &json!(receipt))?;
    tx.commit()?;
    s.account_locks
        .lock()
        .map_err(|_| anyhow!("lock registry unavailable"))?
        .remove(id);
    s.notify.notify_waiters();
    Ok(())
}
