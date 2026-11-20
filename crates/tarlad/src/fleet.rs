use super::*;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use tarla_memory::{MemoryBackend, SqliteMemory};
use tarla_protocol::{MissionPacket, RunReceipt};
use tarla_scheduler::{select_best_slot, AgentSlotSnapshot, SlotHealth, TaskRequirement};
use tarla_sidecar::Herdr;

#[derive(Clone)]
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
    reported_slots(&s.db()?, &s.slots)
}

fn reported_slots(c: &Connection, slots: &[Slot]) -> Result<Vec<AgentSlotSnapshot>> {
    let mut result = vec![];
    for slot in slots {
        let mut snap = slot.snapshot.clone();
        snap.active_runs=c.query_row("SELECT count(*) FROM runs WHERE target_node_id=? AND target_slot_id=? AND observed_state NOT IN ('succeeded','failed','cancelled')",params![snap.node_id,snap.slot_id],|r|r.get(0))?;
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
/// The registry is local to this process. The executor and supervisor use the same
/// credential lock; live reports and nonterminal runs are re-read for each decision.
pub fn subscription_registry(
    home: &Path,
    db_path: &Path,
    slots: &[Slot],
) -> Result<tarla_supervisor::SubscriptionRegistry> {
    use tarla_scheduler::{quota_headroom, BillingTier};
    use tarla_supervisor::{SubscriptionRegistry, SubscriptionSlot};

    let root = home.canonicalize()?.join("slots");
    let registered = slots
        .iter()
        .map(|slot| {
            let id = &slot.snapshot.slot_id;
            (
                id.clone(),
                SubscriptionSlot {
                    driver: slot.snapshot.driver.clone(),
                    executable: PathBuf::from(&slot.argv[0]),
                    credential_dir: root.join(id),
                },
            )
        })
        .collect();
    let db_path = db_path.to_owned();
    let slots = slots.to_vec();
    Ok(SubscriptionRegistry::new(registered, move || {
        let c = Connection::open_with_flags(&db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)?;
        let tx = c.unchecked_transaction()?;
        let timestamp = now();
        let result = reported_slots(&tx, &slots)?
            .into_iter()
            .filter_map(|slot| {
                if slot.active_runs > 0
                    || !matches!(
                        slot.billing_tier,
                        BillingTier::PrepaidSubscription | BillingTier::FreeQuota
                    )
                {
                    return None;
                }
                let remaining = quota_headroom(&slot, timestamp)?;
                Some((slot.slot_id, remaining))
            })
            .collect();
        tx.commit()?;
        Ok(result)
    }))
}

#[cfg(all(test, unix))]
#[path = "subscription_tests.rs"]
mod subscription_tests;

pub fn snapshots(s: &Service) -> Result<Vec<AgentSlotSnapshot>> {
    snapshots_in(s, &s.db()?)
}
fn snapshots_in(s: &Service, c: &Connection) -> Result<Vec<AgentSlotSnapshot>> {
    let mut slots = if s.can_execute() {
        reported_slots(c, &s.slots)?
    } else {
        vec![]
    };
    let mut remote_load = c.prepare(
        "SELECT count(*) FROM runs WHERE target_node_id=? AND target_slot_id=?
         AND observed_state NOT IN ('failed','succeeded','cancelled')",
    )?;
    for p in list(c, "SELECT node_id,slots,seen FROM service_peers", &[])? {
        let node = string(&p, "node_id")?;
        if !s.peers.contains_key(node) {
            continue;
        }
        let remote: Vec<AgentSlotSnapshot> = serde_json::from_str(string(&p, "slots")?)?;
        for mut slot in remote {
            if p["seen"].as_i64().unwrap_or(0) < now() - 15 {
                slot.health = SlotHealth::NodeOffline;
            }
            // Bind reservations are authoritative before a remote heartbeat can include them.
            let reserved: u32 = remote_load.query_row(params![node, slot.slot_id], |r| r.get(0))?;
            slot.active_runs = slot.active_runs.max(reserved);
            slots.push(slot);
        }
    }
    Ok(slots)
}
pub fn dispatch(s: &Service, conv: &str, arg: &Value, run_id: Option<String>) -> Result<Value> {
    dispatch_selected(s, conv, arg, run_id, None, None)
}
pub enum Acknowledgement<'a> {
    Supervisor(&'a str),
    Command(&'a str),
    Continuation(&'a str),
}

pub fn dispatch_selected(
    s: &Service,
    conv: &str,
    arg: &Value,
    run_id: Option<String>,
    skill: Option<&skills::Selection>,
    acknowledgement: Option<Acknowledgement<'_>>,
) -> Result<Value> {
    if !s.is_supervisor() {
        bail!("only the designated supervisor can dispatch")
    }
    let mut c = s.db()?;
    let dbtx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    control::ensure_conversation(&dbtx, s, conv)?;
    let rid = run_id.unwrap_or_else(uid);
    if let Some(result) = existing_dispatch(&dbtx, conv, &rid, arg)? {
        return Ok(result);
    }
    let mut slots = snapshots_in(s, &dbtx)?;
    if let Some(n) = arg["node_id"].as_str() {
        slots.retain(|x| x.node_id == n)
    }
    if let Some(n) = arg["slot_id"].as_str() {
        slots.retain(|x| x.slot_id == n)
    }
    let require = TaskRequirement {
        workspace_id: s.id().into(),
        role: serde_json::from_value(arg.get("role").cloned().unwrap_or(json!("implementer")))?,
        required_caps: strings(arg, "required_caps")?,
        exclude_model_family: arg["exclude_model_family"].as_str().map(String::from),
    };
    if require.role == tarla_scheduler::AgentRole::Reviewer {
        let author_run = arg
            .get("author_run_id")
            .map(|value| {
                value
                    .as_str()
                    .context("author_run_id must be a run identifier")
            })
            .transpose()?;
        let family = author_family(&dbtx, conv, author_run)?;
        slots.retain(|slot| slot.model_family != family);
    } else if arg.get("author_run_id").is_some() {
        bail!("author_run_id applies only to reviews")
    }
    let scope = match arg.get("scope") {
        None => "global",
        Some(value) => value
            .as_str()
            .filter(|scope| {
                *scope == "global"
                    || scope
                        .strip_prefix("project:")
                        .is_some_and(|slug| !slug.trim().is_empty())
            })
            .context("memory scope must be global or project:<slug>")?,
    };
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
    if let Some(skill) = skill {
        decisions.insert(0, skill.instructions.clone());
    }
    for item in tarla_context::related_decisions(&dbtx, conv, goal, 5)? {
        if !decisions.contains(&item) {
            decisions.push(item);
        }
    }
    let mut memory_db = s.db()?;
    let m = SqliteMemory::new(&mut memory_db, s.id(), s.node());
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
            project_memory_snapshot: m
                .prefetch_frozen_snapshot(scope, s.ws.config.memory.prefetch_token_budget)?,
            required_fact_keys: strings(arg, "required_fact_keys")?,
        },
        s.ws.config.context_policy.mission_packet_max_tokens,
    )?;
    if matches!(acknowledgement, Some(Acknowledgement::Continuation(_)))
        && packet.distilled_context.first().map(String::as_str)
            != arg["distilled_context"][0].as_str()
    {
        bail!("continuation checkpoint does not fit the bounded mission")
    }
    if let Some(skill) = skill {
        if packet.distilled_context.first() != Some(&skill.instructions) {
            bail!("selected skill does not fit the complete bounded mission; shorten the skill or task")
        }
    }
    insert_run(
        &dbtx,
        s,
        conv,
        &candidate.node_id,
        &candidate.slot_id,
        &packet,
    )?;
    distillation::freeze_baseline(&dbtx, &rid, scope, goal)?;
    dbtx.execute(
        "INSERT INTO service_requirements VALUES(?,?,?,?)",
        params![rid, role, candidate.model_family, arg.to_string()],
    )?;
    if let Some(Acknowledgement::Continuation(parent)) = acknowledgement {
        handoff::commit_continuation(
            &dbtx,
            conv,
            parent,
            &rid,
            candidate,
            arg["pinned_workdir"].as_str(),
        )?;
    } else if let Some(path) = arg["pinned_workdir"].as_str() {
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
    let result =
        json!({"run_id":rid,"target_node_id":candidate.node_id,"target_slot_id":candidate.slot_id});
    let reply = match acknowledgement {
        None | Some(Acknowledgement::Continuation(_)) => None,
        Some(Acknowledgement::Supervisor(source)) => {
            Some((source, tarla_supervisor::dispatch_acknowledgement(&rid)))
        }
        Some(Acknowledgement::Command(source)) => Some((source, result.to_string())),
    };
    if let Some((source, reply)) = reply {
        control::record_run_reply(&dbtx, s.id(), conv, source, &rid, &reply)?;
    }
    dbtx.commit()?;
    s.notify.notify_waiters();
    Ok(result)
}
fn existing_dispatch(
    db: &Connection,
    conversation: &str,
    run_id: &str,
    args: &Value,
) -> Result<Option<Value>> {
    let saved = db
        .query_row(
            "SELECT r.conversation_id,r.target_node_id,r.target_slot_id,q.request_json
         FROM runs r LEFT JOIN service_requirements q USING(run_id) WHERE r.run_id=?",
            [run_id],
            |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, Option<String>>(3)?,
                ))
            },
        )
        .optional()?;
    let Some((owner, node, slot, request)) = saved else {
        return Ok(None);
    };
    if owner != conversation
        || request.is_some_and(|old| {
            let supplied = args.to_string();
            old != supplied
                && old.strip_prefix("sha256:") != Some(s1::request_digest(&supplied).as_str())
        })
    {
        bail!("run identity conflict")
    }
    Ok(Some(
        json!({"run_id":run_id, "target_node_id":node, "target_slot_id":slot}),
    ))
}

// A caller's optional family exclusion is an additional preference. The
// persisted slot that authored the work always determines reviewer independence.
pub(crate) fn author_family(
    db: &Connection,
    conversation: &str,
    run: Option<&str>,
) -> Result<String> {
    let mut stmt = db.prepare(
        "SELECT q.model_family FROM service_requirements q JOIN runs r USING(run_id)
         WHERE r.conversation_id=?1 AND q.role IN ('architect','implementer')
           AND (?2 IS NULL OR r.run_id=?2) ORDER BY r.created_at DESC,r.rowid DESC LIMIT 2",
    )?;
    let mut rows = stmt.query(params![conversation, run])?;
    let family: String = rows
        .next()?
        .context("review requires an author run in this conversation")?
        .get(0)?;
    if run.is_none() && rows.next()?.is_some() {
        bail!("multiple author runs in this conversation; specify author_run_id")
    }
    Ok(family)
}

pub(crate) fn strings(arg: &Value, k: &str) -> Result<Vec<String>> {
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
    // Recover every outstanding holder first. Only the head of each available
    // slot competes for admission. Cancelled waiters never need an account lock.
    let jobs = list(&s.db()?, "
        WITH occupied AS (
            SELECT r.target_slot_id FROM service_claims c JOIN runs r USING(run_id)
            WHERE c.phase IN ('active','launching','abandoning')
        ), heads AS (
            SELECT min(c.rowid) FROM service_claims c JOIN runs r USING(run_id)
            WHERE c.phase='queued' AND r.desired_state!='cancelled' GROUP BY r.target_slot_id
        )
        SELECT r.*,c.phase,c.terminal,p.workdir AS pinned_workdir,l.state AS permit
        FROM service_claims c JOIN runs r USING(run_id)
        LEFT JOIN service_run_paths p USING(run_id) LEFT JOIN service_launch_permits l USING(run_id)
        WHERE c.phase IN ('active','launching','abandoning') OR (c.phase='queued' AND
              (r.desired_state='cancelled' OR
               (c.rowid IN (SELECT * FROM heads) AND r.target_slot_id NOT IN (SELECT * FROM occupied))))
        ORDER BY CASE WHEN r.desired_state='cancelled' THEN 0 WHEN c.phase='queued' THEN 2 ELSE 1 END,c.rowid",
        &[])?;
    if jobs
        .iter()
        .any(|job| matches!(job["phase"].as_str(), Some("active" | "launching")))
    {
        let hc = herdr(s)?;
        if hc.available().await.is_err() {
            // Holders may survive their RPC server. Reopen this workspace's
            // private inventory even when every slot already has a claim.
            if let Err(e) = async {
                tarla_sidecar::Toolchain::load(&s.home.join("bin"))?
                    .ensure_herdr(&hc)
                    .await
            }
            .await
            {
                eprintln!("executor holder inventory unavailable: {e}");
            }
        }
    }
    for job in jobs {
        if let Err(e) = step(s, &job).await {
            eprintln!("run {}: {e}", job["run_id"]);
        }
    }
    Ok(())
}
async fn step(s: &Service, job: &Value) -> Result<()> {
    let rid = string(job, "run_id")?;
    let mut phase = string(job, "phase")?.to_string();
    let goal = job["desired_state"] == "cancelled";
    let mut terminal = job["terminal"].as_str().map(String::from);
    let slot_id = string(job, "target_slot_id")?;
    if phase == "abandoning" {
        return finish(s, rid, "cancelled", "", "");
    }
    if phase != "queued"
        && goal
        && job["permit"] == "issued"
        && launch::withdraw(&mut s.db()?, rid)?
    {
        return finish(
            s,
            rid,
            "cancelled",
            "Cancelled before the command started.",
            "",
        );
    }
    if phase == "queued" && goal {
        return finish(s, rid, "cancelled", "Cancelled before launch.", "");
    }
    if phase != "queued" && job["permit"].is_null() {
        // An existing installation can still have holders launched by an older
        // daemon. Retain its daemon-side guard until a terminal receipt arrives.
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
    let hc = herdr(s)?;
    if phase == "queued" {
        match tarla_sidecar::try_lock_slot(&s.home, slot_id) {
            Ok(guard) => drop(guard),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => return Ok(()),
            Err(e) => return Err(e.into()),
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
        let argv = launch::command(s, rid, argv)?;
        if !launch::issue(&mut s.db()?, rid)? {
            return Ok(());
        }
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
            terminal = match hc.find_run(rid).await {
                Ok(found) => found,
                Err(e) => {
                    state(s, rid, "unconfirmed", None)?;
                    return Err(e.into());
                }
            };
        }
        let Some(t) = terminal.as_deref() else {
            if launch::withdraw(&mut s.db()?, rid)? {
                return finish(
                    s,
                    rid,
                    "failed",
                    "The interrupted launch was withdrawn before the command started.",
                    "",
                );
            }
            state(s, rid, "unconfirmed", None)?;
            return Ok(());
        };
        let mut db = s.db()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if tx.execute(
            "UPDATE service_claims SET phase='active',terminal=?,updated_at=? WHERE run_id=? AND phase='launching'",
            params![t, now(), rid],
        )? != 1 { return Ok(()); }
        update_live(&tx, s, rid, t)?;
        tx.commit()?;
    }
    if let Some(t) = terminal {
        s.transcripts.ensure(&s.ws.root, rid, &t, &hc).await?;
        let st = match hc.status(rid, &t).await {
            Ok(st) => st,
            Err(e) => {
                state(s, rid, "unconfirmed", Some(&t))?;
                match hc.find_run(rid).await {
                    Ok(Some(found)) if found != t => {
                        s.db()?.execute(
                            "UPDATE service_claims SET phase='launching',terminal=?,updated_at=? WHERE run_id=? AND phase='active'",
                            params![found,now(),rid],
                        )?;
                    }
                    Ok(None) if launch::withdraw(&mut s.db()?, rid)? => {
                        return finish(
                            s,
                            rid,
                            "failed",
                            "The missing holder never admitted this command.",
                            "",
                        );
                    }
                    _ => {}
                }
                return Err(e.into());
            }
        };
        if st.exit_code.is_some()
            || matches!(st.state.as_str(), "exited" | "dead" | "closed" | "finished")
        {
            s.transcripts.drain(rid).await;
            let out = hc.read(rid, &t, 1000).await.unwrap_or_default();
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
            hc.cancel(rid, &t).await?;
            return Ok(());
        }
        let status = st.status.unwrap_or(Value::Null);
        if status["state"] == "blocked" || st.state == "blocked" {
            let reason = status["msg"]
                .as_str()
                .or(status["title"].as_str())
                .unwrap_or("The agent requires human input.");
            let mut c = s.db()?;
            let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
            if !hooks::active(&tx, rid, s.node())? {
                return Ok(());
            }
            let fingerprint = format!("{:x}", Sha256::digest(status.to_string()));
            let existing = tx
                .query_row(
                    "SELECT fingerprint FROM service_osc WHERE run_id=?",
                    [rid],
                    |r| r.get::<_, String>(0),
                )
                .optional()?;
            if existing.as_deref() != Some(&fingerprint) {
                clear_osc(&tx, rid)?;
                let aid = uid();
                let title = status["title"]
                    .as_str()
                    .filter(|x| !x.is_empty())
                    .unwrap_or("Open the terminal to respond");
                tx.execute("INSERT INTO approvals(approval_id,workspace_id,run_id,node_id,tool_or_cmd,risk_reason,created_at) VALUES(?,?,?,?,?,?,?)",params![aid,s.id(),rid,s.node(),title,reason,now()])?;
                tx.execute(
                    "INSERT INTO service_osc VALUES(?,?,?)",
                    params![rid, aid, fingerprint],
                )?;
                update(
                    &tx,
                    s,
                    rid,
                    "blocked",
                    Some(&t),
                    Some(
                        json!({"approval_id":aid,"kind":"terminal_input","fingerprint":fingerprint,"risk_reason":reason,"tool_or_cmd":title}),
                    ),
                )?;
            }
            tx.commit()?;
        } else {
            let mut db = s.db()?;
            let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
            clear_osc(&tx, rid)?;
            update_live(&tx, s, rid, &t)?;
            tx.commit()?;
        }
    };
    Ok(())
}
fn update_live(db: &Connection, s: &Service, id: &str, terminal: &str) -> Result<()> {
    let blocked = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM approvals WHERE run_id=? AND resolution='pending')",
        [id],
        |r| r.get::<_, bool>(0),
    )?;
    update(
        db,
        s,
        id,
        if blocked { "blocked" } else { "running" },
        Some(terminal),
        None,
    )
}
fn clear_osc(c: &Connection, id: &str) -> Result<()> {
    let aid: Option<String> = c
        .query_row(
            "SELECT approval_id FROM service_osc WHERE run_id=?",
            [id],
            |r| r.get(0),
        )
        .optional()?;
    if let Some(aid) = aid {
        c.execute("UPDATE approvals SET resolution='handled_at_terminal',resolved_at=? WHERE approval_id=? AND resolution='pending'",params![now(),aid])?;
        c.execute("DELETE FROM service_osc WHERE run_id=?", [id])?;
        push(
            c,
            id,
            "terminal_input_closed",
            &json!({"run_id":id,"approval_id":aid}),
        )?;
    }
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
    let changed = c.execute(
        "UPDATE runs SET observed_state=?1,hc_terminal_id=coalesce(?2,hc_terminal_id),updated_at=?3
         WHERE run_id=?4 AND (observed_state!=?1 OR (?2 IS NOT NULL AND hc_terminal_id IS NOT ?2) OR ?5)
         AND observed_state NOT IN ('cancelled','failed','succeeded')
         AND NOT EXISTS(SELECT 1 FROM service_run_abandonments WHERE run_id=runs.run_id)",
        params![next,t,now(),id,approval.is_some()],
    )?;
    if changed == 0 {
        return Ok(());
    }
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
pub(crate) fn finish(
    s: &Service,
    id: &str,
    status: &str,
    summary: &str,
    transcript: &str,
) -> Result<()> {
    let mut db = s.db()?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (desired, observed): (String, String) = tx.query_row(
        "SELECT desired_state,observed_state FROM runs WHERE run_id=?",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    if matches!(observed.as_str(), "succeeded" | "failed" | "cancelled") {
        return Ok(());
    }
    let status = if desired == "cancelled" {
        "cancelled"
    } else {
        status
    };
    let abandoned: Option<String> = tx
        .query_row(
            "SELECT reason FROM service_run_abandonments WHERE run_id=?",
            [id],
            |r| r.get(0),
        )
        .optional()?;
    let path = crate::transcript::finish(&s.ws.root, id, status, transcript)?;
    let native = handoff::outcome(&s.ws.root, id)?;
    let summary = native
        .as_ref()
        .filter(|n| status == "succeeded" && !n.answer.is_empty())
        .map_or(summary, |n| n.answer.as_str());
    let observed = if status == "succeeded" && native.as_ref().is_none_or(|n| n.answer.is_empty()) {
        distillation::screen_finding(transcript, s.distiller.is_some())
    } else {
        None
    };
    let mut receipt = RunReceipt {
        run_id: id.to_owned(),
        final_state: status.to_owned(),
        executive_summary: observed.unwrap_or_else(|| summary.chars().take(400).collect()),
        artifact_refs: vec![],
        s0_transcript_pointer: format!("node:{}/run:{id}/transcript.jsonl", s.node()),
    };
    if let Some(raw) = tx
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
    if let Some(reason) = &abandoned {
        receipt.executive_summary =
            format!("Owner closed an unconfirmed run. Earlier effects are unknown. {reason}");
    }
    let receipt =
        tarla_context::bound_receipt(receipt, s.ws.config.context_policy.run_receipt_max_tokens)?;
    clear_osc(&tx, id)?;
    tx.execute("UPDATE approvals SET resolution='ended',resolved_at=? WHERE run_id=? AND resolution='pending'",params![now(),id])?;
    tx.execute("UPDATE runs SET observed_state=?,receipt_summary=?,s0_log_local_path=?,updated_at=? WHERE run_id=?",params![status,receipt.executive_summary,path.to_string_lossy(),now(),id])?;
    tx.execute(
        "UPDATE service_claims SET phase='finished',updated_at=? WHERE run_id=?",
        params![now(), id],
    )?;
    if status == "failed" && abandoned.is_none() {
        if let Some(until) = native.as_ref().and_then(|out| out.rate_limit) {
            let progress = native
                .as_ref()
                .map(|n| n.answer.as_str())
                .filter(|s| !s.trim().is_empty())
                .unwrap_or(&receipt.executive_summary);
            // A run hook can provide a more specific checkpoint and reset time.
            tx.execute(
                "INSERT OR IGNORE INTO service_rate_limits VALUES(?,?,?)",
                params![id, until, progress.chars().take(380).collect::<String>()],
            )?;
        }
        let limit = tx
            .query_row(
                "SELECT resets_at,checkpoint FROM service_rate_limits WHERE run_id=?",
                [id],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()?;
        if let Some((until, checkpoint)) = limit {
            let slot: String = tx.query_row(
                "SELECT target_slot_id FROM runs WHERE run_id=?",
                [id],
                |r| r.get(0),
            )?;
            slot_reports::cooldown(&tx, &slot, until)?;
            push(
                &tx,
                id,
                "quota_checkpoint",
                &json!({"run_id":id,"resets_at":until,"checkpoint":checkpoint}),
            )?;
        }
    }
    if let Some(n) = native {
        tx.execute(
            "UPDATE runs SET native_session_ref=? WHERE run_id=?",
            params![n.session, id],
        )?;
    }
    push(&tx, id, "run_receipt", &json!(receipt))?;
    tx.commit()?;
    s.distill_wake.notify_one();
    s.account_locks
        .lock()
        .map_err(|_| anyhow!("lock registry unavailable"))?
        .remove(id);
    s.sync_wake.notify_one();
    s.notify.notify_waiters();
    Ok(())
}
