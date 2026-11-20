use super::*;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use tarla_scheduler::{
    select_best_slot, select_handoff, AgentRole, AgentSlotSnapshot, TaskRequirement, TurnBoundary,
};

#[path = "native_resume.rs"]
mod native_resume;
#[path = "transfer.rs"]
mod transfer;
pub use transfer::{accept_quota, commit_continuation, record_limit, worker as transfers};

fn active_turn(db: &Connection, conv: &str) -> Result<Option<String>> {
    Ok(db.query_row("SELECT r.run_id FROM runs r JOIN service_run_paths p USING(run_id) WHERE r.conversation_id=? AND r.observed_state NOT IN ('failed','succeeded','cancelled') LIMIT 1",[conv],|r|r.get(0)).optional()?)
}
fn idle(db: &Connection, conv: &str) -> Result<()> {
    if let Some(run) = active_turn(db, conv)? {
        bail!("Pinned turn {run} is still active or unconfirmed. Wait for its receipt before sending or switching.")
    }
    Ok(())
}
fn current(db: &Connection, conv: &str) -> Result<Option<(String, String, String)>> {
    Ok(db.query_row("SELECT pinned_node_id,pinned_slot_id,pinned_workdir FROM conversations WHERE conversation_id=? AND binding_mode='pinned'",[conv],|r|Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?)
}
fn checkpoint(db: &Connection, conv: &str) -> Result<String> {
    let rows=list(db,"SELECT r.run_id AS run,r.target_node_id AS node,r.target_slot_id AS slot,r.observed_state AS state,substr(json_extract(r.mission_packet_json,'$.objective'),1,240) AS objective,substr(r.receipt_summary,1,300) AS result,substr(r.artifacts_json,1,180) AS artifacts FROM runs r JOIN service_run_paths p USING(run_id) WHERE r.conversation_id=? AND r.observed_state IN ('failed','succeeded','cancelled') ORDER BY r.created_at DESC,r.rowid DESC LIMIT 2",&[&conv])?;
    Ok(if rows.is_empty() {
        String::new()
    } else {
        json!({"completed_turns":rows}).to_string()
    })
}
fn change(db: &Connection, conv: &str, node: &str, slot: &str, path: &str) -> Result<()> {
    idle(db, conv)?;
    let old = current(db, conv)?;
    if old
        .as_ref()
        .is_some_and(|(n, s, p)| n == node && s == slot && p == path)
    {
        return Ok(());
    }
    let brief = checkpoint(db, conv)?;
    db.execute("UPDATE conversations SET binding_mode='pinned',pinned_node_id=?,pinned_slot_id=?,pinned_workdir=? WHERE conversation_id=?",params![node,slot,path,conv])?;
    db.execute(
        "INSERT OR REPLACE INTO service_binding_epochs VALUES(?,?,?)",
        params![conv, uid(), brief],
    )?;
    Ok(())
}
fn path_for(s: &Service, slot: &AgentSlotSnapshot) -> Result<String> {
    if slot.node_id == s.node() {
        return s
            .slots
            .iter()
            .find(|p| p.snapshot.slot_id == slot.slot_id)
            .map(|p| p.workdir.to_string_lossy().into_owned())
            .context("missing local slot");
    }
    Ok(s.db()?
        .query_row(
            "SELECT workdir FROM service_slot_paths WHERE node_id=? AND slot_id=?",
            params![slot.node_id, slot.slot_id],
            |r| r.get(0),
        )
        .optional()?
        .context("peer has not advertised the slot workdir")?)
}
pub fn bind(s: &Service, conv: &str, v: &Value) -> Result<Value> {
    let mut c = s.db()?;
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    control::ensure_conversation(&tx, s, conv)?;
    match string(v, "mode")? {
        "orchestrator" => {
            idle(&tx, conv)?;
            tx.execute("UPDATE conversations SET binding_mode='orchestrator',pinned_node_id=NULL,pinned_slot_id=NULL,pinned_workdir=NULL WHERE conversation_id=?",[conv])?;
            tx.execute(
                "DELETE FROM service_binding_epochs WHERE conversation_id=?",
                [conv],
            )?;
        }
        "pinned" => {
            let node = string(v, "node_id")?;
            let slot = string(v, "slot_id")?;
            let path = string(v, "workdir")?;
            let slots = fleet::snapshots(s)?;
            let selected = slots
                .iter()
                .find(|x| x.node_id == node && x.slot_id == slot)
                .context("unknown slot")?;
            if s.node() == node || path_for(s, selected).is_ok() {
                if path != path_for(s, selected)? {
                    bail!("workdir does not match configured slot")
                }
            }
            if path.trim().is_empty() || path.len() > 4096 {
                bail!("invalid pinned workdir")
            }
            change(&tx, conv, node, slot, path)?;
        }
        _ => bail!("unknown binding mode"),
    }
    tx.commit()?;
    Ok(json!({"mode":v["mode"]}))
}
fn req(s: &Service) -> TaskRequirement {
    TaskRequirement {
        workspace_id: s.id().into(),
        role: AgentRole::Implementer,
        required_caps: vec![],
        exclude_model_family: None,
    }
}

fn matches_target(slot: &AgentSlotSnapshot, target: &str) -> bool {
    target.is_empty()
        || slot.slot_id == target
        || slot.driver == target
        || format!("{}/{}", slot.node_id, slot.slot_id) == target
}
fn destination<'a>(
    require: &TaskRequirement,
    slots: &'a [AgentSlotSnapshot],
    node: &str,
    slot: &str,
    target: &str,
    allow_current: bool,
) -> Option<&'a AgentSlotSnapshot> {
    let old = slots
        .iter()
        .find(|s| s.node_id == node && s.slot_id == slot);
    let choices: Vec<_> = slots
        .iter()
        .filter(|s| matches_target(s, target))
        .cloned()
        .collect();
    let selected = if let Some(old) = old {
        select_handoff(require, old, &choices, TurnBoundary::Checkpointed, now())
            .map(|h| h.slot)
            .or_else(|| {
                (allow_current && !target.is_empty() && matches_target(old, target))
                    .then(|| select_best_slot(require, std::slice::from_ref(old), now()))
                    .flatten()
            })
    } else {
        select_best_slot(require, &choices, now())
    }?;
    slots
        .iter()
        .find(|s| s.node_id == selected.node_id && s.slot_id == selected.slot_id)
}

/// Commits switches at an acknowledged turn boundary. Retrying a source message
/// observes the same switch even if its reply was interrupted by a restart.
fn switch(
    s: &Service,
    conv: &str,
    target: &str,
    source: &str,
    followup: Option<&str>,
) -> Result<String> {
    let mut c = s.db()?;
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if let Some((saved_conv, ans)) = tx
        .query_row(
            "SELECT conversation_id,result FROM service_handoffs WHERE source=?",
            [source],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
        )
        .optional()?
    {
        if saved_conv != conv {
            bail!("handoff source belongs to another conversation")
        }
        return Ok(ans);
    }
    let (node, slot, _) =
        current(&tx, conv)?.context("pin the conversation before switching slots")?;
    let ans = if let Some(run) = active_turn(&tx, conv)? {
        if !target.is_empty()
            && !fleet::snapshots(s)?
                .iter()
                .any(|slot| matches_target(slot, target))
        {
            bail!("unknown destination slot");
        }
        let changed = tx.execute("INSERT INTO service_turn_transfers(run_id,target,prompt,manual_source) VALUES(?,?,?,?)
                 ON CONFLICT(run_id) DO UPDATE SET target=excluded.target,prompt=excluded.prompt,manual_source=excluded.manual_source
                 WHERE state='pending' AND (manual_source IS NULL OR manual_source=excluded.manual_source)",
            params![run,target,followup,source])?;
        if changed == 0 {
            bail!("a different switch is already pending for this turn")
        }
        json!({"mode":"pinned", "handoff":"pending", "after_run_id":run, "target":target})
            .to_string()
    } else {
        let slots = fleet::snapshots(s)?;
        let selected = destination(&req(s), &slots, &node, &slot, target, true)
            .context("no eligible destination slot")?;
        change(
            &tx,
            conv,
            &selected.node_id,
            &selected.slot_id,
            &path_for(s, selected)?,
        )?;
        json!({"mode":"pinned","node_id":selected.node_id,"slot_id":selected.slot_id}).to_string()
    };
    tx.execute(
        "INSERT INTO service_handoffs VALUES(?,?,?)",
        params![source, conv, ans],
    )?;
    tx.commit()?;
    Ok(ans)
}
pub fn pinned(
    s: &Service,
    conv: &str,
    text: &str,
    source: &str,
    run_id: String,
    skill: Option<&skills::Selection>,
) -> Result<Option<String>> {
    let db = s.db()?;
    let command = text.trim();
    let mut prompt = skill.map(|v| v.task.as_str()).unwrap_or(command);
    let binding = current(&db, conv)?;
    if command == "/switch" || command.starts_with("/switch ") {
        return Ok(Some(switch(
            s,
            conv,
            command.strip_prefix("/switch").unwrap().trim(),
            source,
            None,
        )?));
    }
    let Some((node, slot, _)) = binding else {
        return Ok(None);
    };
    if let Some(mention) = command.strip_prefix('@') {
        let (target, rest) = mention
            .split_once(char::is_whitespace)
            .unwrap_or((mention, ""));
        let followup = (!rest.trim().is_empty()).then_some(rest.trim());
        let reply = switch(s, conv, target, source, followup)?;
        if followup.is_none() || serde_json::from_str::<Value>(&reply)?["handoff"] == "pending" {
            return Ok(Some(reply));
        }
        prompt = rest.trim()
    } else {
        let slots = fleet::snapshots(s)?;
        let old = slots
            .iter()
            .find(|x| x.node_id == node && x.slot_id == slot);
        if old.is_none_or(|x| select_best_slot(&req(s), std::slice::from_ref(x), now()).is_none()) {
            if active_turn(&db, conv)?.is_none() {
                switch(s, conv, "", source, None)?;
            }
        }
    }
    let (node, slot, path) = current(&db, conv)?.context("binding changed")?;
    let (epoch, brief) = epoch(&db, conv)?;
    let mut context = vec![];
    if !brief.is_empty() {
        context.push(brief)
    }
    // Generic drivers can continue from a bounded receipt checkpoint.
    let generic = fleet::snapshots(s)?
        .iter()
        .find(|x| x.node_id == node && x.slot_id == slot)
        .is_some_and(|x| !native(&x.driver));
    if generic {
        let prior = checkpoint(&db, conv)?;
        if !prior.is_empty() && !context.contains(&prior) {
            context.push(prior)
        }
    }
    Ok(Some(fleet::dispatch_selected(s,conv,&json!({"goal":prompt,"node_id":node,"slot_id":slot,"pinned_workdir":path,"pin_epoch":epoch,"distilled_context":context}),Some(run_id),skill,Some(fleet::Acknowledgement::Command(source)))?.to_string()))
}
fn epoch(db: &Connection, conv: &str) -> Result<(String, String)> {
    Ok(db
        .query_row(
            "SELECT epoch,checkpoint FROM service_binding_epochs WHERE conversation_id=?",
            [conv],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?
        .unwrap_or_else(|| ("legacy".into(), String::new())))
}
pub fn register(
    db: &Connection,
    conv: &str,
    run: &str,
    node: &str,
    slot: &str,
    path: &str,
    arg: &Value,
) -> Result<()> {
    if db.query_row("SELECT EXISTS(SELECT 1 FROM service_turn_transfers t JOIN runs r USING(run_id)
        WHERE r.conversation_id=? AND t.state='pending' AND t.run_id IN (SELECT run_id FROM service_run_paths))",[conv],|r|r.get::<_,bool>(0))? {
        bail!("a turn-boundary handoff is pending for this conversation")
    }
    idle(db, conv)?;
    let current = current(db, conv)?.context("conversation is no longer pinned")?;
    let (ep, _) = epoch(db, conv)?;
    if current != (node.into(), slot.into(), path.into()) || arg["pin_epoch"] != ep {
        bail!("pinned binding changed during dispatch")
    }
    db.execute(
        "INSERT INTO service_run_paths VALUES(?,?)",
        params![run, path],
    )?;
    db.execute(
        "INSERT INTO service_pin_epochs VALUES(?,?)",
        params![run, ep],
    )?;
    Ok(())
}
fn native(driver: &str) -> bool {
    matches!(driver, "codex" | "claude" | "claude-code" | "claude_code")
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Launch {
    argv: Vec<String>,
    driver: String,
    expected: Option<String>,
    #[serde(default)]
    workdir: Option<PathBuf>,
}
fn metadata_path(root: &Path, id: &str) -> PathBuf {
    root.join("runtime/pinned").join(format!("{id}.result"))
}
fn argv_for(slot: &fleet::Slot, prompt: String, session: Option<&str>) -> Result<Vec<String>> {
    let mut args = slot.argv.clone();
    for opt in args.iter().skip(1) {
        if (slot.snapshot.driver != "codex" && opt == "-c")
            || matches!(
                opt.split('=').next().unwrap_or(""),
                "--ephemeral"
                    | "--last"
                    | "--continue"
                    | "--resume"
                    | "-r"
                    | "--session-id"
                    | "--no-session-persistence"
                    | "--fork-session"
                    | "--output-format"
                    | "--json"
                    | "resume"
                    | "fork"
            )
        {
            bail!("pinned driver manages its own session and output options")
        }
    }
    if slot.snapshot.driver == "codex" {
        if args.get(1).map(String::as_str) != Some("exec") {
            bail!("pinned Codex requires argv [codex, exec, ...]")
        }
        if let Some(id) = session {
            args.extend(["resume".into(), "--json".into(), id.into()])
        } else {
            args.push("--json".into())
        }
    } else {
        if !args.iter().any(|a| a == "-p" || a == "--print") {
            args.push("--print".into())
        }
        args.extend(["--output-format".into(), "json".into()]);
        if let Some(id) = session {
            args.extend(["--resume".into(), id.into()])
        }
    }
    args.push(prompt);
    Ok(args)
}
pub fn launch(s: &Service, job: &Value, slot: &fleet::Slot, prompt: String) -> Result<Vec<String>> {
    let pinned = job["pinned_workdir"].is_string();
    if !native(&slot.snapshot.driver) || (!pinned && argv_for(slot, String::new(), None).is_err()) {
        let mut args = slot.argv.clone();
        args.push(prompt);
        return Ok(args);
    }
    let id = string(job, "run_id")?;
    let conv = string(job, "conversation_id")?;
    let db = s.db()?;
    let ep = db
        .query_row(
            "SELECT epoch FROM service_pin_epochs WHERE run_id=?",
            [id],
            |r| r.get::<_, String>(0),
        )
        .optional()?
        .unwrap_or_else(|| "legacy".into());
    let prior: Option<String> = if pinned {
        db.query_row("SELECT r.native_session_ref FROM runs r JOIN service_run_paths p USING(run_id) LEFT JOIN service_pin_epochs e USING(run_id) WHERE r.conversation_id=? AND r.target_node_id=? AND r.target_slot_id=? AND p.workdir=? AND coalesce(e.epoch,'legacy')=? AND r.native_session_ref IS NOT NULL ORDER BY r.rowid DESC LIMIT 1",params![conv,s.node(),slot.snapshot.slot_id,job["pinned_workdir"].as_str(),ep],|r|r.get(0)).optional()?
    } else {
        None
    };
    let session = match prior {
        Some(id) => Some(id),
        None => native_resume::session(s, job, slot).unwrap_or_else(|e| {
            eprintln!("native session unavailable; using bounded checkpoint: {e}");
            None
        }),
    };
    let argv = argv_for(slot, prompt, session.as_deref())?;
    let result = metadata_path(&s.ws.root, id);
    std::fs::create_dir_all(result.parent().unwrap())?;
    let launch = result.with_extension("launch");
    std::fs::write(
        &launch,
        serde_json::to_vec(&Launch {
            argv,
            driver: slot.snapshot.driver.clone(),
            expected: session,
            workdir: Some(slot.workdir.clone()),
        })?,
    )?;
    Ok(vec![
        std::env::current_exe()?
            .into_os_string()
            .into_string()
            .map_err(|_| anyhow!("non-UTF8 daemon path"))?,
        "--pinned-turn-helper".into(),
        launch.to_str().context("non-UTF8 runtime path")?.into(),
    ])
}
#[derive(Default, serde::Serialize, serde::Deserialize)]
pub struct Outcome {
    pub session: Option<String>,
    pub answer: String,
    #[serde(default)]
    pub rate_limit: Option<i64>,
    #[serde(default)]
    failed: bool,
}
fn extract(driver: &str, v: &Value, out: &mut Outcome) {
    let failure = matches!(v["type"].as_str(), Some("error" | "turn.failed"))
        || (v["type"] == "result" && v["is_error"] == true)
        || v["type"] == "rate_limit_event";
    let error = &v["error"];
    let limited = [
        error.get("code"),
        error.get("type"),
        error.get("codexErrorInfo"),
        v.get("codexErrorInfo"),
        v.get("error"),
        v.get("code"),
    ]
    .into_iter()
    .flatten()
    .any(|value| {
        matches!(
            value.as_str(),
            Some(
                "rate_limit_error"
                    | "rate_limit_exceeded"
                    | "usage_limit_reached"
                    | "usageLimitExceeded"
                    | "429"
            )
        ) || value.as_i64() == Some(429)
    }) || error["status"].as_i64() == Some(429)
        || v["status"].as_i64() == Some(429)
        || (v["type"] == "rate_limit_event" && v["rate_limit_info"]["status"] == "rejected");
    // Some Codex releases expose only a provider message on their JSONL error frame.
    // Never infer a control signal from assistant text, a tool result or terminal output.
    let provider_limited = error["message"]
        .as_str()
        .or_else(|| v["message"].as_str())
        .is_some_and(|message| {
            let brief = message
                .chars()
                .take(256)
                .collect::<String>()
                .to_ascii_lowercase();
            brief.starts_with("you've hit your usage limit")
                || brief.starts_with("rate limit reached")
                || brief.starts_with("http 429")
                || brief.contains("status 429 too many requests")
                || brief.contains("status: 429 too many requests")
        });
    if v["type"] == "turn.completed" || (v["type"] == "result" && v["is_error"] == false) {
        out.rate_limit = None;
        out.failed = false;
    }
    if v["type"] == "turn.failed" || (v["type"] == "result" && v["is_error"] == true) {
        out.failed = true;
    }
    if failure && (limited || provider_limited) {
        out.rate_limit = Some(
            error["resets_at"]
                .as_i64()
                .or(v["rate_limit_info"]["resetsAt"].as_i64())
                .unwrap_or_else(|| now() + 300),
        );
    } else if v["type"] == "turn.failed" {
        out.rate_limit = None;
    }
    let session = if driver == "codex" {
        if v["type"] == "item.completed" && v["item"]["type"] == "agent_message" {
            if let Some(s) = v["item"]["text"].as_str() {
                out.answer = s.chars().take(1200).collect()
            }
        }
        (v["type"] == "thread.started")
            .then(|| v["thread_id"].as_str())
            .flatten()
    } else {
        if v["type"] == "result" {
            if let Some(s) = v["result"].as_str().filter(|_| v["is_error"] != true) {
                out.answer = s.chars().take(1200).collect()
            }
            v["session_id"].as_str()
        } else {
            None
        }
    };
    if let Some(id) = session.and_then(|s| Uuid::parse_str(s).ok()) {
        out.session = Some(id.to_string())
    }
}
/// Capture the driver's structured stdout before a PTY can wrap it or evict its first
/// line. The child inherits the slot's workdir, isolated credentials and process group.
pub fn helper(path: &Path) -> Result<i32> {
    use std::io::{BufRead, BufReader, Read, Write};
    use std::process::{Command, Stdio};
    let plan: Launch = serde_json::from_slice(&std::fs::read(path)?)?;
    let mut p = Command::new(plan.argv.first().context("empty native command")?)
        .args(&plan.argv[1..])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .spawn()?;
    let mut reader = BufReader::new(p.stdout.take().context("no driver stdout")?);
    let mut out = Outcome::default();
    let mut buf = Vec::new();
    let mut continued = false;
    loop {
        buf.clear();
        let n = reader
            .by_ref()
            .take(4 * 1024 * 1024)
            .read_until(b'\n', &mut buf)?;
        if n == 0 {
            break;
        }
        if !continued {
            if let Ok(v) = serde_json::from_slice::<Value>(&buf) {
                extract(&plan.driver, &v, &mut out);
            }
        }
        continued = buf.len() == 4 * 1024 * 1024 && !buf.ends_with(b"\n");
        std::io::stdout().write_all(&buf)?;
    }
    let exit = p.wait()?.code().unwrap_or(1);
    let code = if exit == 0 && out.failed { 1 } else { exit };
    if plan.expected.is_some() && out.session != plan.expected {
        out.session = None;
    }
    if out.session.is_some() || (code != 0 && out.rate_limit.is_some()) {
        let tmp = path.with_extension("result.tmp");
        std::fs::write(&tmp, serde_json::to_vec(&out)?)?;
        std::fs::rename(tmp, path.with_extension("result"))?;
    } else if code == 0 {
        bail!("driver did not confirm the expected native session id")
    }
    Ok(code)
}
pub fn outcome(root: &Path, id: &str) -> Result<Option<Outcome>> {
    match std::fs::read(metadata_path(root, id)) {
        Ok(raw) => Ok(Some(serde_json::from_slice(&raw)?)),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub fn peer_paths(
    c: &Connection,
    node: &str,
    slots: &[AgentSlotSnapshot],
    v: &Value,
) -> Result<()> {
    let mut ids = std::collections::HashSet::new();
    for slot in slots {
        tarla_workspace::validate_name(&slot.slot_id)?;
        if !ids.insert(slot.slot_id.as_str())
            || !(0.0..=100.0).contains(&slot.window_used_pct)
            || !(0.0..=100.0).contains(&slot.weekly_used_pct)
            || matches!(&slot.health,tarla_scheduler::SlotHealth::AuthExpired{message} if message.len()>1024)
        {
            bail!("invalid peer slot telemetry")
        }
    }
    let Some(paths) = v.get("slot_paths") else {
        return Ok(());
    };
    let paths = paths.as_object().context("invalid slot workdirs")?;
    c.execute("DELETE FROM service_slot_paths WHERE node_id=?", [node])?;
    for (slot, path) in paths {
        let path = path
            .as_str()
            .filter(|p| !p.is_empty() && p.len() <= 4096)
            .context("invalid advertised workdir")?;
        if !ids.contains(slot.as_str()) {
            bail!("unknown slot workdir")
        }
        c.execute(
            "INSERT INTO service_slot_paths VALUES(?,?,?)",
            params![node, slot, path],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn slot(driver: &str) -> fleet::Slot {
        let snapshot:AgentSlotSnapshot=serde_json::from_value(json!({"workspace_id":"work","node_id":"pc","slot_id":"s","driver":driver,"model_family":driver,
        "supported_roles":["implementer"],"billing_tier":"prepaid_subscription","window_used_pct":2,"health":"healthy"})).unwrap();
        fleet::Slot {
            snapshot,
            workdir: ".".into(),
            argv: if driver == "codex" {
                vec!["codex".into(), "exec".into()]
            } else {
                vec!["claude".into()]
            },
        }
    }
    #[test]
    fn only_provider_failure_frames_classify_quota_exhaustion() {
        let mut out = Outcome::default();
        let message = "Exceeded retry limit, last status: 429 Too Many Requests";
        extract(
            "codex",
            &json!({"type":"item.completed","item":{"type":"agent_message","text":message}}),
            &mut out,
        );
        assert!(out.rate_limit.is_none());
        extract(
            "codex",
            &json!({"type":"turn.failed","error":{"message":message}}),
            &mut out,
        );
        assert!(out.rate_limit.is_some());
        extract(
            "codex",
            &json!({"type":"turn.failed","error":{"message":"permission denied"}}),
            &mut out,
        );
        assert!(out.rate_limit.is_none());
        extract(
            "claude",
            &json!({"type":"rate_limit_event","rate_limit_info":{"status":"rejected","resetsAt":12345678}}),
            &mut out,
        );
        assert_eq!(out.rate_limit, Some(12345678));
        extract(
            "claude",
            &json!({"type":"result","is_error":false,"result":"completed after a retry"}),
            &mut out,
        );
        assert!(out.rate_limit.is_none() && !out.failed);
    }
    #[test]
    fn resume_exact_native_id_on_next_turn() {
        let id = Uuid::now_v7().to_string();
        for driver in ["claude", "codex"] {
            let s = slot(driver);
            let first = argv_for(&s, "first".into(), None).unwrap();
            let mut out = Outcome::default();
            if driver == "codex" {
                assert_eq!(first, ["codex", "exec", "--json", "first"]);
                extract(
                    driver,
                    &json!({"type":"thread.started","thread_id":id}),
                    &mut out,
                );
                extract(
                    driver,
                    &json!({"type":"item.completed","item":{"type":"agent_message","text":"done"}}),
                    &mut out,
                );
            } else {
                assert_eq!(
                    first,
                    ["claude", "--print", "--output-format", "json", "first"]
                );
                extract(
                    driver,
                    &json!({"type":"result","result":"done","session_id":id}),
                    &mut out,
                );
            }
            assert_eq!(out.answer, "done");
            assert_eq!(out.session.as_deref(), Some(id.as_str()));
            let resumed = argv_for(&s, "next".into(), out.session.as_deref()).unwrap();
            if driver == "codex" {
                assert_eq!(resumed, ["codex", "exec", "resume", "--json", &id, "next"])
            } else {
                assert_eq!(
                    resumed,
                    [
                        "claude",
                        "--print",
                        "--output-format",
                        "json",
                        "--resume",
                        &id,
                        "next"
                    ]
                )
            }
        }
    }
    #[test]
    fn unfinished_turn_fences_changes_and_old_generation_cannot_dispatch() {
        let c = tarla_memory::open(Path::new(":memory:"), "w").unwrap();
        c.execute_batch(MIGRATIONS).unwrap();
        c.execute(
            "INSERT INTO conversations(conversation_id,workspace_id,created_at,updated_at) VALUES('c','w',1,1)",
            [],
        )
        .unwrap();
        change(&c, "c", "node", "a", "/work").unwrap();
        let (old, _) = epoch(&c, "c").unwrap();
        c.execute(
            "INSERT INTO runs(run_id,workspace_id,conversation_id,supervisor_term,target_node_id,
                target_slot_id,desired_state,observed_state,created_at,updated_at,mission_packet_json,
                receipt_summary,artifacts_json)
             VALUES('r','w','c',1,'node','a','running','unconfirmed',1,1,'{}','ok','[]')",
            [],
        )
        .unwrap();
        register(
            &c,
            "c",
            "r",
            "node",
            "a",
            "/work",
            &json!({"pin_epoch":old}),
        )
        .unwrap();
        assert!(change(&c, "c", "node", "b", "/different").is_err());
        c.execute("UPDATE runs SET observed_state='succeeded'", [])
            .unwrap();
        change(&c, "c", "node", "b", "/different").unwrap();
        let (new, check) = epoch(&c, "c").unwrap();
        assert_ne!(old, new);
        assert!(check.contains("ok"));
        assert!(register(
            &c,
            "c",
            "next",
            "node",
            "b",
            "/different",
            &json!({"pin_epoch":old})
        )
        .is_err());
    }
}
