use super::*;
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use tarla_scheduler::{
    select_best_slot, select_handoff, AgentRole, AgentSlotSnapshot, TaskRequirement, TurnBoundary,
};

fn idle(db: &Connection, conv: &str) -> Result<()> {
    let pending:Option<String>=db.query_row("SELECT r.run_id FROM runs r JOIN service_run_paths p USING(run_id) WHERE r.conversation_id=? AND r.observed_state NOT IN ('failed','succeeded','cancelled') LIMIT 1",[conv],|r|r.get(0)).optional()?;
    if let Some(run) = pending {
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

/// Commits switches at an acknowledged turn boundary. Retrying a source message
/// observes the same switch even if its reply was interrupted by a restart.
fn switch(s: &Service, conv: &str, target: &str, source: &str) -> Result<String> {
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
    idle(&tx, conv)?;
    let (node, slot, _) =
        current(&tx, conv)?.context("pin the conversation before switching slots")?;
    let slots = fleet::snapshots(s)?;
    let old = slots
        .iter()
        .find(|x| x.node_id == node && x.slot_id == slot)
        .context("previous slot no longer available; use explicit binding")?;
    let matches = |s: &AgentSlotSnapshot| {
        target.is_empty()
            || s.slot_id == target
            || s.driver == target
            || format!("{}/{}", s.node_id, s.slot_id) == target
    };
    let candidates = slots
        .iter()
        .filter(|x| matches(x))
        .cloned()
        .collect::<Vec<_>>();
    let choice = select_handoff(&req(s), old, &candidates, TurnBoundary::Checkpointed, now())
        .map(|x| x.slot);
    let selected = match choice {
        Some(slot) => slot,
        None if !target.is_empty() && matches(old) => old,
        None => bail!("no eligible destination slot"),
    };
    change(
        &tx,
        conv,
        &selected.node_id,
        &selected.slot_id,
        &path_for(s, selected)?,
    )?;
    let ans =
        json!({"mode":"pinned","node_id":selected.node_id,"slot_id":selected.slot_id}).to_string();
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
) -> Result<Option<String>> {
    let db = s.db()?;
    let command = text.trim();
    let mut prompt = command;
    let replay=list(&db,"SELECT run_id,target_node_id,target_slot_id FROM runs WHERE run_id=? AND conversation_id=?",&[&run_id,&conv])?;
    if let Some(row) = replay.first() {
        return Ok(Some(row.to_string()));
    }
    let binding = current(&db, conv)?;
    if command == "/switch" || command.starts_with("/switch ") {
        return Ok(Some(switch(
            s,
            conv,
            command.strip_prefix("/switch").unwrap().trim(),
            source,
        )?));
    }
    let Some((node, slot, _)) = binding else {
        return Ok(None);
    };
    if let Some(mention) = command.strip_prefix('@') {
        let (target, rest) = mention
            .split_once(char::is_whitespace)
            .unwrap_or((mention, ""));
        let reply = switch(s, conv, target, source)?;
        if rest.trim().is_empty() {
            return Ok(Some(reply));
        }
        prompt = rest.trim()
    } else {
        let slots = fleet::snapshots(s)?;
        let old = slots
            .iter()
            .find(|x| x.node_id == node && x.slot_id == slot);
        if old.is_some_and(|x| select_best_slot(&req(s), std::slice::from_ref(x), now()).is_none())
        {
            switch(s, conv, "", source)?;
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
    Ok(Some(fleet::dispatch(s,conv,&json!({"goal":prompt,"node_id":node,"slot_id":slot,"pinned_workdir":path,"pin_epoch":epoch,"distilled_context":context}),Some(run_id))?.to_string()))
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
}
fn metadata_path(root: &Path, id: &str) -> PathBuf {
    root.join("runtime/pinned").join(format!("{id}.result"))
}
fn argv_for(slot: &fleet::Slot, prompt: String, session: Option<&str>) -> Result<Vec<String>> {
    let mut args = slot.argv.clone();
    for opt in args.iter().skip(1) {
        if matches!(
            opt.split('=').next().unwrap_or(""),
            "--ephemeral"
                | "--last"
                | "--continue"
                | "-c-session"
                | "--resume"
                | "-r"
                | "--session-id"
                | "--no-session-persistence"
                | "--fork-session"
                | "--output-format"
                | "--json"
                | "resume"
                | "fork"
        ) {
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
    if job["pinned_workdir"].is_null() || !native(&slot.snapshot.driver) {
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
    let session:Option<String>=db.query_row("SELECT r.native_session_ref FROM runs r JOIN service_run_paths p USING(run_id) LEFT JOIN service_pin_epochs e USING(run_id) WHERE r.conversation_id=? AND r.target_node_id=? AND r.target_slot_id=? AND p.workdir=? AND coalesce(e.epoch,'legacy')=? AND r.native_session_ref IS NOT NULL ORDER BY r.rowid DESC LIMIT 1",params![conv,s.node(),slot.snapshot.slot_id,job["pinned_workdir"].as_str(),ep],|r|r.get(0)).optional()?;
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
}
fn extract(driver: &str, v: &Value, out: &mut Outcome) {
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
            if let Some(s) = v["result"].as_str() {
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
    let code = p.wait()?.code().unwrap_or(1);
    if out.session.is_some() && (plan.expected.is_none() || out.session == plan.expected) {
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
