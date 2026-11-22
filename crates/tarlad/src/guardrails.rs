//! Five Personal Agent safety guardrails (Spec §6):
//! 1. `!acted` vs `acted` failover gate on 429 / crash
//! 2. `Check-Before-Done` heterogeneous verification loop (up to 2 rework retries)
//! 3. `8,000`-character result truncation gate with `runtime/spills/<event_id>.log` paging
use crate::checkpoints::{diff_worktree_paths, WorktreeSnapshot};
use anyhow::{bail, ensure, Context, Result};
use rusqlite::{params, Connection, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use tarla_botflow::{BotflowSpec, ResolvedTarget};
use uuid::Uuid;

pub const DEFAULT_RESULT_GATE_CHARS: usize = 8000;
const GATE_HEAD_CHARS: usize = 2000;
const GATE_TAIL_CHARS: usize = 2000;
pub const MAX_VERIFY_RETRIES: u8 = 2;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct GatedOutput {
    pub truncated: bool,
    pub total_chars: usize,
    pub spill_id: Option<String>,
    pub spill_rel_path: Option<String>,
    pub text: String,
}

fn sanitize_spill_id(raw: &str) -> Result<String> {
    let cleaned: String = raw
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .take(120)
        .collect();
    ensure!(
        !cleaned.is_empty() && cleaned != "." && cleaned != "..",
        "invalid spill identifier"
    );
    Ok(cleaned)
}

fn ensure_spills_dir(runtime_dir: &Path) -> Result<PathBuf> {
    let spills = runtime_dir.join("spills");
    fs::create_dir_all(&spills)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&spills, fs::Permissions::from_mode(0o700))?;
    }
    Ok(spills)
}

/// Gate single-turn outputs, `hc read` captures, and `EventEnvelope` text payloads at `max_chars` (default 8,000).
/// Oversized outputs are persisted in full to `runtime/spills/<event_id>.log` and replaced with
/// `[first 2,000 chars + spill pointer + last 2,000 chars]`.
pub fn gate_output(
    runtime_dir: &Path,
    event_id: &str,
    text: &str,
    max_chars: usize,
) -> Result<GatedOutput> {
    let limit = max_chars.max(GATE_HEAD_CHARS + GATE_TAIL_CHARS + 128);
    let chars: Vec<char> = text.chars().collect();
    let total_chars = chars.len();
    if total_chars <= limit {
        return Ok(GatedOutput {
            truncated: false,
            total_chars,
            spill_id: None,
            spill_rel_path: None,
            text: text.to_owned(),
        });
    }

    let spill_id = sanitize_spill_id(event_id)?;
    let spills_dir = ensure_spills_dir(runtime_dir)?;
    let file_path = spills_dir.join(format!("{spill_id}.log"));
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create(true).truncate(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut f = opts.open(&file_path)?;
    f.write_all(text.as_bytes())?;
    f.flush()?;
    f.sync_all()?;

    let head: String = chars[..GATE_HEAD_CHARS].iter().collect();
    let tail: String = chars[total_chars - GATE_TAIL_CHARS..].iter().collect();
    let omitted = total_chars - GATE_HEAD_CHARS - GATE_TAIL_CHARS;
    let rel = format!("runtime/spills/{spill_id}.log");
    let gated = format!(
        "{head}\n\n[...truncated {omitted} chars (total {total_chars}); full output saved at {rel} — query via GET /v2/spills/{spill_id}?offset_line=1&limit=100&query=<keyword>...]\n\n{tail}"
    );
    Ok(GatedOutput {
        truncated: true,
        total_chars,
        spill_id: Some(spill_id),
        spill_rel_path: Some(rel),
        text: gated,
    })
}

/// Gate arbitrary JSON payload values recursively/as-serialized when exceeding `max_chars`.
pub fn gate_json_payload(
    runtime_dir: &Path,
    event_id: &str,
    payload: Value,
    max_chars: usize,
) -> Result<Value> {
    let serialized = serde_json::to_string(&payload)?;
    if serialized.chars().count() <= max_chars {
        return Ok(payload);
    }
    let gated = gate_output(runtime_dir, event_id, &serialized, max_chars)?;
    Ok(json!({
        "truncated": true,
        "total_chars": gated.total_chars,
        "spill_id": gated.spill_id,
        "spill_path": gated.spill_rel_path,
        "preview": gated.text,
    }))
}

pub fn read_spill_page(
    runtime_dir: &Path,
    spill_id: &str,
    offset_line: usize,
    limit: usize,
    query: &str,
) -> Result<Value> {
    let clean = sanitize_spill_id(spill_id)?;
    ensure!(clean == spill_id, "invalid spill identifier `{spill_id}`");
    let path = runtime_dir.join("spills").join(format!("{clean}.log"));
    if !path.is_file() {
        bail!("unknown spill `{clean}`");
    }
    let raw = fs::read_to_string(&path)?;
    let all_lines: Vec<(usize, &str)> = raw
        .lines()
        .enumerate()
        .map(|(idx, line)| (idx + 1, line))
        .collect();
    let total_lines = all_lines.len();
    let q_lower = query.trim().to_lowercase();
    let start = offset_line.max(1);
    let cap = limit.clamp(1, 500);
    let matched: Vec<Value> = all_lines
        .into_iter()
        .filter(|(line_no, line)| {
            *line_no >= start && (q_lower.is_empty() || line.to_lowercase().contains(&q_lower))
        })
        .take(cap)
        .map(|(line_no, line)| json!({"line": line_no, "text": line}))
        .collect();
    Ok(json!({
        "spill_id": clean,
        "total_lines": total_lines,
        "offset_line": start,
        "matches": matched,
    }))
}

#[derive(Debug, Clone)]
pub enum FailoverDecision {
    /// `!acted`: worktree untouched and zero external `idempotent_ops`; safe to silently replay on `next_target`.
    SilentReplay { next_target: ResolvedTarget },
    /// `acted`: side effects already occurred; pause in `blocked_approval` and notify mobile Master Control.
    BlockedApproval {
        approval_id: String,
        changed_paths: Vec<String>,
        external_ops: i64,
    },
    /// `!acted`, but all candidate `targets` for this Bot are exhausted.
    NoFallbackAvailable,
}

/// Evaluate Guardrail #1 (`!acted` vs `acted`) upon 429 rate-limit or unexpected process crash.
#[allow(clippy::too_many_arguments)]
pub fn evaluate_failover_gate(
    db: &mut Connection,
    workspace_id: &str,
    run_id: &str,
    node_id: &str,
    bot_id: &str,
    workdir: &Path,
    pre_snapshot: &WorktreeSnapshot,
    spec: &BotflowSpec,
    upstream_command: Option<&str>,
    excluded_targets: &BTreeSet<String>,
    failure_reason: &str,
    now_secs: i64,
) -> Result<FailoverDecision> {
    let changed_paths = diff_worktree_paths(workdir, pre_snapshot)?;
    let external_ops: i64 = db.query_row(
        "SELECT COUNT(*) FROM idempotent_ops WHERE run_id = ?1",
        [run_id],
        |r| r.get(0),
    )?;

    if changed_paths.is_empty() && external_ops == 0 {
        if let Some(next_target) =
            spec.select_target_for_bot(bot_id, upstream_command, excluded_targets)?
        {
            return Ok(FailoverDecision::SilentReplay { next_target });
        }
        return Ok(FailoverDecision::NoFallbackAvailable);
    }

    let approval_id = Uuid::now_v7().to_string();
    let summary = format!(
        "Run interrupted after side effects (`acted`: {} modified file(s): [{}], {} external op(s)): {}",
        changed_paths.len(),
        changed_paths.iter().take(8).cloned().collect::<Vec<_>>().join(", "),
        external_ops,
        failure_reason
    );
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute(
        "UPDATE runs SET observed_state = 'blocked_approval', updated_at = ?1 WHERE run_id = ?2",
        params![now_secs, run_id],
    )?;
    tx.execute(
        "INSERT INTO approvals(approval_id, workspace_id, run_id, node_id, tool_or_cmd, risk_reason, resolution, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'pending', ?7)",
        params![
            approval_id,
            workspace_id,
            run_id,
            node_id,
            format!("failover:{bot_id}"),
            summary,
            now_secs
        ],
    )?;
    tx.commit()?;

    Ok(FailoverDecision::BlockedApproval {
        approval_id,
        changed_paths,
        external_ops,
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VerifierVerdict {
    pub passed: bool,
    pub feedback: String,
}

/// Execute Guardrail #2 (`Check-Before-Done` heterogeneous read-only verifier).
pub fn run_check_before_done(
    verifier: &ResolvedTarget,
    run_id: &str,
    objective: &str,
    acceptance_criteria: &[String],
    changed_files: &[String],
    runtime_dir: &Path,
) -> Result<VerifierVerdict> {
    let payload = json!({
        "mode": "verify_before_done",
        "run_id": run_id,
        "objective": objective,
        "acceptance_criteria": acceptance_criteria,
        "changed_files": changed_files,
    });
    let payload_str = serde_json::to_string(&payload)?;
    let mut cmd = Command::new(&verifier.command);
    cmd.current_dir(&verifier.workdir)
        .arg(&payload_str)
        .env("TARLA_VERIFY_PACKET", &payload_str)
        .env_remove("TARLA_TOKEN")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    for (k, _) in std::env::vars_os() {
        if let Some(s) = k.to_str() {
            if s.starts_with("TARLA_SECRET_")
                || s.starts_with("TARLA_PAIR_")
                || s.starts_with("TARLA_BROKER_")
            {
                cmd.env_remove(s);
            }
        }
    }
    let output = cmd
        .output()
        .with_context(|| format!("failed to execute verifier `{}`", verifier.command))?;
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();

    if let Ok(parsed) = serde_json::from_str::<Value>(&stdout) {
        if let Some(passed) = parsed["passed"].as_bool() {
            let fb = parsed["feedback"].as_str().unwrap_or(if passed {
                "verified"
            } else {
                "verification failed"
            });
            let gated = gate_output(
                runtime_dir,
                &format!("verify-{run_id}"),
                fb,
                DEFAULT_RESULT_GATE_CHARS,
            )?;
            return Ok(VerifierVerdict {
                passed,
                feedback: gated.text,
            });
        }
    }

    let combined = if stderr.is_empty() {
        stdout
    } else if stdout.is_empty() {
        stderr
    } else {
        format!("{stdout}\n{stderr}")
    };
    let gated = gate_output(
        runtime_dir,
        &format!("verify-{run_id}"),
        if combined.is_empty() {
            "verifier completed"
        } else {
            &combined
        },
        DEFAULT_RESULT_GATE_CHARS,
    )?;
    Ok(VerifierVerdict {
        passed: output.status.success(),
        feedback: gated.text,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoints::capture_worktree;

    #[test]
    fn eight_thousand_char_gate_spills_to_disk_and_pages_lines() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let runtime = dir.path().join("runtime");
        let big: String = (1..=600)
            .map(|line| {
                format!(
                    "line-{line:04}: compiler diagnostic payload {}\n",
                    "x".repeat(12)
                )
            })
            .collect();
        assert!(big.chars().count() > 8000);

        let gated = gate_output(&runtime, "evt-001", &big, 8000)?;
        assert!(gated.truncated);
        assert!(gated.text.contains("line-0001"));
        assert!(gated.text.contains("line-0600"));
        assert!(gated.text.contains("runtime/spills/evt-001.log"));

        let page = read_spill_page(&runtime, "evt-001", 250, 5, "line-0252")?;
        assert_eq!(page["matches"].as_array().unwrap().len(), 1);
        assert_eq!(page["matches"][0]["line"], 252);
        Ok(())
    }

    #[test]
    fn unacted_crash_replays_silently_while_acted_crash_blocks_for_mobile_approval() -> Result<()> {
        let yaml = r#"
version: "0.2"
workspace: work
nodes:
  desk:
    role: supervisor_and_executor
    herdr_cores:
      code_core:
        session: tarla-work-code_core
    templates:
      codex-impl:
        core: code_core
        command: "/usr/bin/true"
        workdir: "/tmp"
      claude-fallback:
        core: code_core
        command: "/bin/echo"
        workdir: "/tmp"
bots:
  - id: implementer
    name: "Implementer"
    targets: ["desk/codex-impl", "desk/claude-fallback"]
"#;
        let spec = BotflowSpec::from_yaml_str(yaml)?;
        let dir = tempfile::tempdir()?;
        let workdir = dir.path().join("repo");
        fs::create_dir_all(&workdir)?;
        fs::write(workdir.join("lib.rs"), "pub fn v() -> u8 { 1 }\n")?;
        let pre = capture_worktree(&workdir)?;

        let mut db = tarla_memory::open(Path::new(":memory:"), "work")?;
        db.execute(
            "INSERT INTO runs(run_id,workspace_id,conversation_id,supervisor_term,target_node_id,target_slot_id,desired_state,observed_state,mission_packet_json,created_at,updated_at)
             VALUES('r1','work','bot:implementer',1,'desk','codex-impl','running','executing','{}',1,1)",
            [],
        )?;

        let excluded = BTreeSet::from(["desk/codex-impl".to_string()]);
        // !acted -> silent replay on desk/claude-fallback.
        let decision = evaluate_failover_gate(
            &mut db,
            "work",
            "r1",
            "desk",
            "implementer",
            &workdir,
            &pre,
            &spec,
            None,
            &excluded,
            "HTTP 429 rate_limit_exceeded",
            10,
        )?;
        assert!(matches!(
            decision,
            FailoverDecision::SilentReplay { next_target }
                if next_target.template_name == "claude-fallback"
        ));

        // Modify file -> acted -> transitions to blocked_approval and creates pending approval.
        fs::write(workdir.join("lib.rs"), "pub fn v() -> u8 { 2 }\n")?;
        let acted = evaluate_failover_gate(
            &mut db,
            "work",
            "r1",
            "desk",
            "implementer",
            &workdir,
            &pre,
            &spec,
            None,
            &excluded,
            "HTTP 429 after file write",
            20,
        )?;
        assert!(matches!(
            acted,
            FailoverDecision::BlockedApproval { ref changed_paths, .. }
                if changed_paths == &vec!["lib.rs".to_string()]
        ));
        let state: String = db.query_row(
            "SELECT observed_state FROM runs WHERE run_id = 'r1'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(state, "blocked_approval");
        Ok(())
    }
}
