//! Compact single-line JSON `tarla ctl` CLI subcommands (Spec §3.3), replacing MCP schemas.
use anyhow::{ensure, Context, Result};
use clap::{Args, Subcommand};
use reqwest::{Client as HttpClient, Method, Url};
use serde_json::{json, Value};
use std::env;

#[derive(Args, Debug)]
pub struct CtlArgs {
    /// Emit compact single-line JSON (always enabled for `tarla ctl` contract).
    #[arg(long)]
    pub json: bool,
    #[command(subcommand)]
    pub command: CtlCommand,
}

#[derive(Subcommand, Debug)]
pub enum CtlCommand {
    /// (Master only) List online/offline nodes, herdr-core groups, Bot engines, quotas, and queue depths.
    Bots {
        #[arg(long)]
        include_offline: bool,
    },
    /// (Master only) Dispatch a persistent ROS 2 Action goal to a target Bot.
    SendGoal {
        #[arg(long)]
        bot: String,
        #[arg(long)]
        obj: String,
        #[arg(long = "accept")]
        accept: Vec<String>,
        #[arg(long = "op-id")]
        op_id: Option<String>,
        #[arg(long)]
        correlation_id: Option<String>,
    },
    /// (Master only) Safely transition an active goal into `canceling` / `canceled`.
    CancelGoal {
        run_id: String,
        #[arg(long)]
        reason: Option<String>,
    },
    /// (Master & Worker) Emit a structured event on `<port>` (or `feedback` / `blocked_approval`).
    Emit {
        port: String,
        #[arg(long)]
        payload: String,
        #[arg(long)]
        bot: Option<String>,
        #[arg(long)]
        correlation_id: Option<String>,
    },
    /// (Master & Worker) Search S2 memories, propose a revision, or page archived session text.
    Memory {
        #[command(subcommand)]
        op: CtlMemoryOp,
    },
    /// (Master only) Hot-swap a Bot's underlying CLI template while preserving `conversation_id`.
    SwitchEngine {
        #[arg(long)]
        bot: String,
        #[arg(long)]
        target: String,
    },
    /// (Master only) Perform SHA-256 conflict-aware file rollback for a checkpointed Action.
    Rollback { run_id: String },
    /// (Master & Worker) Execute an external side effect via `tarlad`'s zero-plaintext `--op-id` credential broker.
    ExecOp {
        #[arg(long = "op-id")]
        op_id: String,
        #[arg(long = "credential")]
        credential_ref: String,
        #[arg(long)]
        action: String,
        #[arg(long, default_value = "{}")]
        payload: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum CtlMemoryOp {
    Search {
        #[arg(default_value = "")]
        query: String,
        #[arg(long)]
        scope: Option<String>,
        #[arg(long, default_value_t = 20)]
        limit: u64,
    },
    Propose {
        #[arg(long)]
        id: String,
        #[arg(long)]
        content: String,
        #[arg(long = "base-version", default_value_t = 0)]
        base_version: i64,
        #[arg(long)]
        scope: Option<String>,
    },
    Recall {
        #[arg(long)]
        conversation: Option<String>,
        #[arg(long)]
        query: Option<String>,
        #[arg(long = "seq-no")]
        seq_no: Option<i64>,
        #[arg(long)]
        source: Option<String>,
        #[arg(long = "offset-bytes", default_value_t = 0)]
        offset_bytes: u64,
        #[arg(long = "max-bytes", default_value_t = 384)]
        max_bytes: u64,
    },
}

#[allow(clippy::too_many_arguments)]
async fn send_ctl(
    http: &HttpClient,
    base: &Url,
    workspace: &str,
    token: &str,
    role: &str,
    bot_env: &str,
    run_env: &str,
    method: Method,
    segments: &[&str],
    body: Option<Value>,
    query: &[(&str, String)],
) -> Result<Value> {
    let mut url = base.clone();
    url.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("invalid API origin"))?
        .pop_if_empty()
        .extend(segments);
    let mut req = http
        .request(method, url)
        .header("X-Tarla-Workspace", workspace)
        .header("X-Tarla-Ctl-Role", role)
        .bearer_auth(token)
        .query(query);
    if !bot_env.is_empty() {
        req = req.header("X-Tarla-Bot-Id", bot_env);
    }
    if !run_env.is_empty() {
        req = req.header("X-Tarla-Run-Id", run_env);
    }
    if let Some(b) = body {
        req = req.json(&b);
    }
    let resp = req.send().await?;
    let status = resp.status();
    let text = resp.text().await?;
    ensure!(status.is_success(), "API {status}: {text}");
    Ok(serde_json::from_str(&text)?)
}

pub async fn run_ctl(
    http: &HttpClient,
    base: &Url,
    workspace: &str,
    token: &str,
    args: &CtlArgs,
) -> Result<()> {
    let role = env::var("TARLA_CTL_ROLE").unwrap_or_else(|_| "master".into());
    let bot_env = env::var("TARLA_BOT_ID").unwrap_or_default();
    let run_env = env::var("TARLA_RUN_ID").unwrap_or_default();
    let corr_env = env::var("TARLA_CORRELATION_ID").ok();
    let hop_env: u64 = env::var("TARLA_HOP_COUNT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    macro_rules! call {
        ($m:expr, $seg:expr, $body:expr, $q:expr $(,)?) => {
            send_ctl(
                http, base, workspace, token, &role, &bot_env, &run_env, $m, $seg, $body, $q,
            )
        };
    }

    let output = match &args.command {
        CtlCommand::Bots { include_offline } => {
            call!(
                Method::GET,
                &["v2", "ctl", "bots"],
                None,
                &[(
                    "include_offline",
                    if *include_offline { "true" } else { "false" }.into(),
                )],
            )
            .await?
        }
        CtlCommand::SendGoal {
            bot,
            obj,
            accept,
            op_id,
            correlation_id,
        } => {
            call!(
                Method::POST,
                &["v2", "ctl", "goals"],
                Some(json!({
                    "bot_id": bot,
                    "objective": obj,
                    "acceptance_criteria": accept,
                    "op_id": op_id,
                    "correlation_id": correlation_id.clone().or(corr_env),
                })),
                &[],
            )
            .await?
        }
        CtlCommand::CancelGoal { run_id, reason } => {
            call!(
                Method::POST,
                &["v2", "ctl", "goals", run_id, "cancel"],
                Some(json!({"reason": reason})),
                &[],
            )
            .await?
        }
        CtlCommand::Emit {
            port,
            payload,
            bot,
            correlation_id,
        } => {
            let parsed_payload: Value =
                serde_json::from_str(payload).context("--payload must be valid JSON")?;
            call!(
                Method::POST,
                &["v2", "ctl", "emit"],
                Some(json!({
                    "port": port,
                    "bot_id": bot,
                    "payload": parsed_payload,
                    "correlation_id": correlation_id.clone().or(corr_env),
                    "hop_count": hop_env,
                })),
                &[],
            )
            .await?
        }
        CtlCommand::Memory { op } => match op {
            CtlMemoryOp::Search {
                query,
                scope,
                limit,
            } => {
                call!(
                    Method::POST,
                    &["v2", "ctl", "memory"],
                    Some(json!({
                        "action": "search",
                        "query": query,
                        "scope": scope,
                        "limit": limit,
                    })),
                    &[],
                )
                .await?
            }
            CtlMemoryOp::Propose {
                id,
                content,
                base_version,
                scope,
            } => {
                call!(
                    Method::POST,
                    &["v2", "ctl", "memory"],
                    Some(json!({
                        "action": "propose",
                        "memory_id": id,
                        "content": content,
                        "base_version": base_version,
                        "scope": scope,
                    })),
                    &[],
                )
                .await?
            }
            CtlMemoryOp::Recall {
                conversation,
                query,
                seq_no,
                source,
                offset_bytes,
                max_bytes,
            } => {
                call!(
                    Method::POST,
                    &["v2", "ctl", "memory"],
                    Some(json!({
                        "action": "recall",
                        "conversation_id": conversation,
                        "query": query,
                        "seq_no": seq_no,
                        "source": source,
                        "offset_bytes": offset_bytes,
                        "max_bytes": max_bytes,
                    })),
                    &[],
                )
                .await?
            }
        },
        CtlCommand::SwitchEngine { bot, target } => {
            call!(
                Method::POST,
                &["v2", "ctl", "bots", bot, "switch"],
                Some(json!({"target": target})),
                &[],
            )
            .await?
        }
        CtlCommand::Rollback { run_id } => {
            call!(
                Method::POST,
                &["v2", "ctl", "runs", run_id, "rollback"],
                Some(json!({})),
                &[],
            )
            .await?
        }
        CtlCommand::ExecOp {
            op_id,
            credential_ref,
            action,
            payload,
        } => {
            let parsed_payload: Value =
                serde_json::from_str(payload).context("--payload must be valid JSON")?;
            call!(
                Method::POST,
                &["v2", "ctl", "ops"],
                Some(json!({
                    "op_id": op_id,
                    "credential_ref": credential_ref,
                    "action": action,
                    "payload": parsed_payload,
                })),
                &[],
            )
            .await?
        }
    };

    // Spec §3.3: always emit compact single-line JSON for zero-overhead CLI parsing.
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}
