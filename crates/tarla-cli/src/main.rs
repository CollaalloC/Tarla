mod admin;
mod provision;
use anyhow::{bail, ensure, Context, Result};
use clap::{Parser, Subcommand, ValueEnum};
use reqwest::{Method, Url};
use serde_json::{json, Value};
use std::{
    env,
    io::{IsTerminal, Write},
    path::PathBuf,
    time::Duration,
};
use tarla_workspace::Workspace;

#[derive(Parser)]
#[command(version, about = "Tarla workspace client")]
struct Cli {
    #[arg(long, global = true, env = "TARLA_HOME")]
    home: Option<PathBuf>,
    #[arg(
        long,
        global = true,
        env = "TARLA_WORKSPACE",
        default_value = "default"
    )]
    workspace: String,
    #[arg(
        long,
        global = true,
        env = "TARLA_URL",
        default_value = "http://127.0.0.1:7878"
    )]
    url: Url,
    #[command(subcommand)]
    command: Command,
}
#[derive(Subcommand)]
enum Command {
    /// Create private local state; existing profiles and credentials are preserved.
    Init {
        #[arg(long, default_value = "local")]
        node: String,
        #[arg(long, default_value = "supervisor_and_executor")]
        role: String,
        #[arg(long)]
        supervisor: Option<String>,
    },
    /// Print registered workspace skills.
    Skills,
    /// Register a SHA-256-pinned sidecar from a locally verified distribution.
    InstallTool {
        #[arg(value_enum)]
        tool: provision::Tool,
        version: String,
        binary: PathBuf,
        #[arg(long)]
        sha256: String,
    },
    /// Pair one peer by referencing an environment variable on each daemon.
    Pair {
        node: String,
        origin: Url,
        /// HTTPS origin reachable by clients for direct terminal access.
        #[arg(long)]
        client_url: Option<Url>,
        #[arg(long)]
        token_env: String,
    },
    /// Apply an explicit offline supervisor handover, restore or peer reconfiguration.
    Supervisor {
        #[command(subcommand)]
        action: admin::Operation,
    },
    /// Issue a five-minute code binding a channel actor to a conversation.
    PairChannel {
        platform: String,
        actor: String,
        conversation: String,
        #[arg(long)]
        approvals: bool,
    },
    ChannelPairings,
    UnpairChannel {
        id: String,
    },
    Status,
    /// Show executor journals still required by a restored supervisor.
    RecoveryStatus,
    Conversations,
    Runs,
    Actions,
    Memories,
    Nodes,
    Slots,
    Facts,
    Chat {
        conversation: String,
        text: String,
        #[arg(long)]
        source_id: Option<String>,
    },
    History {
        conversation: String,
    },
    Dispatch {
        conversation: String,
        goal: String,
        #[arg(long)]
        request_id: Option<String>,
        #[arg(long = "accept")]
        acceptance: Vec<String>,
        #[arg(long, default_value = "implementer")]
        role: String,
    },
    Run {
        id: String,
    },
    Cancel {
        id: String,
    },
    /// Close an unconfirmed run on its owning executor after checking its process and worktree.
    Abandon {
        id: String,
        #[arg(long, required = true, action = clap::ArgAction::SetTrue)]
        confirm_stopped: bool,
        #[arg(long)]
        reason: String,
    },
    Log {
        id: String,
        #[arg(long, default_value_t = 0)]
        offset: u64,
        #[arg(long, default_value_t = 40)]
        limit: u16,
    },
    Resolve {
        id: String,
        #[arg(value_enum)]
        resolution: Decision,
        #[arg(long)]
        expected_version: Option<i64>,
    },
    Remember {
        id: String,
        content: String,
        #[arg(long, default_value = "global")]
        scope: String,
        #[arg(long)]
        base_version: i64,
    },
    Bind {
        conversation: String,
        node: String,
        slot: String,
        workdir: String,
    },
    Unbind {
        conversation: String,
    },
    /// Connect to one terminal. Use Ctrl-] to detach an interactive session.
    Attach {
        id: String,
        #[arg(long)]
        interactive: bool,
        /// Relay the WebSocket over the supervisor's private route to the executor.
        #[arg(long)]
        via_supervisor: bool,
    },
}
#[derive(Copy, Clone, ValueEnum)]
enum Decision {
    Approved,
    Rejected,
}

struct Client {
    http: reqwest::Client,
    base: Url,
    workspace: String,
    token: String,
}
impl Client {
    fn url(&self, path: &[&str]) -> Result<Url> {
        let mut url = self.base.clone();
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid API origin"))?
            .pop_if_empty()
            .extend(path);
        Ok(url)
    }
    async fn health(&self) -> Result<Value> {
        let res = self.http.get(self.url(&["health"])?).send().await?;
        let status = res.status();
        ensure!(status.is_success(), "workspace discovery failed: {status}");
        let info: Value = res.json().await?;
        ensure!(
            info["workspace_id"].as_str() == Some(&self.workspace),
            "server workspace differs from requested workspace {}",
            self.workspace
        );
        Ok(info)
    }

    async fn api(
        &self,
        method: Method,
        path: &[&str],
        body: Option<Value>,
        query: &[(&str, String)],
    ) -> Result<Value> {
        let mut req = self
            .http
            .request(method, self.url(path)?)
            .header("X-Tarla-Workspace", &self.workspace)
            .bearer_auth(&self.token)
            .query(query);
        if let Some(v) = body {
            req = req.json(&v);
        }
        let res = req.send().await?;
        let status = res.status();
        let text = res.text().await?;
        ensure!(status.is_success(), "API {status}: {text}");
        Ok(serde_json::from_str(&text)?)
    }
    async fn get(&self, path: &[&str]) -> Result<Value> {
        self.api(Method::GET, path, None, &[]).await
    }
    async fn post(&self, path: &[&str], v: Value) -> Result<Value> {
        self.api(Method::POST, path, Some(v), &[]).await
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let home = cli
        .home
        .or_else(|| {
            env::var_os("HOME")
                .or_else(|| env::var_os("USERPROFILE"))
                .map(|p| PathBuf::from(p).join(".tarla"))
        })
        .context("specify --home")?;
    if let Command::Init {
        node,
        role,
        supervisor,
    } = &cli.command
    {
        let ws = Workspace::init(
            &home,
            &cli.workspace,
            node,
            role,
            supervisor.as_deref().unwrap_or(node),
        )?;
        println!(
            "Initialized {} at {}",
            ws.config.workspace.id,
            ws.root.display()
        );
        return Ok(());
    }
    if let Command::InstallTool {
        tool,
        version,
        binary,
        sha256,
    } = &cli.command
    {
        return provision::install(&home, *tool, version, binary, sha256);
    }
    if let Command::Pair {
        node,
        origin,
        client_url,
        token_env,
    } = &cli.command
    {
        return provision::pair(
            &home,
            &cli.workspace,
            node,
            origin,
            token_env,
            client_url.as_ref(),
        );
    }
    if let Command::Supervisor { action } = &cli.command {
        return admin::run(&home, &cli.workspace, action);
    }
    if matches!(cli.command, Command::Skills) {
        let ws = Workspace::open(&home, &cli.workspace)?;
        println!("{}", serde_json::to_string_pretty(&ws.skills()?)?);
        return Ok(());
    }
    ensure!(
        matches!(cli.url.scheme(), "http" | "https")
            && cli.url.username().is_empty()
            && cli.url.password().is_none()
            && cli.url.query().is_none()
            && cli.url.fragment().is_none(),
        "expected an HTTP(S) server URL"
    );
    let token = match env::var("TARLA_TOKEN") {
        Ok(v) => v,
        Err(_) => Workspace::open(&home, &cli.workspace)?.token,
    };
    ensure!(!token.is_empty(), "empty workspace token");
    let c = Client {
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(180))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        base: cli.url,
        workspace: cli.workspace,
        token,
    };
    // Standalone IM gateways can be addressed directly and do not serve /health.
    if !matches!(
        cli.command,
        Command::Status
            | Command::PairChannel { .. }
            | Command::ChannelPairings
            | Command::UnpairChannel { .. }
    ) {
        c.health().await?;
    }
    let result = match cli.command {
        Command::PairChannel { platform, actor, conversation, approvals } => {
            c.post(&["gateway", "pairings"], json!({
                "platform": platform, "actor_id": actor, "conversation_id": conversation, "approvals": approvals
            })).await?
        }
        Command::ChannelPairings => c.get(&["gateway", "pairings"]).await?,
        Command::UnpairChannel { id } => c.api(Method::DELETE, &["gateway", "pairings", &id], None, &[]).await?,
        Command::Status => c.health().await?,
        Command::RecoveryStatus => c.get(&["v1", "recovery"]).await?,
        Command::Conversations => c.get(&["v1", "conversations"]).await?,
        Command::Runs => c.get(&["v1", "runs"]).await?,
        Command::Actions => c.get(&["v1", "actions"]).await?,
        Command::Memories => c.get(&["v1", "memories"]).await?,
        Command::Nodes => c.get(&["v1", "nodes"]).await?,
        Command::Slots => c.get(&["v1", "slots"]).await?,
        Command::Facts => c.get(&["v1", "facts"]).await?,
        Command::Chat { conversation, text, source_id } => {
            let id = source_id.unwrap_or_else(|| uuid::Uuid::now_v7().to_string());
            c.post(&["v1", "conversations", &conversation, "messages"],
                json!({"text":text, "channel":"cli", "source_msg_id":id})).await?
        }
        Command::History { conversation } => c.get(&["v1", "conversations", &conversation, "messages"]).await?,
        Command::Dispatch { conversation, goal, acceptance, role, request_id } => {
            c.post(&["v1", "runs"], json!({
                "conversation_id": conversation, "goal":goal, "acceptance":acceptance,
                "role":role, "request_id":request_id
            })).await?
        }
        Command::Run { id } => c.get(&["v1", "runs", &id]).await?,
        Command::Cancel { id } => c.post(&["v1", "runs", &id, "cancel"], json!({})).await?,
        Command::Abandon { id, confirm_stopped, reason } => c.post(
            &["v1", "runs", &id, "abandon"], json!({"confirmed_stopped":confirm_stopped,"reason":reason})
        ).await?,
        Command::Log { id, offset, limit } => c.api(Method::GET, &["v1", "runs", &id, "log"],
            None, &[("offset",offset.to_string()),("limit",limit.to_string())]).await?,
        Command::Resolve { id, resolution, expected_version } => {
            let choice = match resolution { Decision::Approved => "approved", Decision::Rejected => "rejected" };
            c.post(&["v1", "actions", &id], json!({"resolution":choice, "expected_version":expected_version})).await?
        }
        Command::Remember { id, scope, content, base_version } => c.post(&["v1", "memories"],
            json!({"memory_id":id, "scope":scope, "content":content, "base_version":base_version})).await?,
        Command::Bind { conversation, node, slot, workdir } => {
            c.api(Method::PUT, &["v1", "conversations", &conversation, "binding"],
                Some(json!({"mode":"pinned","node_id":node,"slot_id":slot,"workdir":workdir})), &[]).await?
        }
        Command::Unbind { conversation } => c.api(Method::PUT,
            &["v1", "conversations", &conversation, "binding"], Some(json!({"mode":"orchestrator"})), &[]).await?,
        Command::Attach { id, interactive, via_supervisor } => return attach(&c, &id, interactive, via_supervisor).await,
        Command::Init { .. } | Command::Skills | Command::Supervisor { .. } |
        Command::InstallTool { .. } | Command::Pair { .. } => unreachable!(),
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

fn terminal_url(
    base: &Url,
    id: &str,
    mode: &str,
    via_supervisor: bool,
    grant: &Value,
) -> Result<Url> {
    for (field, expected) in [("run_id", id), ("mode", mode)] {
        if let Some(value) = grant.get(field) {
            ensure!(value == expected, "terminal grant has a different {field}");
        }
    }
    if let Some(route) = grant.get("route") {
        // Local tasks always use a same-origin direct grant, even when the
        // caller requested a relay. Older daemons omit the route field.
        ensure!(
            route == "direct" || (via_supervisor && route == "supervisor"),
            "unexpected terminal grant route"
        );
    }
    let mut url = match grant.get("node_url") {
        None => base.clone(),
        Some(endpoint) => {
            ensure!(!via_supervisor, "expected a same-origin terminal route");
            let url = Url::parse(endpoint.as_str().context("invalid executor origin")?)?;
            ensure!(
                matches!(url.scheme(), "http" | "https")
                    && url.host().is_some()
                    && url.username().is_empty()
                    && url.password().is_none()
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && url.path() == "/",
                "expected a bare HTTP(S) executor origin"
            );
            url
        }
    };
    url.path_segments_mut()
        .map_err(|_| anyhow::anyhow!("invalid terminal endpoint"))?
        .pop_if_empty()
        .extend(["v1", "terminal", id]);
    let scheme = match url.scheme() {
        "https" => "wss",
        "http" => "ws",
        _ => bail!("invalid terminal endpoint"),
    };
    url.set_scheme(scheme)
        .map_err(|_| anyhow::anyhow!("invalid terminal scheme"))?;
    Ok(url)
}

async fn attach(c: &Client, id: &str, interactive: bool, via_supervisor: bool) -> Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use std::io::Read;
    use tokio::{sync::mpsc, time::interval};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{client::IntoClientRequest, Message},
    };
    let mode = if interactive {
        "interactive"
    } else {
        "observe"
    };
    let route = if via_supervisor {
        "supervisor"
    } else {
        "direct"
    };
    let grant = c
        .post(
            &["v1", "runs", id, "attach"],
            json!({"mode":mode,"route":route}),
        )
        .await?;
    let ticket = grant["ticket"]
        .as_str()
        .filter(|ticket| !ticket.is_empty())
        .context("missing terminal ticket")?;
    let url = terminal_url(&c.base, id, mode, via_supervisor, &grant)?;
    let mut req = url.as_str().into_client_request()?;
    req.headers_mut()
        .insert("Authorization", format!("Bearer {ticket}").parse()?);
    let (mut socket, _) = connect_async(req).await?;
    struct Raw(bool);
    impl Drop for Raw {
        fn drop(&mut self) {
            if self.0 {
                let _ = crossterm::terminal::disable_raw_mode();
            }
        }
    }
    let raw = interactive && std::io::stdin().is_terminal();
    if raw {
        crossterm::terminal::enable_raw_mode()?;
    }
    let _guard = Raw(raw);
    let (sender, mut input) = mpsc::channel::<std::io::Result<Vec<u8>>>(1);
    if interactive {
        // The CLI exits after this attachment. An idle console read must not
        // keep the Tokio runtime alive after the remote terminal closes.
        std::thread::Builder::new()
            .name("terminal-stdin".into())
            .spawn(move || {
                let mut stdin = std::io::stdin().lock();
                let mut buf = [0; 4096];
                loop {
                    match stdin.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            if sender.blocking_send(Ok(buf[..n].to_vec())).is_err() {
                                break;
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(error) => {
                            let _ = sender.blocking_send(Err(error));
                            break;
                        }
                    }
                }
            })?;
    }
    let mut resize = interval(Duration::from_millis(350));
    let mut last_size = None;
    loop {
        tokio::select! {
            remote = socket.next() => match remote.transpose()? {
                None | Some(Message::Close(_)) => break,
                Some(Message::Binary(bytes)) => {
                    let mut out = std::io::stdout().lock();
                    out.write_all(&bytes)?;
                    out.flush()?;
                }
                Some(Message::Text(text)) => eprintln!("{text}"),
                Some(Message::Ping(payload)) => socket.send(Message::Pong(payload)).await?,
                _ => {}
            },
            next = input.recv(), if interactive => {
                let Some(mut bytes) = next.transpose()? else { break };
                if let Some(pos) = bytes.iter().position(|b| *b == 29) {
                    bytes.truncate(pos);
                    if !bytes.is_empty() {
                        socket.send(Message::Binary(bytes.into())).await?;
                    }
                    break;
                }
                socket.send(Message::Binary(bytes.into())).await?;
            }
            _ = resize.tick(), if raw => {
                if let Ok(size @ (cols, rows)) = crossterm::terminal::size() {
                    if last_size != Some(size) {
                        let message = json!({"cols":cols,"rows":rows}).to_string();
                        socket.send(Message::Text(message.into())).await?;
                        last_size = Some(size);
                    }
                }
            }
        }
    }
    let _ = socket.close(None).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_destinations_preserve_api_prefix_and_legacy_direct_grants() {
        let base = Url::parse("https://chief.example/work/").unwrap();
        let local = "wss://chief.example/work/v1/terminal/r1";
        for grant in [
            json!({}),
            json!({"route":"direct"}),
            json!({"route":"supervisor"}),
        ] {
            assert_eq!(
                terminal_url(&base, "r1", "observe", true, &grant)
                    .unwrap()
                    .as_str(),
                local
            );
        }
        let old = json!({"run_id":"r1","mode":"observe","node_url":"http://100.64.0.2:7878"});
        assert_eq!(
            terminal_url(&base, "r1", "observe", false, &old)
                .unwrap()
                .as_str(),
            "ws://100.64.0.2:7878/v1/terminal/r1"
        );
        assert!(terminal_url(&base, "r1", "observe", true, &old).is_err());
    }

    #[test]
    fn terminal_grants_validate_scope_and_advertised_origin() {
        let base = Url::parse("https://chief.example").unwrap();
        for grant in [
            json!({"run_id":"r2"}),
            json!({"mode":"interactive"}),
            json!({"route":"supervisor"}),
            json!({"route":"unknown"}),
            json!({"node_url":false}),
            json!({"node_url":"ws://worker.example"}),
            json!({"node_url":"https://user@worker.example"}),
            json!({"node_url":"https://worker.example/path"}),
            json!({"node_url":"https://worker.example/?query"}),
            json!({"node_url":"https://worker.example/#fragment"}),
        ] {
            assert!(
                terminal_url(&base, "r1", "observe", false, &grant).is_err(),
                "{grant}"
            );
        }
    }
}
