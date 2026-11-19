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
        #[arg(long)]
        token_env: String,
    },
    /// Apply an explicit offline supervisor handover, restore or peer reconfiguration.
    Supervisor {
        #[command(subcommand)]
        action: admin::Operation,
    },
    Status,
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
        token_env,
    } = &cli.command
    {
        return provision::pair(&home, &cli.workspace, node, origin, token_env);
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
        token,
    };
    let result=match cli.command{
        Command::Status=>c.get(&["health"]).await?,
        Command::Conversations=>c.get(&["v1","conversations"]).await?, Command::Runs=>c.get(&["v1","runs"]).await?,
        Command::Actions=>c.get(&["v1","actions"]).await?, Command::Memories=>c.get(&["v1","memories"]).await?,
        Command::Nodes=>c.get(&["v1","nodes"]).await?,Command::Slots=>c.get(&["v1","slots"]).await?,Command::Facts=>c.get(&["v1","facts"]).await?,
        Command::Chat{conversation,text,source_id}=>c.post(&["v1","conversations",&conversation,"messages"],json!({"text":text,"channel":"cli","source_msg_id":source_id.unwrap_or_else(||uuid::Uuid::now_v7().to_string())})).await?,
        Command::History{conversation}=>c.get(&["v1","conversations",&conversation,"messages"]).await?,
        Command::Dispatch{conversation,goal,acceptance,role}=>c.post(&["v1","runs"],json!({"conversation_id":conversation,"goal":goal,"acceptance":acceptance,"role":role})).await?,
        Command::Run{id}=>c.get(&["v1","runs",&id]).await?,
        Command::Cancel{id}=>c.post(&["v1","runs",&id,"cancel"],json!({})).await?,
        Command::Log{id,offset,limit}=>c.api(Method::GET,&["v1","runs",&id,"log"],None,&[("offset",offset.to_string()),("limit",limit.to_string())]).await?,
        Command::Resolve{id,resolution,expected_version}=>c.post(&["v1","actions",&id],json!({"resolution":match resolution{Decision::Approved=>"approved",Decision::Rejected=>"rejected"},"expected_version":expected_version})).await?,
        Command::Remember{id,scope,content,base_version}=>c.post(&["v1","memories"],json!({"memory_id":id,"scope":scope,"content":content,"base_version":base_version})).await?,
        Command::Bind{conversation,node,slot,workdir}=>c.api(Method::PUT,&["v1","conversations",&conversation,"binding"],Some(json!({"mode":"pinned","node_id":node,"slot_id":slot,"workdir":workdir})),&[]).await?,
        Command::Unbind{conversation}=>c.api(Method::PUT,&["v1","conversations",&conversation,"binding"],Some(json!({"mode":"orchestrator"})),&[]).await?,
        Command::Attach{id,interactive}=>return attach(&c,&id,interactive).await,
        Command::Init{..}|Command::Skills|Command::Supervisor{..}|Command::InstallTool{..}|Command::Pair{..}=>unreachable!(),
    };
    println!("{}", serde_json::to_string_pretty(&result)?);
    Ok(())
}

async fn attach(c: &Client, id: &str, interactive: bool) -> Result<()> {
    use futures_util::{SinkExt, StreamExt};
    use tokio::{io::AsyncReadExt, time::interval};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{client::IntoClientRequest, Message},
    };
    let mode = if interactive {
        "interactive"
    } else {
        "observe"
    };
    let grant = c
        .post(&["v1", "runs", id, "attach"], json!({"mode":mode}))
        .await?;
    let ticket = grant["ticket"]
        .as_str()
        .context("missing terminal ticket")?;
    let mut url = c.url(&["v1", "terminal", id])?;
    if let Some(endpoint) = grant.get("node_url").and_then(Value::as_str) {
        url = Url::parse(endpoint)?;
        url.path_segments_mut()
            .map_err(|_| anyhow::anyhow!("invalid executor origin"))?
            .pop_if_empty()
            .extend(["v1", "terminal", id]);
    }
    let scheme = match url.scheme() {
        "https" | "wss" => "wss",
        "http" | "ws" => "ws",
        _ => bail!("invalid terminal endpoint"),
    };
    url.set_scheme(scheme)
        .map_err(|_| anyhow::anyhow!("invalid terminal scheme"))?;
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
    let mut input = tokio::io::stdin();
    let mut chunk = [0; 4096];
    let mut resize = interval(Duration::from_millis(350));
    let mut last_size = None;
    loop {
        tokio::select! {
            remote=socket.next()=>match remote.transpose()?{
                None|Some(Message::Close(_))=>break,Some(Message::Binary(bytes))=>{let mut o=std::io::stdout().lock();o.write_all(&bytes)?;o.flush()?;},
                Some(Message::Text(s))=>eprintln!("{s}"),Some(Message::Ping(p))=>socket.send(Message::Pong(p)).await?,_=>{}
            },
            n=input.read(&mut chunk), if interactive=>{let n=n?;if n==0{break}
                if let Some(pos)=chunk[..n].iter().position(|b|*b==29){if pos>0{socket.send(Message::Binary(chunk[..pos].to_vec().into())).await?;}break}
                socket.send(Message::Binary(chunk[..n].to_vec().into())).await?;
            },
            _=resize.tick(), if raw=>{if let Ok((cols,rows))=crossterm::terminal::size(){if last_size!=Some((cols,rows)){
                socket.send(Message::Text(json!({"cols":cols,"rows":rows}).to_string().into())).await?;
                last_size=Some((cols,rows));
            }}},
        }
    }
    let _ = socket.close(None).await;
    Ok(())
}
