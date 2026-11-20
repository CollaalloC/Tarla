mod api;
mod authority;
mod control;
mod distillation;
mod drafts;
mod facts;
mod fleet;
mod handoff;
mod hooks;
mod launch;
mod local_distiller;
mod local_endpoint;
mod maintenance;
mod peers;
mod s1;
mod skills;
mod slot_reports;
mod terminal;
mod terminal_relay;
mod transcript;

use anyhow::{anyhow, bail, Context, Result};
use axum::{
    extract::DefaultBodyLimit,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post, put},
    Json, Router,
};
use clap::Parser;
use rusqlite::{params_from_iter, Connection};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    future::IntoFuture,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;
use tarla_supervisor::SupervisorEngine;
use tarla_workspace::Workspace;
use uuid::Uuid;

#[derive(Parser)]
#[command(version, about = "Tarla headless workspace service")]
struct Args {
    #[arg(long, env = "TARLA_HOME")]
    home: Option<PathBuf>,
    #[arg(long, env = "TARLA_WORKSPACE", default_value = "default")]
    workspace: String,
    #[arg(long, env = "TARLA_LISTEN", default_value = "127.0.0.1:7878")]
    listen: SocketAddr,
    /// Publish this loopback listener for an explicitly enabled desktop service.
    #[arg(long)]
    publish_local_endpoint: bool,
}

struct Service {
    distiller: Option<local_distiller::LocalDistiller>,
    distill_wake: tokio::sync::Notify,
    memory_index: Option<tarla_memory::MnemosyneIndex>,
    hosted_index: Option<tarla_memory::HostedIndex>,
    transcripts: transcript::Captures,
    drafts: drafts::Drafts,
    ws: Workspace,
    local_url: String,
    home: PathBuf,
    db_path: PathBuf,
    brain: SupervisorEngine,
    slots: Vec<fleet::Slot>,
    peers: BTreeMap<String, peers::Peer>,
    locks: tokio::sync::Mutex<BTreeMap<String, std::sync::Weak<tokio::sync::Mutex<()>>>>,
    account_locks: std::sync::Mutex<BTreeMap<String, std::fs::File>>,
    http: reqwest::Client,
    notify: tokio::sync::Notify,
    s2_changed: tokio::sync::watch::Sender<i64>,
    sync_wake: tokio::sync::Notify,
    receipt_wake: tokio::sync::Notify,
    context_wake: tokio::sync::Notify,
    reload: tokio::sync::Notify,
    authority_change: std::sync::Mutex<()>,
    retired: std::sync::atomic::AtomicBool,
}
type S = Arc<Service>;
impl Service {
    fn db(&self) -> Result<Connection> {
        anyhow::ensure!(
            !self.retired.load(std::sync::atomic::Ordering::Acquire),
            "authority changed; reconnect"
        );
        let c = Connection::open(&self.db_path)?;
        c.busy_timeout(Duration::from_secs(5))?;
        c.pragma_update(None, "foreign_keys", true)?;
        Ok(c)
    }
    fn id(&self) -> &str {
        &self.ws.config.workspace.id
    }
    fn node(&self) -> &str {
        &self.ws.config.workspace.node_id
    }
    fn supervisor(&self) -> &str {
        &self.ws.config.workspace.supervisor_node_id
    }
    fn is_supervisor(&self) -> bool {
        self.supervisor() == self.node() && self.ws.config.workspace.role_mode != "executor_only"
    }
    fn can_execute(&self) -> bool {
        self.ws.config.workspace.role_mode != "supervisor_only"
    }
    fn term(&self) -> i64 {
        self.ws.config.workspace.supervisor_term
    }
    async fn lock(&self, id: &str) -> tokio::sync::OwnedMutexGuard<()> {
        let mut map = self.locks.lock().await;
        map.retain(|_, v| v.strong_count() > 0);
        let key = map.get(id).and_then(|w| w.upgrade()).unwrap_or_else(|| {
            let m = Arc::new(tokio::sync::Mutex::new(()));
            map.insert(id.into(), Arc::downgrade(&m));
            m
        });
        drop(map);
        key.lock_owned().await
    }
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
fn uid() -> String {
    Uuid::now_v7().to_string()
}
fn secret_equal(a: &str, b: &str) -> bool {
    bool::from(Sha256::digest(a.as_bytes()).ct_eq(&Sha256::digest(b.as_bytes())))
}
fn validate_id(s: &str) -> Result<()> {
    if s.is_empty()
        || s.len() > 160
        || !s
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.:@".contains(&b))
    {
        bail!("invalid identifier")
    }
    Ok(())
}
fn string<'a>(v: &'a Value, key: &str) -> Result<&'a str> {
    v.get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| anyhow!("missing {key}"))
}
fn list(c: &Connection, sql: &str, args: &[&dyn rusqlite::ToSql]) -> Result<Vec<Value>> {
    let mut s = c.prepare(sql)?;
    let names = s
        .column_names()
        .iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>();
    let rows = s.query_map(params_from_iter(args.iter()), |row| {
        use rusqlite::types::ValueRef::*;
        let mut m = serde_json::Map::new();
        for (i, name) in names.iter().enumerate() {
            let val = match row.get_ref(i)? {
                Null => Value::Null,
                Integer(n) => json!(n),
                Real(n) => json!(n),
                Text(v) => json!(String::from_utf8_lossy(v)),
                Blob(_) => Value::Null,
            };
            m.insert(name.clone(), val);
        }
        Ok(Value::Object(m))
    })?;
    Ok(rows.collect::<rusqlite::Result<_>>()?)
}
type Api<T = Json<Value>> = std::result::Result<T, ApiError>;
struct ApiError(StatusCode, String);
impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        let status = if matches!(
            e.downcast_ref::<tarla_supervisor::BrainError>(),
            Some(tarla_supervisor::BrainError::Unavailable(_))
        ) {
            StatusCode::SERVICE_UNAVAILABLE
        } else {
            StatusCode::BAD_REQUEST
        };
        Self(status, e.to_string())
    }
}
impl From<rusqlite::Error> for ApiError {
    fn from(e: rusqlite::Error) -> Self {
        eprintln!("workspace database: {e}");
        Self(
            StatusCode::INTERNAL_SERVER_ERROR,
            "workspace database error".into(),
        )
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error":self.1}))).into_response()
    }
}
fn ensure_supervisor(s: &Service) -> Api<()> {
    if !s.is_supervisor() {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "operation requires the designated supervisor".into(),
        ));
    }
    Ok(())
}

const MIGRATIONS: &str = include_str!("service.sql");
fn main() -> Result<()> {
    if std::env::args_os().nth(1).as_deref()
        == Some(std::ffi::OsStr::new("--executor-launch-helper"))
    {
        std::process::exit(launch::helper(std::env::args_os().skip(2))?);
    }
    if std::env::args_os().nth(1).as_deref() == Some(std::ffi::OsStr::new("--pinned-turn-helper")) {
        let path = std::env::args_os()
            .nth(2)
            .context("missing pinned launch record")?;
        std::process::exit(handoff::helper(Path::new(&path))?);
    }
    while serve()? {}
    Ok(())
}

#[tokio::main]
async fn serve() -> Result<bool> {
    let a = Args::parse();
    if a.publish_local_endpoint && !a.listen.ip().is_loopback() {
        bail!("local endpoint discovery requires a loopback listener");
    }
    let account_home = || {
        std::env::var_os("HOME")
            .or_else(|| std::env::var_os("USERPROFILE"))
            .map(PathBuf::from)
            .context("specify --home")
    };
    let home = match a.home {
        // launchd passes arguments without a shell. Resolve the account directory
        // here so a registered agent selects exactly the same state as its app.
        Some(path) if path.starts_with("~") => account_home()?.join(path.strip_prefix("~")?),
        Some(path) => path,
        None => account_home()?.join(".tarla"),
    };
    let ws = Workspace::open(&home, &a.workspace)?;
    let guard = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(ws.root.join("runtime/daemon.lock"))?;
    fs2::FileExt::try_lock_exclusive(&guard).context("this workspace daemon or its offline administration command already owns the workspace lock")?;
    // Re-read identity after locking to serialize startup against handover.
    let ws = Workspace::open(&home, &a.workspace)?;
    let conf = std::fs::read_to_string(ws.root.join("workspace.toml"))?.parse::<toml::Table>()?;
    let mut table = conf
        .get("supervisor")
        .and_then(|s| s.get("brain"))
        .and_then(|s| s.get("backends"))
        .cloned()
        .unwrap_or_else(|| toml::Value::Table(toml::Table::new()));
    let db_path = ws.root.join("state.db");
    let mut initial = tarla_memory::open(&db_path, &a.workspace)?;
    initial.execute_batch(MIGRATIONS)?;
    initial.execute_batch(launch::SCHEMA)?;
    initial.execute_batch(s1::SCHEMA)?;
    drafts::migrate(&mut initial)?;
    peers::check_term(&mut initial, &ws)?;
    initial.execute_batch(tarla_workspace::authority::SCHEMA)?;
    let identity = &ws.config.workspace;
    if identity.node_id == identity.supervisor_node_id && identity.role_mode != "executor_only" {
        tarla_workspace::authority::ensure_signer(&mut initial, identity)?;
    }
    facts::configure(&mut initial, &conf, &ws.config.workspace.node_id)?;
    let slots = fleet::load(&conf, &ws, &home)?;
    let peers = peers::load(&conf, &ws)?;
    if identity.node_id == identity.supervisor_node_id && identity.role_mode != "executor_only" {
        s1::begin_recovery(&mut initial, &identity.node_id, &peers)?;
    }
    let listener = tokio::net::TcpListener::bind(a.listen).await?;
    let actual = listener.local_addr()?;
    let runtime = ws.root.join("runtime");
    let network = &ws.config.network;
    let mut transports = Vec::new();
    if network.mode == "managed_sidecar" && (network.enable_easytier || network.enable_tailscale) {
        let tc = tarla_sidecar::Toolchain::load(&home.join("bin"))?;
        let mut specs = Vec::new();
        if network.enable_easytier {
            let key = network.easytier_network_secret_ref.clone();
            let seeds = network
                .extra
                .get("easytier_peers")
                .cloned()
                .map(toml::Value::try_into)
                .transpose()?
                .unwrap_or_default();
            let spec = if let Some(ip) = network.extra.get("easytier_ipv4").and_then(|x| x.as_str())
            {
                tarla_sidecar::NetworkSpec::EasyTierStatic {
                    network_name: network.easytier_network_name.clone(),
                    secret_env: key,
                    peers: seeds,
                    ipv4: ip.into(),
                }
            } else {
                tarla_sidecar::NetworkSpec::EasyTier {
                    network_name: network.easytier_network_name.clone(),
                    secret_env: key,
                    peers: seeds,
                }
            };
            specs.push(("easytier", spec));
        }
        if network.enable_tailscale {
            specs.push(("tailscale", tarla_sidecar::NetworkSpec::Tailscale));
        }
        for (name, spec) in specs {
            let ports = tarla_sidecar::NetworkPorts {
                api: actual,
                easytier_listen: network.easytier_listen,
            };
            if let Some(net) = tarla_sidecar::start_network_for(&tc, &runtime, &spec, ports).await?
            {
                transports.push((name, net))
            }
        }
    }
    let mut http = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(10))
        .redirect(reqwest::redirect::Policy::none());
    if !transports.is_empty() {
        let preferred = network
            .extra
            .get("preferred")
            .and_then(|x| x.as_str())
            .unwrap_or(transports[0].0);
        let net = &transports
            .iter()
            .find(|(name, _)| *name == preferred)
            .context("preferred network is disabled")?
            .1;
        http = http.proxy(reqwest::Proxy::all(net.proxy_url())?);
    }
    if let Some(entries) = table.as_table_mut() {
        for (_, entry) in entries.iter_mut() {
            if let Some(proxy) = entry
                .get("socks_proxy")
                .and_then(|x| x.as_str())
                .and_then(|x| x.strip_prefix("managed:"))
            {
                let url = transports
                    .iter()
                    .find(|(name, _)| *name == proxy)
                    .context("brain requires an unavailable managed transport")?
                    .1
                    .proxy_url();
                entry
                    .as_table_mut()
                    .context("invalid backend")?
                    .insert("socks_proxy".into(), toml::Value::String(url));
            }
        }
    }
    let brain = SupervisorEngine::new(
        tarla_supervisor::backends_from_config_with_slots(
            &ws.config.supervisor.brain.fallback_chain,
            &table.try_into()?,
            Some(&fleet::subscription_registry(&home, &db_path, &slots)?),
        )?,
        ws.config.supervisor.brain.max_tool_steps,
    )
    .with_window_budget(ws.config.context_policy.supervisor_active_budget_tokens);
    let mut hosted_index = None;
    let distiller = ws
        .config
        .memory
        .distiller
        .as_ref()
        .map(local_distiller::LocalDistiller::new)
        .transpose()?;
    let memory_index = match ws.config.memory.primary_backend.as_str() {
        "builtin_sqlite" => None,
        "mnemosyne" => {
            let memory = conf.get("memory").context("missing [memory] config")?;
            let interpreter=memory.get("mnemosyne_python").and_then(toml::Value::as_str)
                .context("set [memory].mnemosyne_python to the absolute path of an interpreter with mnemosyne-memory installed")?;
            let database = memory
                .get("mnemosyne_db_path")
                .and_then(toml::Value::as_str)
                .map(Path::new);
            Some(tarla_memory::MnemosyneIndex::new(
                PathBuf::from(interpreter),
                &ws.root,
                database,
            )?)
        }
        provider @ ("mem0"|"honcho") => {
            if ws.config.workspace.node_id == ws.config.workspace.supervisor_node_id && ws.config.workspace.role_mode != "executor_only" {
                let cfg=conf.get("memory").context("configure [memory]")?;
                let namespace=cfg.get("cloud_namespace").and_then(toml::Value::as_str).context("set a unique [memory].cloud_namespace for this deployment")?;
                let key_env=cfg.get("api_key_env").and_then(toml::Value::as_str).unwrap_or(if provider=="mem0" {"MEM0_API_KEY"} else {"HONCHO_API_KEY"});
                let key=std::env::var(key_env).with_context(||format!("missing memory provider credential in {key_env}"))?;
                hosted_index=Some(tarla_memory::HostedIndex::new(provider,&ws.config.workspace.id,namespace,cfg.get("endpoint").and_then(toml::Value::as_str),key)?);
            }
            None
        }
        other=>bail!("unsupported memory.primary_backend '{other}'; use builtin_sqlite, mnemosyne, mem0 or honcho"),
    };
    let host = if actual.ip().is_unspecified() {
        format!("127.0.0.1:{}", actual.port())
    } else {
        actual.to_string()
    };
    let service = Arc::new(Service {
        distiller,
        distill_wake: Default::default(),
        memory_index,
        hosted_index,
        local_url: format!("http://{host}"),
        transcripts: Default::default(),
        drafts: Default::default(),
        ws,
        home,
        db_path,
        brain,
        slots,
        peers,
        locks: Default::default(),
        account_locks: Default::default(),
        http: http.build()?,
        notify: Default::default(),
        s2_changed: tokio::sync::watch::channel(0).0,
        sync_wake: Default::default(),
        receipt_wake: Default::default(),
        context_wake: Default::default(),
        reload: Default::default(),
        authority_change: Default::default(),
        retired: Default::default(),
    });

    let routes = Router::new()
        .route("/v1/conversations/{id}/draft", get(drafts::get))
        .route("/v1/conversations", get(api::conversations))
        .route(
            "/v1/conversations/{id}/messages",
            get(api::messages).post(api::post_message),
        )
        .route("/v1/conversations/{id}/binding", put(api::binding))
        .route("/v1/runs", get(api::runs).post(api::create_run))
        .route("/v1/runs/{id}", get(api::get_run))
        .route("/v1/runs/{id}/log", get(api::logs))
        .route("/v1/runs/{id}/cancel", post(api::cancel))
        .route("/v1/runs/{id}/abandon", post(launch::abandon))
        .route("/v1/runs/{id}/attach", post(terminal::issue))
        .route("/v1/memories", get(api::memories).post(api::put_memory))
        .route("/v1/actions", get(api::actions))
        .route("/v1/actions/{id}", post(api::resolve))
        .route("/v1/facts", get(api::facts))
        .route("/v1/slots", get(api::slots))
        .route("/v1/nodes", get(api::nodes))
        .route("/v1/recovery", get(s1::recovery_status))
        .route(
            "/v1/slots/{slot}/telemetry-token",
            post(slot_reports::issue),
        )
        .layer(axum::middleware::from_fn_with_state(
            service.clone(),
            api::client_auth,
        ))
        .route("/health", get(api::health))
        .route("/v1/terminal/{id}", get(terminal::socket))
        .route("/telemetry/v1/slots/{slot}", post(slot_reports::ingest))
        .route("/hooks/v1/{id}/{op}", post(hooks::callback))
        .route("/peer/v1/{op}", post(peers::receive))
        .with_state(service.clone());

    let mut routes = routes;
    if let Some(config) = tarla_gateway::GatewayConfig::from_env(
        &service.ws.root,
        &format!("http://{host}"),
        &service.ws.token,
    )? {
        routes = routes.nest("/gateway", tarla_gateway::router(config).await?);
    }
    let _local_endpoint = if a.publish_local_endpoint {
        routes = routes.merge(local_endpoint::proof_route(service.clone()));
        Some(local_endpoint::publish(&service.ws.root, actual)?)
    } else {
        None
    };
    tokio::spawn(fleet::worker(service.clone()));
    authority::start(&service);
    peers::start_s2_notifications(&service);
    s1::startup(&service);
    tokio::spawn(peers::replicate(service.clone()));
    tokio::spawn(handoff::transfers(service.clone()));
    tokio::spawn(control::deliver_receipts(service.clone()));
    tokio::spawn(distillation::worker(service.clone()));
    tokio::spawn(maintenance::maintain_context(service.clone()));
    if service.hosted_index.is_some() {
        tokio::spawn(control::publish_memories(service.clone()));
    }
    println!(
        "tarlad {} {} listening on {}",
        service.id(),
        service.node(),
        actual
    );
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(listener, routes.layer(DefaultBodyLimit::max(128 * 1024)))
        .with_graceful_shutdown(async {
            let _ = stopped.await;
        })
        .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => { result?; Ok(false) },
        _ = service.reload.notified() => Ok(true),
        _ = shutdown_signal() => {
            let _ = stop.send(());
            // A slow HTTP client or terminal viewer must not retain the workspace
            // lock on shutdown. Herdr holds the actual jobs independently.
            if let Ok(result) = tokio::time::timeout(Duration::from_secs(3), &mut server).await {
                result?;
            }
            Ok(false)
        }
    }
}

async fn shutdown_signal() {
    #[cfg(unix)]
    if let Ok(mut term) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
    {
        tokio::select! {
            _ = term.recv() => (),
            _ = tokio::signal::ctrl_c() => (),
        }
        return;
    }
    let _ = tokio::signal::ctrl_c().await;
}
