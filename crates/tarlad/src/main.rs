mod api;
mod control;
mod fleet;
mod peers;
mod terminal;

use std::{collections::BTreeMap,net::SocketAddr,path::{Path,PathBuf},sync::Arc,time::{Duration,SystemTime,UNIX_EPOCH}};
use anyhow::{anyhow,bail,Context,Result};
use axum::{extract::DefaultBodyLimit,http::StatusCode,response::{IntoResponse,Response},routing::{get,post,put},Json,Router};
use clap::Parser;
use rusqlite::{params_from_iter,Connection};
use serde_json::{json,Value};
use sha2::{Digest,Sha256};
use subtle::ConstantTimeEq;
use tarla_supervisor::SupervisorEngine;
use tarla_workspace::Workspace;
use uuid::Uuid;

#[derive(Parser)]
#[command(version,about="Tarla headless workspace service")]
struct Args {
    #[arg(long,env="TARLA_HOME")] home: Option<PathBuf>,
    #[arg(long,env="TARLA_WORKSPACE",default_value="default")] workspace:String,
    #[arg(long,env="TARLA_LISTEN",default_value="127.0.0.1:7878")] listen:SocketAddr
}

struct Service {
    memory_index: Option<tarla_memory::MnemosyneIndex>,
    ws:Workspace, home:PathBuf, db_path:PathBuf, brain:SupervisorEngine,
    slots:Vec<fleet::Slot>, peers:BTreeMap<String,peers::Peer>,
    locks: tokio::sync::Mutex<BTreeMap<String,std::sync::Weak<tokio::sync::Mutex<()>>>>,
    account_locks:std::sync::Mutex<BTreeMap<String,std::fs::File>>,http:reqwest::Client, notify:tokio::sync::Notify,
}
type S=Arc<Service>;
impl Service {
    fn db(&self)->Result<Connection>{
        let c=Connection::open(&self.db_path)?;
        c.busy_timeout(Duration::from_secs(5))?;
        c.pragma_update(None,"foreign_keys",true)?;
        Ok(c)
    }
    fn id(&self)->&str { &self.ws.config.workspace.id }
    fn node(&self)->&str { &self.ws.config.workspace.node_id }
    fn supervisor(&self)->&str { &self.ws.config.workspace.supervisor_node_id }
    fn is_supervisor(&self)->bool { self.supervisor()==self.node() && self.ws.config.workspace.role_mode!="executor_only" }
    fn can_execute(&self)->bool { self.ws.config.workspace.role_mode!="supervisor_only" }
    fn term(&self)->i64 { self.ws.config.workspace.supervisor_term }
    async fn lock(&self,id:&str)->tokio::sync::OwnedMutexGuard<()>{
        let mut map=self.locks.lock().await;
        map.retain(|_,v|v.strong_count()>0);
        let key=map.get(id).and_then(|w|w.upgrade()).unwrap_or_else(||{
            let m=Arc::new(tokio::sync::Mutex::new(())); map.insert(id.into(),Arc::downgrade(&m)); m
        });
        drop(map);
        key.lock_owned().await
    }
}
fn now()->i64 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64 }
fn uid()->String {Uuid::now_v7().to_string()}
fn secret_equal(a:&str,b:&str)->bool{bool::from(Sha256::digest(a.as_bytes()).ct_eq(&Sha256::digest(b.as_bytes())))}
fn validate_id(s:&str)->Result<()>{if s.is_empty()||s.len()>160||!s.bytes().all(|b|b.is_ascii_alphanumeric()||b"-_.:@".contains(&b)){bail!("invalid identifier")} Ok(())}
fn string<'a>(v:&'a Value,key:&str)->Result<&'a str>{v.get(key).and_then(Value::as_str).filter(|s|!s.trim().is_empty()).ok_or_else(||anyhow!("missing {key}"))}
fn list(c:&Connection,sql:&str,args:&[&dyn rusqlite::ToSql])->Result<Vec<Value>>{
    let mut s=c.prepare(sql)?;
    let names=s.column_names().iter().map(|x|x.to_string()).collect::<Vec<_>>();
    let rows=s.query_map(params_from_iter(args.iter()),|row|{
        use rusqlite::types::ValueRef::*;
        let mut m=serde_json::Map::new();
        for (i,name) in names.iter().enumerate(){let val=match row.get_ref(i)?{
            Null=>Value::Null,Integer(n)=>json!(n),Real(n)=>json!(n),Text(v)=>json!(String::from_utf8_lossy(v)),Blob(_)=>Value::Null
        };m.insert(name.clone(),val);}
        Ok(Value::Object(m))
    })?; Ok(rows.collect::<rusqlite::Result<_>>()?)
}
type Api<T=Json<Value>>=std::result::Result<T,ApiError>;
struct ApiError(StatusCode,String);
impl From<anyhow::Error> for ApiError { fn from(e:anyhow::Error)->Self{Self(StatusCode::BAD_REQUEST,e.to_string())} }
impl From<rusqlite::Error> for ApiError { fn from(e:rusqlite::Error)->Self{eprintln!("workspace database: {e}"); Self(StatusCode::INTERNAL_SERVER_ERROR,"workspace database error".into())} }
impl IntoResponse for ApiError {fn into_response(self)->Response{(self.0,Json(json!({"error":self.1}))).into_response()}}
fn ensure_supervisor(s:&Service)->Api<()> {if !s.is_supervisor(){return Err(ApiError(StatusCode::CONFLICT,"operation requires the designated supervisor".into()))}Ok(())}

const MIGRATIONS:&str=include_str!("service.sql");
#[tokio::main]
async fn main()->Result<()>{
    let a=Args::parse(); let home=a.home.or_else(||std::env::var_os("HOME").or_else(||std::env::var_os("USERPROFILE")).map(|p|PathBuf::from(p).join(".tarla"))).context("specify --home")?;
    let ws=Workspace::open(&home,&a.workspace)?;
    let guard=std::fs::OpenOptions::new().read(true).write(true).create(true).truncate(false).open(ws.root.join("runtime/daemon.lock"))?;
    fs2::FileExt::try_lock_exclusive(&guard).context("this workspace daemon or its offline administration command already owns the workspace lock")?;
    // Re-read identity after locking to serialize startup against handover.
    let ws=Workspace::open(&home,&a.workspace)?;
    let conf=std::fs::read_to_string(ws.root.join("workspace.toml"))?.parse::<toml::Table>()?;
    let table=conf.get("supervisor").and_then(|s|s.get("brain")).and_then(|s|s.get("backends")).cloned().unwrap_or_else(||toml::Value::Table(toml::Table::new()));
    let brain=SupervisorEngine::new(tarla_supervisor::backends_from_config(&ws.config.supervisor.brain.fallback_chain,&table.try_into()?)?,ws.config.supervisor.brain.max_tool_steps).with_window_budget(ws.config.context_policy.supervisor_active_budget_tokens);
    let db_path=ws.root.join("state.db");
    let mut initial=tarla_memory::open(&db_path,&a.workspace)?;
    initial.execute_batch(MIGRATIONS)?;
    peers::check_term(&mut initial,&ws)?;
    let slots=fleet::load(&conf,&ws,&home)?;
    let peers=peers::load(&conf,&ws)?;
    let runtime=ws.root.join("runtime");
    let network=&ws.config.network;
    let mut transports=Vec::new();
    if network.mode=="managed_sidecar" && (network.enable_easytier || network.enable_tailscale) {
        let tc=tarla_sidecar::Toolchain::load(&home.join("bin"))?;
        let mut specs=Vec::new();
        if network.enable_easytier {
            let key=network.easytier_network_secret_ref.strip_prefix("env:").context("EasyTier requires env:VARIABLE secret reference")?;
            let seeds=network.extra.get("easytier_peers").cloned().map(toml::Value::try_into).transpose()?.unwrap_or_default();
            specs.push(("easytier",tarla_sidecar::NetworkSpec::EasyTier{network_name:network.easytier_network_name.clone(),secret_env:key.into(),peers:seeds}));
        }
        if network.enable_tailscale {specs.push(("tailscale",tarla_sidecar::NetworkSpec::Tailscale));}
        for (name,spec) in specs {if let Some(net)=tarla_sidecar::start_network(&tc,&runtime,&spec).await?{transports.push((name,net))}}
    }
    let mut http=reqwest::Client::builder().no_proxy().timeout(Duration::from_secs(10)).redirect(reqwest::redirect::Policy::none());
    if !transports.is_empty(){
        let preferred=network.extra.get("preferred").and_then(|x|x.as_str()).unwrap_or(transports[0].0);
        let net=&transports.iter().find(|(name,_)|*name==preferred).context("preferred network is disabled")?.1;
        http=http.proxy(reqwest::Proxy::all(net.proxy_url())?);
    }
    let memory_index=match ws.config.memory.primary_backend.as_str() {
        "builtin_sqlite" => None,
        "mnemosyne" => {
            let memory=conf.get("memory").context("missing [memory] config")?;
            let interpreter=memory.get("mnemosyne_python").and_then(toml::Value::as_str)
                .context("set [memory].mnemosyne_python to the absolute path of an interpreter with mnemosyne-memory installed")?;
            let database=memory.get("mnemosyne_db_path").and_then(toml::Value::as_str).map(Path::new);
            Some(tarla_memory::MnemosyneIndex::new(PathBuf::from(interpreter),&ws.root,database)?)
        }
        other=>bail!("unsupported memory.primary_backend '{other}'; use builtin_sqlite or mnemosyne"),
    };
    let service=Arc::new(Service{memory_index,ws,home,db_path,brain,slots,peers,locks:Default::default(),account_locks:Default::default(),http:http.build()?,notify:Default::default()});
    let listener=tokio::net::TcpListener::bind(a.listen).await?;
    let actual=listener.local_addr()?;
    let routes=Router::new()
        .route("/v1/conversations",get(api::conversations)).route("/v1/conversations/{id}/messages",get(api::messages).post(api::post_message))
        .route("/v1/conversations/{id}/binding",put(api::binding))
        .route("/v1/runs",get(api::runs).post(api::create_run)).route("/v1/runs/{id}",get(api::get_run))
        .route("/v1/runs/{id}/log",get(api::logs)).route("/v1/runs/{id}/cancel",post(api::cancel))
        .route("/v1/runs/{id}/attach",post(terminal::issue))
        .route("/v1/memories",get(api::memories).post(api::put_memory)).route("/v1/actions",get(api::actions)).route("/v1/actions/{id}",post(api::resolve))
        .route("/v1/facts",get(api::facts)).route("/v1/slots",get(api::slots)).route("/v1/nodes",get(api::nodes))
        .layer(axum::middleware::from_fn_with_state(service.clone(),api::client_auth))
        .route("/health",get(api::health)).route("/v1/terminal/{id}",get(terminal::socket))
        .route("/peer/v1/{op}",post(peers::receive)).with_state(service.clone());
    let host=if actual.ip().is_unspecified(){format!("127.0.0.1:{}",actual.port())}else{actual.to_string()};
    let mut routes=routes;
    if let Some(config)=tarla_gateway::GatewayConfig::from_env(&service.ws.root,&format!("http://{host}"),&service.ws.token)?{
        routes=routes.nest("/gateway",tarla_gateway::router(config).await?);
    }
    tokio::spawn(fleet::worker(service.clone()));
    tokio::spawn(peers::replicate(service.clone()));
    tokio::spawn(control::deliver_receipts(service.clone()));
    println!("tarlad {} {} listening on {}",service.id(),service.node(),actual);
    axum::serve(listener,routes.layer(DefaultBodyLimit::max(128*1024))).with_graceful_shutdown(async {let _=tokio::signal::ctrl_c().await;}).await?;
    Ok(())
}
