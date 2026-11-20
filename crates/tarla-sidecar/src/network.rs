//! Workspace-owned userspace transports. A stable loopback frontend keeps all
//! peer and model connections on the chosen mesh across native process restarts.
use crate::{invalid, managed::clean_command, private_dir, Toolchain};
use serde::{Deserialize, Serialize};
use serde_json::json;
#[cfg(windows)]
use sha2::{Digest, Sha256};
use std::{
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{self, Write},
    net::{Ipv4Addr, SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process::Child,
    time::{sleep, timeout},
};
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NetworkSpec {
    ReuseHostNetwork,
    EasyTier {
        network_name: String,
        secret_env: String,
        peers: Vec<String>,
    },
    EasyTierStatic {
        network_name: String,
        secret_env: String,
        peers: Vec<String>,
        ipv4: String,
    },
    Tailscale,
}
pub struct ManagedNetwork {
    supervisor: tokio::task::JoinHandle<()>,
    state: Arc<Mutex<Status>>,
    socks5: SocketAddr,
}
impl ManagedNetwork {
    pub fn proxy_url(&self) -> String {
        format!("socks5h://{}", self.socks5)
    }
    pub fn is_running(&mut self) -> io::Result<bool> {
        let state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("transport state unavailable"))?;
        Ok(state.online && !self.supervisor.is_finished())
    }
}
impl Drop for ManagedNetwork {
    fn drop(&mut self) {
        self.supervisor.abort();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        state.closed = true;
        let ours = fs::read(&state.record)
            .ok()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
            .is_some_and(|v| v["owner"] == state.owner);
        if ours {
            let _ = fs::remove_file(&state.record);
        }
    }
}

struct Status {
    owner: String,
    record: PathBuf,
    online: bool,
    closed: bool,
}

struct Transport {
    child: Child,
    socks: SocketAddr,
    rpc: String,
}
struct Supervisor {
    tools: Toolchain,
    runtime: PathBuf,
    spec: NetworkSpec,
    api_port: Option<u16>,
    listen: Option<SocketAddr>,
    state: Arc<Mutex<Status>>,
    frontend: tokio::net::TcpListener,
}

impl Supervisor {
    fn publish(&self, transport: Option<&Transport>) -> io::Result<()> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| io::Error::other("transport state unavailable"))?;
        if state.closed {
            return Err(io::Error::other("workspace transport stopped"));
        }
        let online = transport.is_some();
        let data = json!({"status": if online {"ready"} else {"reconnecting"},
            "pid": transport.and_then(|t| t.child.id()), "socks5": self.frontend.local_addr()?,
            "rpc": transport.map(|t| &t.rpc), "api_port": self.api_port, "owner": state.owner});
        state.online = false;
        write_record(&state.record, &serde_json::to_vec(&data)?)?;
        state.online = online;
        Ok(())
    }

    async fn run(self, mut transport: Transport) {
        let mut sessions = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                _ = transport.child.wait() => {},
                accepted = self.frontend.accept() => {
                    let (mut client, _) = match accepted {
                        Ok(pair) => pair,
                        Err(e) => {
                            eprintln!("managed SOCKS accept deferred: {e}");
                            sleep(Duration::from_millis(100)).await;
                            continue;
                        }
                    };
                    let upstream = transport.socks;
                    sessions.spawn(async move {
                        if let Ok(mut stream) = TcpStream::connect(upstream).await {
                            let _ = tokio::io::copy_bidirectional(&mut client, &mut stream).await;
                        }
                    });
                    continue;
                }
                _ = sessions.join_next(), if !sessions.is_empty() => continue,
            }
            sessions.shutdown().await;
            if let Err(e) = self.publish(None) {
                eprintln!("unable to publish managed transport outage: {e}");
            }
            // Keep the advertised endpoint bound for the lifetime of the workspace.
            // During an outage clients reconnect via their ordinary retry paths.
            let restart = async {
                let mut delay = Duration::from_millis(250);
                loop {
                    sleep(delay).await;
                    match spawn_transport(
                        &self.tools,
                        &self.runtime,
                        &self.spec,
                        self.api_port,
                        self.listen,
                    )
                    .await
                    {
                        Ok(t) => match self.publish(Some(&t)) {
                            Ok(()) => break t,
                            Err(e) => eprintln!("unable to publish restarted transport: {e}"),
                        },
                        Err(e) => eprintln!("managed transport restart deferred: {e}"),
                    }
                    delay = (delay * 2).min(Duration::from_secs(10));
                }
            };
            tokio::pin!(restart);
            loop {
                tokio::select! {
                    replacement = &mut restart => { transport = replacement; break; },
                    accepted = self.frontend.accept() => {
                        if accepted.is_err() { sleep(Duration::from_millis(100)).await; }
                    }
                }
            }
        }
    }
}

fn write_record(path: &Path, data: &[u8]) -> io::Result<()> {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let tmp = path.with_extension(format!("{}-{suffix}.tmp", std::process::id()));
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let mut file = opts.open(&tmp)?;
    let result = (|| {
        file.write_all(data)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&tmp, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(tmp);
    }
    result
}

async fn socks_ready(addr: SocketAddr) -> bool {
    let handshake = async {
        let mut stream = TcpStream::connect(addr).await?;
        stream.write_all(&[5u8, 1, 0]).await?;
        let mut reply = [0u8; 2];
        stream.read_exact(&mut reply).await?;
        io::Result::Ok(reply == [5, 0])
    };
    matches!(
        timeout(Duration::from_millis(400), handshake).await,
        Ok(Ok(true))
    )
}

/// Start an owned network process in userspace, then publish its actual private
/// endpoint only after a successful SOCKS handshake. A separate tailnet login
/// is required for a new Tailscale state, using this workspace's local socket.
pub async fn start_network(
    tools: &Toolchain,
    runtime: &Path,
    spec: &NetworkSpec,
) -> io::Result<Option<ManagedNetwork>> {
    start_network_impl(tools, runtime, spec, None, None).await
}

pub struct NetworkPorts {
    pub api: SocketAddr,
    pub easytier_listen: Option<SocketAddr>,
}

impl From<SocketAddr> for NetworkPorts {
    fn from(api: SocketAddr) -> Self {
        Self {
            api,
            easytier_listen: None,
        }
    }
}

/// Publish only the authenticated API port through a managed userspace mesh.
/// Both backends connect incoming tailnet/overlay TCP flows to local IPv4
/// loopback on the same port. Tailscale's grants are set by the tailnet admin.
pub async fn start_network_for(
    tools: &Toolchain,
    runtime: &Path,
    spec: &NetworkSpec,
    ports: impl Into<NetworkPorts>,
) -> io::Result<Option<ManagedNetwork>> {
    let NetworkPorts {
        api,
        easytier_listen,
    } = ports.into();
    if !api.is_ipv4() || !(api.ip().is_loopback() || api.ip().is_unspecified()) || api.port() == 0 {
        return Err(invalid(
            "managed mesh ingress requires a bound IPv4 loopback-compatible API address",
        ));
    }
    start_network_impl(tools, runtime, spec, Some(api.port()), easytier_listen).await
}

fn easytier_identity(runtime: &Path) -> io::Result<uuid::Uuid> {
    let file = runtime.join("easytier-instance");
    match fs::read_to_string(&file) {
        Ok(id) => uuid::Uuid::parse_str(id.trim())
            .map_err(|_| invalid("invalid saved EasyTier instance id")),
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            let id = uuid::Uuid::new_v4();
            write_record(&file, id.to_string().as_bytes())?;
            Ok(id)
        }
        Err(e) => Err(e),
    }
}

fn address_config(spec: &NetworkSpec) -> io::Result<String> {
    if let NetworkSpec::EasyTierStatic { ipv4, .. } = spec {
        let (ip, prefix) = ipv4.split_once('/').unwrap_or((ipv4, "24"));
        let ip = ip
            .parse::<Ipv4Addr>()
            .map_err(|_| invalid("invalid EasyTier IPv4 address"))?;
        let prefix = prefix
            .parse::<u8>()
            .map_err(|_| invalid("invalid EasyTier prefix length"))?;
        if ip.is_loopback()
            || ip.is_unspecified()
            || ip.is_multicast()
            || ip.is_broadcast()
            || prefix == 0
            || prefix > 32
        {
            return Err(invalid("invalid EasyTier virtual IPv4 CIDR"));
        }
        return Ok(format!("dhcp = false\nipv4 = \"{ip}/{prefix}\""));
    }
    Ok("dhcp = true".into())
}

async fn start_network_impl(
    tools: &Toolchain,
    runtime: &Path,
    spec: &NetworkSpec,
    api_port: Option<u16>,
    easytier_listen: Option<SocketAddr>,
) -> io::Result<Option<ManagedNetwork>> {
    if matches!(spec, NetworkSpec::ReuseHostNetwork) {
        return Ok(None);
    }
    private_dir(runtime)?;
    let runtime = runtime.canonicalize()?;
    let name = match spec {
        NetworkSpec::Tailscale => "tailscale",
        _ => "easytier",
    };
    let record = runtime.join(format!("{name}.json"));
    let owner = uuid::Uuid::new_v4().to_string();
    let state = Arc::new(Mutex::new(Status {
        owner,
        record,
        closed: false,
        online: false,
    }));
    let frontend = tokio::net::TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let socks5 = frontend.local_addr()?;
    let transport = spawn_transport(tools, &runtime, spec, api_port, easytier_listen).await?;
    let manager = Supervisor {
        tools: tools.clone(),
        runtime,
        spec: spec.clone(),
        api_port,
        listen: easytier_listen,
        state: state.clone(),
        frontend,
    };
    manager.publish(Some(&transport))?;
    Ok(Some(ManagedNetwork {
        supervisor: tokio::spawn(manager.run(transport)),
        state,
        socks5,
    }))
}

async fn spawn_transport(
    tools: &Toolchain,
    runtime: &Path,
    spec: &NetworkSpec,
    api_port: Option<u16>,
    easytier_listen: Option<SocketAddr>,
) -> io::Result<Transport> {
    let reserve = || TcpListener::bind((Ipv4Addr::LOCALHOST, 0));
    let socks_guard = reserve()?;
    let socks = socks_guard.local_addr()?;
    let rpc_guard = reserve()?;
    let rpc_addr = rpc_guard.local_addr()?;
    let mut config = None;
    let (mut cmd, name, rpc) = match spec {
        NetworkSpec::ReuseHostNetwork => unreachable!(),
        NetworkSpec::EasyTier {
            network_name,
            secret_env,
            peers,
        }
        | NetworkSpec::EasyTierStatic {
            network_name,
            secret_env,
            peers,
            ..
        } => {
            if network_name.is_empty() || secret_env.is_empty() {
                return Err(invalid("EasyTier requires a private network identity"));
            }
            let secret = crate::secrets::resolve(secret_env).await?;
            if secret.is_empty() {
                return Err(invalid("EasyTier secret must be nonempty"));
            }
            // v2.7.0's --socks5 accepts only a port and binds 0.0.0.0. Use
            // the structured config to explicitly constrain SOCKS to loopback.
            let q = |s: &str| serde_json::to_string(s).map_err(io::Error::from);
            let listen = easytier_listen.unwrap_or_else(|| (Ipv4Addr::UNSPECIFIED, 0).into());
            let mut toml = format!(
                concat!(
                    "instance_id = \"{}\"\n{}\nlisteners = [\"tcp://{}\"]\n",
                    "socks5_proxy = \"socks5://{}\"\n",
                    "tcp_whitelist = [\"{}\"]\nudp_whitelist = [\"0\"]\n",
                    "[network_identity]\nnetwork_name = {}\nnetwork_secret = {}\n",
                    // With no kernel TUN there are no overlay routes to exclude.
                    // Pinning a connector to a physical NIC prevents loopback
                    // (and some split-routed) bootstrap addresses from working.
                    "[flags]\nno_tun = true\nbind_device = false\nprivate_mode = true\n"
                ),
                easytier_identity(runtime)?,
                address_config(spec)?,
                listen,
                socks,
                api_port.unwrap_or(0),
                q(network_name)?,
                q(&secret)?
            );
            for peer in peers {
                toml.push_str(&format!("[[peer]]\nuri = {}\n", q(peer)?));
            }
            let mut c = clean_command(&tools.verified("easytier")?);
            c.args([
                "--config-file",
                "-",
                "--disable-env-parsing",
                "--rpc-portal",
                &rpc_addr.to_string(),
            ])
            .stdin(Stdio::piped());
            config = Some(toml);
            (c, "easytier", rpc_addr.to_string())
        }
        NetworkSpec::Tailscale => {
            let mut c = clean_command(&tools.verified("tailscale")?);
            // The host may trust a private CA for a self-hosted Headscale server.
            for key in ["SSL_CERT_FILE", "SSL_CERT_DIR"] {
                if let Some(value) = std::env::var_os(key) {
                    c.env(key, value);
                }
            }
            #[cfg(not(windows))]
            let socket = runtime.join("ts.sock").into_os_string();
            #[cfg(windows)]
            let socket = std::ffi::OsString::from(format!(
                r"\\.\pipe\tarla-ts-{:x}",
                Sha256::digest(runtime.to_string_lossy().as_bytes())
            ));
            let state = runtime.join("ts.state");
            // v1.82 derives its auxiliary VarRoot only when the state file's
            // parent is named "tailscale". Keep certs and other files private
            // while preserving established node identities in ts.state.
            let state_dir = runtime.join("tailscale");
            private_dir(&state_dir)?;
            c.args([
                OsStr::new("--tun=userspace-networking"),
                OsStr::new("--state"),
                state.as_os_str(),
                OsStr::new("--statedir"),
                state_dir.as_os_str(),
                OsStr::new("--socket"),
                &socket,
            ])
            .arg("--port=0")
            .arg(format!("--socks5-server={socks}"));
            (c, "tailscale", socket.to_string_lossy().into_owned())
        }
    };
    let err_log = runtime.join(format!("{name}.log"));
    let mut opts = OpenOptions::new();
    opts.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let log = opts.open(err_log)?;
    cmd.current_dir(runtime)
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .kill_on_drop(true);
    #[cfg(target_os = "linux")]
    cmd.as_std_mut().prctl_parent_death();
    drop((socks_guard, rpc_guard));
    let mut child = cmd.spawn()?;
    if let Some(toml) = config {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| invalid("no private config pipe"))?;
        stdin.write_all(toml.as_bytes()).await?;
        stdin.shutdown().await?;
    }
    timeout(Duration::from_secs(12), async {
        loop {
            if let Some(status) = child.try_wait()? {
                return Err(io::Error::other(format!(
                    "{name} exited ({status}); inspect private runtime log"
                )));
            }
            if socks_ready(socks).await {
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }
        io::Result::Ok(())
    })
    .await??;
    Ok(Transport { child, socks, rpc })
}

// A workspace-owned network daemon cannot outlive tarlad on Linux. Herdr's
// holder processes deliberately do outlive it and are not started this way.
#[cfg(target_os = "linux")]
trait LinuxChild {
    fn prctl_parent_death(&mut self);
}
#[cfg(target_os = "linux")]
impl LinuxChild for std::process::Command {
    fn prctl_parent_death(&mut self) {
        use std::os::unix::process::CommandExt;
        let parent = std::process::id();
        unsafe {
            self.pre_exec(move || {
                if libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM) != 0 {
                    return Err(io::Error::last_os_error());
                }
                if libc::getppid() as u32 != parent {
                    return Err(io::Error::new(io::ErrorKind::Interrupted, "parent exited"));
                }
                Ok(())
            });
        }
    }
}
