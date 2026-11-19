use crate::{invalid, private_dir, Herdr};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    ffi::OsStr,
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpListener},
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    process::{Child, Command},
    time::{sleep, timeout},
};

#[derive(Deserialize)]
struct PinnedTool {
    version: String,
    file: String,
    sha256: String,
}
#[derive(Deserialize)]
struct Manifest {
    tools: BTreeMap<String, PinnedTool>,
}

fn component(id: &str) -> bool {
    !id.is_empty()
        && id != "."
        && id != ".."
        && !id.starts_with('.')
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
}

/// Trust root is the operator's installed private manifest. Nothing resolves from PATH.
pub struct Toolchain {
    bin: PathBuf,
    manifest: Manifest,
}
impl Toolchain {
    pub fn load(bin: &Path) -> io::Result<Self> {
        let bin = bin.canonicalize()?;
        let manifest =
            serde_json::from_reader(fs::File::open(bin.join("toolchain-manifest.json"))?)?;
        Ok(Self { bin, manifest })
    }
    pub fn verified(&self, id: &str) -> io::Result<PathBuf> {
        let tool = self
            .manifest
            .tools
            .get(id)
            .ok_or_else(|| invalid(format!("tool not pinned: {id}")))?;
        if ![id, &tool.file, &tool.version].into_iter().all(component)
            || tool.sha256.len() != 64
            || !tool.sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(invalid("invalid tool pin"));
        }
        let file = self
            .bin
            .join(id)
            .join(&tool.version)
            .join(&tool.file)
            .canonicalize()?;
        if !file.starts_with(&self.bin) || !file.is_file() {
            return Err(invalid("tool escapes binary cache"));
        }
        let mut reader = fs::File::open(&file)?;
        let mut hash = Sha256::new();
        let mut buf = [0u8; 65536];
        loop {
            let count = reader.read(&mut buf)?;
            if count == 0 {
                break;
            }
            hash.update(&buf[..count]);
        }
        if !format!("{:x}", hash.finalize()).eq_ignore_ascii_case(&tool.sha256) {
            return Err(invalid(format!("checksum mismatch: {id}")));
        }
        Ok(file)
    }
    pub async fn ensure_herdr(&self, herdr: &Herdr) -> io::Result<()> {
        if herdr.available().await.is_ok() {
            return Ok(());
        }
        let mut cmd = clean_command(&self.verified("herdr-core")?);
        cmd.current_dir(&herdr.runtime)
            .envs(herdr.env()?)
            .args(["server", "start"])
            .kill_on_drop(true);
        let status = timeout(Duration::from_secs(15), cmd.status()).await??;
        if !status.success() {
            return Err(io::Error::other(format!(
                "herdr-core startup failed ({status})"
            )));
        }
        timeout(Duration::from_secs(5), async {
            loop {
                if herdr.available().await.is_ok() {
                    break;
                }
                sleep(Duration::from_millis(50)).await
            }
        })
        .await?;
        Ok(())
    }
}

fn clean_command(program: &Path) -> Command {
    let mut cmd = Command::new(program);
    cmd.env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    for key in [
        "PATH",
        "HOME",
        "USER",
        "LOGNAME",
        "SHELL",
        "LANG",
        "LC_ALL",
        "TMPDIR",
        "TMP",
        "TEMP",
        "SystemRoot",
        "USERPROFILE",
        "LOCALAPPDATA",
        "APPDATA",
        "COMSPEC",
        "USERNAME",
        "PATHEXT",
    ] {
        if let Some(val) = std::env::var_os(key) {
            cmd.env(key, val);
        }
    }
    cmd
}

/// Acquire the same account lock used by stateless supervisor borrowing.
pub fn try_lock_slot(home: &Path, slot_id: &str) -> io::Result<fs::File> {
    if !component(slot_id) {
        return Err(invalid("invalid slot name"));
    }
    let dir = home.join("slots").join(slot_id);
    private_dir(&dir)?;
    let mut opts = OpenOptions::new();
    opts.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let guard = opts.open(dir.join(".tarla-slot.lock"))?;
    fs2::FileExt::try_lock_exclusive(&guard)?;
    Ok(guard)
}

/// Each recognized CLI receives only its own protected credential directory.
pub fn credential_environment(
    home: &Path,
    slot_id: &str,
    driver: &str,
) -> io::Result<BTreeMap<String, String>> {
    if !component(slot_id) {
        return Err(invalid("invalid slot name"));
    }
    let dir = home.join("slots").join(slot_id);
    private_dir(&dir)?;
    let dir = dir
        .canonicalize()?
        .into_os_string()
        .into_string()
        .map_err(|_| invalid("non UTF-8 slot path"))?;
    let key = match driver {
        "claude" | "claude_code" | "claude-code" => "CLAUDE_CONFIG_DIR",
        "codex" => "CODEX_HOME",
        "gemini" | "gemini-cli" => "GEMINI_CLI_HOME",
        _ => "TARLA_SLOT_HOME",
    };
    Ok(BTreeMap::from([(key.to_owned(), dir)]))
}

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
    child: Child,
    socks5: SocketAddr,
    record: PathBuf,
    pid: u32,
}
impl ManagedNetwork {
    pub fn proxy_url(&self) -> String {
        format!("socks5h://{}", self.socks5)
    }
    pub fn is_running(&mut self) -> io::Result<bool> {
        Ok(self.child.try_wait()?.is_none())
    }
}
impl Drop for ManagedNetwork {
    fn drop(&mut self) {
        let ours = fs::read(&self.record)
            .ok()
            .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
            .and_then(|v| v["pid"].as_u64())
            == Some(u64::from(self.pid));
        if ours {
            let _ = fs::remove_file(&self.record);
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
    start_network_impl(tools, runtime, spec, None).await
}

/// Publish only the authenticated API port through a managed userspace mesh.
/// Both backends connect incoming tailnet/overlay TCP flows to local IPv4
/// loopback on the same port. Tailscale's grants are set by the tailnet admin.
pub async fn start_network_for(
    tools: &Toolchain,
    runtime: &Path,
    spec: &NetworkSpec,
    api: SocketAddr,
) -> io::Result<Option<ManagedNetwork>> {
    if !api.is_ipv4() || !(api.ip().is_loopback() || api.ip().is_unspecified()) || api.port() == 0 {
        return Err(invalid(
            "managed mesh ingress requires a bound IPv4 loopback-compatible API address",
        ));
    }
    start_network_impl(tools, runtime, spec, Some(api.port())).await
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
) -> io::Result<Option<ManagedNetwork>> {
    if matches!(spec, NetworkSpec::ReuseHostNetwork) {
        return Ok(None);
    }
    private_dir(runtime)?;
    let runtime = runtime.canonicalize()?;
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
            let mut toml = format!(
                concat!(
                    "instance_id = \"{}\"\n{}\nlisteners = [\"tcp://0.0.0.0:0\"]\n",
                    "socks5_proxy = \"socks5://{}\"\n",
                    "tcp_whitelist = [\"{}\"]\nudp_whitelist = [\"0\"]\n",
                    "[network_identity]\nnetwork_name = {}\nnetwork_secret = {}\n",
                    "[flags]\nno_tun = true\nprivate_mode = true\n"
                ),
                easytier_identity(&runtime)?,
                address_config(spec)?,
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
            #[cfg(not(windows))]
            let socket = runtime.join("ts.sock").into_os_string();
            #[cfg(windows)]
            let socket = std::ffi::OsString::from(format!(
                r"\\.\pipe\tarla-ts-{:x}",
                Sha256::digest(runtime.to_string_lossy().as_bytes())
            ));
            let state = runtime.join("ts.state");
            c.args([
                OsStr::new("--tun=userspace-networking"),
                OsStr::new("--state"),
                state.as_os_str(),
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
    cmd.current_dir(&runtime)
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
    let pid = child.id().ok_or_else(|| invalid("missing sidecar pid"))?;
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
    let record = runtime.join(format!("{name}.json"));
    write_record(
        &record,
        &serde_json::to_vec(&json!({"pid":pid,"socks5":socks,"rpc":rpc,"api_port":api_port}))?,
    )?;
    Ok(Some(ManagedNetwork {
        child,
        socks5: socks,
        pid,
        record,
    }))
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
