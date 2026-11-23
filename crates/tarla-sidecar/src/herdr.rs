use crate::{invalid, private_dir};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    ffi::OsString,
    io,
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::io::{
    AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadHalf,
    WriteHalf,
};
use tokio::time::timeout;
#[cfg(unix)]
pub(crate) type Ipc = tokio::net::UnixStream;
#[cfg(windows)]
pub(crate) type Ipc = tokio::net::windows::named_pipe::NamedPipeClient;

pub const MAX_HERDR_FRAME_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_TERMINAL_INPUT_BYTES: usize = 64 * 1024;
pub const MAX_HERDR_DOC_BYTES: usize = 1024 * 1024;
const RPC_TIMEOUT: Duration = Duration::from_secs(12);

#[derive(Debug, Clone, serde::Serialize, Deserialize, PartialEq)]
pub struct DocRecord {
    pub name: String,
    pub version: u64,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub struct TerminalSpawnSpec {
    pub run_id: String,
    pub name: Option<String>,
    pub template: Option<String>,
    pub argv: Vec<String>,
    pub prompt: Option<String>,
    pub cwd: PathBuf,
    pub env: BTreeMap<String, String>,
    pub labels: BTreeMap<String, String>,
    pub parent: Option<String>,
    pub outlive_parent: bool,
    pub notify: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct Herdr {
    pub(crate) runtime: PathBuf,
    endpoint: PathBuf,
    session: String,
}

#[derive(Debug, Deserialize)]
pub struct TerminalStatus {
    pub state: String,
    pub reason: Option<String>,
    pub exit_code: Option<i32>,
    pub status: Option<Value>,
    #[serde(default)]
    records: BTreeMap<String, Value>,
}

impl TerminalStatus {
    /// Herdr puts an unqualified OSC 7501 report in `status`. Reports with an
    /// id independently update or clear entries in `records` on the same terminal.
    pub fn blocked_prompt(&self) -> Option<(Option<&str>, &Value)> {
        let root = self.status.as_ref().unwrap_or(&Value::Null);
        if root["state"] == "blocked" || self.state == "blocked" {
            return Some((None, root));
        }
        self.records
            .iter()
            .find(|(_, report)| report["state"] == "blocked")
            .map(|(id, report)| (Some(id.as_str()), report))
    }
}

fn local_terminal(id: &str) -> io::Result<()> {
    // herdr-core 0.1.19 terminals::id_shaped. Names and remote-qualified
    // addresses must never be resolved on a user's behalf.
    let Some(rest) = id.strip_prefix('t') else {
        return Err(invalid("invalid local terminal address"));
    };
    if !rest.starts_with(|c: char| matches!(c, '1'..='9'))
        || !rest
            .bytes()
            .all(|b| b"0123456789bcdfghjkmnpqrstvwxyz".contains(&b))
    {
        return Err(invalid("invalid local terminal address"));
    }
    Ok(())
}

fn check_owner(record: &Value, run_id: &str, address: &str) -> io::Result<()> {
    if run_id.is_empty()
        || record["address"].as_str() != Some(address)
        || record["labels"]["tarla_run"].as_str() != Some(run_id)
    {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "terminal no longer belongs to this run",
        ));
    }
    Ok(())
}

fn reply_result(reply: Value, id: u64) -> io::Result<Value> {
    if reply.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
        || reply.get("id").and_then(Value::as_u64) != Some(id)
    {
        return Err(invalid("unexpected RPC response"));
    }
    if let Some(error) = reply.get("error") {
        return Err(io::Error::other(error.to_string()));
    }
    reply
        .get("result")
        .cloned()
        .ok_or_else(|| invalid("missing RPC result"))
}

async fn plain<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut BufReader<S>,
    method: &str,
    params: Value,
) -> io::Result<Value> {
    let mut request =
        serde_json::to_vec(&json!({"jsonrpc":"2.0","id":1,"method":method,"params":params}))?;
    request.push(b'\n');
    stream.get_mut().write_all(&request).await?;
    let mut reply = Vec::new();
    (&mut *stream)
        .take((MAX_HERDR_FRAME_BYTES + 1) as u64)
        .read_until(b'\n', &mut reply)
        .await?;
    if reply.len() > MAX_HERDR_FRAME_BYTES || reply.last() != Some(&b'\n') {
        return Err(invalid("truncated or oversized RPC response"));
    }
    reply_result(serde_json::from_slice(&reply)?, 1)
}

fn derive_session_name(runtime: &Path) -> String {
    let workspace = runtime
        .parent()
        .filter(|ws| {
            ws.parent()
                .and_then(Path::file_name)
                .is_some_and(|name| name == "workspaces")
        })
        .and_then(Path::file_name)
        .and_then(|s| s.to_str())
        .filter(|s| {
            !s.is_empty()
                && s.len() <= 40
                && s.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        });
    if let Some(ws) = workspace {
        format!("tarla-{ws}")
    } else {
        let digest = format!("{:x}", Sha256::digest(runtime.to_string_lossy().as_bytes()));
        format!("tarla-{}", &digest[..12])
    }
}

pub(crate) fn build_spawn_params(spec: &TerminalSpawnSpec) -> io::Result<Value> {
    if spec.run_id.is_empty() || !spec.cwd.is_absolute() {
        return Err(invalid("run requires run_id and absolute workdir"));
    }
    if spec.template.as_deref().is_none_or(str::is_empty)
        && spec.argv.first().is_none_or(String::is_empty)
    {
        return Err(invalid("run requires either template or non-empty argv"));
    }
    let name = match spec.name.as_deref().filter(|s| !s.is_empty()) {
        Some(custom) => {
            crate::templates::validate_safe_ident("terminal name", custom, 64)?;
            custom.to_owned()
        }
        None => {
            let digest = format!("{:x}", Sha256::digest(&spec.run_id));
            format!("tarla-{}", &digest[..26])
        }
    };
    let mut labels = spec.labels.clone();
    labels.insert("tarla_run".into(), spec.run_id.clone());
    let mut params = json!({
        "name": name,
        "cwd": spec.cwd,
        "env": spec.env,
        "labels": labels,
        "wait": false,
        "rm": false,
        "keep": false,
    });
    if let Some(tpl) = spec.template.as_deref().filter(|s| !s.is_empty()) {
        crate::templates::validate_safe_ident("template", tpl, 64)?;
        params["template"] = json!(tpl);
    }
    if !spec.argv.is_empty() {
        params["argv"] = json!(spec.argv);
    }
    if let Some(prompt) = spec.prompt.as_deref().filter(|s| !s.is_empty()) {
        params["prompt"] = json!(prompt);
    }
    if let Some(parent) = spec.parent.as_deref().filter(|s| !s.is_empty()) {
        local_terminal(parent)?;
        params["parent"] = json!(parent);
        params["outlive_parent"] = json!(spec.outlive_parent);
        if !spec.notify.is_empty() {
            params["notify"] = json!(spec.notify);
        }
    } else {
        params["no_parent"] = json!(true);
    }
    Ok(params)
}

impl Herdr {
    pub fn new(runtime_dir: &Path) -> io::Result<Self> {
        private_dir(runtime_dir)?;
        let runtime = runtime_dir.canonicalize()?;
        let session = derive_session_name(&runtime);
        Self::from_canonical_runtime(runtime, session)
    }

    /// Create an isolated `Herdr` instance under `<workspace_runtime>/cores/<core_group>`.
    pub fn for_core_group(
        workspace_runtime: &Path,
        workspace_id: &str,
        core_group: &str,
        custom_session: Option<&str>,
    ) -> io::Result<Self> {
        crate::templates::validate_safe_ident("workspace id", workspace_id, 40)?;
        crate::templates::validate_safe_ident("core group", core_group, 40)?;
        private_dir(workspace_runtime)?;
        let cores_root = workspace_runtime.join("cores");
        private_dir(&cores_root)?;
        let group_dir = cores_root.join(core_group);
        private_dir(&group_dir)?;
        let runtime = group_dir.canonicalize()?;
        let session = match custom_session.filter(|s| !s.is_empty()) {
            Some(explicit) => {
                crate::templates::validate_safe_ident("herdr session", explicit, 80)?;
                explicit.to_owned()
            }
            None => format!("tarla-{workspace_id}-{core_group}"),
        };
        Self::from_canonical_runtime(runtime, session)
    }

    fn from_canonical_runtime(runtime: PathBuf, session: String) -> io::Result<Self> {
        #[cfg(unix)]
        let endpoint = crate::unix_socket_path(&runtime, "hc.sock", "hc")?;
        #[cfg(windows)]
        let endpoint = PathBuf::from(format!(
            r"\\.\pipe\tarla-hc-{:x}",
            Sha256::digest(runtime.to_string_lossy().as_bytes())
        ));
        Ok(Self {
            runtime,
            endpoint,
            session,
        })
    }

    pub fn runtime_dir(&self) -> &Path {
        &self.runtime
    }
    pub fn socket_path(&self) -> &Path {
        &self.endpoint
    }
    pub fn session_name(&self) -> &str {
        &self.session
    }
    pub(crate) fn env(&self) -> io::Result<Vec<(&'static str, OsString)>> {
        let dir = |name| -> io::Result<OsString> {
            let p = self.runtime.join(name);
            private_dir(&p)?;
            Ok(p.into_os_string())
        };
        Ok(vec![
            ("HC_SOCKET", self.endpoint.clone().into_os_string()),
            ("HC_SESSION", self.session.clone().into()),
            ("XDG_STATE_HOME", dir("hc-state")?),
            ("XDG_CONFIG_HOME", dir("hc-config")?),
            ("XDG_RUNTIME_DIR", dir("hc-runtime")?),
        ])
    }
    async fn socket(&self) -> io::Result<BufReader<Ipc>> {
        #[cfg(unix)]
        let ipc = Ipc::connect(&self.endpoint).await?;
        #[cfg(windows)]
        let ipc = tokio::net::windows::named_pipe::ClientOptions::new().open(&self.endpoint)?;
        Ok(BufReader::new(ipc))
    }
    async fn rpc(&self, method: &str, params: Value) -> io::Result<Value> {
        timeout(RPC_TIMEOUT, async {
            let mut stream = self.socket().await?;
            plain(&mut stream, method, params).await
        })
        .await?
    }
    pub async fn available(&self) -> io::Result<()> {
        self.machine_id().await.map(|_| ())
    }
    pub async fn machine_id(&self) -> io::Result<String> {
        let hello = self.rpc("hello", json!({"generation":1})).await?;
        hello["core"]["id"]
            .as_str()
            .filter(|id| !id.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| invalid("missing core identity"))
    }
    pub async fn find_run(&self, run_id: &str) -> io::Result<Option<String>> {
        let list = self
            .rpc(
                "terminal.list",
                json!({"all":true,"labels":{"tarla_run":run_id}}),
            )
            .await?;
        let terminals = list["terminals"]
            .as_array()
            .ok_or_else(|| invalid("missing terminal list"))?;
        let mut matches = terminals
            .iter()
            .filter(|t| t["labels"]["tarla_run"].as_str() == Some(run_id));
        let Some(record) = matches.next() else {
            return Ok(None);
        };
        if matches.next().is_some() {
            return Err(invalid("multiple local terminals claim this run"));
        }
        let address = record["address"]
            .as_str()
            .ok_or_else(|| invalid("missing terminal address"))?;
        local_terminal(address)?;
        Ok(Some(address.to_owned()))
    }
    /// Spawn a root or child terminal (raw `argv` or managed `.md` template) in this `Herdr` core.
    pub async fn spawn_terminal(&self, spec: &TerminalSpawnSpec) -> io::Result<String> {
        let params = build_spawn_params(spec)?;
        let result = self.rpc("terminal.run", params).await?;
        let addr = result["address"]
            .as_str()
            .ok_or_else(|| invalid("missing terminal address"))?;
        local_terminal(addr)?;
        check_owner(&result, &spec.run_id, addr)?;
        Ok(addr.into())
    }
    /// Call only after persisting the executor's unique run claim. A timeout is
    /// ambiguous. Reconcile with find_run instead of dispatching again.
    pub async fn start_run(
        &self,
        run_id: &str,
        argv: &[String],
        cwd: &Path,
        env: &BTreeMap<String, String>,
    ) -> io::Result<String> {
        if run_id.is_empty() || argv.first().is_none_or(String::is_empty) || !cwd.is_absolute() {
            return Err(invalid("run requires id, argv and absolute workdir"));
        }
        self.spawn_terminal(&TerminalSpawnSpec {
            run_id: run_id.to_owned(),
            name: None,
            template: None,
            argv: argv.to_vec(),
            prompt: None,
            cwd: cwd.to_path_buf(),
            env: env.clone(),
            labels: BTreeMap::new(),
            parent: None,
            outlive_parent: false,
            notify: Vec::new(),
        })
        .await
    }
    // Keep both requests on one connection. A replacement core may reuse the
    // same address, but cannot inherit this connection. Addresses are immutable
    // within a core. Check the retained, workspace-private run label every time.
    async fn scoped_socket(
        &self,
        run_id: &str,
        terminal: &str,
    ) -> io::Result<(BufReader<Ipc>, Value)> {
        local_terminal(terminal)?;
        let mut socket = self.socket().await?;
        let record = plain(&mut socket, "terminal.get", json!({"terminal":terminal})).await?;
        check_owner(&record, run_id, terminal)?;
        Ok((socket, record))
    }
    async fn run_rpc(
        &self,
        run_id: &str,
        terminal: &str,
        method: &str,
        params: Value,
    ) -> io::Result<Value> {
        timeout(RPC_TIMEOUT, async {
            let (mut socket, _) = self.scoped_socket(run_id, terminal).await?;
            plain(&mut socket, method, params).await
        })
        .await?
    }
    pub async fn status(&self, run_id: &str, terminal: &str) -> io::Result<TerminalStatus> {
        let (_, record) = timeout(RPC_TIMEOUT, self.scoped_socket(run_id, terminal)).await??;
        Ok(serde_json::from_value(record)?)
    }
    pub async fn read(&self, run_id: &str, terminal: &str, lines: u32) -> io::Result<String> {
        self.run_rpc(
            run_id,
            terminal,
            "terminal.read",
            json!({"terminal":terminal,"lines":lines.min(1000)}),
        )
        .await?["text"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| invalid("missing terminal text"))
    }
    pub async fn interrupt(&self, run_id: &str, terminal: &str) -> io::Result<()> {
        match self
            .run_rpc(
                run_id,
                terminal,
                "terminal.interrupt",
                json!({"terminal":terminal}),
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(_) => self.cancel(run_id, terminal).await,
        }
    }
    pub async fn cancel(&self, run_id: &str, terminal: &str) -> io::Result<()> {
        self.run_rpc(
            run_id,
            terminal,
            "terminal.kill",
            json!({"terminal":terminal}),
        )
        .await?;
        Ok(())
    }
    pub async fn send(&self, run_id: &str, terminal: &str, text: &str) -> io::Result<()> {
        if text.len() > MAX_TERMINAL_INPUT_BYTES {
            return Err(invalid("message too long"));
        }
        self.run_rpc(
            run_id,
            terminal,
            "terminal.send",
            json!({"terminal":terminal,"text":text,"wait":false}),
        )
        .await?;
        Ok(())
    }
    /// Read a versioned blackboard JSON document (`doc.get`) within this `Herdr` core.
    pub async fn doc_get(&self, name: &str) -> io::Result<Option<DocRecord>> {
        crate::templates::validate_safe_ident("doc name", name, 64)?;
        let reply = self.rpc("doc.get", json!({"name": name})).await?;
        if reply.is_null() || reply.get("version").is_none() {
            return Ok(None);
        }
        Ok(Some(DocRecord {
            name: reply["name"].as_str().unwrap_or(name).to_owned(),
            version: reply["version"]
                .as_u64()
                .ok_or_else(|| invalid("missing doc version"))?,
            value: reply.get("value").cloned().unwrap_or(Value::Null),
        }))
    }
    /// Write a versioned blackboard JSON document (`doc.set`, capped at 1 MiB with optimistic `version`).
    pub async fn doc_set(
        &self,
        name: &str,
        expected_version: Option<u64>,
        value: &Value,
    ) -> io::Result<DocRecord> {
        crate::templates::validate_safe_ident("doc name", name, 64)?;
        let encoded = serde_json::to_vec(value)?;
        if encoded.len() > MAX_HERDR_DOC_BYTES {
            return Err(invalid("blackboard document exceeds 1 MiB limit"));
        }
        let mut params = json!({"name": name, "value": value});
        if let Some(ver) = expected_version {
            params["version"] = json!(ver);
        }
        let reply = self.rpc("doc.set", params).await?;
        Ok(DocRecord {
            name: reply["name"].as_str().unwrap_or(name).to_owned(),
            version: reply["version"]
                .as_u64()
                .ok_or_else(|| invalid("missing doc version"))?,
            value: reply.get("value").cloned().unwrap_or_else(|| value.clone()),
        })
    }
    /// Subscribe to the real-time monotonic `events.subscribe` stream on this `Herdr` core.
    pub async fn subscribe_events(&self, since_seq: Option<u64>) -> io::Result<crate::EventStream> {
        timeout(RPC_TIMEOUT, async {
            let stream = self.socket().await?;
            crate::EventStream::connect(stream, since_seq).await
        })
        .await?
    }
    pub async fn attach(
        &self,
        run_id: &str,
        terminal: &str,
        observe: bool,
        cols: u16,
        rows: u16,
    ) -> io::Result<(TerminalReader, TerminalWriter)> {
        if cols == 0 || rows == 0 {
            return Err(invalid("invalid viewport"));
        }
        timeout(RPC_TIMEOUT, async {
            let (socket, _) = self.scoped_socket(run_id, terminal).await?;
            attach_stream(socket, run_id, terminal, observe, cols, rows).await
        })
        .await?
    }
}

async fn attach_stream<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: BufReader<S>,
    run_id: &str,
    terminal: &str,
    observe: bool,
    cols: u16,
    rows: u16,
) -> io::Result<(TerminalReader<S>, TerminalWriter<S>)> {
    let features = json!({"framed":true,"generation":1,"features":["resize_notifications"]});
    let hello = plain(&mut stream, "hello", features).await?;
    if hello["framed"] != true {
        return Err(invalid("core did not enable framed mode"));
    }
    let attach = json!({"jsonrpc":"2.0","id":2,"method":"terminal.attach","params":{
        "terminal":terminal,"observe":observe,"cols":cols,"rows":rows,"resize_notifications":observe}});
    write_frame(stream.get_mut(), 0, 0, &serde_json::to_vec(&attach)?).await?;
    let (channel, kind, data) = read_frame(&mut stream).await?;
    if (channel, kind) != (0, 0) {
        return Err(invalid("missing framed attach response"));
    }
    let reply = reply_result(serde_json::from_slice(&data)?, 2)?;
    let channel = reply["channel"]
        .as_u64()
        .and_then(|c| u32::try_from(c).ok())
        .filter(|c| *c != 0)
        .ok_or_else(|| invalid("invalid terminal channel"))?;
    check_owner(&reply["terminal"], run_id, terminal)?;
    // Keep the buffered first paint when switching protocols.
    let (read, write) = tokio::io::split(stream);
    Ok((
        TerminalReader {
            read,
            channel,
            pending: Vec::new(),
        },
        TerminalWriter {
            write,
            channel,
            observe,
        },
    ))
}

async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> io::Result<(u32, u8, Vec<u8>)> {
    let channel = r.read_u32_le().await?;
    let kind = r.read_u8().await?;
    let len = r.read_u32_le().await? as usize;
    if len > MAX_HERDR_FRAME_BYTES {
        return Err(invalid("oversized core frame"));
    }
    let mut bytes = vec![0; len];
    r.read_exact(&mut bytes).await?;
    Ok((channel, kind, bytes))
}
async fn write_frame<W: AsyncWrite + Unpin>(
    w: &mut W,
    ch: u32,
    kind: u8,
    data: &[u8],
) -> io::Result<()> {
    let mut head = [0u8; 9];
    head[..4].copy_from_slice(&ch.to_le_bytes());
    head[4] = kind;
    head[5..].copy_from_slice(&(data.len() as u32).to_le_bytes());
    w.write_all(&head).await?;
    w.write_all(data).await?;
    w.flush().await
}

pub enum TerminalEvent {
    Output(Vec<u8>),
    Closed(Vec<u8>),
    Resize(u16, u16),
}
pub struct TerminalReader<S = Ipc> {
    pending: Vec<u8>,
    read: ReadHalf<BufReader<S>>,
    channel: u32,
}
impl<S: AsyncRead + AsyncWrite + Unpin> TerminalReader<S> {
    /// Partial frames stay buffered across cancellation in an async select loop.
    pub async fn next(&mut self) -> io::Result<TerminalEvent> {
        loop {
            if self.pending.len() >= 9 {
                let channel = u32::from_le_bytes(self.pending[..4].try_into().unwrap());
                let kind = self.pending[4];
                let length = u32::from_le_bytes(self.pending[5..9].try_into().unwrap()) as usize;
                if length > MAX_HERDR_FRAME_BYTES {
                    return Err(invalid("oversized core frame"));
                }
                if self.pending.len() >= 9 + length {
                    let bytes = self.pending[9..9 + length].to_vec();
                    self.pending.drain(..9 + length);
                    if channel != self.channel {
                        return Err(invalid("unexpected core channel"));
                    }
                    match kind {
                        0 => return Ok(TerminalEvent::Output(bytes)),
                        2 => return Ok(TerminalEvent::Closed(bytes)),
                        3 if bytes.len() == 4 => {
                            return Ok(TerminalEvent::Resize(
                                u16::from_le_bytes([bytes[0], bytes[1]]),
                                u16::from_le_bytes([bytes[2], bytes[3]]),
                            ))
                        }
                        1 if bytes.len() == 4 => continue,
                        _ => return Err(invalid("unexpected core frame")),
                    }
                }
            }
            let mut fragment = [0; 8192];
            let n = self.read.read(&mut fragment).await?;
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "terminal stream ended",
                ));
            }
            self.pending.extend_from_slice(&fragment[..n]);
        }
    }
}
pub struct TerminalWriter<S = Ipc> {
    write: WriteHalf<BufReader<S>>,
    channel: u32,
    observe: bool,
}
impl<S: AsyncRead + AsyncWrite + Unpin> TerminalWriter<S> {
    fn check_interactive(&self) -> io::Result<()> {
        if self.observe {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "read-only terminal lease",
            ));
        }
        Ok(())
    }
    pub async fn input(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.check_interactive()?;
        if bytes.len() > MAX_TERMINAL_INPUT_BYTES {
            return Err(invalid("oversized input"));
        }
        write_frame(&mut self.write, self.channel, 0, bytes).await
    }
    pub async fn resize(&mut self, cols: u16, rows: u16) -> io::Result<()> {
        self.check_interactive()?;
        if cols == 0 || rows == 0 {
            return Err(invalid("invalid viewport"));
        }
        let [x, y] = cols.to_le_bytes();
        let [a, b] = rows.to_le_bytes();
        write_frame(&mut self.write, self.channel, 3, &[x, y, a, b]).await
    }
    pub async fn credit(&mut self, consumed: usize) -> io::Result<()> {
        let n = u32::try_from(consumed).map_err(|_| invalid("invalid credit"))?;
        write_frame(&mut self.write, self.channel, 1, &n.to_le_bytes()).await
    }
    pub async fn close(&mut self) -> io::Result<()> {
        write_frame(&mut self.write, self.channel, 2, &[]).await
    }
}

#[cfg(test)]
mod protocol_tests {
    use super::*;
    use tokio::io::{duplex, DuplexStream};

    #[test]
    fn scoped_blocked_reports_survive_an_idle_or_absent_root() {
        let report = json!({"state":"blocked","kind":"permission","msg":"Apply?"});
        let mut terminal: TerminalStatus = serde_json::from_value(json!({
            "state":"live", "status":null, "records":{"a/b":report, "a/c":report}
        }))
        .unwrap();
        assert_eq!(terminal.blocked_prompt(), Some((Some("a/b"), &report)));
        terminal.status = Some(json!({"state":"working"}));
        terminal.records.remove("a/b");
        assert_eq!(terminal.blocked_prompt(), Some((Some("a/c"), &report)));
        terminal.status = Some(report.clone());
        assert_eq!(terminal.blocked_prompt(), Some((None, &report)));
        terminal.status = None;
        terminal.records.clear();
        assert_eq!(terminal.blocked_prompt(), None);
    }

    #[test]
    fn upstream_local_addresses_exclude_names_and_remote_targets() {
        for id in ["t1", "t9", "t10", "t1b", "t9z", "t100", "t4qh"] {
            assert!(local_terminal(id).is_ok(), "{id}");
        }
        for id in [
            "", "t", "t0", "t01", "t1a", "t1l", "tF", "t1:t1", "other/t1", "reviewer", "t1B",
        ] {
            assert!(local_terminal(id).is_err(), "{id}");
        }
    }

    // An independent fixture emits the upstream on-wire header; production framing
    // is exercised even on CI runners that deny AF_UNIX or lack herdr-core.
    async fn frame(s: &mut DuplexStream, channel: u32, kind: u8, body: &[u8]) {
        let mut bytes = channel.to_le_bytes().to_vec();
        bytes.push(kind);
        bytes.extend_from_slice(&(body.len() as u32).to_le_bytes());
        bytes.extend(body);
        for chunk in bytes.chunks(3) {
            s.write_all(chunk).await.unwrap();
        }
    }

    #[tokio::test]
    async fn framed_observer_receives_paint_and_cannot_write() {
        let (client, server) = duplex(256);
        let host = tokio::spawn(async move {
            let mut s = BufReader::new(server);
            let mut hello = String::new();
            s.read_line(&mut hello).await.unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(&hello).unwrap()["params"]["framed"],
                true
            );
            s.get_mut()
                .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"framed\":true}}\n")
                .await
                .unwrap();
            let (channel, kind, body) = read_frame(&mut s).await.unwrap();
            assert_eq!((channel, kind), (0, 0));
            let ask: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(ask["params"]["observe"], true);
            assert_eq!(ask["params"]["terminal"], "t7");
            frame(
                s.get_mut(),
                0,
                0,
                br#"{"jsonrpc":"2.0","id":2,"result":{"channel":17,"terminal":{"address":"t7","labels":{"tarla_run":"fixture"}}}}"#,
            )
            .await;
            frame(s.get_mut(), 17, 3, &[80, 0, 24, 0]).await;
            frame(s.get_mut(), 17, 0, b"abc").await;
            let (c, k, credit) = read_frame(&mut s).await.unwrap();
            assert_eq!((c, k, credit), (17, 1, 3u32.to_le_bytes().to_vec()));
            frame(s.get_mut(), 17, 2, b"finished").await;
        });
        let (mut r, mut w) = attach_stream(BufReader::new(client), "fixture", "t7", true, 80, 24)
            .await
            .unwrap();
        assert!(matches!(
            r.next().await.unwrap(),
            TerminalEvent::Resize(80, 24)
        ));
        assert!(matches!(r.next().await.unwrap(),TerminalEvent::Output(v) if v==b"abc"));
        assert_eq!(
            w.input(b"x").await.unwrap_err().kind(),
            io::ErrorKind::PermissionDenied
        );
        assert!(w.resize(50, 20).await.is_err());
        w.credit(3).await.unwrap();
        assert!(matches!(r.next().await.unwrap(), TerminalEvent::Closed(_)));
        host.await.unwrap();
    }
    #[tokio::test]
    async fn framed_attach_rechecks_run_label_before_exposing_paint() {
        let (client, server) = duplex(1024);
        let host = tokio::spawn(async move {
            let mut server = BufReader::new(server);
            let mut hello = String::new();
            server.read_line(&mut hello).await.unwrap();
            server
                .get_mut()
                .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{\"framed\":true}}\n")
                .await
                .unwrap();
            let (_, _, attach) = read_frame(&mut server).await.unwrap();
            assert_eq!(
                serde_json::from_slice::<Value>(&attach).unwrap()["params"]["terminal"],
                "t1b"
            );
            frame(server.get_mut(), 0, 0, br#"{"jsonrpc":"2.0","id":2,"result":{"channel":4,"terminal":{"address":"t1b","labels":{"tarla_run":"different-run"}}}}"#).await;
        });
        let result =
            attach_stream(BufReader::new(client), "expected-run", "t1b", false, 80, 24).await;
        assert!(matches!(result, Err(e) if e.kind() == io::ErrorKind::PermissionDenied));
        host.await.unwrap();
    }
    #[tokio::test]
    async fn partial_frames_survive_a_cancelled_read() {
        let (client, mut server) = duplex(64);
        let (read, _) = tokio::io::split(BufReader::new(client));
        let mut reader = TerminalReader {
            read,
            channel: 4,
            pending: Vec::new(),
        };
        server.write_all(&[4, 0, 0]).await.unwrap();
        assert!(timeout(Duration::from_millis(10), reader.next())
            .await
            .is_err());
        server.write_all(&[0, 0, 1, 0, 0, 0, b'x']).await.unwrap();
        assert!(matches!(reader.next().await.unwrap(),TerminalEvent::Output(b) if b == b"x"));
    }

    #[cfg(unix)]
    #[test]
    fn deep_unix_runtime_falls_back_under_sun_len_and_scopes_session_per_workspace() {
        let tmp = tempfile::tempdir().unwrap();
        let work_rt = tmp.path().join("workspaces/work/runtime");
        let life_rt = tmp.path().join("workspaces/life/runtime");
        let work = Herdr::new(&work_rt).unwrap();
        let life = Herdr::new(&life_rt).unwrap();
        assert_eq!(work.session_name(), "tarla-work");
        assert_eq!(life.session_name(), "tarla-life");

        let deep_rt = tmp
            .path()
            .join("very-deep-macos-private-var-folders-segment-0123456789abcdef")
            .join("workspaces")
            .join("engineering-production-workspace-alpha")
            .join("runtime");
        let deep = Herdr::new(&deep_rt).unwrap();
        assert!(
            deep.socket_path().as_os_str().as_encoded_bytes().len() <= 96,
            "socket path exceeded SUN_LEN budget: {}",
            deep.socket_path().display()
        );
        assert_eq!(
            deep.session_name(),
            "tarla-engineering-production-workspace-alpha"
        );
    }

    #[test]
    fn multi_core_groups_and_parent_child_template_delegation_params() {
        let tmp = tempfile::tempdir().unwrap();
        let rt = tmp.path().join("workspaces/work/runtime");
        let code_core = Herdr::for_core_group(&rt, "work", "code_core", None).unwrap();
        let ops_core = Herdr::for_core_group(&rt, "work", "ops_core", None).unwrap();
        assert_eq!(code_core.session_name(), "tarla-work-code_core");
        assert_eq!(ops_core.session_name(), "tarla-work-ops_core");
        assert_ne!(code_core.socket_path(), ops_core.socket_path());
        assert_ne!(code_core.runtime_dir(), ops_core.runtime_dir());

        let child_spec = TerminalSpawnSpec {
            run_id: "run-child-1".into(),
            name: Some("gemini-reviewer".into()),
            template: Some("gemini-rev".into()),
            argv: Vec::new(),
            prompt: Some("核查刚生成的补丁".into()),
            cwd: tmp.path().canonicalize().unwrap(),
            env: BTreeMap::new(),
            labels: BTreeMap::from([("tarla_bot".into(), "reviewer".into())]),
            parent: Some("t2".into()),
            outlive_parent: false,
            notify: vec!["done".into(), "error".into(), "blocked".into()],
        };
        let params = build_spawn_params(&child_spec).unwrap();
        assert_eq!(params["name"], "gemini-reviewer");
        assert_eq!(params["template"], "gemini-rev");
        assert_eq!(params["parent"], "t2");
        assert_eq!(params["outlive_parent"], false);
        assert_eq!(params["notify"], json!(["done", "error", "blocked"]));
        assert!(params.get("no_parent").is_none());
        assert_eq!(params["labels"]["tarla_run"], "run-child-1");
        assert_eq!(params["labels"]["tarla_bot"], "reviewer");

        let mut bad = child_spec;
        bad.parent = Some("remote/t2".into());
        assert!(build_spawn_params(&bad).is_err());
    }
}
