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
type Ipc = tokio::net::UnixStream;
#[cfg(windows)]
type Ipc = tokio::net::windows::named_pipe::NamedPipeClient;

const MAX_FRAME: usize = 16 * 1024 * 1024;
const MAX_INPUT: usize = 64 * 1024;
const RPC_TIMEOUT: Duration = Duration::from_secs(12);

#[derive(Debug, Clone)]
pub struct Herdr {
    pub(crate) runtime: PathBuf,
    endpoint: PathBuf,
}

#[derive(Debug, Deserialize)]
pub struct TerminalStatus {
    pub state: String,
    pub reason: Option<String>,
    pub exit_code: Option<i32>,
    pub status: Option<Value>,
}

fn local_terminal(id: &str) -> io::Result<()> {
    // Only immutable local addresses returned by terminal.run, never machine aliases.
    if !id
        .strip_prefix('t')
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
    {
        return Err(invalid("invalid local terminal address"));
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
        .take((MAX_FRAME + 1) as u64)
        .read_until(b'\n', &mut reply)
        .await?;
    if reply.len() > MAX_FRAME || reply.last() != Some(&b'\n') {
        return Err(invalid("truncated or oversized RPC response"));
    }
    reply_result(serde_json::from_slice(&reply)?, 1)
}

impl Herdr {
    pub fn new(runtime_dir: &Path) -> io::Result<Self> {
        private_dir(runtime_dir)?;
        let runtime = runtime_dir.canonicalize()?;
        #[cfg(unix)]
        let endpoint = runtime.join("hc.sock");
        #[cfg(windows)]
        let endpoint = PathBuf::from(format!(
            r"\\.\pipe\tarla-hc-{:x}",
            Sha256::digest(runtime.to_string_lossy().as_bytes())
        ));
        Ok(Self { runtime, endpoint })
    }
    pub(crate) fn env(&self) -> io::Result<Vec<(&'static str, OsString)>> {
        let dir = |name| -> io::Result<OsString> {
            let p = self.runtime.join(name);
            private_dir(&p)?;
            Ok(p.into_os_string())
        };
        Ok(vec![
            ("HC_SOCKET", self.endpoint.clone().into_os_string()),
            ("HC_SESSION", "tarla".into()),
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
        self.rpc("hello", json!({"generation":1})).await.map(|_| ())
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
            .filter(|t| t["labels"]["tarla_run"].as_str() == Some(run_id))
            .filter_map(|t| t["address"].as_str())
            .filter(|t| local_terminal(t).is_ok());
        let first = matches.next().map(str::to_owned);
        if matches.next().is_some() {
            return Err(invalid("multiple local terminals claim this run"));
        }
        Ok(first)
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
        let digest = format!("{:x}", Sha256::digest(run_id));
        let name = format!("tarla-{}", &digest[..26]);
        let result = self
            .rpc(
                "terminal.run",
                json!({"name":name,"argv":argv,"cwd":cwd,"env":env,
            "labels":{"tarla_run":run_id},"wait":false,"rm":false,"keep":false,"no_parent":true}),
            )
            .await?;
        let addr = result["address"]
            .as_str()
            .ok_or_else(|| invalid("missing terminal address"))?;
        local_terminal(addr)?;
        Ok(addr.into())
    }
    pub async fn status(&self, terminal: &str) -> io::Result<TerminalStatus> {
        local_terminal(terminal)?;
        Ok(serde_json::from_value(
            self.rpc("terminal.get", json!({"terminal":terminal}))
                .await?,
        )?)
    }
    pub async fn read(&self, terminal: &str, lines: u32) -> io::Result<String> {
        local_terminal(terminal)?;
        self.rpc(
            "terminal.read",
            json!({"terminal":terminal,"lines":lines.min(1000)}),
        )
        .await?["text"]
            .as_str()
            .map(str::to_owned)
            .ok_or_else(|| invalid("missing terminal text"))
    }
    pub async fn cancel(&self, terminal: &str) -> io::Result<()> {
        local_terminal(terminal)?;
        self.rpc("terminal.kill", json!({"terminal":terminal}))
            .await
            .map(|_| ())
    }
    pub async fn send(&self, terminal: &str, text: &str) -> io::Result<()> {
        local_terminal(terminal)?;
        if text.len() > MAX_INPUT {
            return Err(invalid("message too long"));
        }
        self.rpc(
            "terminal.send",
            json!({"terminal":terminal,"text":text,"wait":false}),
        )
        .await
        .map(|_| ())
    }
    pub async fn attach(
        &self,
        terminal: &str,
        observe: bool,
        cols: u16,
        rows: u16,
    ) -> io::Result<(TerminalReader, TerminalWriter)> {
        local_terminal(terminal)?;
        if cols == 0 || rows == 0 {
            return Err(invalid("invalid viewport"));
        }
        timeout(RPC_TIMEOUT, async {
            let socket = self.socket().await?;
            attach_stream(socket, terminal, observe, cols, rows).await
        })
        .await?
    }
}

async fn attach_stream<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: BufReader<S>,
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
    if reply["terminal"]["address"].as_str() != Some(terminal) {
        return Err(invalid("terminal scope mismatch"));
    }
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
    if len > MAX_FRAME {
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
                if length > MAX_FRAME {
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
        if bytes.len() > MAX_INPUT {
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
                br#"{"jsonrpc":"2.0","id":2,"result":{"channel":17,"terminal":{"address":"t7"}}}"#,
            )
            .await;
            frame(s.get_mut(), 17, 3, &[80, 0, 24, 0]).await;
            frame(s.get_mut(), 17, 0, b"abc").await;
            let (c, k, credit) = read_frame(&mut s).await.unwrap();
            assert_eq!((c, k, credit), (17, 1, 3u32.to_le_bytes().to_vec()));
            frame(s.get_mut(), 17, 2, b"finished").await;
        });
        let (mut r, mut w) = attach_stream(BufReader::new(client), "t7", true, 80, 24)
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
}
