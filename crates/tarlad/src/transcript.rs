//! Executor-only S0 journal. The index stores the end byte of each committed JSONL
//! record so a caller can seek directly to a page without loading the transcript.
//! A reconnect starts with a terminal repaint, not a replay of missed PTY bytes.
use anyhow::{ensure, Context, Result};
use fs2::FileExt;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::Mutex,
    time::{SystemTime, UNIX_EPOCH},
};
use tarla_sidecar::{Herdr, TerminalEvent};
use tokio::task::JoinHandle;

// A page inspects at most this many records even when a text search finds no matches.
const SCAN_LIMIT: u64 = 1024;
const CHUNK_BYTES: usize = 2048;
const PAGE_BYTES: usize = 48 * 1024;

#[derive(Default)]
pub struct Captures {
    active: Mutex<HashMap<String, JoinHandle<()>>>,
}

impl Captures {
    /// Wait for final output and the close frame once the executor observes exit.
    /// Some older cores keep the viewer open after process exit, so shutdown is bounded.
    pub async fn drain(&self, id: &str) {
        let task = self.active.lock().ok().and_then(|mut map| map.remove(id));
        if let Some(mut task) = task {
            if tokio::time::timeout(std::time::Duration::from_secs(5), &mut task)
                .await
                .is_err()
            {
                task.abort();
                let _ = task.await;
                eprintln!(
                    "terminal capture drain timed out for {id}; final screen will be recorded"
                );
            }
        }
    }
    /// Attach once per live run. A later reconciliation retries if the socket closes.
    pub async fn ensure(&self, root: &Path, id: &str, terminal: &str, hc: &Herdr) -> Result<()> {
        let dir = location(root, id)?;
        if dir.join("finished").is_file() {
            return Ok(());
        }
        let mut active = self
            .active
            .lock()
            .map_err(|_| anyhow::anyhow!("capture registry unavailable"))?;
        active.retain(|_, task| !task.is_finished());
        if !active.contains_key(id) {
            let terminal = terminal.to_owned();
            let hc = hc.clone();
            active.insert(
                id.to_owned(),
                tokio::spawn(async move {
                    if let Err(e) = capture(&dir, &terminal, &hc).await {
                        eprintln!("terminal capture interrupted: {e}");
                    }
                }),
            );
        }
        Ok(())
    }
}

fn location(root: &Path, id: &str) -> Result<PathBuf> {
    ensure!(
        !id.is_empty() && id.len() <= 160,
        "invalid transcript run ID"
    );
    // IDs can be supplied by a paired node. Never interpolate them into filesystem paths.
    Ok(root
        .join("runtime")
        .join("runs")
        .join(format!("{:x}", Sha256::digest(id.as_bytes()))))
}
fn private_dir(dir: &Path) -> Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(dir)?;
    ensure!(
        fs::symlink_metadata(dir)?.is_dir(),
        "invalid transcript directory"
    );
    Ok(())
}
fn open_private(path: &Path) -> Result<File> {
    let mut opts = OpenOptions::new();
    opts.read(true).write(true).create(true).truncate(false);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    Ok(opts.open(path)?)
}
fn stamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
fn read_end(idx: &mut File, count: u64) -> Result<u64> {
    if count == 0 {
        return Ok(0);
    }
    idx.seek(SeekFrom::Start((count - 1) * 8))?;
    let mut n = [0; 8];
    idx.read_exact(&mut n)?;
    Ok(u64::from_le_bytes(n))
}

struct Journal {
    data: File,
    index: File,
}
impl Journal {
    fn open(dir: &Path) -> Result<Self> {
        private_dir(dir)?;
        let mut index = open_private(&dir.join("offsets"))?;
        FileExt::lock_exclusive(&index)?;
        let mut data = open_private(&dir.join("transcript.jsonl"))?;
        let len = index.metadata()?.len();
        let count = len / 8;
        if len % 8 != 0 {
            index.set_len(count * 8)?;
        }
        let end = read_end(&mut index, count)?;
        ensure!(
            end <= data.metadata()?.len(),
            "transcript index extends beyond committed data"
        );
        // Only an interrupted append can leave a data suffix without index entries.
        // Recover each complete row, and discard the torn last row.
        data.seek(SeekFrom::Start(end))?;
        index.seek(SeekFrom::End(0))?;
        if end < data.metadata()?.len() {
            let mut good = end;
            let mut reader = BufReader::new(&mut data);
            let mut line = Vec::new();
            loop {
                line.clear();
                let n = (&mut reader).take(32 * 1024).read_until(b'\n', &mut line)?;
                if n == 0
                    || !line.ends_with(b"\n")
                    || serde_json::from_slice::<Value>(&line).is_err()
                {
                    break;
                }
                good += n as u64;
                index.write_all(&good.to_le_bytes())?;
            }
            data.set_len(good)?;
            index.sync_data()?;
        }
        data.seek(SeekFrom::End(0))?;
        Ok(Self { data, index })
    }
    fn append(&mut self, records: impl IntoIterator<Item = Value>) -> Result<()> {
        let mut end = self.data.stream_position()?;
        let mut offsets = Vec::new();
        let mut data = Vec::new();
        for record in records {
            let prev = data.len();
            serde_json::to_writer(&mut data, &record)?;
            data.push(b'\n');
            end += (data.len() - prev) as u64;
            offsets.extend(end.to_le_bytes());
        }
        if !data.is_empty() {
            self.data.write_all(&data)?;
            self.data.sync_data()?;
            self.index.write_all(&offsets)?;
            self.index.sync_data()?;
        }
        Ok(())
    }
}
fn payload_records<'a>(kind: &'a str, bytes: &'a [u8]) -> impl Iterator<Item = Value> + 'a {
    let timestamp = stamp();
    bytes
        .split_inclusive(|b| *b == b'\n')
        .flat_map(|line| line.chunks(CHUNK_BYTES))
        .map(move |chunk| match std::str::from_utf8(chunk) {
            Ok(text) => json!({"time":timestamp,"kind":kind,"text":text}),
            Err(_) => {
                let mut hex = String::with_capacity(chunk.len() * 2);
                for b in chunk {
                    use std::fmt::Write as _;
                    write!(hex, "{b:02x}").expect("string write");
                }
                json!({"time":timestamp,"kind":kind,"hex":hex})
            }
        })
}
async fn disk<F>(dir: &Path, action: F) -> Result<()>
where
    F: FnOnce(&mut Journal) -> Result<()> + Send + 'static,
{
    let d = dir.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut journal = Journal::open(&d)?;
        if !d.join("finished").is_file() {
            action(&mut journal)?;
        }
        Ok(())
    })
    .await?
}
async fn capture(dir: &Path, terminal: &str, hc: &Herdr) -> Result<()> {
    let (mut reader, mut writer) = hc.attach(terminal, true, 80, 24).await?;
    disk(dir,|j|j.append([json!({"time":stamp(),"kind":"gap","text":"Capture attached. Output before this paint, including any offline interval, may be missing."})])).await?;
    loop {
        match reader.next().await? {
            TerminalEvent::Output(bytes)=>{
                let count=bytes.len();
                // Limit RAM and time holding the write lock for a large terminal frame.
                for chunk in bytes.chunks(64*1024) {
                    let payload=chunk.to_vec();disk(dir,move|j|j.append(payload_records("output",&payload))).await?;
                }
                writer.credit(count).await?;
            }
            TerminalEvent::Resize(cols,rows)=>disk(dir,move|j|j.append([json!({"time":stamp(),"kind":"resize","text":format!("Viewport {cols} × {rows}; subsequent output may include an updated screen.")})])).await?,
            TerminalEvent::Closed(_)=>return Ok(()),
        }
    }
}

/// A final screen is a bounded reconstruction supplied by Herdr, distinct from the
/// continuous byte stream. It also covers short processes that exited before attach.
pub fn finish(root: &Path, id: &str, state: &str, final_screen: &str) -> Result<PathBuf> {
    let dir = location(root, id)?;
    let mut j = Journal::open(&dir)?;
    let path = dir.join("transcript.jsonl");
    if dir.join("finished").is_file() {
        return Ok(path);
    }
    if j.index.metadata()?.len() == 0 {
        j.append([json!({"time":stamp(),"kind":"gap","text":"No live capture was available before the terminal exited."})])?;
    }
    for chunk in final_screen.as_bytes().chunks(64 * 1024) {
        j.append(payload_records("final_screen", chunk))?;
    }
    j.append([json!({"time":stamp(),"kind":"finished","text":state})])?;
    open_private(&dir.join("finished"))?.sync_data()?;
    Ok(path)
}

/// Zero-based offsets count journal records. Byte-identical output is retained locally.
/// next_offset also advances through non-matching rows; a bounded scan may return no matches.
pub fn page(root: &Path, id: &str, offset: u64, limit: u64, query: &str) -> Result<Value> {
    ensure!(query.len() <= 1024, "log query exceeds limit");
    let dir = location(root, id)?;
    let index_path = dir.join("offsets");
    if !index_path.exists() {
        return Ok(
            json!({"run_id":id,"offset":offset,"next_offset":offset,"lines":[],"records":[],"eof":true}),
        );
    }
    let mut index = File::open(index_path)?;
    FileExt::lock_shared(&index)?;
    let count = index.metadata()?.len() / 8;
    let mut pos = offset.min(count);
    let end = count.min(pos.saturating_add(SCAN_LIMIT));
    let file = File::open(dir.join("transcript.jsonl"))?;
    let mut data = BufReader::new(file);
    data.seek(SeekFrom::Start(read_end(&mut index, pos)?))?;
    let mut lines = Vec::new();
    let mut records = Vec::new();
    let mut total = 0usize;
    let limit = limit.clamp(1, 100) as usize;
    while pos < end && lines.len() < limit {
        let mut buf = Vec::new();
        (&mut data).take(32 * 1024).read_until(b'\n', &mut buf)?;
        ensure!(
            buf.ends_with(b"\n"),
            "incomplete committed transcript entry"
        );
        let v: Value = serde_json::from_slice(&buf)?;
        let text = if let Some(t) = v["text"].as_str() {
            t.trim_end_matches(['\r', '\n']).to_owned()
        } else {
            let h = v["hex"]
                .as_str()
                .context("invalid transcript record")?
                .as_bytes();
            let bytes = h
                .chunks_exact(2)
                .map(|b| {
                    u8::from_str_radix(std::str::from_utf8(b)?, 16).map_err(anyhow::Error::from)
                })
                .collect::<Result<Vec<_>>>()?;
            String::from_utf8_lossy(&bytes)
                .trim_end_matches(['\r', '\n'])
                .to_owned()
        };
        if text.contains(query) {
            if total + text.len() > PAGE_BYTES && !lines.is_empty() {
                break;
            }
            total += text.len();
            records.push(json!({"offset":pos,"kind":v["kind"],"time":v["time"]}));
            lines.push(text);
        }
        pos += 1;
    }
    Ok(
        json!({"run_id":id,"offset":offset,"next_offset":pos,"lines":lines,"records":records,"eof":pos==count}),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    struct Temp(PathBuf);
    impl Temp {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!("tarla-journal-{}", Uuid::now_v7())))
        }
    }
    impl Drop for Temp {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn survives_reopen_and_recovers_interrupted_append() {
        let root = Temp::new();
        let d = location(&root.0, "id").unwrap();
        Journal::open(&d)
            .unwrap()
            .append(payload_records("output", b"one\n"))
            .unwrap();
        let mut data = OpenOptions::new()
            .append(true)
            .open(d.join("transcript.jsonl"))
            .unwrap();
        data.write_all(b"{\"time\":1,\"kind\":\"output\",\"text\":\"two\"}\n{\"time\":")
            .unwrap();
        let mut idx = OpenOptions::new()
            .append(true)
            .open(d.join("offsets"))
            .unwrap();
        idx.write_all(&[4, 9]).unwrap();
        Journal::open(&d)
            .unwrap()
            .append(payload_records("output", b"three\n"))
            .unwrap();
        let v = page(&root.0, "id", 1, 6, "").unwrap();
        assert_eq!(v["lines"], json!(["two", "three"]));
        assert_eq!(v["next_offset"], 3);
        assert_eq!(v["eof"], true);
    }

    #[test]
    fn pages_scan_boundedly_and_finish_is_idempotent() {
        let root = Temp::new();
        let d = location(&root.0, "../alias").unwrap();
        assert!(d.starts_with(root.0.join("runtime/runs")));
        let many = "old\n".repeat(1040) + "needle\n";
        Journal::open(&d)
            .unwrap()
            .append(payload_records("output", many.as_bytes()))
            .unwrap();
        let first = page(&root.0, "../alias", 0, 3, "needle").unwrap();
        assert_eq!(first["lines"], json!([]));
        assert_eq!(first["next_offset"], SCAN_LIMIT);
        assert_eq!(first["eof"], false);
        let next = page(&root.0, "../alias", SCAN_LIMIT, 7, "needle").unwrap();
        assert_eq!(next["lines"], json!(["needle"]));
        finish(&root.0, "../alias", "succeeded", "final").unwrap();
        finish(&root.0, "../alias", "succeeded", "final").unwrap();
        assert_eq!(
            page(&root.0, "../alias", 1041, 10, "").unwrap()["lines"],
            json!(["final", "succeeded"])
        );
    }

    #[test]
    fn binary_output_round_trips_and_missing_live_capture_is_marked() {
        let root = Temp::new();
        let d = location(&root.0, "one").unwrap();
        Journal::open(&d)
            .unwrap()
            .append(payload_records("output", &[0xff, 0, 0x0a]))
            .unwrap();
        let v: Value =
            serde_json::from_slice(&fs::read(d.join("transcript.jsonl")).unwrap()).unwrap();
        assert_eq!(v["hex"], "ff000a");
        finish(&root.0, "short", "failed", "").unwrap();
        let value = page(&root.0, "short", 0, 10, "").unwrap();
        assert_eq!(value["records"][0]["kind"], "gap");
    }
}
