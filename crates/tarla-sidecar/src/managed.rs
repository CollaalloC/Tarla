use crate::{invalid, private_dir, Herdr};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs::{self, OpenOptions},
    io::{self, Read},
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{
    process::Command,
    time::{sleep, timeout},
};

#[derive(Clone, Deserialize)]
struct PinnedTool {
    version: String,
    file: String,
    sha256: String,
}
#[derive(Clone, Deserialize)]
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
#[derive(Clone)]
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

pub(crate) fn clean_command(program: &Path) -> Command {
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
