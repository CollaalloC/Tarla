use anyhow::{ensure, Context, Result};
use clap::ValueEnum;
use fs2::FileExt;
use reqwest::Url;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use tempfile::NamedTempFile;

#[derive(Copy, Clone, ValueEnum)]
pub enum Tool {
    HerdrCore,
    Easytier,
    Tailscale,
}
impl Tool {
    fn names(self) -> (&'static str, &'static str) {
        match self {
            Self::HerdrCore => ("herdr-core", "hc"),
            Self::Easytier => ("easytier", "easytier-core"),
            Self::Tailscale => ("tailscale", "tailscaled"),
        }
    }
}
fn real_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path)?;
    ensure!(
        fs::symlink_metadata(path)?.is_dir(),
        "unsafe directory {}",
        path.display()
    );
    Ok(())
}
fn new_lock(path: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)?;
    FileExt::lock_exclusive(&file)?;
    Ok(file)
}
fn save(path: &Path, data: &[u8]) -> Result<()> {
    let parent = path.parent().context("invalid path")?;
    let mut tmp = NamedTempFile::new_in(parent)?;
    tmp.write_all(data)?;
    tmp.as_file().sync_all()?;
    tmp.persist(path)?;
    #[cfg(unix)]
    File::open(parent)?.sync_all()?;
    Ok(())
}
pub fn pair(
    home: &Path,
    id: &str,
    node: &str,
    origin: &Url,
    token_env: &str,
    client_url: Option<&Url>,
) -> Result<()> {
    tarla_workspace::validate_name(node)?;
    ensure!(
        !token_env.is_empty()
            && token_env.len() <= 128
            && token_env
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
            && !token_env.as_bytes()[0].is_ascii_digit(),
        "invalid environment key"
    );
    ensure!(
        matches!(origin.scheme(), "http" | "https")
            && origin.host().is_some()
            && origin.username().is_empty()
            && origin.password().is_none()
            && origin.query().is_none()
            && origin.fragment().is_none()
            && origin.path() == "/",
        "peer must be a bare HTTP(S) origin"
    );
    let ws = tarla_workspace::Workspace::open(home, id)?;
    ensure!(
        node != ws.config.workspace.node_id,
        "cannot pair the node with itself"
    );
    let _lock = admin_lock(&ws.root)?;
    let path = ws.root.join("workspace.toml");
    let mut cfg: toml::Table = fs::read_to_string(&path)?.parse()?;
    let peers = cfg
        .entry("peers")
        .or_insert_with(|| toml::Value::Array(vec![]))
        .as_array_mut()
        .context("expected peer array")?;
    let mut candidate = toml::Table::from_iter([
        ("node_id".into(), node.into()),
        ("url".into(), origin.as_str().trim_end_matches('/').into()),
        ("token_env".into(), token_env.into()),
    ]);
    if let Some(client) = client_url {
        ensure!(
            client.scheme() == "https"
                && client.host().is_some()
                && client.username().is_empty()
                && client.password().is_none()
                && client.query().is_none()
                && client.fragment().is_none()
                && client.path() == "/",
            "client URL must be a bare HTTPS origin"
        );
        candidate.insert(
            "client_url".into(),
            client.as_str().trim_end_matches('/').into(),
        );
    }
    if let Some(existing) = peers
        .iter()
        .find(|p| p.get("node_id").and_then(toml::Value::as_str) == Some(node))
    {
        ensure!(
            existing.as_table() == Some(&candidate),
            "a different pairing already exists for this node"
        );
        return Ok(());
    }
    peers.push(toml::Value::Table(candidate));
    save(&path, toml::to_string_pretty(&cfg)?.as_bytes())?;
    println!("Paired {node} for workspace {id}. Configure the reciprocal peer and supply {token_env} to both daemons.");
    Ok(())
}
fn admin_lock(root: &Path) -> Result<File> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(root.join("runtime/daemon.lock"))?;
    FileExt::try_lock_exclusive(&file)
        .context("stop the daemon before editing peer configuration")?;
    Ok(file)
}
pub fn install(home: &Path, tool: Tool, version: &str, binary: &Path, sha: &str) -> Result<()> {
    ensure!(
        !version.is_empty()
            && version.len() <= 64
            && version.as_bytes()[0].is_ascii_alphanumeric()
            && version
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b".-_".contains(&b)),
        "invalid version"
    );
    ensure!(
        sha.len() == 64 && sha.bytes().all(|b| b.is_ascii_hexdigit()),
        "expected 64 hex characters in SHA-256"
    );
    let (tool, bin) = tool.names();
    let bin = format!("{bin}{}", std::env::consts::EXE_SUFFIX);
    let cache = home.join("bin");
    real_dir(&cache)?;
    let _lock = new_lock(&cache.join("install.lock"))?;
    let dir = cache.join(tool).join(version);
    real_dir(&cache.join(tool))?;
    real_dir(&dir)?;
    let manifest = cache.join("toolchain-manifest.json");
    let mut pins: Value = if manifest.exists() {
        serde_json::from_reader(File::open(&manifest)?)?
    } else {
        json!({"tools":{}})
    };
    ensure!(pins["tools"].is_object(), "invalid tool manifest");
    let mut source = File::open(binary)?;
    let mut tmp = NamedTempFile::new_in(&dir)?;
    let mut digest = Sha256::new();
    let mut buf = [0; 65536];
    loop {
        let n = source.read(&mut buf)?;
        if n == 0 {
            break;
        }
        tmp.write_all(&buf[..n])?;
        digest.update(&buf[..n]);
    }
    let actual = format!("{:x}", digest.finalize());
    ensure!(
        actual.eq_ignore_ascii_case(sha),
        "binary checksum does not match the requested pin"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tmp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o700))?;
    }
    tmp.as_file().sync_all()?;
    let dst = dir.join(&bin);
    if dst.exists() {
        let metadata = fs::symlink_metadata(&dst)?;
        ensure!(metadata.is_file(), "invalid binary destination");
        let mut stored = Sha256::new();
        std::io::copy(&mut File::open(&dst)?, &mut stored)?;
        ensure!(
            format!("{:x}", stored.finalize()) == actual,
            "the same version already has different bytes"
        );
    } else {
        tmp.persist_noclobber(&dst)?;
    }
    pins["tools"][tool] = json!({"version":version,"file":bin,"sha256":actual});
    save(&manifest, &serde_json::to_vec_pretty(&pins)?)?;
    println!(
        "Pinned {tool} {version} in {}",
        PathBuf::from(&cache).display()
    );
    Ok(())
}
