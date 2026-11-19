//! On-disk workspace identity, private bootstrap files, role profile and skill discovery.
use anyhow::{ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs,
    io::Write,
    path::{Path, PathBuf},
};
use tempfile::NamedTempFile;
use uuid::Uuid;

#[derive(Clone, Deserialize, Serialize)]
pub struct Identity {
    pub id: String,
    #[serde(default)]
    pub display_name: String,
    #[serde(default)]
    pub node_id: String,
    pub role_mode: String,
    pub supervisor_node_id: String,
    pub supervisor_term: i64,
}
#[derive(Clone, Default, Deserialize, Serialize)]
#[serde(default)]
pub struct Supervisor {
    pub brain: Brain,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Brain {
    pub max_tool_steps: usize,
    pub fallback_chain: Vec<String>,
    pub backends: BTreeMap<String, toml::Value>,
}
impl Default for Brain {
    fn default() -> Self {
        Self {
            max_tool_steps: 5,
            fallback_chain: vec![],
            backends: BTreeMap::new(),
        }
    }
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ContextPolicy {
    pub supervisor_active_budget_tokens: usize,
    pub compaction_high_watermark_pct: usize,
    pub protected_verbatim_turns: usize,
    pub mission_packet_max_tokens: usize,
    pub run_receipt_max_tokens: usize,
}
impl Default for ContextPolicy {
    fn default() -> Self {
        Self {
            supervisor_active_budget_tokens: 24000,
            compaction_high_watermark_pct: 75,
            protected_verbatim_turns: 8,
            mission_packet_max_tokens: 1800,
            run_receipt_max_tokens: 500,
        }
    }
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct MemoryConfig {
    pub primary_backend: String,
    pub prefetch_token_budget: usize,
}
impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            primary_backend: "builtin_sqlite".into(),
            prefetch_token_budget: 1200,
        }
    }
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct Network {
    pub mode: String,
    pub enable_easytier: bool,
    pub enable_tailscale: bool,
    pub easytier_network_name: String,
    pub easytier_network_secret_ref: String,
    #[serde(flatten)]
    pub extra: BTreeMap<String, toml::Value>,
}
impl Default for Network {
    fn default() -> Self {
        Self {
            mode: "managed_sidecar".into(),
            enable_easytier: false,
            enable_tailscale: false,
            easytier_network_name: String::new(),
            easytier_network_secret_ref: String::new(),
            extra: BTreeMap::new(),
        }
    }
}
#[derive(Clone, Deserialize, Serialize)]
pub struct WorkspaceConfig {
    pub workspace: Identity,
    #[serde(default)]
    pub supervisor: Supervisor,
    #[serde(default)]
    pub context_policy: ContextPolicy,
    #[serde(default)]
    pub network: Network,
    #[serde(default)]
    pub memory: MemoryConfig,
    #[serde(flatten)]
    pub extra: BTreeMap<String, toml::Value>,
}
pub struct Workspace {
    pub root: PathBuf,
    pub config: WorkspaceConfig,
    pub token: String,
    pub attach_key: String,
    pub soul: String,
}

pub fn validate_name(id: &str) -> Result<()> {
    ensure!(
        !id.is_empty()
            && id.len() <= 64
            && id.as_bytes()[0].is_ascii_alphanumeric()
            && id
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_'),
        "identifier must be a single safe path component"
    );
    // Windows device names also resolve specially on case-insensitive filesystems.
    let upper = id.to_ascii_uppercase();
    ensure!(
        !matches!(upper.as_str(), "CON" | "PRN" | "AUX" | "NUL")
            && !(upper.len() == 4
                && (upper.starts_with("COM") || upper.starts_with("LPT"))
                && upper.as_bytes()[3].is_ascii_digit()),
        "reserved identifier"
    );
    Ok(())
}
fn private_dir(path: &Path) -> Result<()> {
    use std::io::ErrorKind;
    if let Err(e) = fs::symlink_metadata(path) {
        ensure!(
            e.kind() == ErrorKind::NotFound,
            "cannot inspect directory {}",
            path.display()
        );
        let mut b = fs::DirBuilder::new();
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            b.mode(0o700);
        }
        if let Err(e) = b.create(path) {
            if e.kind() != ErrorKind::AlreadyExists {
                return Err(e.into());
            }
        }
    }
    ensure!(
        fs::symlink_metadata(path)?.is_dir(),
        "expected a real directory at {}",
        path.display()
    );
    Ok(())
}
fn regular(path: &Path) -> Result<()> {
    ensure!(
        fs::symlink_metadata(path)?.is_file(),
        "expected a regular file at {}",
        path.display()
    );
    Ok(())
}
fn install_once(path: &Path, data: &[u8]) -> Result<()> {
    let parent = path.parent().context("invalid path")?;
    let mut tmp = NamedTempFile::new_in(parent)?;
    tmp.write_all(data)?;
    tmp.as_file().sync_all()?;
    match tmp.persist_noclobber(path) {
        Ok(_) => Ok(()),
        Err(e) if e.error.kind() == std::io::ErrorKind::AlreadyExists => regular(path),
        Err(e) => Err(e.error.into()),
    }
}
fn read_key(path: &Path) -> Result<String> {
    regular(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            fs::metadata(path)?.permissions().mode() & 0o077 == 0,
            "credential permissions must be 0600: {}",
            path.display()
        );
    }
    let s = fs::read_to_string(path)?.trim().to_owned();
    ensure!(s.len() >= 32 && s.len() <= 512, "invalid credential file");
    Ok(s)
}
fn secret() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}
impl Workspace {
    pub fn init(
        home: &Path,
        id: &str,
        node_id: &str,
        role: &str,
        supervisor_id: &str,
    ) -> Result<Self> {
        for name in [id, node_id, supervisor_id] {
            validate_name(name)?;
        }
        let cfg = WorkspaceConfig {
            workspace: Identity {
                id: id.into(),
                display_name: id.into(),
                node_id: node_id.into(),
                role_mode: role.into(),
                supervisor_node_id: supervisor_id.into(),
                supervisor_term: 1,
            },
            supervisor: Supervisor::default(),
            context_policy: ContextPolicy::default(),
            memory: MemoryConfig::default(),
            network: Network::default(),
            extra: BTreeMap::new(),
        };
        validate(&cfg, id)?;
        if let Some(p) = home.parent().filter(|p| !p.as_os_str().is_empty()) {
            fs::create_dir_all(p)?;
        }
        private_dir(home)?;
        for dir in ["bin", "slots", "workspaces"] {
            private_dir(&home.join(dir))?;
        }
        install_once(&home.join("config.toml"), b"# Tarla host configuration\n")?;
        let root = home.join("workspaces").join(id);
        private_dir(&root)?;
        for dir in ["runtime", "skills"] {
            private_dir(&root.join(dir))?;
        }
        for file in ["api.token", "attach.key"] {
            install_once(&root.join("runtime").join(file), secret().as_bytes())?;
        }
        install_once(&root.join("SOUL.md"),b"You coordinate work within this workspace. Use the configured control tools and preserve the user's decisions.\n")?;
        install_once(
            &root.join("workspace.toml"),
            toml::to_string_pretty(&cfg)?.as_bytes(),
        )?;
        let ws = Self::open(home, id)?;
        let actual = &ws.config.workspace;
        ensure!(
            actual.node_id == node_id
                && actual.role_mode == role
                && actual.supervisor_node_id == supervisor_id,
            "workspace already belongs to a different node or role"
        );
        Ok(ws)
    }
    pub fn open(home: &Path, id: &str) -> Result<Self> {
        validate_name(id)?;
        let root = home.join("workspaces").join(id);
        for path in [
            home,
            &home.join("workspaces"),
            root.as_path(),
            &root.join("runtime"),
            &root.join("skills"),
        ] {
            ensure!(
                fs::symlink_metadata(path)
                    .with_context(|| format!("workspace unavailable at {}", path.display()))?
                    .is_dir(),
                "workspace directory cannot be a symlink"
            );
        }
        for name in ["workspace.toml", "SOUL.md"] {
            regular(&root.join(name))?;
        }
        let mut config: WorkspaceConfig =
            toml::from_str(&fs::read_to_string(root.join("workspace.toml"))?)?;
        if config.workspace.node_id.is_empty() && config.workspace.role_mode != "executor_only" {
            config.workspace.node_id = config.workspace.supervisor_node_id.clone();
        }
        validate(&config, id)?;
        let token = read_key(&root.join("runtime/api.token"))?;
        let attach_key = read_key(&root.join("runtime/attach.key"))?;
        ensure!(
            token != attach_key,
            "attach and API credentials must differ"
        );
        let soul = fs::read_to_string(root.join("SOUL.md"))?;
        Ok(Self {
            root,
            config,
            token,
            attach_key,
            soul,
        })
    }
    pub fn skills(&self) -> Result<Vec<Skill>> {
        let mut skills = vec![];
        for entry in fs::read_dir(self.root.join("skills"))? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| anyhow::anyhow!("invalid skill name"))?;
            validate_name(&name)?;
            let path = entry.path().join("SKILL.md");
            if fs::symlink_metadata(&path).is_err() {
                continue;
            }
            regular(&path)?;
            ensure!(fs::metadata(&path)?.len() <= 256 * 1024, "skill too large");
            let content = fs::read_to_string(path)?;
            skills.push(Skill { name, content });
        }
        skills.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(skills)
    }
}
#[derive(Debug, Serialize)]
pub struct Skill {
    pub name: String,
    pub content: String,
}
fn validate(cfg: &WorkspaceConfig, id: &str) -> Result<()> {
    let ident = &cfg.workspace;
    ensure!(ident.id == id, "workspace identity mismatch");
    for v in [&ident.id, &ident.node_id, &ident.supervisor_node_id] {
        validate_name(v)?;
    }
    ensure!(
        matches!(
            ident.role_mode.as_str(),
            "supervisor_and_executor" | "supervisor_only" | "executor_only"
        ) && ident.supervisor_term >= 1,
        "invalid workspace role or term"
    );
    if ident.role_mode != "executor_only" {
        ensure!(
            ident.node_id == ident.supervisor_node_id,
            "supervisor identity does not match designated supervisor"
        );
    }
    ensure!(
        matches!(
            cfg.network.mode.as_str(),
            "managed_sidecar" | "reuse_host_network"
        ),
        "unknown network mode"
    );
    let p = &cfg.context_policy;
    ensure!(
        p.supervisor_active_budget_tokens >= 128
            && (1..=100).contains(&p.compaction_high_watermark_pct)
            && p.protected_verbatim_turns > 0
            && (1..=1800).contains(&p.mission_packet_max_tokens)
            && (1..=500).contains(&p.run_receipt_max_tokens),
        "invalid context policy"
    );
    ensure!(
        (1..=5).contains(&cfg.supervisor.brain.max_tool_steps),
        "max_tool_steps outside 1..5"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn private_workspaces_and_repeat_init() {
        let t = tempfile::tempdir().unwrap();
        let a = Workspace::init(t.path(), "a", "n", "supervisor_only", "n").unwrap();
        let b = Workspace::init(t.path(), "b", "n", "supervisor_only", "n").unwrap();
        assert_ne!(a.token, b.token);
        assert_ne!(a.attach_key, a.token);
        assert_eq!(
            a.token,
            Workspace::init(t.path(), "a", "n", "supervisor_only", "n")
                .unwrap()
                .token
        );
        assert!(Workspace::init(t.path(), "a", "other", "supervisor_only", "other").is_err());
        assert!(Workspace::open(t.path(), "../b").is_err());
    }
    #[cfg(unix)]
    #[test]
    fn symlinks_do_not_bridge_workspaces() {
        use std::os::unix::fs::symlink;
        let t = tempfile::tempdir().unwrap();
        let w = Workspace::init(t.path(), "a", "n", "supervisor_only", "n").unwrap();
        symlink(&w.root, t.path().join("workspaces/b")).unwrap();
        assert!(Workspace::open(t.path(), "b").is_err());
    }
}
