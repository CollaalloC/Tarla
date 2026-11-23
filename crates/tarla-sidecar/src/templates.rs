//! Managed `herdr-core` agent templates (`*.md`) and core group configuration (`config.toml`).
use crate::{herdr::Herdr, invalid, private_dir};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    fs,
    io::{self, Write},
    path::PathBuf,
};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentTemplateSpec {
    pub name: String,
    pub description: String,
    pub command: String,
    pub submit: String,
    pub stop: String,
    pub paste_gap_ms: u64,
    pub ready: Option<String>,
    pub system_prompt: String,
}

impl AgentTemplateSpec {
    pub fn new(
        name: impl Into<String>,
        description: impl Into<String>,
        command: impl Into<String>,
        system_prompt: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            command: command.into(),
            submit: "enter".into(),
            stop: "ctrl+c".into(),
            paste_gap_ms: 80,
            ready: None,
            system_prompt: system_prompt.into(),
        }
    }

    pub fn with_ready(mut self, ready: impl Into<String>) -> Self {
        self.ready = Some(ready.into());
        self
    }

    /// Standard Master Orchestrator template (`master-chief.md`) with zero-MCP native `hc` + `tarla ctl` delegation.
    pub fn master_chief(
        name: impl Into<String>,
        command: impl Into<String>,
        ready_quiet: bool,
    ) -> Self {
        let mut spec = Self::new(
            name,
            "Workspace Master Orchestrator with native hc & tarla-ctl delegation",
            command,
            concat!(
                "你是当前工作空间的主控协调 Bot（chief）。严禁直接修改业务仓库源码，你的职责是拆解目标、调度异构子 Bot 并在验收后向用户汇报。\n",
                "1. 本机快速协同：直接运行 `hc --skill` 查看终端编排原语，使用 `hc run --bg --name <name> <template> \"<task>\"` 拉起本机异构协作者，使用 `hc wait --children any` 等待回执，或读写 `hc` 文档黑板。\n",
                "2. 跨机与持久任务调度：运行 `tarla ctl --json <subcommand>` 查询全网节点/Bot 状态、跨机派发持久化 Action、发布 Botflow 事件或检索修订 S2 长期记忆。"
            ),
        );
        if ready_quiet {
            spec.ready = Some("quiet".into());
        }
        spec
    }
}

pub(crate) fn validate_safe_ident(label: &str, value: &str, max_len: usize) -> io::Result<()> {
    if value.is_empty()
        || value.len() > max_len
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(invalid(format!("invalid {label}: {value}")));
    }
    Ok(())
}

fn yaml_scalar(value: &str) -> io::Result<String> {
    if value.chars().any(|c| c == '\n' || c == '\r' || c == '\0') {
        return Err(invalid("template header field cannot contain newlines"));
    }
    if !value.is_empty()
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_./+".contains(&b))
    {
        Ok(value.to_owned())
    } else {
        serde_json::to_string(value).map_err(io::Error::from)
    }
}

pub fn render_template_markdown(spec: &AgentTemplateSpec) -> io::Result<String> {
    validate_safe_ident("template name", &spec.name, 64)?;
    if spec.command.trim().is_empty() {
        return Err(invalid("template command must be non-empty"));
    }
    if spec.submit.trim().is_empty() || spec.stop.trim().is_empty() {
        return Err(invalid("template submit and stop keys must be non-empty"));
    }
    let mut out = String::from("---\n");
    out.push_str(&format!(
        "description: {}\n",
        yaml_scalar(spec.description.trim())?
    ));
    out.push_str(&format!("command: {}\n", yaml_scalar(spec.command.trim())?));
    out.push_str(&format!("submit: {}\n", yaml_scalar(spec.submit.trim())?));
    out.push_str(&format!("stop: {}\n", yaml_scalar(spec.stop.trim())?));
    out.push_str(&format!("paste_gap_ms: {}\n", spec.paste_gap_ms));
    if let Some(ready) = spec
        .ready
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        out.push_str(&format!("ready: {}\n", yaml_scalar(ready)?));
    }
    out.push_str("---\n");
    let body = spec.system_prompt.trim();
    if !body.is_empty() {
        out.push_str(body);
        out.push('\n');
    }
    Ok(out)
}

pub fn render_core_config_toml(notes_quiet_secs: u32, draft_wait_secs: u32) -> String {
    format!(
        "notes_quiet = {}\ndraft_wait = {}\n",
        notes_quiet_secs.max(1),
        draft_wait_secs.max(5)
    )
}

fn write_private_file(
    dir: &std::path::Path,
    filename: &str,
    contents: &str,
) -> io::Result<PathBuf> {
    private_dir(dir)?;
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let nonce = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let tmp_path = dir.join(format!(".{filename}.{}.{nonce}.tmp", std::process::id()));
    let mut opts = fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.mode(0o600);
    }
    let write_res = (|| -> io::Result<()> {
        let mut f = opts.open(&tmp_path)?;
        f.write_all(contents.as_bytes())?;
        f.flush()?;
        f.sync_all()?;
        Ok(())
    })();
    if let Err(err) = write_res {
        let _ = fs::remove_file(&tmp_path);
        return Err(err);
    }
    let dest = dir.join(filename);
    if let Err(err) = fs::rename(&tmp_path, &dest) {
        let _ = fs::remove_file(&tmp_path);
        return Err(err);
    }
    Ok(dest)
}

/// Atomically materialize `hc-config/herdr-core/config.toml` and `templates/*.md` for one `Herdr` core group.
pub fn sync_core_templates(
    herdr: &Herdr,
    notes_quiet_secs: u32,
    draft_wait_secs: u32,
    templates: &[AgentTemplateSpec],
) -> io::Result<PathBuf> {
    let hc_root = herdr.runtime_dir().join("hc-config");
    private_dir(&hc_root)?;
    let core_cfg_dir = hc_root.join("herdr-core");
    private_dir(&core_cfg_dir)?;
    write_private_file(
        &core_cfg_dir,
        "config.toml",
        &render_core_config_toml(notes_quiet_secs, draft_wait_secs),
    )?;
    let templates_dir = core_cfg_dir.join("templates");
    private_dir(&templates_dir)?;

    let mut expected_files = BTreeSet::new();
    for spec in templates {
        let rendered = render_template_markdown(spec)?;
        let fname = format!("{}.md", spec.name);
        if !expected_files.insert(fname.clone()) {
            return Err(invalid(format!("duplicate template name: {}", spec.name)));
        }
        write_private_file(&templates_dir, &fname, &rendered)?;
    }

    for entry in fs::read_dir(&templates_dir)? {
        let entry = entry?;
        let name = entry.file_name();
        if let Some(s) = name.to_str() {
            if s.ends_with(".md") && !expected_files.contains(s) {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
    Ok(templates_dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_herdr_templates_and_prunes_stale_entries() {
        let dir = tempfile::tempdir().unwrap();
        let herdr = Herdr::for_core_group(
            &dir.path().join("workspaces/work/runtime"),
            "work",
            "master_core",
            None,
        )
        .unwrap();
        assert_eq!(herdr.session_name(), "tarla-work-master_core");

        let chief = AgentTemplateSpec::master_chief("master-chief", "codex", false);
        let legacy = AgentTemplateSpec::new(
            "custom-cli",
            "Non-OSC third-party CLI",
            "/usr/local/bin/pi",
            "Follow instructions.",
        )
        .with_ready("quiet");

        let tdir = sync_core_templates(&herdr, 3, 60, &[chief.clone(), legacy]).unwrap();
        let cfg = fs::read_to_string(tdir.parent().unwrap().join("config.toml")).unwrap();
        assert!(cfg.contains("notes_quiet = 3"));
        assert!(cfg.contains("draft_wait = 60"));

        let chief_md = fs::read_to_string(tdir.join("master-chief.md")).unwrap();
        assert!(chief_md.starts_with("---\n"));
        assert!(chief_md.contains("command: codex\n"));
        assert!(chief_md.contains("paste_gap_ms: 80\n"));
        assert!(chief_md.contains("tarla ctl --json"));

        let custom_md = fs::read_to_string(tdir.join("custom-cli.md")).unwrap();
        assert!(custom_md.contains("ready: quiet\n"));

        sync_core_templates(&herdr, 3, 60, &[chief]).unwrap();
        assert!(tdir.join("master-chief.md").exists());
        assert!(!tdir.join("custom-cli.md").exists());
    }
}
