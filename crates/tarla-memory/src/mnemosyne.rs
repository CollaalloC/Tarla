//! On-demand local search projection for a user-installed Mnemosyne SDK.
//! The workspace WAL and S2 log remain authoritative, including while offline.
use super::{cursor, verify_workspace, Memory};
use anyhow::{bail, ensure, Context, Result};
use rusqlite::{params, Connection, OpenFlags, OptionalExtension};
use serde_json::json;
use std::{
    fs::{File, OpenOptions},
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};
use wait_timeout::ChildExt;

#[derive(Clone)]
pub struct MnemosyneIndex {
    interpreter: PathBuf,
    database: PathBuf,
    private_config: PathBuf,
}

impl MnemosyneIndex {
    pub fn new(python: PathBuf, workspace_root: &Path, db_path: Option<&Path>) -> Result<Self> {
        ensure!(python.is_absolute() && python.is_file(),"mnemosyne_python must be an absolute path to a Python interpreter with mnemosyne-memory installed");
        // Preserve a venv executable's path. Resolving its symlink here would switch environments.
        let root = workspace_root.canonicalize()?;
        let requested = db_path.unwrap_or(Path::new("mnemosyne.db"));
        let path = if requested.starts_with("~") {
            let home = std::env::var_os("HOME")
                .or_else(|| std::env::var_os("USERPROFILE"))
                .context("cannot expand home directory")?;
            PathBuf::from(home).join(requested.strip_prefix("~")?)
        } else {
            root.join(requested)
        };
        let parent = path
            .parent()
            .context("missing Mnemosyne directory")?
            .canonicalize()?;
        ensure!(
            parent.starts_with(&root),
            "Mnemosyne database must stay in its workspace"
        );
        let name = path
            .file_name()
            .context("missing Mnemosyne database name")?;
        let database = parent.join(name);
        ensure!(
            database.extension().is_some_and(|ext| ext == "db")
                && database != root.join("state.db")
                && !std::fs::symlink_metadata(&database).is_ok_and(|m| m.file_type().is_symlink()),
            "invalid Mnemosyne projection path"
        );
        let private_config = root.join("runtime").join("mnemosyne");
        std::fs::create_dir_all(&private_config)?;
        ensure!(
            private_config.canonicalize()?.starts_with(&root),
            "Mnemosyne config must stay in its workspace"
        );
        Ok(Self {
            interpreter: python,
            database,
            private_config,
        })
    }

    pub fn search(
        &self,
        db: &mut Connection,
        workspace: &str,
        _node: &str,
        query: &str,
        limit: usize,
    ) -> Result<Vec<Memory>> {
        verify_workspace(db, workspace)?;
        let limit = limit.min(256);
        if limit == 0 {
            return Ok(Vec::new());
        }
        // The lock also serializes separate daemon instances using this workspace.
        let lock_path = self.private_config.join("projection.lock");
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        lock.lock()?;
        let tx = db.transaction()?;
        let seq = cursor(&tx)?;
        let rebuild = if self.database.exists() {
            let index =
                Connection::open_with_flags(&self.database, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            // An interrupted first SQLite backup may leave an empty database.
            let tables: i64 = index.query_row(
                "SELECT count(*) FROM sqlite_schema WHERE type='table'",
                [],
                |r| r.get(0),
            )?;
            if tables == 0 {
                true
            } else {
                let (owner, current): (String, i64) = index
                    .query_row("SELECT workspace,cursor FROM tarla_projection", [], |r| {
                        Ok((r.get(0)?, r.get(1)?))
                    })
                    .context("existing database is not a Tarla Mnemosyne projection")?;
                ensure!(
                    owner == workspace,
                    "Mnemosyne projection belongs to a different workspace"
                );
                current != seq
            }
        } else {
            true
        };
        let records: Vec<(String, i64, String)> = if rebuild {
            let mut stmt=tx.prepare("SELECT memory_id,version,content FROM memories WHERE is_deleted=0 ORDER BY sync_seq")?;
            let rows = stmt
                .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
                .collect::<rusqlite::Result<_>>()?;
            rows
        } else {
            Vec::new()
        };
        tx.commit()?;
        let mut input = tempfile::tempfile_in(&self.private_config)?;
        serde_json::to_writer(
            &input,
            &json!({"db":self.database,"workspace":workspace,"cursor":seq,
            "records":records,"rebuild":rebuild,"query":query,"limit":limit}),
        )?;
        input.seek(SeekFrom::Start(0))?;
        let mut output = tempfile::tempfile_in(&self.private_config)?;
        let mut errors = tempfile::tempfile_in(&self.private_config)?;
        let mut command = Command::new(&self.interpreter);
        command.env_clear();
        for name in [
            "SystemRoot",
            "WINDIR",
            "TEMP",
            "TMP",
            "TMPDIR",
            "LANG",
            "LC_ALL",
        ] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        let mut child = command
            .arg("-I")
            .arg("-c")
            .arg(include_str!("mnemosyne.py"))
            .env("MNEMOSYNE_DATA_DIR", &self.private_config)
            .env("HERMES_HOME", &self.private_config)
            .env("HF_HUB_OFFLINE", "1")
            .env("ORT_DISABLE_TELEMETRY", "1")
            .current_dir(&self.private_config)
            .stdin(input)
            .stdout(Stdio::from(output.try_clone()?))
            .stderr(Stdio::from(errors.try_clone()?))
            .spawn()?;
        let Some(status) = child.wait_timeout(Duration::from_secs(120))? else {
            let _ = child.kill();
            let _ = child.wait();
            bail!("Mnemosyne query timed out");
        };
        if !status.success() {
            let detail = read_tail(&mut errors, 2048)?;
            bail!(
                "Mnemosyne provider failed: {}",
                detail.lines().last().unwrap_or("unknown provider error")
            );
        }
        ensure!(
            output.metadata()?.len() <= 2 * 1024 * 1024,
            "oversized Mnemosyne response"
        );
        output.seek(SeekFrom::Start(0))?;
        let ids: Vec<(String, i64)> = serde_json::from_reader(output)?;
        let mut result = Vec::new();
        let mut stmt = db.prepare(
            "SELECT c.revision_json FROM memories m JOIN memory_changes c ON c.sync_seq=m.sync_seq
             WHERE m.memory_id=?1 AND m.version=?2 AND m.is_deleted=0",
        )?;
        for (id, version) in ids.into_iter().take(limit) {
            if let Some(raw) = stmt
                .query_row(params![id, version], |r| r.get::<_, String>(0))
                .optional()?
            {
                result.push(serde_json::from_str(&raw)?);
            }
        }
        Ok(result)
    }
}

fn read_tail(file: &mut File, limit: u64) -> Result<String> {
    file.seek(SeekFrom::Start(
        file.metadata()?.len().saturating_sub(limit),
    ))?;
    let mut data = Vec::new();
    file.read_to_end(&mut data)?;
    Ok(String::from_utf8_lossy(&data).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemoryBackend, Mutation, SqliteMemory};
    #[test]
    #[ignore = "set TARLA_MNEMOSYNE_PYTHON to a venv with mnemosyne-memory installed"]
    fn real_mnemosyne_search_rebuild_and_archive() -> Result<()> {
        let python = PathBuf::from(
            std::env::var_os("TARLA_MNEMOSYNE_PYTHON").context("set TARLA_MNEMOSYNE_PYTHON")?,
        );
        let temp = tempfile::tempdir()?;
        let mut db = crate::open(&temp.path().join("state.db"), "work")?;
        let provider = MnemosyneIndex::new(python.clone(), temp.path(), None)?;
        assert!(MnemosyneIndex::new(python, temp.path(), Some(Path::new("state.db"))).is_err());
        let mut p = Mutation {
            proposal_id: "p1".into(),
            memory_id: "robot".into(),
            scope: "global".into(),
            content: "The parking robot uses a laser scanner to locate cars.".into(),
            base_version: 0,
            source_node_id: "a".into(),
            source_run_id: None,
            is_deleted: false,
            hlc_timestamp: None,
        };
        SqliteMemory::new(&mut db, "work", "a").propose_mutation(&p)?;
        assert_eq!(
            provider.search(&mut db, "work", "a", "laser scanner", 5)?[0].memory_id,
            "robot"
        );
        assert_eq!(
            provider.search(&mut db, "work", "a", "laser scanner", 5)?[0].version,
            1
        );
        p.proposal_id = "p2".into();
        p.base_version = 1;
        p.content = "The robot uses a depth camera to locate cars.".into();
        SqliteMemory::new(&mut db, "work", "a").propose_mutation(&p)?;
        assert_eq!(
            provider.search(&mut db, "work", "a", "depth camera", 5)?[0].version,
            2
        );
        p.proposal_id = "p3".into();
        p.base_version = 2;
        p.is_deleted = true;
        SqliteMemory::new(&mut db, "work", "a").propose_mutation(&p)?;
        assert!(provider
            .search(&mut db, "work", "a", "camera", 5)?
            .is_empty());
        Ok(())
    }
}
