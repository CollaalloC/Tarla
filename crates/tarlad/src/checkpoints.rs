//! SHA-256 conflict-aware file checkpoints and non-destructive rollback (Spec §6.4).
use anyhow::{bail, ensure, Context, Result};
use rusqlite::{params, Connection, TransactionBehavior};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::Write,
    path::{Component, Path, PathBuf},
};

const MAX_CHECKPOINT_FILE_BYTES: u64 = 4 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct FileBaseline {
    pub sha256: String,
    pub bytes: Vec<u8>,
}

pub type WorktreeSnapshot = BTreeMap<String, FileBaseline>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RollbackReport {
    pub run_id: String,
    pub restored_paths: Vec<String>,
    pub removed_created_paths: Vec<String>,
    pub conflicted_paths: Vec<String>,
}

fn should_skip_dir(name: &str) -> bool {
    matches!(
        name,
        ".git" | "target" | "node_modules" | ".tarla" | "runtime" | "__pycache__" | ".venv"
    )
}

fn validate_rel_path(rel: &str) -> Result<PathBuf> {
    let p = Path::new(rel);
    ensure!(
        !rel.is_empty() && !p.is_absolute(),
        "invalid checkpoint relative path"
    );
    for comp in p.components() {
        if !matches!(comp, Component::Normal(_)) {
            bail!("unsafe path component in checkpoint `{rel}`");
        }
    }
    Ok(p.to_path_buf())
}

fn sha256_hex(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn walk_dir(root: &Path, dir: &Path, out: &mut WorktreeSnapshot) -> Result<()> {
    let Ok(entries) = fs::read_dir(dir) else {
        return Ok(());
    };
    for entry in entries {
        let entry = entry?;
        let meta = fs::symlink_metadata(entry.path())?;
        if meta.file_type().is_symlink() {
            continue;
        }
        let path = entry.path();
        if meta.is_dir() {
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if should_skip_dir(name) {
                    continue;
                }
            }
            walk_dir(root, &path, out)?;
        } else if meta.is_file() && meta.len() <= MAX_CHECKPOINT_FILE_BYTES {
            let rel = path
                .strip_prefix(root)
                .context("checkpoint outside workdir")?
                .to_string_lossy()
                .replace('\\', "/");
            let bytes = fs::read(&path)?;
            out.insert(
                rel,
                FileBaseline {
                    sha256: sha256_hex(&bytes),
                    bytes,
                },
            );
        }
    }
    Ok(())
}

pub fn capture_worktree(workdir: &Path) -> Result<WorktreeSnapshot> {
    let mut snapshot = BTreeMap::new();
    if workdir.is_dir() {
        walk_dir(workdir, workdir, &mut snapshot)?;
    }
    Ok(snapshot)
}

/// Return sorted relative paths of files added, modified, or deleted relative to `pre`.
pub fn diff_worktree_paths(workdir: &Path, pre: &WorktreeSnapshot) -> Result<Vec<String>> {
    let post = capture_worktree(workdir)?;
    let mut changed = BTreeSet::new();
    for (path, before) in pre {
        match post.get(path) {
            Some(after) if after.sha256 == before.sha256 => {}
            _ => {
                changed.insert(path.clone());
            }
        }
    }
    for path in post.keys() {
        if !pre.contains_key(path) {
            changed.insert(path.clone());
        }
    }
    Ok(changed.into_iter().collect())
}

/// Persist `(run_id, rel_path, pre_sha256, post_sha256, backup_blob)` for all files touched by `run_id`.
pub fn finalize_run_checkpoints(
    db: &mut Connection,
    run_id: &str,
    workdir: &Path,
    pre: &WorktreeSnapshot,
) -> Result<Vec<String>> {
    let post = capture_worktree(workdir)?;
    let mut all_keys = BTreeSet::new();
    all_keys.extend(pre.keys().cloned());
    all_keys.extend(post.keys().cloned());

    let mut touched = Vec::new();
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    for rel in all_keys {
        let before = pre.get(&rel);
        let after = post.get(&rel);
        if before.map(|b| &b.sha256) == after.map(|a| &a.sha256) {
            continue;
        }
        let pre_sha = before.map(|b| b.sha256.as_str());
        let post_sha = after.map(|a| a.sha256.as_str());
        let backup = before.map(|b| b.bytes.as_slice());
        tx.execute(
            "INSERT INTO file_checkpoints(run_id, rel_path, pre_sha256, post_sha256, backup_blob)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(run_id, rel_path) DO UPDATE SET
                pre_sha256 = excluded.pre_sha256,
                post_sha256 = excluded.post_sha256,
                backup_blob = excluded.backup_blob",
            params![run_id, rel, pre_sha, post_sha, backup],
        )?;
        touched.push(rel);
    }
    tx.commit()?;
    Ok(touched)
}

fn atomic_restore(dest: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = dest.with_extension(format!("tarla-rollback-{}", std::process::id()));
    {
        let mut f = fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.flush()?;
        f.sync_all()?;
    }
    fs::rename(&tmp, dest)?;
    Ok(())
}

/// Conflict-aware rollback: verifies current on-disk SHA-256 equals `post_sha256` before
/// restoring `backup_blob`. Any file edited manually by a human after the run finished is
/// skipped and reported in `conflicted_paths`.
pub fn rollback_run(
    db: &mut Connection,
    workspace_id: &str,
    run_id: &str,
    workdir: &Path,
    now_secs: i64,
) -> Result<RollbackReport> {
    type Row = (String, Option<String>, Option<String>, Option<Vec<u8>>);
    let rows: Vec<Row> = {
        let mut stmt = db.prepare(
            "SELECT rel_path, pre_sha256, post_sha256, backup_blob
             FROM file_checkpoints
             WHERE run_id = ?1
             ORDER BY rel_path ASC",
        )?;
        let out = stmt
            .query_map([run_id], |r| {
                Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        out
    };
    ensure!(
        !rows.is_empty(),
        "no file checkpoints recorded for run `{run_id}`"
    );

    let mut restored_paths = Vec::new();
    let mut removed_created_paths = Vec::new();
    let mut conflicted_paths = Vec::new();

    for (rel, pre_sha, post_sha, backup_blob) in rows {
        let rel_path = validate_rel_path(&rel)?;
        let target = workdir.join(&rel_path);
        let current_sha = if target.exists() {
            Some(sha256_hex(&fs::read(&target)?))
        } else {
            None
        };
        if current_sha != post_sha {
            conflicted_paths.push(rel);
            continue;
        }
        match (pre_sha, backup_blob) {
            (Some(_), Some(bytes)) => {
                atomic_restore(&target, &bytes)?;
                restored_paths.push(rel);
            }
            (None, _) => {
                if target.exists() {
                    fs::remove_file(&target)?;
                }
                removed_created_paths.push(rel);
            }
            (Some(_), None) => {
                conflicted_paths.push(rel);
            }
        }
    }

    let report = RollbackReport {
        run_id: run_id.to_owned(),
        restored_paths,
        removed_created_paths,
        conflicted_paths,
    };
    tarla_memory::record_sync_event(
        db,
        workspace_id,
        "rollback",
        run_id,
        &json!(report),
        now_secs,
    )?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conflict_aware_rollback_preserves_post_run_human_edits() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let workdir = dir.path().join("repo");
        fs::create_dir_all(workdir.join("src"))?;
        fs::write(workdir.join("src/a.rs"), "fn a() -> i32 { 1 }\n")?;
        fs::write(workdir.join("src/b.rs"), "fn b() -> i32 { 2 }\n")?;

        let pre = capture_worktree(&workdir)?;
        // Simulate Bot modifying a.rs, b.rs, and creating c.rs.
        fs::write(workdir.join("src/a.rs"), "fn a() -> i32 { 100 }\n")?;
        fs::write(workdir.join("src/b.rs"), "fn b() -> i32 { 200 }\n")?;
        fs::write(workdir.join("src/c.rs"), "fn c() {}\n")?;

        let mut db = tarla_memory::open(Path::new(":memory:"), "work")?;
        let touched = finalize_run_checkpoints(&mut db, "run-1", &workdir, &pre)?;
        assert_eq!(touched, vec!["src/a.rs", "src/b.rs", "src/c.rs"]);

        // Human manually edits src/b.rs after the Bot finished!
        fs::write(
            workdir.join("src/b.rs"),
            "fn b() -> i32 { 999 /* human */ }\n",
        )?;

        let report = rollback_run(&mut db, "work", "run-1", &workdir, 100)?;
        assert_eq!(report.restored_paths, vec!["src/a.rs"]);
        assert_eq!(report.removed_created_paths, vec!["src/c.rs"]);
        assert_eq!(report.conflicted_paths, vec!["src/b.rs"]);

        assert_eq!(
            fs::read_to_string(workdir.join("src/a.rs"))?,
            "fn a() -> i32 { 1 }\n"
        );
        assert_eq!(
            fs::read_to_string(workdir.join("src/b.rs"))?,
            "fn b() -> i32 { 999 /* human */ }\n"
        );
        assert!(!workdir.join("src/c.rs").exists());
        Ok(())
    }
}
