//! The SQLite file belongs to one workspace on one host. Replication sends revisions,
//! never the file or its WAL. Executor and supervisor authorization is enforced by tarlad.
mod hosted;
pub use hosted::{HostedIndex, Projection};
mod mnemosyne;
pub use mnemosyne::MnemosyneIndex;

use anyhow::{bail, ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

pub fn open(path: &Path, workspace: &str) -> Result<Connection> {
    ensure!(!workspace.trim().is_empty(), "workspace id is required");
    let mut db = Connection::open(path)?;
    db.busy_timeout(Duration::from_secs(5))?;
    db.execute_batch("PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL;")?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let version: i64 = tx.query_row("PRAGMA user_version", [], |row| row.get(0))?;
    match version {
        0 => {
            tx.execute_batch(include_str!("schema.sql"))?;
            tx.execute("INSERT INTO workspace_identity VALUES(1,?1)", [workspace])?;
        }
        1 | 2 => verify_workspace(&tx, workspace)?,
        version => bail!("unsupported database schema version {version}"),
    }
    if version < 2 {
        tx.execute_batch("CREATE TRIGGER prevent_archived_message_delete BEFORE DELETE ON messages BEGIN
            SELECT RAISE(ABORT,'conversation archive is immutable'); END;
            CREATE INDEX idx_active_messages ON messages(conversation_id,seq_no) WHERE compacted_into IS NULL;
            CREATE INDEX idx_trigger_turns ON messages(conversation_id,seq_no) WHERE sender_role IN ('user','run_receipt');")?;
        tx.pragma_update(None, "user_version", 2)?;
    }
    tx.commit()?;
    Ok(db)
}

pub fn verify_workspace(db: &Connection, workspace: &str) -> Result<()> {
    let actual: String = db.query_row(
        "SELECT workspace_id FROM workspace_identity WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    ensure!(
        workspace == actual,
        "database belongs to a different workspace"
    );
    Ok(())
}

fn unix_ms() -> Result<i64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_millis()
        .try_into()?)
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Hlc {
    pub physical_ms: i64,
    pub logical: i64,
    pub node_id: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Memory {
    pub memory_id: String,
    pub workspace_id: String,
    pub scope: String,
    pub content: String,
    pub content_hash: String,
    pub version: i64,
    pub hlc_timestamp: Hlc,
    pub updated_by: String,
    pub is_deleted: bool,
    pub sync_seq: i64,
    pub updated_at: i64,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Mutation {
    /// Stable across retries. Mint a new ID for a new user decision.
    pub proposal_id: String,
    pub memory_id: String,
    pub scope: String,
    pub content: String,
    pub base_version: i64,
    pub source_node_id: String,
    #[serde(default)]
    pub source_run_id: Option<String>,
    #[serde(default)]
    pub is_deleted: bool,
    #[serde(default)]
    pub hlc_timestamp: Option<Hlc>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum MutationResult {
    Applied {
        memory: Memory,
    },
    Conflict {
        candidate_id: String,
        current_version: i64,
    },
}

pub trait MemoryBackend {
    fn prefetch_frozen_snapshot(&self, scope: &str, budget: usize) -> Result<String>;
    fn propose_mutation(&mut self, proposal: &Mutation) -> Result<MutationResult>;
    fn pull_delta_since(&self, after: i64, limit: usize) -> Result<Vec<Memory>>;
    fn search(&self, query: &str, limit: usize) -> Result<Vec<Memory>>;
}

pub struct SqliteMemory<'a> {
    db: &'a mut Connection,
    workspace: &'a str,
    node: &'a str,
}

impl<'a> SqliteMemory<'a> {
    pub fn new(db: &'a mut Connection, workspace: &'a str, node: &'a str) -> Self {
        Self {
            db,
            workspace,
            node,
        }
    }

    pub fn sync_cursor(&self) -> Result<i64> {
        verify_workspace(self.db, self.workspace)?;
        cursor(self.db)
    }

    /// Replays are accepted only if their original content matches exactly.
    /// Missing or out-of-order revisions roll back the entire page.
    pub fn apply_delta(&mut self, revisions: &[Memory]) -> Result<i64> {
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        verify_workspace(&tx, self.workspace)?;
        let mut last = cursor(&tx)?;
        for item in revisions {
            ensure!(
                item.workspace_id == self.workspace,
                "wrong workspace in delta"
            );
            ensure!(item.sync_seq > 0 && item.version > 0, "invalid revision");
            ensure!(
                item.content_hash == hash(&item.content),
                "memory hash mismatch"
            );
            validate_clock(&item.hlc_timestamp)?;
            if item.sync_seq <= last {
                let old: String = tx.query_row(
                    "SELECT revision_json FROM memory_changes WHERE sync_seq=?1",
                    [item.sync_seq],
                    |r| r.get(0),
                )?;
                ensure!(
                    &serde_json::from_str::<Memory>(&old)? == item,
                    "conflicting replay for an acknowledged cursor"
                );
                continue;
            }
            ensure!(
                item.sync_seq == last.checked_add(1).context("cursor exhausted")?,
                "missing memory delta after cursor {last}"
            );
            ensure!(
                item.version
                    == version(&tx, &item.memory_id)?
                        .checked_add(1)
                        .context("version exhausted")?,
                "nonconsecutive memory version"
            );
            persist_revision(&tx, item)?;
            merge_clock(&tx, self.node, Some(&item.hlc_timestamp))?;
            last = item.sync_seq;
        }
        tx.commit()?;
        Ok(last)
    }

    /// The caller includes the version shown to the reviewer. A later change
    /// invalidates that decision and leaves the candidate pending for another review.
    pub fn resolve_candidate(
        &mut self,
        id: &str,
        accept: bool,
        expected_version: i64,
    ) -> Result<Option<Memory>> {
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        verify_workspace(&tx, self.workspace)?;
        let (status, request): (String,String) = tx.query_row(
            "SELECT c.status,d.request_json FROM memory_candidates c JOIN memory_candidate_details d USING(candidate_id) WHERE c.candidate_id=?1",
            [id], |r| Ok((r.get(0)?,r.get(1)?)))?;
        let proposal: Mutation = serde_json::from_str(&request)?;
        ensure!(status == "pending", "candidate was already resolved");
        ensure!(
            version(&tx, &proposal.memory_id)? == expected_version,
            "memory changed after review"
        );
        let applied = if accept {
            Some(apply_proposal(
                &tx,
                self.workspace,
                self.node,
                &proposal,
                expected_version,
                "user_manual",
            )?)
        } else {
            None
        };
        tx.execute(
            "UPDATE memory_candidates SET status=?1 WHERE candidate_id=?2",
            params![if accept { "merged" } else { "dismissed" }, id],
        )?;
        tx.commit()?;
        Ok(applied)
    }
}

fn cursor(db: &Connection) -> Result<i64> {
    Ok(db.query_row(
        "SELECT COALESCE(MAX(sync_seq),0) FROM memory_changes",
        [],
        |r| r.get(0),
    )?)
}
fn version(db: &Connection, id: &str) -> Result<i64> {
    Ok(db
        .query_row(
            "SELECT version FROM memories WHERE memory_id=?1",
            [id],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0))
}
fn hash(text: &str) -> String {
    format!("{:x}", Sha256::digest(text.as_bytes()))
}
fn validate_clock(clock: &Hlc) -> Result<()> {
    ensure!(
        clock.physical_ms >= 0 && clock.logical >= 0 && !clock.node_id.is_empty(),
        "invalid HLC"
    );
    Ok(())
}

fn merge_clock(tx: &Transaction<'_>, node: &str, remote: Option<&Hlc>) -> Result<Hlc> {
    let (local_p, local_l): (i64, i64) = tx
        .query_row(
            "SELECT physical_ms,logical FROM memory_clock WHERE singleton=1",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?
        .unwrap_or((0, 0));
    if let Some(clock) = remote {
        validate_clock(clock)?;
    }
    let (remote_p, remote_l) = remote.map_or((0, 0), |c| (c.physical_ms, c.logical));
    let physical_ms = unix_ms()?.max(local_p).max(remote_p);
    let logical = match (physical_ms == local_p, physical_ms == remote_p) {
        (true, true) => Some(local_l.max(remote_l)),
        (true, false) => Some(local_l),
        (false, true) => Some(remote_l),
        _ => None,
    }
    .map_or(Some(0), |last| last.checked_add(1))
    .context("logical clock exhausted")?;
    tx.execute("INSERT INTO memory_clock VALUES(1,?1,?2) ON CONFLICT(singleton) DO UPDATE SET physical_ms=excluded.physical_ms, logical=excluded.logical", params![physical_ms, logical])?;
    Ok(Hlc {
        physical_ms,
        logical,
        node_id: node.to_owned(),
    })
}

fn persist_revision(tx: &Transaction<'_>, item: &Memory) -> Result<()> {
    tx.execute("INSERT INTO memories(memory_id,workspace_id,scope,content,content_hash,version,hlc_timestamp,updated_by,is_deleted,sync_seq,updated_at)
        VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)
        ON CONFLICT(memory_id) DO UPDATE SET scope=excluded.scope,content=excluded.content,content_hash=excluded.content_hash,
        version=excluded.version,hlc_timestamp=excluded.hlc_timestamp,updated_by=excluded.updated_by,is_deleted=excluded.is_deleted,
        sync_seq=excluded.sync_seq,updated_at=excluded.updated_at",
        params![item.memory_id,item.workspace_id,item.scope,item.content,item.content_hash,item.version,
            serde_json::to_string(&item.hlc_timestamp)?,item.updated_by,item.is_deleted,item.sync_seq,item.updated_at])?;
    tx.execute(
        "INSERT INTO memory_changes VALUES(?1,?2,?3)",
        params![item.sync_seq, item.memory_id, serde_json::to_string(item)?],
    )?;
    Ok(())
}

fn apply_proposal(
    tx: &Transaction<'_>,
    workspace: &str,
    node: &str,
    p: &Mutation,
    previous: i64,
    author: &str,
) -> Result<Memory> {
    ensure!(
        previous != 0 || !p.is_deleted,
        "cannot archive a missing memory"
    );
    let content = if p.is_deleted {
        String::new()
    } else {
        p.content.clone()
    };
    let item = Memory {
        memory_id: p.memory_id.clone(),
        workspace_id: workspace.to_owned(),
        scope: p.scope.clone(),
        content_hash: hash(&content),
        content,
        version: previous.checked_add(1).context("version exhausted")?,
        hlc_timestamp: merge_clock(tx, node, p.hlc_timestamp.as_ref())?,
        updated_by: author.to_owned(),
        is_deleted: p.is_deleted,
        sync_seq: cursor(tx)?.checked_add(1).context("cursor exhausted")?,
        updated_at: unix_ms()?,
    };
    persist_revision(tx, &item)?;
    Ok(item)
}

impl MemoryBackend for SqliteMemory<'_> {
    fn propose_mutation(&mut self, p: &Mutation) -> Result<MutationResult> {
        ensure!(
            !p.memory_id.trim().is_empty()
                && !p.proposal_id.trim().is_empty()
                && !p.source_node_id.trim().is_empty(),
            "missing proposal identity"
        );
        ensure!(
            p.scope == "global"
                || p.scope
                    .strip_prefix("project:")
                    .is_some_and(|s| !s.trim().is_empty()),
            "invalid memory scope"
        );
        ensure!(
            p.base_version >= 0
                && p.content.len() <= 65536
                && (p.is_deleted || !p.content.trim().is_empty()),
            "invalid proposal content or version"
        );
        if let Some(clock) = &p.hlc_timestamp {
            validate_clock(clock)?;
        }
        let tx = self
            .db
            .transaction_with_behavior(TransactionBehavior::Immediate)?;
        verify_workspace(&tx, self.workspace)?;
        let request = serde_json::to_string(p)?;
        let previous: Option<(String, String)> = tx
            .query_row(
                "SELECT request_json,result_json FROM memory_proposals WHERE proposal_id=?1",
                [&p.proposal_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((saved, result)) = previous {
            ensure!(
                saved == request,
                "proposal id reused with a different payload"
            );
            return Ok(serde_json::from_str(&result)?);
        }
        let current = version(&tx, &p.memory_id)?;
        ensure!(
            p.base_version <= current,
            "base version is ahead of supervisor"
        );
        let result = if p.base_version < current {
            let stamp = merge_clock(&tx, self.node, p.hlc_timestamp.as_ref())?;
            tx.execute("INSERT INTO memory_candidates(candidate_id,memory_id,workspace_id,proposed_text,base_version,hlc_timestamp,source_node_id,source_run_id,created_at)
                VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
                params![p.proposal_id,p.memory_id,self.workspace,p.content,p.base_version,serde_json::to_string(&stamp)?,p.source_node_id,p.source_run_id.as_deref().unwrap_or(""),unix_ms()?])?;
            tx.execute(
                "INSERT INTO memory_candidate_details VALUES(?1,?2)",
                params![p.proposal_id, request],
            )?;
            MutationResult::Conflict {
                candidate_id: p.proposal_id.clone(),
                current_version: current,
            }
        } else {
            let author = p
                .source_run_id
                .as_ref()
                .map_or_else(|| "user_manual".to_owned(), |r| format!("run:{r}"));
            MutationResult::Applied {
                memory: apply_proposal(&tx, self.workspace, self.node, p, current, &author)?,
            }
        };
        tx.execute(
            "INSERT INTO memory_proposals VALUES(?1,?2,?3)",
            params![p.proposal_id, request, serde_json::to_string(&result)?],
        )?;
        tx.commit()?;
        Ok(result)
    }

    fn pull_delta_since(&self, after: i64, limit: usize) -> Result<Vec<Memory>> {
        verify_workspace(self.db, self.workspace)?;
        ensure!(after >= 0 && after <= cursor(self.db)?, "invalid cursor");
        let mut stmt = self.db.prepare(
            "SELECT revision_json FROM memory_changes WHERE sync_seq>?1 ORDER BY sync_seq LIMIT ?2",
        )?;
        decode_rows(&mut stmt, params![after, limit.min(1024)])
    }

    fn search(&self, query: &str, limit: usize) -> Result<Vec<Memory>> {
        verify_workspace(self.db, self.workspace)?;
        let mut stmt = self.db.prepare("SELECT c.revision_json FROM memories m JOIN memory_changes c ON c.sync_seq=m.sync_seq
            WHERE m.is_deleted=0 AND (?1='' OR m.rowid IN (SELECT rowid FROM memory_search WHERE memory_search MATCH ?2)
               OR instr(lower(m.content),lower(?1))>0)
            ORDER BY m.sync_seq DESC LIMIT ?3")?;
        let phrase = format!("\"{}\"", query.replace('"', "\"\""));
        decode_rows(&mut stmt, params![query, phrase, limit.min(1024)])
    }

    fn prefetch_frozen_snapshot(&self, scope: &str, budget: usize) -> Result<String> {
        verify_workspace(self.db, self.workspace)?;
        let mut stmt = self.db.prepare(
            "SELECT memory_id,version,content FROM memories WHERE is_deleted=0
            AND scope IN ('global',?1) ORDER BY (scope=?1) DESC, sync_seq DESC",
        )?;
        let mut rows = stmt.query([scope])?;
        let mut snapshot = String::new();
        while let Some(row) = rows.next()? {
            let text = format!(
                "[{} v{}] {}\n",
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?
            );
            if text.len() <= budget.saturating_sub(snapshot.len()) {
                snapshot.push_str(&text);
            }
            if snapshot.len() == budget {
                break;
            }
        }
        Ok(snapshot)
    }
}

fn decode_rows(
    stmt: &mut rusqlite::Statement<'_>,
    p: impl rusqlite::Params,
) -> Result<Vec<Memory>> {
    let mut result = Vec::new();
    let mut rows = stmt.query(p)?;
    while let Some(r) = rows.next()? {
        result.push(serde_json::from_str(&r.get::<_, String>(0)?)?);
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn proposal(id: &str, base_version: i64, content: &str) -> Mutation {
        Mutation {
            proposal_id: id.into(),
            memory_id: "style".into(),
            scope: "global".into(),
            content: content.into(),
            base_version,
            source_node_id: "workstation".into(),
            source_run_id: None,
            is_deleted: false,
            hlc_timestamp: None,
        }
    }

    #[test]
    fn versions_candidates_tombstones_and_offline_replay() -> Result<()> {
        let mut source = open(Path::new(":memory:"), "work")?;
        let mut dest = open(Path::new(":memory:"), "work")?;
        let mut a = SqliteMemory::new(&mut source, "work", "a");
        let first = proposal("p1", 0, "旧版本");
        let first_result = a.propose_mutation(&first)?;
        assert_eq!(a.propose_mutation(&first)?, first_result);
        let bad_replay = proposal("p1", 0, "other");
        assert!(a.propose_mutation(&bad_replay).is_err());
        a.propose_mutation(&proposal("p2", 1, "manual correction"))?;
        let stale = proposal("p3", 1, "新的配置");
        assert_eq!(
            a.propose_mutation(&stale)?,
            MutationResult::Conflict {
                candidate_id: "p3".into(),
                current_version: 2
            }
        );
        assert!(a.resolve_candidate("p3", true, 1).is_err());
        assert_eq!(a.search("manual", 10)?.len(), 1);
        a.resolve_candidate("p3", true, 2)?;
        assert!(a.resolve_candidate("p3", true, 3).is_err());
        let mut b = SqliteMemory::new(&mut dest, "work", "b");
        let revisions = a.pull_delta_since(0, 100)?;
        assert_eq!(revisions.len(), 3);
        assert!(b
            .apply_delta(&[revisions[0].clone(), revisions[2].clone()])
            .is_err());
        assert_eq!(b.sync_cursor()?, 0);
        assert_eq!(b.apply_delta(&revisions)?, 3);
        assert_eq!(b.apply_delta(&revisions)?, 3);
        let mut tampered = revisions[1].clone();
        tampered.version += 1;
        assert!(b.apply_delta(&[tampered]).is_err());
        assert_eq!(b.search("的配", 10)?[0].version, 3);
        assert_eq!(
            a.prefetch_frozen_snapshot("global", 1000)?,
            b.prefetch_frozen_snapshot("global", 1000)?
        );
        assert!(b.prefetch_frozen_snapshot("global", 3)?.is_empty());
        let mut deleted = proposal("p4", 3, "");
        deleted.is_deleted = true;
        a.propose_mutation(&deleted)?;
        b.apply_delta(&a.pull_delta_since(3, 10)?)?;
        assert!(b.search("", 10)?.is_empty());
        assert!(a.pull_delta_since(0, 4)?[3].is_deleted);
        Ok(())
    }

    #[test]
    fn restart_keeps_identity_cursor_and_clock() -> Result<()> {
        let path = std::env::temp_dir().join(format!(
            "tarla-memory-{}-{}.db",
            std::process::id(),
            SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos()
        ));
        {
            let mut db = open(&path, "work")?;
            let mut a = SqliteMemory::new(&mut db, "work", "supervisor");
            let mut p = proposal("p1", 0, "example");
            p.hlc_timestamp = Some(Hlc {
                physical_ms: unix_ms()? + 60000,
                logical: 41,
                node_id: "b".into(),
            });
            a.propose_mutation(&p)?;
        }
        assert!(open(&path, "life").is_err());
        {
            let mut db = open(&path, "work")?;
            let mut a = SqliteMemory::new(&mut db, "work", "supervisor");
            a.propose_mutation(&proposal("p2", 1, "updated"))?;
            let history = a.pull_delta_since(0, 10)?;
            assert_eq!(history[0].hlc_timestamp.logical, 42);
            assert_eq!(history[1].hlc_timestamp.logical, 43);
            let mut bad = history[0].clone();
            bad.workspace_id = "life".into();
            assert!(a.apply_delta(&[bad]).is_err());
        }
        std::fs::remove_file(path)?;
        Ok(())
    }
}

#[cfg(test)]
mod concurrent_tests {
    use super::*;
    #[test]
    fn simultaneous_versions_have_one_winner_and_one_reviewable_conflict() -> Result<()> {
        let temp = tempfile::tempdir()?;
        let path = temp.path().join("state.db");
        let db = open(&path, "work")?;
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let workers = (0..2)
            .map(|i| {
                let path = path.clone();
                let barrier = barrier.clone();
                std::thread::spawn(move || -> Result<MutationResult> {
                    let mut db = open(&path, "work")?;
                    let mut store = SqliteMemory::new(&mut db, "work", "supervisor");
                    let p = Mutation {
                        proposal_id: format!("p{i}"),
                        memory_id: "project".into(),
                        scope: "global".into(),
                        content: format!("decision {i}"),
                        base_version: 0,
                        source_node_id: format!("node-{i}"),
                        source_run_id: None,
                        is_deleted: false,
                        hlc_timestamp: None,
                    };
                    barrier.wait();
                    store.propose_mutation(&p)
                })
            })
            .collect::<Vec<_>>();
        let mut winners = 0;
        for task in workers {
            if matches!(task.join().unwrap()?, MutationResult::Applied { .. }) {
                winners += 1;
            }
        }
        assert_eq!(winners, 1);
        assert_eq!(cursor(&db)?, 1);
        assert_eq!(
            db.query_row(
                "SELECT COUNT(*) FROM memory_candidates WHERE status='pending'",
                [],
                |r| r.get::<_, usize>(0)
            )?,
            1
        );
        Ok(())
    }
}
