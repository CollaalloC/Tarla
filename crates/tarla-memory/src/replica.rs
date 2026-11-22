//! Cursor proofs distinguish a slow replica from a different history at the same sequence.
//! During repair, the visible offline mirror stays intact until the replacement is complete.
use super::*;

const PAGE: usize = 16;
const UPLOAD_BYTES: usize = 512 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(super) struct Checkpoint {
    seq: i64,
    pub(super) hash: String,
}
fn genesis() -> Checkpoint {
    Checkpoint {
        seq: 0,
        hash: hash("tarla:s2:history:v1"),
    }
}
pub(super) fn extend(previous: &str, revision: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(previous.as_bytes());
    digest.update(revision.as_bytes());
    format!("{:x}", digest.finalize())
}
pub(super) fn migrate(tx: &Transaction<'_>) -> Result<()> {
    tx.execute_batch("ALTER TABLE memory_changes ADD COLUMN history_hash TEXT;
        CREATE INDEX idx_memory_history ON memory_changes(memory_id,sync_seq);
        CREATE TABLE memory_repair(singleton INTEGER PRIMARY KEY CHECK(singleton=1),term INTEGER NOT NULL,
            base_seq INTEGER NOT NULL,uploaded INTEGER NOT NULL);
        CREATE TABLE memory_repair_stage(seq INTEGER PRIMARY KEY,revision TEXT NOT NULL,history_hash TEXT NOT NULL);
        CREATE TABLE memory_recovered(revision_id TEXT PRIMARY KEY,source_node TEXT NOT NULL,revision_json TEXT NOT NULL);
        CREATE TRIGGER unindex_removed_memory AFTER DELETE ON memories BEGIN
            INSERT INTO memory_search(memory_search,rowid,content) VALUES('delete',old.rowid,old.content); END;")?;
    let mut query =
        tx.prepare("SELECT sync_seq,revision_json FROM memory_changes ORDER BY sync_seq")?;
    let mut rows = query.query([])?;
    let mut previous = genesis();
    while let Some(row) = rows.next()? {
        let seq: i64 = row.get(0)?;
        ensure!(seq == previous.seq + 1, "gap in stored S2 history");
        let raw: String = row.get(1)?;
        let chain = extend(&previous.hash, &raw);
        tx.execute(
            "UPDATE memory_changes SET history_hash=? WHERE sync_seq=?",
            params![chain, seq],
        )?;
        previous = Checkpoint { seq, hash: chain };
    }
    Ok(())
}
pub(super) fn checkpoint(c: &Connection, seq: i64) -> Result<Option<Checkpoint>> {
    if seq == 0 {
        return Ok(Some(genesis()));
    }
    Ok(c.query_row(
        "SELECT history_hash FROM memory_changes WHERE sync_seq=?",
        [seq],
        |r| {
            Ok(Checkpoint {
                seq,
                hash: r.get(0)?,
            })
        },
    )
    .optional()?)
}
fn head(c: &Connection) -> Result<Checkpoint> {
    checkpoint(c, cursor(c)?)?.context("missing S2 head")
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RecoveryRevision {
    revision: Memory,
    /// Semantic identity of the immediately preceding version of this memory.
    before: Option<String>,
}
fn meaning(m: &Memory) -> String {
    hash(
        &serde_json::to_string(&(m.version, &m.content_hash, &m.scope, m.is_deleted))
            .expect("infallible scalar serialization"),
    )
}
fn decoded(raw: String) -> Result<Memory> {
    Ok(serde_json::from_str(&raw)?)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicaRequest {
    at: Checkpoint,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    repair: Option<Upload>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
struct Upload {
    after: i64,
    records: Vec<RecoveryRevision>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ReplicaReply {
    Page {
        delta: Vec<Memory>,
        head_seq: i64,
        head_hash: String,
        acknowledged: Option<i64>,
    },
    Reset {
        head_seq: i64,
        head_hash: String,
    },
}

fn load_state(c: &Connection, term: i64) -> Result<Option<(i64, i64)>> {
    Ok(c.query_row(
        "SELECT base_seq,uploaded FROM memory_repair WHERE singleton=1 AND term=?",
        [term],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .optional()?)
}
fn staged_head(c: &Connection, base: i64) -> Result<Checkpoint> {
    if let Some(at) = c
        .query_row(
            "SELECT seq,history_hash FROM memory_repair_stage ORDER BY seq DESC LIMIT 1",
            [],
            |r| {
                Ok(Checkpoint {
                    seq: r.get(0)?,
                    hash: r.get(1)?,
                })
            },
        )
        .optional()?
    {
        return Ok(at);
    }
    checkpoint(c, base)?.context("missing common history")
}
fn page(c: &Connection, after: i64) -> Result<Vec<Memory>> {
    let mut stmt = c.prepare(
        "SELECT revision_json FROM memory_changes WHERE sync_seq>? ORDER BY sync_seq LIMIT ?",
    )?;
    decode_rows(&mut stmt, params![after, PAGE])
}

impl SqliteMemory<'_> {
    pub fn replica_request(&mut self, term: i64) -> Result<ReplicaRequest> {
        let tx = self.db.transaction()?;
        verify_workspace(&tx, self.workspace)?;
        let request = if let Some((base, uploaded)) = load_state(&tx, term)? {
            let mut records = Vec::new();
            let mut size = 0;
            for revision in page(&tx, uploaded)? {
                let old: Option<String>=tx.query_row("SELECT revision_json FROM memory_changes WHERE memory_id=? AND sync_seq<? ORDER BY sync_seq DESC LIMIT 1",params![revision.memory_id,revision.sync_seq],|r|r.get(0)).optional()?;
                let record = RecoveryRevision {
                    revision,
                    before: old.map(decoded).transpose()?.as_ref().map(meaning),
                };
                let bytes = serde_json::to_vec(&record)?.len();
                if !records.is_empty() && size + bytes > UPLOAD_BYTES {
                    break;
                }
                size += bytes;
                records.push(record);
            }
            ReplicaRequest {
                at: staged_head(&tx, base)?,
                repair: Some(Upload {
                    after: uploaded,
                    records,
                }),
            }
        } else {
            ReplicaRequest {
                at: head(&tx)?,
                repair: None,
            }
        };
        tx.commit()?;
        Ok(request)
    }

    /// The designated supervisor decides every recovered version. An executor can offer
    /// a previous observation but cannot install a sequence or override newer contents.
    pub fn answer_replica(&mut self, peer: &str, req: &ReplicaRequest) -> Result<ReplicaReply> {
        let behavior = if req.repair.is_some() {
            TransactionBehavior::Immediate
        } else {
            TransactionBehavior::Deferred
        };
        let tx = self.db.transaction_with_behavior(behavior)?;
        verify_workspace(&tx, self.workspace)?;
        ensure!(req.at.seq >= 0, "invalid replica cursor");
        let known = checkpoint(&tx, req.at.seq)?;
        if known.as_ref() != Some(&req.at) {
            let top = head(&tx)?;
            return Ok(ReplicaReply::Reset {
                head_seq: top.seq,
                head_hash: top.hash,
            });
        }
        let acknowledged = if let Some(upload) = &req.repair {
            ensure!(
                upload.after >= 0 && upload.records.len() <= PAGE,
                "invalid recovery upload"
            );
            let mut last = upload.after;
            for record in &upload.records {
                last = last.checked_add(1).context("S2 cursor exhausted")?;
                ensure!(
                    record.revision.sync_seq == last,
                    "nonconsecutive recovery page"
                );
                recover(&tx, self.workspace, self.node, peer, record)?;
            }
            Some(last)
        } else {
            None
        };
        let top = head(&tx)?;
        let delta = page(&tx, req.at.seq)?;
        tx.commit()?;
        Ok(ReplicaReply::Page {
            delta,
            head_seq: top.seq,
            head_hash: top.hash,
            acknowledged,
        })
    }

    /// Returns true if another page can be fetched immediately.
    pub fn consume_replica_reply(
        &mut self,
        term: i64,
        req: &ReplicaRequest,
        reply: &ReplicaReply,
    ) -> Result<bool> {
        match reply {
            ReplicaReply::Reset {
                head_seq,
                head_hash,
            } => {
                ensure!(*head_seq >= 0, "invalid authoritative head");
                let tx = self
                    .db
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                verify_workspace(&tx, self.workspace)?;
                // If the authority is an exact prefix, skip uploading that prefix.
                // For divergent branches walk from genesis and skip matching records at the authority.
                let base = checkpoint(&tx, *head_seq)?
                    .filter(|p| p.hash == *head_hash)
                    .map_or(0, |p| p.seq);
                tx.execute("DELETE FROM memory_repair_stage", [])?;
                tx.execute(
                    "INSERT OR REPLACE INTO memory_repair VALUES(1,?,?,?)",
                    params![term, base, base],
                )?;
                tx.commit()?;
                Ok(true)
            }
            ReplicaReply::Page {
                delta,
                head_seq,
                head_hash,
                acknowledged,
            } => {
                ensure!(
                    delta.len() <= PAGE && *head_seq >= req.at.seq,
                    "invalid authoritative page"
                );
                let mut at = req.at.clone();
                for item in delta {
                    validate_revision(self.workspace, item)?;
                    ensure!(
                        item.sync_seq == at.seq.checked_add(1).context("S2 cursor exhausted")?,
                        "gap in authoritative page"
                    );
                    at = Checkpoint {
                        seq: item.sync_seq,
                        hash: extend(&at.hash, &serde_json::to_string(item)?),
                    };
                }
                ensure!(
                    at.seq <= *head_seq && (at.seq == *head_seq || !delta.is_empty()),
                    "incomplete authoritative page"
                );
                if at.seq == *head_seq {
                    ensure!(at.hash == *head_hash, "authoritative history hash mismatch");
                }
                let Some(upload) = &req.repair else {
                    ensure!(
                        acknowledged.is_none(),
                        "unexpected recovery acknowledgement"
                    );
                    self.apply_delta(delta)?;
                    return Ok(at.seq < *head_seq);
                };
                let ack = upload
                    .records
                    .last()
                    .map_or(upload.after, |r| r.revision.sync_seq);
                ensure!(*acknowledged == Some(ack), "wrong recovery acknowledgement");
                let tx = self
                    .db
                    .transaction_with_behavior(TransactionBehavior::Immediate)?;
                verify_workspace(&tx, self.workspace)?;
                let (base, uploaded) = load_state(&tx, term)?.context("no repair in progress")?;
                ensure!(
                    uploaded == upload.after && staged_head(&tx, base)? == req.at,
                    "stale recovery response"
                );
                let mut chain = req.at.hash.clone();
                for item in delta {
                    let raw = serde_json::to_string(item)?;
                    chain = extend(&chain, &raw);
                    tx.execute(
                        "INSERT INTO memory_repair_stage VALUES(?,?,?)",
                        params![item.sync_seq, raw, chain],
                    )?;
                }
                tx.execute(
                    "UPDATE memory_repair SET uploaded=? WHERE singleton=1",
                    [ack],
                )?;
                let done = ack == cursor(&tx)? && at.seq == *head_seq;
                if done {
                    install(&tx, self.node, base)?;
                }
                tx.commit()?;
                Ok(!done)
            }
        }
    }
}

fn validate_revision(workspace: &str, m: &Memory) -> Result<()> {
    ensure!(
        m.workspace_id == workspace
            && m.sync_seq > 0
            && m.version > 0
            && m.content_hash == hash(&m.content),
        "invalid S2 revision or workspace"
    );
    validate_clock(&m.hlc_timestamp)
}

fn recover(
    tx: &Transaction<'_>,
    workspace: &str,
    node: &str,
    peer: &str,
    record: &RecoveryRevision,
) -> Result<()> {
    let m = &record.revision;
    validate_revision(workspace, m)?;
    let raw = serde_json::to_string(m)?;
    if tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM memory_changes WHERE sync_seq=? AND revision_json=?)",
        params![m.sync_seq, raw],
        |r| r.get::<_, bool>(0),
    )? {
        return Ok(());
    }
    let id = format!("recovery-{}", hash(&raw));
    // Older releases admitted identifiers that new mutations cannot address. Keep
    // the exact revision in memory_recovered and offer a safe key for owner review.
    // Never publish renamed data (or overwrite a colliding key) automatically.
    let legacy_key = !valid_memory_id(&m.memory_id);
    ensure!(
        !m.memory_id.trim().is_empty(),
        "missing recovery memory identity"
    );
    let p = Mutation {
        proposal_id: id.clone(),
        memory_id: if legacy_key {
            format!("legacy-memory-{}", hash(&m.memory_id))
        } else {
            m.memory_id.clone()
        },
        scope: m.scope.clone(),
        content: m.content.clone(),
        base_version: m.version - 1,
        source_node_id: peer.to_owned(),
        source_run_id: m
            .source_run_id
            .clone()
            .or_else(|| m.updated_by.strip_prefix("run:").map(String::from)),
        is_deleted: m.is_deleted,
        hlc_timestamp: Some(m.hlc_timestamp.clone()),
    };
    validate_mutation(&p)?;
    if tx.execute(
        "INSERT OR IGNORE INTO memory_recovered VALUES(?,?,?)",
        params![id, peer, raw],
    )? == 0
    {
        return Ok(());
    }
    let historical: Option<String> = tx.query_row(
        "SELECT revision_json FROM memory_changes WHERE memory_id=? AND json_extract(revision_json,'$.version')=?",
        params![m.memory_id,m.version],|r|r.get(0)).optional()?;
    if historical
        .map(decoded)
        .transpose()?
        .as_ref()
        .is_some_and(|old| meaning(old) == meaning(m))
    {
        return Ok(());
    }
    let current:Option<String>=tx.query_row("SELECT revision_json FROM memory_changes WHERE memory_id=? ORDER BY sync_seq DESC LIMIT 1",[&p.memory_id],|r|r.get(0)).optional()?;
    let current = current.map(decoded).transpose()?;
    if current.is_none() && m.is_deleted {
        return Ok(());
    }
    let ver = current.as_ref().map_or(0, |r| r.version);
    if !legacy_key && ver == p.base_version && current.as_ref().map(meaning) == record.before {
        apply_proposal(tx, workspace, node, &p, ver, &format!("recovery:{peer}"))?;
    } else {
        // A restored authority may no longer have this key. The details retain its
        // identity when SQLite's optional reference to the current materialization is NULL.
        let stamp = merge_clock(tx, node, p.hlc_timestamp.as_ref())?;
        tx.execute("INSERT INTO memory_candidates(candidate_id,memory_id,workspace_id,proposed_text,base_version,hlc_timestamp,source_node_id,source_run_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            params![id,current.as_ref().map(|x|&x.memory_id),workspace,p.content,p.base_version,serde_json::to_string(&stamp)?,peer,p.source_run_id.as_deref().unwrap_or(""),unix_ms()?])?;
        tx.execute(
            "INSERT INTO memory_candidate_details VALUES(?,?)",
            params![id, serde_json::to_string(&p)?],
        )?;
    }
    Ok(())
}

fn install(tx: &Transaction<'_>, node: &str, base: i64) -> Result<()> {
    // Remove only the divergent canonical log suffix. All old revisions were acknowledged
    // by the authority before this transaction can replace what offline readers see.
    tx.execute("DELETE FROM memory_changes WHERE sync_seq>?", [base])?;
    tx.execute("UPDATE memory_candidates SET memory_id=NULL", [])?;
    tx.execute("DELETE FROM memories", [])?;
    {
        let mut stmt=tx.prepare("SELECT revision_json FROM memory_changes WHERE sync_seq IN (SELECT MAX(sync_seq) FROM memory_changes GROUP BY memory_id)")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            materialize(tx, &decoded(row.get(0)?)?)?;
        }
    }
    {
        let mut stmt = tx.prepare("SELECT revision FROM memory_repair_stage ORDER BY seq")?;
        let mut rows = stmt.query([])?;
        while let Some(row) = rows.next()? {
            let item = decoded(row.get(0)?)?;
            ensure!(
                version(tx, &item.memory_id)?.checked_add(1) == Some(item.version),
                "nonconsecutive replacement version"
            );
            persist_revision(tx, &item)?;
            merge_clock(tx, node, Some(&item.hlc_timestamp))?;
        }
    }
    tx.execute("UPDATE memory_candidates SET memory_id=(SELECT m.memory_id FROM memory_candidate_details d JOIN memories m ON m.memory_id=json_extract(d.request_json,'$.memory_id') WHERE d.candidate_id=memory_candidates.candidate_id)",[])?;
    tx.execute("DELETE FROM memory_repair", [])?;
    tx.execute("DELETE FROM memory_repair_stage", [])?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn add(s: &mut SqliteMemory<'_>, id: usize, content: &str) -> Result<()> {
        s.propose_mutation(&Mutation {
            proposal_id: format!("p-{id}"),
            memory_id: format!("m-{id}"),
            scope: "global".into(),
            content: content.into(),
            base_version: 0,
            source_node_id: "chief".into(),
            source_run_id: None,
            is_deleted: false,
            hlc_timestamp: None,
        })?;
        Ok(())
    }
    #[test]
    fn legacy_key_is_archived_and_reviewable_without_blocking_recovery() -> Result<()> {
        let mut db = open(Path::new(":memory:"), "w")?;
        let mut store = SqliteMemory::new(&mut db, "w", "chief");
        add(&mut store, 1, "preserved")?;
        let mut old = store.pull_delta_since(0, 1)?.remove(0);
        old.memory_id = "legacy] name".into();
        let record = RecoveryRevision {
            revision: old,
            before: None,
        };
        let tx = db.transaction()?;
        recover(&tx, "w", "chief", "peer", &record)?;
        recover(&tx, "w", "chief", "peer", &record)?;
        let original: String =
            tx.query_row("SELECT revision_json FROM memory_recovered", [], |r| {
                r.get(0)
            })?;
        assert_eq!(decoded(original)?.memory_id, "legacy] name");
        let request: String = tx.query_row(
            "SELECT request_json FROM memory_candidate_details",
            [],
            |r| r.get(0),
        )?;
        let pending: Mutation = serde_json::from_str(&request)?;
        validate_mutation(&pending)?;
        assert_eq!(version(&tx, &pending.memory_id)?, 0);
        assert_eq!(pending.content, "preserved");
        tx.commit()?;
        Ok(())
    }

    #[test]
    fn interrupted_repair_is_idempotent_and_keeps_the_entire_old_mirror_visible() -> Result<()> {
        let tmp = tempfile::tempdir()?;
        let path = tmp.path().join("replica.db");
        let mut executor = open(&path, "w")?;
        let mut original = open(Path::new(":memory:"), "w")?;
        let mut restored = open(Path::new(":memory:"), "w")?;
        let mut source = SqliteMemory::new(&mut original, "w", "original");
        let mut a = SqliteMemory::new(&mut restored, "w", "restored");
        add(&mut source, 0, "shared baseline")?;
        a.apply_delta(&source.pull_delta_since(0, 1)?)?;
        for i in 1..42 {
            add(&mut source, i, "disconnected history")?;
        }
        {
            let mut b = SqliteMemory::new(&mut executor, "w", "executor");
            b.apply_delta(&source.pull_delta_since(0, 60)?)?;
            let request = b.replica_request(2)?;
            let reset = a.answer_replica("executor", &request)?;
            assert!(b.consume_replica_reply(2, &request, &reset)?);
            let request = b.replica_request(2)?;
            let first = a.answer_replica("executor", &request)?;
            // Lost response: the authority sees the same recovery page twice.
            let replay = a.answer_replica("executor", &request)?;
            assert_eq!(
                serde_json::to_value(&first)?,
                serde_json::to_value(&replay)?
            );
            assert!(b.consume_replica_reply(2, &request, &replay)?);
            assert_eq!(b.search("disconnected", 100)?.len(), 41);
        }
        drop(executor);
        let mut executor = open(&path, "w")?;
        let mut b = SqliteMemory::new(&mut executor, "w", "executor");
        assert_eq!(b.search("disconnected", 100)?.len(), 41);
        let mut rounds = 0;
        loop {
            let req = b.replica_request(2)?;
            let reply = a.answer_replica("executor", &req)?;
            rounds += 1;
            if !b.consume_replica_reply(2, &req, &reply)? {
                break;
            }
            assert!(rounds < 6);
        }
        assert_eq!(rounds, 2);
        assert_eq!(a.pull_delta_since(0, 100)?, b.pull_delta_since(0, 100)?);
        assert_eq!(b.search("disconnected", 100)?.len(), 41);
        Ok(())
    }

    #[test]
    fn outgoing_hlc_advances_past_observed_clocks_and_rolls_back_with_the_outbox() -> Result<()> {
        let mut c = open(Path::new(":memory:"), "w")?;
        let observed = Hlc {
            physical_ms: unix_ms()? + 60_000,
            logical: 123,
            node_id: "authority".into(),
        };
        let tx = c.transaction()?;
        merge_clock(&tx, "worker", Some(&observed))?;
        let first = next_local_hlc(&tx, "worker")?;
        assert_eq!(first.logical, 125);
        tx.commit()?;
        let tx = c.transaction()?;
        let lost = next_local_hlc(&tx, "worker")?;
        tx.rollback()?;
        let tx = c.transaction()?;
        assert_eq!(lost, next_local_hlc(&tx, "worker")?);
        tx.commit()?;
        Ok(())
    }
}
