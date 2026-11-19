//! Immutable conversation archives and bounded, recoverable model windows.
mod firewall;
pub use firewall::{bound_mission, bound_receipt};

use anyhow::{ensure, Context, Result};
use rusqlite::{params, Connection, OptionalExtension, Transaction, TransactionBehavior};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ContextPolicy {
    pub supervisor_active_budget_tokens: usize,
    pub compaction_high_watermark_pct: usize,
    pub protected_verbatim_turns: usize,
}

impl Default for ContextPolicy {
    fn default() -> Self {
        Self {
            supervisor_active_budget_tokens: 24000,
            compaction_high_watermark_pct: 75,
            protected_verbatim_turns: 8,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ContextMessage {
    pub role: String,
    pub content: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArchivedMessage {
    pub seq_no: i64,
    pub role: String,
    pub content: String,
}

/// UTF-8 bytes bound byte-level tokenizer input counts even for unsegmented CJK.
/// This budget also reserves per-message chat framing. Providers can reserve additional
/// tool/schema tokens in their own request budget before calling assemble().
pub fn estimate(role: &str, content: &str) -> usize {
    16 + role.len() + content.len()
}

fn now() -> Result<i64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)?
        .as_secs()
        .try_into()?)
}

/// Returns true only on the first insert. Reusing a source identifier for different
/// content or a different conversation is an integrity error.
pub fn append(
    db: &mut Connection,
    workspace: &str,
    conversation: &str,
    channel: &str,
    source_id: &str,
    role: &str,
    text: &str,
) -> Result<bool> {
    ensure!(
        !workspace.is_empty()
            && !conversation.is_empty()
            && !channel.is_empty()
            && !source_id.is_empty(),
        "missing message identity"
    );
    ensure!(
        matches!(role, "user" | "assistant" | "run_receipt"),
        "invalid message role"
    );
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let actual: String = tx.query_row(
        "SELECT workspace_id FROM workspace_identity WHERE singleton=1",
        [],
        |r| r.get(0),
    )?;
    ensure!(actual == workspace, "wrong workspace");
    let seen: Option<(String,String,String)> = tx.query_row(
        "SELECT conversation_id,sender_role,content FROM messages WHERE channel_type=?1 AND source_msg_id=?2",
        params![channel,source_id], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?))).optional()?;
    if let Some((old_conversation, old_role, old_content)) = seen {
        ensure!(
            old_conversation == conversation && old_role == role && old_content == text,
            "source id reused"
        );
        return Ok(false);
    }
    let time = now()?;
    tx.execute("INSERT INTO conversations(conversation_id,workspace_id,title,created_at,updated_at) VALUES(?1,?2,?3,?4,?4)
        ON CONFLICT(conversation_id) DO UPDATE SET updated_at=excluded.updated_at", params![conversation,workspace,clip(text,80),time])?;
    tx.execute("INSERT INTO messages(conversation_id,channel_type,source_msg_id,sender_role,content,token_estimate,created_at)
        VALUES(?1,?2,?3,?4,?5,?6,?7)", params![conversation,channel,source_id,role,text,estimate(model_role(role),text),time])?;
    tx.commit()?;
    Ok(true)
}

fn model_role(role: &str) -> &str {
    if role == "run_receipt" {
        "user"
    } else {
        role
    }
}

pub fn assemble(
    db: &mut Connection,
    conversation: &str,
    soul: &str,
    frozen_memory: &str,
    policy: &ContextPolicy,
) -> Result<Vec<ContextMessage>> {
    ensure!(
        policy.supervisor_active_budget_tokens > 0
            && (1..=100).contains(&policy.compaction_high_watermark_pct)
            && policy.protected_verbatim_turns > 0,
        "invalid context policy"
    );
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    tx.execute("INSERT INTO conversation_prefix(conversation_id,frozen_memory) VALUES(?1,?2) ON CONFLICT DO NOTHING", params![conversation,frozen_memory])?;
    let snapshot: String = tx.query_row(
        "SELECT frozen_memory FROM conversation_prefix WHERE conversation_id=?1",
        [conversation],
        |r| r.get(0),
    )?;
    let prefix = ContextMessage {
        role: "system".into(),
        content: if snapshot.is_empty() {
            soul.to_owned()
        } else {
            format!("{soul}\n\nWorkspace memories:\n{snapshot}")
        },
    };
    let prefix_cost = estimate(&prefix.role, &prefix.content);
    let budget = policy.supervisor_active_budget_tokens;
    ensure!(
        prefix_cost <= budget,
        "static prefix exceeds model context budget"
    );
    let raw_cost: usize = tx.query_row("SELECT COALESCE(SUM(token_estimate),0) FROM messages WHERE conversation_id=?1 AND compacted_into IS NULL", [conversation], |r| r.get(0))?;
    let summary_cost: usize = tx.query_row("SELECT COALESCE(SUM(token_estimate),0) FROM context_dag_nodes WHERE conversation_id=?1 AND parent_dag_id IS NULL", [conversation], |r| r.get(0))?;
    let total = prefix_cost
        .saturating_add(raw_cost)
        .saturating_add(summary_cost);
    let high = budget.saturating_mul(policy.compaction_high_watermark_pct) / 100;
    if total > high {
        compact(&tx, conversation, policy.protected_verbatim_turns)?;
    }

    let raw = raw_messages(&tx, conversation)?;
    let raw_cost: usize = raw.iter().map(|m| estimate(&m.role, &m.content)).sum();
    ensure!(
        prefix_cost + raw_cost <= budget,
        "protected verbatim turns and static prefix exceed model context budget"
    );
    let summary_budget = budget - prefix_cost - raw_cost;
    let mut roots = read_roots(&tx, conversation)?;
    // Merge only when a batch is large enough or the active roots no longer fit.
    // Leaves retain their original ranges through every merge.
    while roots.len() >= 2 {
        let same_depth = roots
            .windows(4)
            .position(|w| w.iter().all(|r| r.depth == w[0].depth));
        let root_cost: usize = roots.iter().map(|r| r.cost()).sum();
        let (start, end) = if let Some(i) = same_depth {
            (i, i + 4)
        } else if root_cost > summary_budget {
            (0, roots.len().min(4))
        } else {
            break;
        };
        let parent = merge_nodes(&tx, conversation, &roots[start..end])?;
        roots.splice(start..end, [parent]);
    }
    let mut window = vec![prefix];
    let mut left = summary_budget;
    for root in roots {
        let header = format!("[history {} {}..{}] ", root.id, root.start, root.end);
        if left < estimate("user", &header) {
            continue;
        }
        let body = clip(&root.summary, left - estimate("user", &header));
        let content = format!("{header}{body}");
        left -= estimate("user", &content);
        window.push(ContextMessage {
            role: "user".into(),
            content,
        });
    }
    window.extend(raw);
    let active: usize = window.iter().map(|m| estimate(&m.role, &m.content)).sum();
    tx.execute(
        "UPDATE conversations SET active_tokens=?1 WHERE conversation_id=?2",
        params![active, conversation],
    )?;
    tx.commit()?;
    Ok(window)
}

fn raw_messages(tx: &Transaction<'_>, conversation: &str) -> Result<Vec<ContextMessage>> {
    let mut stmt = tx.prepare("SELECT sender_role,content FROM messages WHERE conversation_id=?1 AND compacted_into IS NULL ORDER BY seq_no")?;
    let data = stmt
        .query_map([conversation], |r| {
            Ok(ContextMessage {
                role: model_role(&r.get::<_, String>(0)?).into(),
                content: r.get(1)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(data)
}

fn compact(tx: &Transaction<'_>, conversation: &str, turns: usize) -> Result<()> {
    // A turn begins with the user's message and includes all following replies
    // and completion events, even when several events arrive before the next user turn.
    let oldest: Option<i64> = tx
        .query_row(
            "SELECT seq_no FROM messages WHERE conversation_id=?1
        AND sender_role='user' ORDER BY seq_no DESC LIMIT 1 OFFSET ?2",
            params![conversation, turns - 1],
            |r| r.get(0),
        )
        .optional()?;
    let Some(cut) = oldest else { return Ok(()) };
    loop {
        let mut stmt = tx.prepare(
            "SELECT seq_no,sender_role,content FROM messages WHERE conversation_id=?1
            AND compacted_into IS NULL AND seq_no<?2 ORDER BY seq_no LIMIT 32",
        )?;
        let block = stmt
            .query_map(params![conversation, cut], |r| {
                Ok(ArchivedMessage {
                    seq_no: r.get(0)?,
                    role: r.get(1)?,
                    content: r.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if block.is_empty() {
            break;
        }
        let start = block[0].seq_no;
        let end = block.last().context("empty block")?.seq_no;
        let parts = block.iter().map(|m| m.content.as_str()).collect::<Vec<_>>();
        let (summary, entities) = distill(&parts);
        let node = new_node(tx, conversation, start, end, 1, summary, entities)?;
        tx.execute(
            "UPDATE messages SET compacted_into=?1 WHERE conversation_id=?2
            AND compacted_into IS NULL AND seq_no BETWEEN ?3 AND ?4",
            params![node.id, conversation, start, end],
        )?;
    }
    Ok(())
}

struct Node {
    id: String,
    start: i64,
    end: i64,
    depth: i64,
    summary: String,
    entities: String,
}
impl Node {
    fn cost(&self) -> usize {
        estimate(
            "user",
            &format!(
                "[history {} {}..{}] {}",
                self.id, self.start, self.end, self.summary
            ),
        )
    }
}

fn new_node(
    tx: &Transaction<'_>,
    conversation: &str,
    start: i64,
    end: i64,
    depth: i64,
    summary: String,
    entities: String,
) -> Result<Node> {
    // seq_no is database-global, and parent depth exceeds every child's depth.
    let node = Node {
        id: format!("dag-{depth}-{start}-{end}"),
        start,
        end,
        depth,
        summary,
        entities,
    };
    tx.execute("INSERT INTO context_dag_nodes(dag_node_id,conversation_id,depth_level,start_seq_no,end_seq_no,summary_text,key_entities,token_estimate,created_at)
        VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",params![node.id,conversation,node.depth,node.start,node.end,node.summary,node.entities,node.cost(),now()?])?;
    Ok(node)
}

fn merge_nodes(tx: &Transaction<'_>, conversation: &str, children: &[Node]) -> Result<Node> {
    let parts = children
        .iter()
        .map(|n| n.summary.as_str())
        .collect::<Vec<_>>();
    let (summary, _) = distill(&parts);
    let mut entities = BTreeSet::new();
    for n in children {
        entities.extend(serde_json::from_str::<Vec<String>>(&n.entities)?);
    }
    let entities = serde_json::to_string(&entities.iter().take(24).collect::<Vec<_>>())?;
    let node = new_node(
        tx,
        conversation,
        children[0].start,
        children.last().context("empty merge")?.end,
        children
            .iter()
            .map(|r| r.depth)
            .max()
            .context("empty merge")?
            + 1,
        summary,
        entities,
    )?;
    for child in children {
        tx.execute(
            "UPDATE context_dag_nodes SET parent_dag_id=?1 WHERE dag_node_id=?2",
            params![node.id, child.id],
        )?;
    }
    Ok(node)
}

fn read_roots(tx: &Transaction<'_>, conversation: &str) -> Result<Vec<Node>> {
    let mut stmt = tx.prepare("SELECT dag_node_id,start_seq_no,end_seq_no,depth_level,summary_text,key_entities
        FROM context_dag_nodes WHERE conversation_id=?1 AND parent_dag_id IS NULL ORDER BY start_seq_no")?;
    let data = stmt
        .query_map([conversation], |r| {
            Ok(Node {
                id: r.get(0)?,
                start: r.get(1)?,
                end: r.get(2)?,
                depth: r.get(3)?,
                summary: r.get(4)?,
                entities: r.get(5)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(data)
}

fn distill(parts: &[&str]) -> (String, String) {
    let mut entities = BTreeSet::new();
    for piece in parts {
        for word in piece.split_whitespace() {
            let key = word.trim_matches(|c| matches!(c, '\'' | '"' | '`' | ',' | ';' | '(' | ')'));
            if entities.len() < 24
                && key.len() <= 160
                && (key.contains('/') || key.contains('.') || key.starts_with("--"))
            {
                entities.insert(key);
            }
        }
    }
    let first = parts.first().copied().unwrap_or("").trim();
    let last = parts.last().copied().unwrap_or("").trim();
    let mut summary = clip(first, 100).to_owned();
    if first != last {
        summary.push_str(" … ");
        summary.push_str(clip(last, 120));
    }
    for key in entities.iter().filter(|k| !k.is_empty()) {
        if !summary.contains(*key) && summary.len() + key.len() + 1 <= 360 {
            summary.push(' ');
            summary.push_str(key);
        }
    }
    (
        summary,
        serde_json::to_string(&entities).expect("string collection is serializable"),
    )
}

fn clip(text: &str, max_bytes: usize) -> &str {
    let mut end = max_bytes.min(text.len());
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

pub fn recall(
    db: &Connection,
    conversation: &str,
    dag: Option<&str>,
    query: &str,
    after_seq: i64,
    limit: usize,
) -> Result<Vec<ArchivedMessage>> {
    let (start, end): (i64, i64) = if let Some(id) = dag {
        db.query_row("SELECT start_seq_no,end_seq_no FROM context_dag_nodes WHERE dag_node_id=?1 AND conversation_id=?2",
           params![id,conversation], |r| Ok((r.get(0)?,r.get(1)?))).context("unknown history node in this conversation")?
    } else {
        (0, i64::MAX)
    };
    let mut stmt = db.prepare("SELECT seq_no,sender_role,content FROM messages WHERE conversation_id=?1
        AND seq_no>?2 AND seq_no BETWEEN ?3 AND ?4 AND (?5='' OR
        seq_no IN (SELECT rowid FROM message_search WHERE message_search MATCH ?6) OR instr(lower(content),lower(?5))>0)
        ORDER BY seq_no LIMIT ?7")?;
    let phrase = format!("\"{}\"", query.replace('"', "\"\""));
    let data = stmt
        .query_map(
            params![
                conversation,
                after_seq,
                start,
                end,
                query,
                phrase,
                limit.min(256)
            ],
            |r| {
                Ok(ArchivedMessage {
                    seq_no: r.get(0)?,
                    role: r.get(1)?,
                    content: r.get(2)?,
                })
            },
        )?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    #[test]
    fn hundreds_of_turns_stay_bounded_and_originals_remain_searchable() -> Result<()> {
        let mut db = tarla_memory::open(Path::new(":memory:"), "work")?;
        let policy = ContextPolicy {
            supervisor_active_budget_tokens: 1200,
            compaction_high_watermark_pct: 75,
            protected_verbatim_turns: 3,
        };
        append(
            &mut db,
            "work",
            "other",
            "cli",
            "o",
            "user",
            "This conversation references file src/000.rs too.",
        )?;
        for turn in 0..150 {
            append(
                &mut db,
                "work",
                "main",
                "cli",
                &format!("u{turn}"),
                "user",
                &format!("请阅读 src/{turn:03}.rs and record decision {turn}"),
            )?;
            append(
                &mut db,
                "work",
                "main",
                "daemon",
                &format!("a{turn}"),
                "assistant",
                &format!("Verified the exact path for turn {turn}."),
            )?;
            let window = assemble(
                &mut db,
                "main",
                "My profile",
                &format!("snapshot {turn}"),
                &policy,
            )?;
            assert!(
                window
                    .iter()
                    .map(|m| estimate(&m.role, &m.content))
                    .sum::<usize>()
                    <= 1200
            );
            assert!(window[0].content.ends_with("snapshot 0"));
            let back = window.iter().rev().take(6).collect::<Vec<_>>();
            assert_eq!(
                back[0].content,
                format!("Verified the exact path for turn {turn}.")
            );
            if turn >= 2 {
                assert_eq!(
                    back[5].content,
                    format!(
                        "请阅读 src/{:03}.rs and record decision {}",
                        turn - 2,
                        turn - 2
                    )
                );
            }
        }
        assert_eq!(recall(&db, "main", None, "src/000.rs", 0, 10)?.len(), 1);
        let (id,level):(String,i64)=db.query_row("SELECT dag_node_id,depth_level FROM context_dag_nodes WHERE conversation_id='main' ORDER BY depth_level DESC LIMIT 1",[],|r|Ok((r.get(0)?,r.get(1)?)))?;
        assert!(level >= 2);
        assert!(!recall(&db, "main", Some(&id), "", 0, 1)?.is_empty());
        assert!(recall(&db, "other", Some(&id), "", 0, 1).is_err());
        let count: i64 = db.query_row(
            "SELECT count(*) FROM messages WHERE conversation_id='main'",
            [],
            |r| r.get(0),
        )?;
        assert_eq!(count, 300);
        Ok(())
    }

    #[test]
    fn duplicate_delivery_and_oversized_protected_turn_are_explicit() -> Result<()> {
        let mut db = tarla_memory::open(Path::new(":memory:"), "work")?;
        let text = "太长".repeat(256);
        assert!(append(&mut db, "work", "c", "cli", "m", "user", &text)?);
        assert!(!append(&mut db, "work", "c", "cli", "m", "user", &text)?);
        assert!(append(&mut db, "work", "different", "cli", "m", "user", &text).is_err());
        let policy = ContextPolicy {
            supervisor_active_budget_tokens: 256,
            ..Default::default()
        };
        assert!(assemble(&mut db, "c", "profile", "", &policy).is_err());
        assert_eq!(recall(&db, "c", None, "", 0, 1)?[0].content, text);
        Ok(())
    }
}
