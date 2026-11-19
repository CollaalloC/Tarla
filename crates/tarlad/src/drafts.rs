//! A draft exists only while its own input is in flight. Durable messages
//! remain the authority for completed replies, including after process restart.
use super::*;
use axum::extract::{Path as Route, Query, State};
use rusqlite::{params, OptionalExtension};
use serde::Deserialize;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Mutex,
};

#[derive(Default)]
pub struct Drafts {
    active: Mutex<BTreeMap<String, Entry>>,
    revision: AtomicU64,
}
struct Entry {
    channel: String,
    source: String,
    text: String,
    revision: u64,
}
pub struct Live<'a> {
    store: &'a Drafts,
    conv: &'a str,
}
impl Drafts {
    pub fn begin<'a>(&'a self, conv: &'a str, channel: &str, source: &str) -> Live<'a> {
        let mut map = self.active.lock().unwrap_or_else(|e| e.into_inner());
        map.insert(
            conv.to_owned(),
            Entry {
                channel: channel.into(),
                source: source.into(),
                text: String::new(),
                revision: self.rev(),
            },
        );
        Live { store: self, conv }
    }
    fn rev(&self) -> u64 {
        self.revision.fetch_add(1, Ordering::Relaxed) + 1
    }
    fn snapshot(&self, conv: &str, channel: &str, source: &str) -> Option<Value> {
        let map = self.active.lock().unwrap_or_else(|e| e.into_inner());
        let entry = map
            .get(conv)
            .filter(|e| e.channel == channel && e.source == source)?;
        Some(json!({"status":"streaming","text":entry.text,"revision":entry.revision}))
    }
}
impl Live<'_> {
    pub fn write(&self, text: &str) {
        let mut map = self.store.active.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = map.get_mut(self.conv) {
            let mut end = text.len().min(8192);
            while !text.is_char_boundary(end) {
                end -= 1
            }
            let value = &text[..end];
            if entry.text != value {
                entry.text.clear();
                entry.text.push_str(value);
                entry.revision = self.store.rev();
            }
        }
    }
}
impl Drop for Live<'_> {
    fn drop(&mut self) {
        self.store
            .active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(self.conv);
    }
}
/// Preserve correlation with replies recorded before live previews were introduced.
pub fn migrate(db: &mut Connection) -> Result<()> {
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    if !tx.query_row(
        "SELECT exists(SELECT 1 FROM service_reply_origin_migration)",
        [],
        |r| r.get::<_, bool>(0),
    )? {
        tx.execute("INSERT OR IGNORE INTO service_reply_origins SELECT 'in:'||r.channel||':'||r.source,r.conversation_id,r.channel,r.source FROM service_responses r ORDER BY EXISTS(SELECT 1 FROM messages m WHERE m.source_msg_id='in:'||r.channel||':'||r.source AND m.channel_type='supervisor' AND m.conversation_id=r.conversation_id) DESC",[])?;
        tx.execute("INSERT INTO service_reply_origin_migration VALUES(1)", [])?;
    }
    tx.commit()?;
    Ok(())
}
/// The common source key retains the existing wire convention for gateway platforms.
/// Escaping channel separators prevents ambiguous custom channel/source combinations.
pub fn reply_key(db: &Connection, conv: &str, channel: &str, source: &str) -> Result<String> {
    let saved=db.query_row("SELECT reply_key FROM service_reply_origins WHERE conversation_id=? AND channel=? AND source=?",params![conv,channel,source],|r|r.get::<_,String>(0)).optional()?;
    if let Some(key) = saved {
        return Ok(key);
    }
    let mut key = format!("in:{}:{source}", channel.replace(':', "%3A"));
    if db.query_row(
        "SELECT exists(SELECT 1 FROM service_reply_origins WHERE reply_key=?)",
        [&key],
        |r| r.get::<_, bool>(0),
    )? {
        key = format!(
            "in2:{:x}",
            Sha256::digest(serde_json::to_vec(&(channel, source))?)
        );
    }
    db.execute(
        "INSERT INTO service_reply_origins VALUES(?,?,?,?)",
        params![key, conv, channel, source],
    )?;
    Ok(key)
}
#[derive(Deserialize)]
pub struct Origin {
    channel: String,
    source_msg_id: String,
}
pub async fn get(
    State(s): State<S>,
    Route(conv): Route<String>,
    Query(origin): Query<Origin>,
) -> Api {
    validate_id(&conv)?;
    validate_id(&origin.channel)?;
    if origin.source_msg_id.is_empty() || origin.source_msg_id.len() > 512 {
        return Err(anyhow!("invalid source message identifier").into());
    }
    let c = s.db()?;
    let completed=c.query_row("SELECT m.seq_no,m.content FROM service_reply_origins o JOIN messages m ON m.channel_type='supervisor' AND m.conversation_id=o.conversation_id AND m.source_msg_id=o.reply_key WHERE o.conversation_id=? AND o.channel=? AND o.source=?",params![conv,origin.channel,origin.source_msg_id],|r|Ok((r.get::<_,i64>(0)?,r.get::<_,String>(1)?))).optional()?;
    if let Some((seq_no, text)) = completed {
        return Ok(Json(
            json!({"status":"completed","text":text,"revision":seq_no,"seq_no":seq_no}),
        ));
    }
    Ok(Json(
        s.drafts
            .snapshot(&conv, &origin.channel, &origin.source_msg_id)
            .unwrap_or_else(|| json!({"status":"idle","text":"","revision":0})),
    ))
}
