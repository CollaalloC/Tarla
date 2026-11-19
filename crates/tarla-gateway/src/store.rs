use super::platform::{decision, Event, Input, Route};
use anyhow::{Context, Result};
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};
use uuid::Uuid;
pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
fn random() -> String {
    Uuid::new_v4().simple().to_string()
}
fn hash(s: &str) -> String {
    format!("{:x}", Sha256::digest(s.as_bytes()))
}

pub fn open(path: &Path) -> Result<Connection> {
    let db = Connection::open(path)?;
    db.busy_timeout(std::time::Duration::from_secs(5))?;
    db.execute_batch("
    PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS codes(hash TEXT PRIMARY KEY, platform TEXT NOT NULL, actor TEXT NOT NULL, conv TEXT NOT NULL, approvals INTEGER NOT NULL, cursor INTEGER NOT NULL, expires INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS routes(id TEXT PRIMARY KEY, platform TEXT NOT NULL, chat TEXT NOT NULL, thread TEXT NOT NULL, actor TEXT NOT NULL, conv TEXT NOT NULL, approvals INTEGER NOT NULL, cursor INTEGER NOT NULL, dest TEXT NOT NULL, UNIQUE(platform,chat,thread));
    CREATE TABLE IF NOT EXISTS cards(token TEXT PRIMARY KEY, route TEXT NOT NULL REFERENCES routes(id) ON DELETE CASCADE, action TEXT NOT NULL, UNIQUE(route,action));
    CREATE TABLE IF NOT EXISTS inbox(n INTEGER PRIMARY KEY, platform TEXT NOT NULL, event TEXT NOT NULL, route TEXT NOT NULL REFERENCES routes(id) ON DELETE CASCADE, job TEXT NOT NULL, tries INTEGER NOT NULL DEFAULT 0, retry INTEGER NOT NULL DEFAULT 0, done INTEGER NOT NULL DEFAULT 0, UNIQUE(platform,event));
    CREATE TABLE IF NOT EXISTS outgoing(n INTEGER PRIMARY KEY AUTOINCREMENT, route TEXT NOT NULL REFERENCES routes(id) ON DELETE CASCADE, job TEXT NOT NULL, tries INTEGER NOT NULL DEFAULT 0, retry INTEGER NOT NULL DEFAULT 0);
    ")?;
    db.execute_batch(crate::drafts::SCHEMA)?;
    Ok(db)
}
pub fn issue(
    db: &Connection,
    platform: &str,
    actor: &str,
    conv: &str,
    approvals: bool,
    cursor: i64,
) -> Result<String> {
    let code = random();
    db.execute("DELETE FROM codes WHERE expires<=?1", [now()])?;
    db.execute(
        "INSERT INTO codes VALUES (?,?,?,?,?,?,?)",
        params![
            hash(&code),
            platform,
            actor,
            conv,
            approvals,
            cursor,
            now() + 300
        ],
    )?;
    Ok(code)
}
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum Job {
    Message {
        source: String,
        text: String,
    },
    Action {
        id: String,
        resolution: String,
        callback: Option<String>,
    },
}
#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub enum Delivery {
    Text(String),
    Draft { source: String },
    Final { source: String, text: String },
    Approval { title: String, token: String },
    Callback(String),
}
pub fn ingest(db: &mut Connection, e: Event) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let r = &e.route;
    if let Input::Message(text) = &e.input {
        if let Some(code) = text
            .trim()
            .strip_prefix("/pair ")
            .or_else(|| text.trim().strip_prefix("tarla pair "))
        {
            let row=tx.query_row("SELECT conv, approvals, cursor FROM codes WHERE hash=? AND platform=? AND actor=? AND expires>?",params![hash(code.trim()),r.platform,r.actor,now()],|q|Ok((q.get::<_,String>(0)?,q.get::<_,bool>(1)?,q.get::<_,i64>(2)?))).optional()?;
            if let Some((conv, approvals, cursor)) = row {
                let rid = random();
                let inserted = tx.execute(
                    "INSERT OR IGNORE INTO routes VALUES (?,?,?,?,?,?,?,?,?)",
                    params![
                        rid,
                        r.platform,
                        r.chat,
                        r.thread,
                        r.actor,
                        conv,
                        approvals,
                        cursor,
                        serde_json::to_string(r)?
                    ],
                )?;
                if inserted == 1 {
                    tx.execute("DELETE FROM codes WHERE hash=?", [hash(code.trim())])?;
                    tx.execute(
                        "INSERT INTO outgoing(route,job) VALUES (?,?)",
                        params![
                            rid,
                            serde_json::to_string(&Delivery::Text("Connected to Tarla.".into()))?
                        ],
                    )?;
                }
            }
            tx.commit()?;
            return Ok(());
        }
    }
    let (rid, job) = match e.input {
        Input::Message(text) => {
            let rid: Option<String> = tx
                .query_row(
                    "SELECT id FROM routes WHERE platform=? AND chat=? AND thread=? AND actor=?",
                    params![r.platform, r.chat, r.thread, r.actor],
                    |q| q.get(0),
                )
                .optional()?;
            let Some(rid) = rid else { return Ok(()) };
            (
                rid,
                Job::Message {
                    source: e.event_id.clone(),
                    text,
                },
            )
        }
        Input::Button { value, callback_id } => {
            let Ok((resolution, token)) = decision(&value) else {
                return Ok(());
            };
            let row:Option<(String,String)>=tx.query_row("SELECT r.id,c.action FROM cards c JOIN routes r ON c.route=r.id WHERE c.token=? AND r.platform=? AND r.chat=? AND r.actor=? AND r.approvals=1 AND (r.platform='feishu' OR r.thread=?)",params![token,r.platform,r.chat,r.actor,r.thread],|q|Ok((q.get(0)?,q.get(1)?))).optional()?;
            let Some((rid, action)) = row else {
                return Ok(());
            };
            (
                rid,
                Job::Action {
                    id: action,
                    resolution: resolution.into(),
                    callback: callback_id,
                },
            )
        }
    };
    let added = tx.execute(
        "INSERT OR IGNORE INTO inbox(platform,event,route,job) VALUES (?,?,?,?)",
        params![r.platform, e.event_id, rid, serde_json::to_string(&job)?],
    )?;
    if added == 1 && matches!(job, Job::Message { .. }) {
        crate::drafts::track(&tx, &rid, &crate::drafts::source(&r.platform, &e.event_id))?;
    }
    tx.commit()?;
    Ok(())
}
#[derive(Serialize)]
pub struct Binding {
    pub id: String,
    pub conversation_id: String,
    pub approvals: bool,
    pub cursor: i64,
    pub route: Route,
}
pub fn bindings(db: &Connection) -> Result<Vec<Binding>> {
    let mut q =
        db.prepare("SELECT id, conv, approvals, cursor, dest FROM routes ORDER BY rowid")?;
    let rows = q.query_map([], |r| {
        Ok((
            r.get(0)?,
            r.get(1)?,
            r.get(2)?,
            r.get(3)?,
            r.get::<_, String>(4)?,
        ))
    })?;
    rows.map(|row| {
        let (id, conversation_id, approvals, cursor, dest) = row?;
        Ok(Binding {
            id,
            conversation_id,
            approvals,
            cursor,
            route: serde_json::from_str(&dest)?,
        })
    })
    .collect()
}
pub fn queue_messages(
    db: &mut Connection,
    b: &Binding,
    messages: &[serde_json::Value],
) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let Some(mut cursor): Option<i64> = tx
        .query_row("SELECT cursor FROM routes WHERE id=?", [&b.id], |q| {
            q.get(0)
        })
        .optional()?
    else {
        return Ok(());
    };
    for m in messages {
        let seq = m["seq_no"]
            .as_i64()
            .context("invalid conversation sequence")?;
        if seq <= cursor {
            continue;
        }
        if m["sender_role"] == "assistant" {
            let text = m["content"].as_str().context("invalid assistant message")?;
            let source = m["source_msg_id"].as_str().unwrap_or("");
            let final_preview = crate::drafts::close(&tx, &b.id, source)?;
            let mut fragments = chunks(
                text,
                if b.route.platform == "discord" {
                    1900
                } else {
                    3500
                },
            );
            if final_preview && fragments.is_empty() {
                fragments.push("(Empty reply)".to_owned());
            }
            for (ix, fragment) in fragments.into_iter().enumerate() {
                let job = if ix == 0 && final_preview {
                    Delivery::Final {
                        source: source.into(),
                        text: fragment,
                    }
                } else {
                    Delivery::Text(fragment)
                };
                tx.execute(
                    "INSERT INTO outgoing(route,job) VALUES (?,?)",
                    params![b.id, serde_json::to_string(&job)?],
                )?;
            }
        }
        cursor = seq
    }
    tx.execute(
        "UPDATE routes SET cursor=? WHERE id=?",
        params![cursor, b.id],
    )?;
    tx.commit()?;
    Ok(())
}
/// Telegram's text bound counts UTF-16 code units, including surrogate pairs.
fn chunks(text: &str, limit: usize) -> Vec<String> {
    let mut out = Vec::new();
    let mut piece = String::new();
    let mut n = 0;
    for c in text.chars() {
        if n + c.len_utf16() > limit {
            out.push(std::mem::take(&mut piece));
            n = 0
        }
        piece.push(c);
        n += c.len_utf16()
    }
    if !piece.is_empty() {
        out.push(piece)
    }
    out
}
pub fn queue_card(db: &mut Connection, b: &Binding, action: &str, title: &str) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let token = random();
    if tx.execute(
        "INSERT OR IGNORE INTO cards VALUES (?,?,?)",
        params![token, b.id, action],
    )? == 1
    {
        let job = Delivery::Approval {
            title: title.chars().take(1000).collect(),
            token,
        };
        tx.execute(
            "INSERT INTO outgoing(route,job) VALUES (?,?)",
            params![b.id, serde_json::to_string(&job)?],
        )?;
    }
    tx.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn message(actor: &str, text: &str, id: &str) -> Event {
        Event {
            event_id: id.into(),
            route: Route {
                platform: "telegram".into(),
                chat: "1".into(),
                thread: "2".into(),
                actor: actor.into(),
                reply_to: None,
            },
            input: Input::Message(text.into()),
        }
    }
    #[test]
    fn code_redeems_once_for_authorized_sender_only() {
        let mut db = open(Path::new(":memory:")).unwrap();
        let code = issue(&db, "telegram", "7", "conv", false, 42).unwrap();
        for who in ["8", "7", "7"] {
            ingest(&mut db, message(who, &format!("/pair {code}"), "pair")).unwrap();
        }
        let bs = bindings(&db).unwrap();
        assert_eq!(bs.len(), 1);
        assert_eq!(bs[0].cursor, 42);
        for (who, id) in [("8", "event-1"), ("7", "event-2"), ("7", "event-2")] {
            ingest(&mut db, message(who, "hi", id)).unwrap();
        }
        assert_eq!(
            db.query_row("SELECT count(*) FROM inbox", [], |row| row.get::<_, i64>(0))
                .unwrap(),
            1
        );
        assert!(chunks(&"🐱".repeat(2700), 3500)
            .iter()
            .all(|t| t.encode_utf16().count() <= 3500));
    }

    #[test]
    fn replayed_and_forwarded_cards_cannot_cross_thread_or_actor() {
        let mut db = open(Path::new(":memory:")).unwrap();
        let code = issue(&db, "telegram", "7", "conv", true, 0).unwrap();
        ingest(&mut db, message("7", &format!("/pair {code}"), "pair")).unwrap();
        let b = bindings(&db).unwrap().pop().unwrap();
        queue_card(&mut db, &b, "approval1", "test").unwrap();
        let token: String = db
            .query_row("SELECT token FROM cards", [], |r| r.get(0))
            .unwrap();
        for (actor, thread, event) in [
            ("8", "2", "forward"),
            ("7", "3", "wrong-thread"),
            ("7", "2", "same"),
            ("7", "2", "same"),
        ] {
            let mut e = message(actor, "", event);
            e.route.thread = thread.into();
            e.input = Input::Button {
                value: format!("a.{token}"),
                callback_id: Some("q".into()),
            };
            ingest(&mut db, e).unwrap();
        }
        assert_eq!(
            db.query_row("SELECT count(*) FROM inbox", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        let raw: String = db
            .query_row("SELECT job FROM inbox", [], |r| r.get(0))
            .unwrap();
        assert!(
            matches!(serde_json::from_str(&raw).unwrap(),Job::Action{id,resolution,..} if id=="approval1" && resolution=="approved")
        );
    }
}
