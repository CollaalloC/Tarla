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
    let mut db = Connection::open(path)?;
    db.busy_timeout(std::time::Duration::from_secs(5))?;
    db.execute_batch("
    PRAGMA foreign_keys=ON; PRAGMA journal_mode=WAL;
    CREATE TABLE IF NOT EXISTS codes(hash TEXT PRIMARY KEY, platform TEXT NOT NULL, actor TEXT NOT NULL, conv TEXT NOT NULL, approvals INTEGER NOT NULL, cursor INTEGER NOT NULL, expires INTEGER NOT NULL);
    CREATE TABLE IF NOT EXISTS routes(id TEXT PRIMARY KEY, platform TEXT NOT NULL, chat TEXT NOT NULL, thread TEXT NOT NULL, actor TEXT NOT NULL, conv TEXT NOT NULL, approvals INTEGER NOT NULL, cursor INTEGER NOT NULL, dest TEXT NOT NULL, UNIQUE(platform,chat,thread));
    CREATE TABLE IF NOT EXISTS cards(token TEXT PRIMARY KEY, route TEXT NOT NULL REFERENCES routes(id) ON DELETE CASCADE, action TEXT NOT NULL, UNIQUE(route,action));
    CREATE TABLE IF NOT EXISTS inbox(n INTEGER PRIMARY KEY, platform TEXT NOT NULL, event TEXT NOT NULL, route TEXT NOT NULL REFERENCES routes(id) ON DELETE CASCADE, job TEXT NOT NULL, tries INTEGER NOT NULL DEFAULT 0, retry INTEGER NOT NULL DEFAULT 0, done INTEGER NOT NULL DEFAULT 0, UNIQUE(platform,event));
    CREATE TABLE IF NOT EXISTS outgoing(n INTEGER PRIMARY KEY AUTOINCREMENT, route TEXT NOT NULL REFERENCES routes(id) ON DELETE CASCADE, job TEXT NOT NULL, tries INTEGER NOT NULL DEFAULT 0, retry INTEGER NOT NULL DEFAULT 0);
    CREATE INDEX IF NOT EXISTS gateway_route_conversation ON routes(conv);
    CREATE INDEX IF NOT EXISTS gateway_pending_inbox ON inbox(route,n) WHERE done=0;
    CREATE INDEX IF NOT EXISTS gateway_pending_outgoing ON outgoing(route,n);
    CREATE TABLE IF NOT EXISTS retired_events(platform TEXT NOT NULL, event TEXT NOT NULL, PRIMARY KEY(platform,event)) WITHOUT ROWID;
    CREATE TRIGGER IF NOT EXISTS retire_inbox_event BEFORE DELETE ON inbox BEGIN
        INSERT OR IGNORE INTO retired_events(platform,event) VALUES (OLD.platform,OLD.event);
    END;
    CREATE TRIGGER IF NOT EXISTS discard_card_delivery AFTER DELETE ON cards BEGIN
        DELETE FROM outgoing WHERE route=OLD.route AND json_extract(job,'$.Approval.token')=OLD.token;
    END;
    CREATE TABLE IF NOT EXISTS terminal_notices(route TEXT NOT NULL REFERENCES routes(id) ON DELETE CASCADE, action TEXT NOT NULL, PRIMARY KEY(route,action));
    ")?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let bound_cards: bool = tx.query_row(
        "SELECT EXISTS(SELECT 1 FROM pragma_table_info('cards') WHERE name='message_id')",
        [],
        |r| r.get(0),
    )?;
    if !bound_cards {
        // The next poll reissues legacy delivered Feishu cards for pending
        // decisions. Only callbacks on a recorded delivery may authorize one.
        tx.execute_batch("ALTER TABLE cards ADD COLUMN message_id TEXT;
            UPDATE inbox SET done=1,job='' WHERE done=0 AND platform='feishu' AND json_type(job,'$.Action')='object';
            DELETE FROM cards WHERE route IN (SELECT id FROM routes WHERE platform='feishu')
            AND NOT EXISTS (SELECT 1 FROM outgoing o WHERE o.route=cards.route
                AND json_extract(o.job,'$.Approval.token')=cards.token);")?;
    }
    for table in ["inbox", "outgoing"] {
        let present: bool = tx.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info(?) WHERE name='lease')",
            [table],
            |row| row.get(0),
        )?;
        if !present {
            tx.execute(&format!("ALTER TABLE {table} ADD COLUMN lease TEXT"), [])?;
        }
    }
    tx.commit()?;
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
        Input::Button {
            value,
            callback_id,
            message_id,
        } => {
            let Ok((resolution, token)) = decision(&value) else {
                return Ok(());
            };
            let row: Option<(String, String)> = tx
                .query_row(
                    "SELECT r.id,c.action FROM cards c JOIN routes r ON c.route=r.id
                 WHERE c.token=? AND r.platform=? AND r.chat=? AND r.actor=? AND r.approvals=1
                 AND CASE WHEN r.platform='feishu' THEN c.message_id=? ELSE r.thread=? END",
                    params![token, r.platform, r.chat, r.actor, message_id, r.thread],
                    |q| Ok((q.get(0)?, q.get(1)?)),
                )
                .optional()?;
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
        "INSERT OR IGNORE INTO inbox(platform,event,route,job)
         SELECT ?1,?2,?3,?4 WHERE NOT EXISTS (SELECT 1 FROM retired_events WHERE platform=?1 AND event=?2)",
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
    route_id: &str,
    platform: &str,
    messages: &[serde_json::Value],
) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let Some(mut cursor): Option<i64> = tx
        .query_row("SELECT cursor FROM routes WHERE id=?", [route_id], |q| {
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
            let final_preview = crate::drafts::close(&tx, route_id, source)?;
            let mut fragments = chunks(text, if platform == "discord" { 1900 } else { 3500 });
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
                    params![route_id, serde_json::to_string(&job)?],
                )?;
            }
        }
        cursor = seq
    }
    tx.execute(
        "UPDATE routes SET cursor=? WHERE id=?",
        params![cursor, route_id],
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
        "INSERT OR IGNORE INTO cards(token,route,action) VALUES (?,?,?)",
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

/// An approval can be decided in the CLI or another paired route while a
/// delivery is waiting for provider recovery.
pub fn retire_finished_cards(db: &mut Connection, actions: &[serde_json::Value]) -> Result<()> {
    let mut pending = Vec::new();
    for action in actions.iter().filter(|a| a["kind"] == "approval") {
        if action["resolution"].as_str().is_none_or(|r| r == "pending") {
            pending.push(action["id"].as_str().context("invalid pending approval")?);
        }
    }
    db.execute(
        "DELETE FROM cards WHERE action NOT IN (SELECT value FROM json_each(?))",
        [serde_json::to_string(&pending)?],
    )?;
    Ok(())
}

pub fn queue_terminal_notice(
    db: &mut Connection,
    binding: &Binding,
    action: &str,
    text: String,
) -> Result<()> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if tx.execute(
        "INSERT OR IGNORE INTO terminal_notices VALUES (?,?)",
        params![binding.id, action],
    )? == 1
    {
        tx.execute(
            "INSERT INTO outgoing(route,job) VALUES (?,?)",
            params![binding.id, serde_json::to_string(&Delivery::Text(text))?],
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
                message_id: None,
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

    #[test]
    fn provider_retry_after_pairing_change_stays_with_the_retired_event() {
        let mut db = open(Path::new(":memory:")).unwrap();
        for conv in ["old", "new"] {
            let code = issue(&db, "telegram", "7", conv, false, 0).unwrap();
            ingest(&mut db, message("7", &format!("/pair {code}"), conv)).unwrap();
            ingest(&mut db, message("7", "old question", "previous")).unwrap();
            if conv == "old" {
                db.execute("DELETE FROM routes WHERE conv='old'", [])
                    .unwrap();
            }
        }
        ingest(&mut db, message("7", "new question", "current")).unwrap();
        let (count, event): (u32, String) = db
            .query_row("SELECT count(*),min(event) FROM inbox", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        assert_eq!((count, event.as_str()), (1, "current"));
    }

    #[test]
    fn upgrade_keeps_unsent_feishu_cards_and_reissues_unbound_deliveries() {
        let file = std::env::temp_dir().join(format!("tarla-gateway-{}.db", Uuid::new_v4()));
        let mut db = open(&file).unwrap();
        let code = issue(&db, "feishu", "7", "conv", true, 0).unwrap();
        let mut pair = message("7", &format!("/pair {code}"), "pair");
        pair.route.platform = "feishu".into();
        ingest(&mut db, pair).unwrap();
        let b = bindings(&db).unwrap().pop().unwrap();
        for action in ["delivered", "unsent"] {
            queue_card(&mut db, &b, action, "Review").unwrap();
        }
        let old_token: String = db
            .query_row(
                "SELECT token FROM cards WHERE action='delivered'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        db.execute(
            "DELETE FROM outgoing WHERE json_extract(job,'$.Approval.token')=?",
            [&old_token],
        )
        .unwrap();
        db.execute(
            "INSERT INTO inbox(platform,event,route,job) VALUES('feishu','old-click',?,?)",
            params![
                b.id,
                serde_json::to_string(&Job::Action {
                    id: "delivered".into(),
                    resolution: "approved".into(),
                    callback: None
                })
                .unwrap()
            ],
        )
        .unwrap();
        db.execute_batch(
            "ALTER TABLE cards DROP COLUMN message_id;
            DROP TRIGGER retire_inbox_event; DROP TABLE retired_events;",
        )
        .unwrap();
        drop(db);

        let mut db = open(&file).unwrap();
        assert_eq!(
            db.query_row("SELECT action FROM cards", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "unsent"
        );
        assert_eq!(
            db.query_row("SELECT done FROM inbox WHERE event='old-click'", [], |r| r
                .get::<_, u8>(
                0
            ))
            .unwrap(),
            1
        );
        for action in ["delivered", "unsent"] {
            queue_card(&mut db, &b, action, "Review").unwrap();
        }
        assert_eq!(
            db.query_row(
                "SELECT count(*) FROM outgoing WHERE json_type(job,'$.Approval')='object'",
                [],
                |r| r.get::<_, u32>(0)
            )
            .unwrap(),
            2
        );
        assert_ne!(
            db.query_row(
                "SELECT token FROM cards WHERE action='delivered'",
                [],
                |r| r.get::<_, String>(0)
            )
            .unwrap(),
            old_token
        );
        db.execute("DELETE FROM routes WHERE id=?", [&b.id])
            .unwrap();
        assert_eq!(
            db.query_row("SELECT event FROM retired_events", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "old-click"
        );
        drop(db);
        std::fs::remove_file(file).unwrap();
    }
}
