//! One optional live preview per paired inbound event. The committed message
//! cursor owns final delivery; it stays correct if no drafts ever arrive.
use crate::{platform::Route, store::Delivery, Gateway};
use anyhow::Result;
use reqwest::Method;
use rusqlite::{params, Connection, OptionalExtension, TransactionBehavior};
use serde_json::json;
use std::time::Duration;

pub(crate) const SCHEMA:&str="CREATE TABLE IF NOT EXISTS live_drafts(
 route TEXT NOT NULL REFERENCES routes(id) ON DELETE CASCADE, source TEXT NOT NULL, remote_id TEXT,
 latest TEXT NOT NULL DEFAULT '', sent TEXT NOT NULL DEFAULT '', queued INTEGER REFERENCES outgoing(n) ON DELETE SET NULL,
 touched INTEGER NOT NULL DEFAULT 0, closed INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(route,source));";

pub(crate) fn source(platform: &str, event: &str) -> String {
    format!("in:{platform}:{event}")
}
pub(crate) fn track(c: &Connection, rid: &str, key: &str) -> Result<()> {
    c.execute(
        "INSERT OR IGNORE INTO live_drafts(route,source) VALUES(?,?)",
        params![rid, key],
    )?;
    Ok(())
}
fn preview(r: &Route, text: &str) -> String {
    if text.is_empty() {
        return "…".into();
    }
    let limit = if r.platform == "discord" { 1850 } else { 3400 };
    let mut used = 0;
    let mut out = String::new();
    for ch in text.chars() {
        used += ch.len_utf16();
        if used > limit {
            break;
        }
        out.push(ch);
    }
    out.push_str(" ▍");
    out
}
fn observe(
    c: &mut Connection,
    rid: &str,
    key: &str,
    r: &Route,
    text: &str,
    now_ms: i64,
) -> Result<()> {
    let tx = c.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let row=tx.query_row("SELECT latest,sent,queued,touched,remote_id FROM live_drafts WHERE route=? AND source=? AND closed=0",params![rid,key],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,Option<i64>>(2)?,r.get::<_,i64>(3)?,r.get::<_,Option<String>>(4)?))).optional()?;
    let Some((latest, sent, queued, touched, remote)) = row else {
        return Ok(());
    };
    if text.is_empty() && latest.is_empty() && remote.is_none() {
        return Ok(());
    }
    let value = preview(r, text);
    if value != latest {
        tx.execute(
            "UPDATE live_drafts SET latest=? WHERE route=? AND source=?",
            params![value, rid, key],
        )?;
    }
    if queued.is_none() && sent != value && now_ms - touched >= 1800 {
        tx.execute(
            "INSERT INTO outgoing(route,job) VALUES(?,?)",
            params![
                rid,
                serde_json::to_string(&Delivery::Draft { source: key.into() })?
            ],
        )?;
        tx.execute(
            "UPDATE live_drafts SET queued=?,touched=? WHERE route=? AND source=?",
            params![tx.last_insert_rowid(), now_ms, rid, key],
        )?;
    }
    tx.commit()?;
    Ok(())
}
/// Close the preview before advancing the finalized history cursor. A late GET
/// response from an earlier model attempt can no longer enqueue another edit.
pub(crate) fn close(c: &Connection, rid: &str, source: &str) -> Result<bool> {
    Ok(c.execute(
        "UPDATE live_drafts SET closed=1 WHERE route=? AND source=? AND closed=0",
        params![rid, source],
    )? == 1)
}
pub(crate) async fn post(
    g: &Gateway,
    rid: &str,
    r: &Route,
    conv: &str,
    event: &str,
    text: &str,
) -> Result<()> {
    let endpoint = ["conversations", conv, "messages"];
    let pending = g.core(
        Method::POST,
        &endpoint,
        &[],
        Some(json!({"text":text,"source_msg_id":event,"channel":r.platform})),
    );
    tokio::pin!(pending);
    let key = source(&r.platform, event);
    let previews = async {
        let mut caught_up = false;
        loop {
            tokio::time::sleep(Duration::from_millis(650)).await;
            let query = [
                ("channel", r.platform.clone()),
                ("source_msg_id", event.to_owned()),
            ];
            let path = ["conversations", conv, "draft"];
            let req = g.core(Method::GET, &path, &query, None);
            if let Ok(Ok(v)) = tokio::time::timeout(Duration::from_secs(2), req).await {
                if v["status"] == "streaming" {
                    if let Some(t) = v["text"].as_str() {
                        // A prior turn may have finished since the last poll.
                        // Queue its answer before displaying this turn's preview.
                        if !caught_up {
                            caught_up = g
                                .queue_history_page(rid, &r.platform, conv)
                                .await
                                .unwrap_or(false);
                            if !caught_up {
                                continue;
                            }
                        }
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as i64;
                        if let Ok(mut c) = g.db() {
                            let _ = observe(&mut c, rid, &key, r, t, now_ms);
                        }
                    }
                }
            }
        }
    };
    tokio::select! {biased;out=&mut pending=>out.map(|_|()),_=previews=>unreachable!()}
}
pub(crate) async fn send(g: &Gateway, rid: &str, r: &Route, n: i64, job: Delivery) -> Result<()> {
    let (key, final_text) = match job {
        Delivery::Draft { source } => (source, None),
        Delivery::Final { source, text } => (source, Some(text)),
        other => return g.deliver(r, n, other, None).await.map(|_| ()),
    };
    let row: Option<(Option<String>, String, bool)> = g
        .db()?
        .query_row(
            "SELECT remote_id,latest,closed FROM live_drafts WHERE route=? AND source=?",
            params![rid, key],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .optional()?;
    if let Some(text) = final_text {
        let remote = row.and_then(|(id, _, _)| id);
        let delivered = g
            .deliver(r, n, Delivery::Text(text.clone()), remote.as_deref())
            .await;
        if let Err(e) = delivered {
            let transient = e.downcast_ref::<reqwest::Error>().is_some_and(|e| {
                e.status().is_none_or(|code| {
                    code.is_server_error()
                        || code == reqwest::StatusCode::TOO_MANY_REQUESTS
                        || code == reqwest::StatusCode::REQUEST_TIMEOUT
                })
            });
            if remote.is_none() || transient {
                return Err(e);
            }
            // A user may delete the preview or the provider's edit window may
            // expire. A fresh message preserves final delivery.
            g.deliver(r, n, Delivery::Text(text), None).await?;
        }
        // Retain the destination until the same transaction deletes the outbox
        // row: a crash between the HTTP edit and local ack replays the edit.
        return Ok(());
    }
    let Some((remote, latest, false)) = row else {
        return Ok(());
    };
    let update = g
        .deliver(r, n, Delivery::Text(latest.clone()), remote.as_deref())
        .await;
    if let Ok(id) = update {
        g.db()?.execute("UPDATE live_drafts SET remote_id=coalesce(?,remote_id),sent=? WHERE route=? AND source=?",params![id,latest,rid,key])?;
    }
    // Preview failures must never hold up an authoritative reply in the outbox.
    Ok(())
}
pub(crate) fn ack(db: &Connection, rid: &str, raw: &str) -> Result<()> {
    if let Delivery::Final { source, .. } = serde_json::from_str(raw)? {
        db.execute(
            "DELETE FROM live_drafts WHERE route=? AND source=?",
            params![rid, source],
        )?;
    }
    Ok(())
}
pub(crate) fn abort(
    db: &Connection,
    rid: &str,
    platform: &str,
    event: &str,
    text: &str,
) -> Result<()> {
    let key = source(platform, event);
    let job = if close(db, rid, &key)? {
        Delivery::Final {
            source: key,
            text: text.into(),
        }
    } else {
        Delivery::Text(text.into())
    };
    db.execute(
        "INSERT INTO outgoing(route,job) VALUES (?,?)",
        params![rid, serde_json::to_string(&job)?],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::{Discord, Feishu, GatewayConfig, Slack, Telegram},
        store::{self, Binding},
    };
    use axum::{extract::State, http::Uri, routing::any, Json, Router};
    use reqwest::Url;
    use serde_json::Value;
    use std::{
        path::Path,
        sync::{Arc, Mutex},
    };
    use uuid::Uuid;
    type Calls = Arc<Mutex<Vec<(Method, String, Value)>>>;
    async fn fake(
        State(calls): State<Calls>,
        method: Method,
        uri: Uri,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        let path = uri.path().to_owned();
        calls.lock().unwrap().push((method, path.clone(), body));
        let v = if path.contains("/bot") {
            json!({"ok":true,"result":{"message_id":17}})
        } else if path.contains("/auth/") {
            json!({"code":0,"tenant_access_token":"token","expire":600})
        } else if path.contains("/im/") {
            json!({"code":0,"data":{"message_id":"om_draft"}})
        } else if path.contains("/api/chat.") {
            json!({"ok":true,"ts":"17.1"})
        } else {
            json!({"id":"17"})
        };
        Json(v)
    }
    async fn origin() -> (Url, Calls, tokio::task::JoinHandle<()>) {
        let calls = Calls::default();
        let app = Router::new().fallback(any(fake)).with_state(calls.clone());
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap())
            .parse()
            .unwrap();
        (
            url,
            calls,
            tokio::spawn(async { axum::serve(l, app).await.unwrap() }),
        )
    }
    fn gw(db: &Path, api: Url) -> Gateway {
        Gateway {
            cfg: GatewayConfig {
                database: db.into(),
                core: api.clone(),
                token: "owner".into(),
                telegram: Some(Telegram {
                    token: "55:test".into(),
                    secret: "webhook".into(),
                }),
                feishu: Some(Feishu {
                    app_id: "app".into(),
                    app_secret: "secret".into(),
                    verification_token: "v".into(),
                    encrypt_key: None,
                }),
                slack: Some(Slack {
                    team: "T".into(),
                    signing: "s".into(),
                    bot: "b".into(),
                }),
                discord: Some(Discord {
                    app: "d".into(),
                    public: vec![],
                    bot: "b".into(),
                }),
            },
            connection: Mutex::new(store::open(db).unwrap()),
            client: reqwest::Client::new(),
            feishu_token: Default::default(),
            instance: "test".into(),
            test_platform_origin: Some(api),
        }
    }
    fn binding(platform: &str) -> Binding {
        Binding {
            id: "R".into(),
            conversation_id: "C".into(),
            approvals: false,
            cursor: 0,
            route: Route {
                platform: platform.into(),
                chat: "123".into(),
                thread: "7".into(),
                actor: "11".into(),
                reply_to: (platform == "feishu").then(|| "om_parent".into()),
            },
        }
    }
    async fn drain(g: &Gateway, b: &Binding) {
        let (n, raw): (i64, String) = g
            .db()
            .unwrap()
            .query_row("SELECT n,job FROM outgoing ORDER BY n LIMIT 1", [], |r| {
                Ok((r.get(0)?, r.get(1)?))
            })
            .unwrap();
        send(g, &b.id, &b.route, n, serde_json::from_str(&raw).unwrap())
            .await
            .unwrap();
        let mut db = g.db().unwrap();
        let tx = db.transaction().unwrap();
        ack(&tx, &b.id, &raw).unwrap();
        tx.execute("DELETE FROM outgoing WHERE n=?", [n]).unwrap();
        tx.commit().unwrap();
    }
    async fn stream(platform: &str) {
        let dir = std::env::temp_dir().join(Uuid::new_v4().simple().to_string());
        std::fs::create_dir(&dir).unwrap();
        let file = dir.join("gateway.db");
        let (api, calls, server) = origin().await;
        let g = gw(&file, api.clone());
        let b = binding(platform);
        let key = source(platform, "E");
        {
            let db = g.db().unwrap();
            db.execute(
                "INSERT INTO routes VALUES('R',?,'123','7','11','C',0,0,?)",
                params![platform, serde_json::to_string(&b.route).unwrap()],
            )
            .unwrap();
            track(&db, &b.id, &key).unwrap();
        }
        let sample = |g: &Gateway, text: &str, ts| {
            observe(&mut *g.db().unwrap(), &b.id, &key, &b.route, text, ts).unwrap()
        };
        sample(&g, "H", 10000);
        sample(&g, "Hello", 10100);
        drain(&g, &b).await;
        sample(&g, "Hello world", 10400);
        assert_eq!(
            g.db()
                .unwrap()
                .query_row("SELECT count(*) FROM outgoing", [], |r| r.get::<_, i32>(0))
                .unwrap(),
            0
        );
        sample(&g, "Hello world", 12000);
        drain(&g, &b).await;
        // Restart after the edit. The permanent final must edit this same remote ID.
        drop(g);
        let g = gw(&file, api);
        store::queue_messages(&mut *g.db().unwrap(),&b.id,&b.route.platform,&[json!({"seq_no":2,"source_msg_id":key,"sender_role":"assistant","content":"Final answer."})]).unwrap();
        sample(&g, "late partial", 15000);
        drain(&g, &b).await;
        let transcript: Vec<_> = calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(_, p, _)| !p.contains("/auth/"))
            .cloned()
            .collect();
        assert_eq!(transcript.len(), 3, "{transcript:?}");
        assert!(transcript[0].2.to_string().contains("Hello ▍"));
        assert!(transcript[1].2.to_string().contains("Hello world ▍"));
        assert!(transcript[2].2.to_string().contains("Final answer."));
        let (method, path, body) = &transcript[2];
        match platform {
            "telegram" => {
                assert!(path.ends_with("/editMessageText"));
                assert_eq!(body["message_id"], 17);
                assert_eq!(transcript[0].2["message_thread_id"], 7)
            }
            "feishu" => {
                assert_eq!(method, Method::PUT);
                assert!(path.ends_with("/om_draft"));
                assert!(transcript[0].1.ends_with("/om_parent/reply"));
            }
            "slack" => {
                assert!(path.ends_with("chat.update"));
                assert_eq!(body["ts"], "17.1")
            }
            _ => {
                assert_eq!(method, Method::PATCH);
                assert!(path.ends_with("/channels/123/messages/17"))
            }
        }
        assert_eq!(
            g.db()
                .unwrap()
                .query_row("SELECT count(*) FROM live_drafts", [], |r| r
                    .get::<_, usize>(0))
                .unwrap(),
            0
        );
        drop(g);
        server.abort();
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[tokio::test]
    async fn telegram_draft_edits_survive_restart() {
        stream("telegram").await
    }
    #[tokio::test]
    async fn feishu_draft_edits_survive_restart() {
        stream("feishu").await
    }
    #[tokio::test]
    async fn slack_draft_edits_survive_restart() {
        stream("slack").await
    }
    #[tokio::test]
    async fn discord_draft_edits_survive_restart() {
        stream("discord").await
    }

    #[tokio::test]
    async fn prior_committed_reply_is_queued_before_the_next_turns_live_preview() {
        use axum::{
            extract::Query,
            http::HeaderMap,
            routing::{get, post},
        };
        use std::collections::HashMap;
        use tokio::sync::Notify;
        let finished = Arc::new(Notify::new());
        let done = finished.clone();
        let app=Router::new()
            .route("/v1/conversations/C/messages",post(move|headers:HeaderMap,Json(body):Json<Value>|async move {
                assert_eq!(headers["authorization"],"Bearer owner");
                assert_eq!(body["source_msg_id"],"E");
                done.notified().await;
                Json(json!({"reply":"done"}))
            }).get(|Query(q):Query<HashMap<String,String>>| async move {
                assert_eq!(q["after_seq"],"0");
                Json(json!([
                    {"seq_no":1,"channel_type":"feishu","source_msg_id":"older","sender_role":"user","content":"first question"},
                    {"seq_no":2,"channel_type":"supervisor","source_msg_id":"in:feishu:older","sender_role":"assistant","content":"first answer"},
                    {"seq_no":3,"channel_type":"telegram","source_msg_id":"E","sender_role":"user","content":"question"}
                ]))
            }))
            .route("/v1/conversations/C/draft",get(|headers:HeaderMap,Query(q):Query<HashMap<String,String>>|async move{
                assert_eq!(headers["authorization"],"Bearer owner");
                assert_eq!(q["source_msg_id"],"E");assert_eq!(q["channel"],"telegram");
                Json(json!({"status":"streaming","text":"Visible before completion","revision":2}))
            }));
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let g = Arc::new(gw(
            Path::new(":memory:"),
            format!("http://{}", l.local_addr().unwrap())
                .parse()
                .unwrap(),
        ));
        let b = binding("telegram");
        let server = tokio::spawn(async { axum::serve(l, app).await.unwrap() });
        {
            let c = g.db().unwrap();
            c.execute(
                "INSERT INTO routes VALUES('R','telegram','123','7','11','C',0,0,?)",
                [serde_json::to_string(&b.route).unwrap()],
            )
            .unwrap();
            track(&c, &b.id, &source("telegram", "E")).unwrap();
        }
        let running = g.clone();
        let msg = tokio::spawn(async move {
            super::post(&running, &b.id, &b.route, "C", "E", "question").await
        });
        tokio::time::timeout(Duration::from_secs(5),async{loop{
            let n=g.db().unwrap().query_row("SELECT count(*) FROM live_drafts WHERE latest LIKE 'Visible%' AND queued IS NOT NULL",[],|r|r.get::<_,usize>(0)).unwrap();
            if n==1{break}tokio::time::sleep(Duration::from_millis(25)).await;
        }}).await.unwrap();
        assert!(!msg.is_finished());
        {
            let db = g.db().unwrap();
            let mut query = db.prepare("SELECT job FROM outgoing ORDER BY n").unwrap();
            let queue: Vec<Value> = query
                .query_map([], |row| row.get::<_, String>(0))
                .unwrap()
                .map(|raw| serde_json::from_str(&raw.unwrap()).unwrap())
                .collect();
            assert_eq!(
                queue,
                [
                    json!({"Text":"first answer"}),
                    json!({"Draft":{"source":"in:telegram:E"}})
                ]
            );
        }
        finished.notify_one();
        msg.await.unwrap().unwrap();
        server.abort();
    }
}
