use crate::{
    platform::{self, Card, Route},
    store::{self, Delivery, Job},
    Gateway,
};
use anyhow::{bail, Context, Result};
use reqwest::{Method, StatusCode, Url};
use rusqlite::{params, OptionalExtension, TransactionBehavior};
use serde_json::{json, Value};
use std::{
    sync::{Arc, Weak},
    time::Duration,
};
use tokio::time::sleep;

impl Gateway {
    pub(crate) async fn core(
        &self,
        method: Method,
        segments: &[&str],
        query: &[(&str, String)],
        body: Option<Value>,
    ) -> Result<Value> {
        let mut url = self.cfg.core.clone();
        {
            let mut p = url
                .path_segments_mut()
                .map_err(|_| anyhow::anyhow!("invalid Core URL"))?;
            p.clear().push("v1").extend(segments.iter().copied());
        }
        let mut req = self
            .client
            .request(method, url)
            .bearer_auth(&self.cfg.token)
            .query(query);
        if let Some(b) = body {
            req = req.json(&b)
        }
        Ok(req.send().await?.error_for_status()?.json().await?)
    }
    async fn feishu_token(&self) -> Result<String> {
        let mut cached = self.feishu_token.lock().await;
        if let Some((expires, token)) = &*cached {
            if store::now() < *expires {
                return Ok(token.clone());
            }
        }
        let f = self.cfg.feishu.as_ref().context("Feishu disabled")?;
        let body: Value = self
            .client
            .post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal")
            .json(&json!({"app_id":f.app_id,"app_secret":f.app_secret}))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if body["code"] != 0 {
            bail!("Feishu token rejected")
        }
        let token = body["tenant_access_token"]
            .as_str()
            .context("missing tenant token")?
            .to_owned();
        *cached = Some((
            store::now() + (body["expire"].as_i64().unwrap_or(60) - 30).max(1),
            token.clone(),
        ));
        Ok(token)
    }
    async fn deliver(&self, route: &Route, n: i64, d: Delivery) -> Result<()> {
        let (text, card) = match &d {
            Delivery::Text(s) => (s.as_str(), None),
            Delivery::Approval { title, token } => ("", Some(Card { title, token })),
            Delivery::Callback(_) => ("", None),
        };
        match route.platform.as_str() {
            "telegram" => {
                let t = self.cfg.telegram.as_ref().context("Telegram disabled")?;
                let (method, body) = if let Delivery::Callback(id) = &d {
                    (
                        "answerCallbackQuery",
                        json!({"callback_query_id":id,"text":"Recorded"}),
                    )
                } else {
                    (
                        "sendMessage",
                        platform::telegram_payload(route, text, card)?,
                    )
                };
                let url = format!("https://api.telegram.org/bot{}/{method}", t.token);
                let response = self
                    .client
                    .post(url)
                    .json(&body)
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<Value>()
                    .await?;
                if response["ok"] != true {
                    bail!("Telegram declined operation")
                }
            }
            "feishu" => {
                let mut body = platform::feishu_payload(route, text, card);
                body["uuid"] = json!(format!("{}-{n}", &self.instance));
                let mut url = Url::parse("https://open.feishu.cn/open-apis/im/v1/messages")?;
                if let Some(id) = &route.reply_to {
                    url.path_segments_mut().unwrap().push(id).push("reply");
                } else {
                    url.query_pairs_mut()
                        .append_pair("receive_id_type", "chat_id");
                }
                let res = self
                    .client
                    .post(url)
                    .bearer_auth(self.feishu_token().await?)
                    .json(&body)
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<Value>()
                    .await?;
                if res["code"] != 0 {
                    if matches!(res["code"].as_i64(), Some(99991663 | 99991664)) {
                        *self.feishu_token.lock().await = None;
                    }
                    bail!("Feishu declined operation")
                }
            }
            "slack" => {
                let f = self.cfg.slack.as_ref().context("Slack disabled")?;
                let r: Value = self
                    .client
                    .post("https://slack.com/api/chat.postMessage")
                    .bearer_auth(&f.bot)
                    .json(&crate::workchat::slack_payload(route, text, card))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                if r["ok"] != true {
                    bail!("Slack declined operation")
                }
            }
            "discord" => {
                let f = self.cfg.discord.as_ref().context("Discord disabled")?;
                let mut u = Url::parse("https://discord.com/api/v10/channels")?;
                u.path_segments_mut()
                    .unwrap()
                    .push(&route.chat)
                    .push("messages");
                self.client
                    .post(u)
                    .header("Authorization", format!("Bot {}", f.bot))
                    .json(&crate::workchat::discord_payload(text, card))
                    .send()
                    .await?
                    .error_for_status()?;
            }
            _ => bail!("unknown platform"),
        }
        Ok(())
    }
}
struct Item {
    n: i64,
    route: Route,
    rid: String,
    conv: String,
    job: String,
    tries: i64,
}
#[derive(Copy, Clone)]
enum Work {
    In,
    Out,
}
impl Work {
    fn table(self) -> &'static str {
        match self {
            Self::In => "inbox",
            Self::Out => "outgoing",
        }
    }
    fn claim(self, g: &Gateway) -> Result<Option<Item>> {
        let mut db = g.db()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let (table,pending)=match self{Self::In=>("inbox","AND i.done=0 AND NOT EXISTS(SELECT 1 FROM inbox p WHERE p.route=i.route AND p.n<i.n AND p.done=0)"),
            Self::Out=>("outgoing","AND NOT EXISTS(SELECT 1 FROM outgoing p WHERE p.route=i.route AND p.n<i.n)")};
        let sql=format!("SELECT i.n, r.dest,r.id,r.conv,i.job,i.tries FROM {table} i JOIN routes r ON r.id=i.route WHERE i.retry<=? {pending} ORDER BY i.n LIMIT 1");
        let row = tx
            .query_row(&sql, [store::now()], |r| {
                Ok((
                    r.get(0)?,
                    r.get::<_, String>(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                ))
            })
            .optional()?;
        if let Some((n, dest, rid, conv, job, tries)) = row {
            tx.execute(
                &format!("UPDATE {table} SET retry=? WHERE n=?"),
                params![store::now() + 180, n],
            )?;
            tx.commit()?;
            Ok(Some(Item {
                n,
                route: serde_json::from_str(&dest)?,
                rid,
                conv,
                job,
                tries,
            }))
        } else {
            Ok(None)
        }
    }
    async fn one(self, g: &Gateway) -> Result<bool> {
        let Some(i) = self.claim(g)? else {
            return Ok(false);
        };
        let mut callback = None;
        let result: Result<()> = match self {
            Self::In => match serde_json::from_str(&i.job)? {
                Job::Message { source, text } => g
                    .core(
                        Method::POST,
                        &["conversations", &i.conv, "messages"],
                        &[],
                        Some(
                            json!({"text":text,"source_msg_id":source,"channel":i.route.platform}),
                        ),
                    )
                    .await
                    .map(|_| ()),
                Job::Action {
                    id,
                    resolution,
                    callback: cb,
                } => {
                    callback = cb;
                    g.core(
                        Method::POST,
                        &["actions", &id],
                        &[],
                        Some(json!({"resolution":resolution})),
                    )
                    .await
                    .map(|_| ())
                }
            },
            Self::Out => {
                g.deliver(&i.route, i.n, serde_json::from_str(&i.job)?)
                    .await
            }
        };
        let table = self.table();
        let mut db = g.db()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Err(e) = result {
            let status = e.downcast_ref::<reqwest::Error>().and_then(|e| e.status());
            let terminal = status.is_some_and(|s| {
                s.is_client_error()
                    && !matches!(
                        s,
                        StatusCode::TOO_MANY_REQUESTS
                            | StatusCode::REQUEST_TIMEOUT
                            | StatusCode::UNAUTHORIZED
                    )
            });
            if !terminal {
                let delay = (2i64.pow((i.tries + 1).min(8) as u32)).min(300);
                tx.execute(
                    &format!("UPDATE {table} SET tries=tries+1, retry=? WHERE n=?"),
                    params![store::now() + delay, i.n],
                )?;
                tx.commit()?;
                return Ok(true);
            }
            if matches!(self, Self::In) {
                tx.execute(
                    "INSERT INTO outgoing(route,job) VALUES(?,?)",
                    params![
                        i.rid,
                        serde_json::to_string(&Delivery::Text(
                            "Tarla could not accept this request.".into()
                        ))?
                    ],
                )?;
            }
            callback = None;
        }
        match self {
            Self::In => {
                tx.execute("UPDATE inbox SET done=1,job='' WHERE n=?", [i.n])?;
                if let Some(id) = callback {
                    tx.execute(
                        "INSERT INTO outgoing(route,job) VALUES(?,?)",
                        params![i.rid, serde_json::to_string(&Delivery::Callback(id))?],
                    )?;
                }
            }
            Self::Out => {
                tx.execute("DELETE FROM outgoing WHERE n=?", [i.n])?;
            }
        }
        tx.commit()?;
        Ok(true)
    }
}
async fn poll(g: &Gateway) -> Result<()> {
    let bindings = store::bindings(&*g.db()?)?;
    if bindings.is_empty() {
        return Ok(());
    }
    let approvals = if bindings.iter().any(|b| b.approvals) {
        g.core(Method::GET, &["actions"], &[], None)
            .await
            .ok()
            .and_then(|v| v.as_array().cloned())
            .unwrap_or_default()
    } else {
        vec![]
    };
    for b in bindings {
        if let Ok(msgs) = g
            .core(
                Method::GET,
                &["conversations", &b.conversation_id, "messages"],
                &[("after_seq", b.cursor.to_string()), ("limit", "100".into())],
                None,
            )
            .await
        {
            if let Some(rows) = msgs.as_array() {
                store::queue_messages(&mut *g.db()?, &b, rows)?
            }
        }
        if !b.approvals {
            continue;
        }
        for a in &approvals {
            if a["kind"] != "approval" || a["resolution"].as_str().is_some_and(|r| r != "pending") {
                continue;
            }
            let Some(action) = a["id"].as_str() else {
                continue;
            };
            let conv = if let Some(c) = a["conversation_id"].as_str() {
                c.to_owned()
            } else {
                let Some(id) = a["run_id"].as_str() else {
                    continue;
                };
                let Ok(run) = g.core(Method::GET, &["runs", id], &[], None).await else {
                    continue;
                };
                let Some(c) = run["conversation_id"].as_str() else {
                    continue;
                };
                c.into()
            };
            if conv == b.conversation_id {
                store::queue_card(
                    &mut *g.db()?,
                    &b,
                    action,
                    a["title"].as_str().unwrap_or("Approval requested"),
                )?
            }
        }
    }
    Ok(())
}
pub fn start(g: &Arc<Gateway>) {
    for work in [Work::In, Work::Out] {
        let w = Arc::downgrade(g);
        tokio::spawn(async move {
            while let Some(g) = w.upgrade() {
                let busy = work.one(&g).await.unwrap_or(false);
                drop(g);
                sleep(Duration::from_millis(if busy { 150 } else { 700 })).await;
            }
        });
    }
    let w: Weak<Gateway> = Arc::downgrade(g);
    tokio::spawn(async move {
        while let Some(g) = w.upgrade() {
            let _ = poll(&g).await;
            drop(g);
            sleep(Duration::from_secs(3)).await;
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::GatewayConfig,
        platform::{Event, Input, Route},
    };
    use axum::{extract::State, http::HeaderMap, routing::post, Json, Router};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::{path::Path, sync::Mutex};

    #[tokio::test]
    async fn retried_platform_event_is_forwarded_to_core_once() {
        let count = Arc::new(AtomicUsize::new(0));
        async fn receive(
            State(n): State<Arc<AtomicUsize>>,
            h: HeaderMap,
            Json(v): Json<Value>,
        ) -> Json<Value> {
            assert_eq!(h["authorization"], "Bearer owner-token");
            assert_eq!(
                v,
                json!({"channel":"telegram","text":"hello","source_msg_id":"e1"})
            );
            n.fetch_add(1, Ordering::SeqCst);
            Json(json!({"reply":"hi"}))
        }
        let app = Router::new()
            .route("/v1/conversations/main/messages", post(receive))
            .with_state(count.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let core = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let db = store::open(Path::new(":memory:")).unwrap();
        let code = store::issue(&db, "telegram", "actor", "main", false, 0).unwrap();
        let g = Gateway {
            cfg: GatewayConfig {
                database: ":memory:".into(),
                core,
                token: "owner-token".into(),
                telegram: None,
                feishu: None,
                slack: None,
                discord: None,
            },
            connection: Mutex::new(db),
            client: reqwest::Client::new(),
            feishu_token: Default::default(),
            instance: "test".into(),
        };
        let route = Route {
            platform: "telegram".into(),
            chat: "chat".into(),
            thread: "thread".into(),
            actor: "actor".into(),
            reply_to: None,
        };
        for (id, msg) in [
            ("pair", format!("/pair {code}")),
            ("e1", "hello".into()),
            ("e1", "hello".into()),
        ] {
            store::ingest(
                &mut *g.db().unwrap(),
                Event {
                    event_id: id.into(),
                    route: route.clone(),
                    input: Input::Message(msg),
                },
            )
            .unwrap();
        }
        assert!(Work::In.one(&g).await.unwrap());
        assert!(!Work::In.one(&g).await.unwrap());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        server.abort();
    }
}
