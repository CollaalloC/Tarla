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
    fn platform_url(&self, url: Url) -> Url {
        #[cfg(test)]
        if let Some(base) = &self.test_platform_origin {
            let mut target = base.clone();
            target.set_path(url.path());
            target.set_query(url.query());
            return target;
        }
        url
    }
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
    /// Queue one page of committed replies in history order. A short page
    /// means this route has caught up to the history visible at request time.
    pub(crate) async fn queue_history_page(
        &self,
        route_id: &str,
        platform: &str,
        conversation: &str,
    ) -> Result<bool> {
        const LIMIT: usize = 100;
        let cursor: i64 =
            self.db()?
                .query_row("SELECT cursor FROM routes WHERE id=?", [route_id], |r| {
                    r.get(0)
                })?;
        let page = self
            .core(
                Method::GET,
                &["conversations", conversation, "messages"],
                &[
                    ("after_seq", cursor.to_string()),
                    ("limit", LIMIT.to_string()),
                ],
                None,
            )
            .await?;
        let rows = page.as_array().context("invalid conversation history")?;
        store::queue_messages(&mut *self.db()?, route_id, platform, rows)?;
        Ok(rows.len() < LIMIT)
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
            .post(self.platform_url(Url::parse(
                "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",
            )?))
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
    pub(crate) async fn deliver(
        &self,
        route: &Route,
        n: i64,
        d: Delivery,
        edit: Option<&str>,
    ) -> Result<Option<String>> {
        let (text, card) = match &d {
            Delivery::Text(s) => (s.as_str(), None),
            Delivery::Approval { title, token } => ("", Some(Card { title, token })),
            Delivery::Callback(_) => ("", None),
            Delivery::Draft { .. } | Delivery::Final { .. } => {
                bail!("expected a rendered delivery")
            }
        };
        match route.platform.as_str() {
            "telegram" => {
                let t = self.cfg.telegram.as_ref().context("Telegram disabled")?;
                let (method, body) = if let Delivery::Callback(id) = &d {
                    (
                        "answerCallbackQuery",
                        json!({"callback_query_id":id,"text":"Recorded"}),
                    )
                } else if let Some(id) = edit {
                    (
                        "editMessageText",
                        json!({"chat_id":route.chat,"message_id":id.parse::<i64>()?,"text":text}),
                    )
                } else {
                    (
                        "sendMessage",
                        platform::telegram_payload(route, text, card)?,
                    )
                };
                let url = format!("https://api.telegram.org/bot{}/{method}", t.token);
                let raw = self
                    .client
                    .post(self.platform_url(Url::parse(&url)?))
                    .json(&body)
                    .send()
                    .await?;
                let http_error = raw.error_for_status_ref().err();
                let response: Value = raw.json().await?;
                if response["ok"] != true {
                    if edit.is_some()
                        && response["description"]
                            .as_str()
                            .is_some_and(|s| s.contains("message is not modified"))
                    {
                        return Ok(edit.map(str::to_owned));
                    }
                    if let Some(err) = http_error {
                        return Err(err.into());
                    }
                    bail!("Telegram declined operation")
                }
                Ok(response["result"]["message_id"]
                    .as_i64()
                    .map(|v| v.to_string()))
            }
            "feishu" => {
                let mut body = platform::feishu_payload(route, text, card);
                body["uuid"] = json!(format!("{}-{n}", &self.instance));
                let mut url = Url::parse("https://open.feishu.cn/open-apis/im/v1/messages")?;
                let method = if let Some(mid) = edit {
                    url.path_segments_mut().unwrap().push(mid);
                    body = json!({"msg_type":"text","content":json!({"text":text}).to_string()});
                    Method::PUT
                } else if let Some(id) = &route.reply_to {
                    url.path_segments_mut().unwrap().push(id).push("reply");
                    Method::POST
                } else {
                    url.query_pairs_mut()
                        .append_pair("receive_id_type", "chat_id");
                    Method::POST
                };
                let res = self
                    .client
                    .request(method, self.platform_url(url))
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
                Ok(res["data"]["message_id"]
                    .as_str()
                    .or(edit)
                    .map(str::to_owned))
            }
            "slack" => {
                let f = self.cfg.slack.as_ref().context("Slack disabled")?;
                let mut body = crate::workchat::slack_payload(route, text, card);
                let endpoint = if let Some(ts) = edit {
                    body = json!({"channel":route.chat,"ts":ts,"text":text,"mrkdwn":false,"parse":"none"});
                    "https://slack.com/api/chat.update"
                } else {
                    "https://slack.com/api/chat.postMessage"
                };
                let r: Value = self
                    .client
                    .post(self.platform_url(Url::parse(endpoint)?))
                    .bearer_auth(&f.bot)
                    .json(&body)
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                if r["ok"] != true {
                    bail!("Slack declined operation")
                }
                Ok(r["ts"].as_str().or(edit).map(str::to_owned))
            }
            "discord" => {
                let f = self.cfg.discord.as_ref().context("Discord disabled")?;
                let mut u = Url::parse("https://discord.com/api/v10/channels")?;
                u.path_segments_mut()
                    .unwrap()
                    .push(&route.chat)
                    .push("messages");
                let method = if let Some(id) = edit {
                    u.path_segments_mut().unwrap().push(id);
                    Method::PATCH
                } else {
                    Method::POST
                };
                let result: Value = self
                    .client
                    .request(method, self.platform_url(u))
                    .header("Authorization", format!("Bot {}", f.bot))
                    .json(&crate::workchat::discord_payload(text, card))
                    .send()
                    .await?
                    .error_for_status()?
                    .json()
                    .await?;
                Ok(result["id"].as_str().or(edit).map(str::to_owned))
            }
            _ => bail!("unknown platform"),
        }
    }
}
struct Item {
    n: i64,
    route: Route,
    rid: String,
    conv: String,
    job: String,
    tries: i64,
    lease: String,
}
const LEASE_SECONDS: i64 = 12;
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
        let table = self.table();
        let pending = match self {
            Self::In => {
                "AND i.done=0 AND NOT EXISTS (
                SELECT 1 FROM routes earlier_route JOIN inbox p ON p.route=earlier_route.id
                WHERE earlier_route.conv=r.conv AND p.done=0 AND p.n<i.n)"
            }
            Self::Out => {
                "AND NOT EXISTS(SELECT 1 FROM outgoing p WHERE p.route=i.route AND p.n<i.n)"
            }
        };
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
            let lease = uuid::Uuid::new_v4().to_string();
            tx.execute(
                &format!("UPDATE {table} SET retry=?, lease=? WHERE n=?"),
                params![store::now() + LEASE_SECONDS, lease, n],
            )?;
            tx.commit()?;
            Ok(Some(Item {
                n,
                route: serde_json::from_str(&dest)?,
                rid,
                conv,
                job,
                tries,
                lease,
            }))
        } else {
            Ok(None)
        }
    }
    async fn perform(self, g: &Gateway, i: &Item) -> Result<Option<String>> {
        let mut callback = None;
        let result: Result<()> = match self {
            Self::In => match serde_json::from_str(&i.job)? {
                Job::Message { source, text } => {
                    crate::drafts::post(g, &i.rid, &i.route, &i.conv, &source, &text).await
                }
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
                crate::drafts::send(g, &i.rid, &i.route, i.n, serde_json::from_str(&i.job)?).await
            }
        };
        result?;
        Ok(callback)
    }

    async fn one(self, g: &Gateway) -> Result<bool> {
        let Some(i) = self.claim(g)? else {
            return Ok(false);
        };
        let table = self.table();
        let pending = self.perform(g, &i);
        tokio::pin!(pending);
        let result = loop {
            tokio::select! {
                biased;
                outcome = &mut pending => break outcome,
                _ = sleep(Duration::from_secs(4)) => {
                    let renewed = g.db()?.execute(&format!("UPDATE {table} SET retry=? WHERE n=? AND lease=?"),
                        params![store::now() + LEASE_SECONDS, i.n, i.lease])?;
                    if renewed == 0 { return Ok(true); }
                }
            }
        };
        let mut db = g.db()?;
        let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if tx.execute(
            &format!("UPDATE {table} SET lease=NULL WHERE n=? AND lease=?"),
            params![i.n, i.lease],
        )? != 1
        {
            return Ok(true);
        }
        if let Err(ref e) = result {
            let status = e.downcast_ref::<reqwest::Error>().and_then(|e| e.status());
            let final_reply = matches!(self, Self::Out)
                && matches!(
                    serde_json::from_str::<Delivery>(&i.job),
                    Ok(Delivery::Final { .. })
                );
            let terminal = !final_reply
                && status.is_some_and(|s| {
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
                if let Job::Message { source, .. } = serde_json::from_str(&i.job)? {
                    crate::drafts::abort(
                        &tx,
                        &i.rid,
                        &i.route.platform,
                        &source,
                        "Tarla could not accept this request.",
                    )?;
                } else {
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
            }
        }
        match self {
            Self::In => {
                tx.execute("UPDATE inbox SET done=1,job='' WHERE n=?", [i.n])?;
                if let Some(id) = result.ok().flatten() {
                    tx.execute(
                        "INSERT INTO outgoing(route,job) VALUES(?,?)",
                        params![i.rid, serde_json::to_string(&Delivery::Callback(id))?],
                    )?;
                }
            }
            Self::Out => {
                crate::drafts::ack(&tx, &i.rid, &i.job)?;
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
        let _ = g
            .queue_history_page(&b.id, &b.route.platform, &b.conversation_id)
            .await;
        if !b.approvals {
            continue;
        }
        for a in &approvals {
            let terminal = a["kind"] == "terminal_input";
            if (!terminal && a["kind"] != "approval")
                || a["resolution"].as_str().is_some_and(|r| r != "pending")
            {
                continue;
            }
            let Some(action) = a["id"].as_str() else {
                continue;
            };
            let run = if terminal || a["conversation_id"].as_str().is_none() {
                let Some(id) = a["run_id"].as_str() else {
                    continue;
                };
                let Ok(run) = g.core(Method::GET, &["runs", id], &[], None).await else {
                    continue;
                };
                Some(run)
            } else {
                None
            };
            let conv = run
                .as_ref()
                .and_then(|r| r["conversation_id"].as_str())
                .or(a["conversation_id"].as_str());
            if conv != Some(b.conversation_id.as_str()) {
                continue;
            }
            if terminal {
                let Some(run) = run else {
                    continue;
                };
                let (Some(workspace), Some(id)) =
                    (run["workspace_id"].as_str(), run["run_id"].as_str())
                else {
                    continue;
                };
                let title: String = a["title"]
                    .as_str()
                    .unwrap_or("Terminal input needed")
                    .chars()
                    .take(400)
                    .collect();
                let detail: String = a["risk_reason"]
                    .as_str()
                    .unwrap_or("")
                    .chars()
                    .take(600)
                    .collect();
                let modes = run["terminal_modes"].as_array();
                if modes.is_some_and(|m| m.is_empty()) {
                    continue;
                }
                let interactive = modes.is_none_or(|m| m.iter().any(|v| v == "interactive"));
                let (hint, option) = if interactive {
                    (
                        "Open this run on a paired CLI to respond.",
                        " --interactive",
                    )
                } else {
                    (
                        run["terminal_input_hint"].as_str().unwrap_or(
                            "Reply in this conversation. The CLI can show the terminal.",
                        ),
                        "",
                    )
                };
                let text = format!("{title}\n{detail}\n{hint}\ntarla --workspace {workspace} attach {id}{option} --via-supervisor");
                store::queue_terminal_notice(&mut *g.db()?, &b, action, text)?;
            } else {
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
    for work in std::iter::repeat_n(Work::In, 4).chain(std::iter::repeat_n(Work::Out, 4)) {
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

    fn gateway(core: Url, db: rusqlite::Connection) -> Gateway {
        Gateway {
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
            test_platform_origin: None,
        }
    }

    #[tokio::test]
    async fn terminal_notice_is_scoped_and_never_issues_a_decision_token() {
        use axum::routing::get;
        let app = Router::new()
            .route("/v1/actions", get(|| async { Json(json!([
                {"id":"osc","kind":"terminal_input","run_id":"run17","title":"Choice required","risk_reason":"Choose a path"},
                {"id":"chat","kind":"terminal_input","run_id":"run18","title":"Follow up in chat"},
                {"id":"tool","kind":"approval","conversation_id":"main","title":"Review tool"}
            ])) }))
            .route("/v1/runs/run17", get(|| async { Json(json!({"run_id":"run17","workspace_id":"work","conversation_id":"main"})) }))
            .route("/v1/runs/run18", get(|| async { Json(json!({"run_id":"run18","workspace_id":"work","conversation_id":"main","terminal_modes":["observe"],"terminal_input_hint":"Send the next instruction in chat."})) }))
            .route("/v1/conversations/{id}/messages", get(|| async { Json(json!([])) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let core = format!("http://{}", listener.local_addr().unwrap())
            .parse()
            .unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let g = gateway(core, store::open(Path::new(":memory:")).unwrap());
        for (thread, conv, approvals) in [
            ("on", "main", true),
            ("off", "main", false),
            ("other", "other", true),
        ] {
            let mut db = g.db().unwrap();
            let code = store::issue(&db, "telegram", "actor", conv, approvals, 0).unwrap();
            store::ingest(
                &mut db,
                Event {
                    event_id: thread.into(),
                    route: Route {
                        platform: "telegram".into(),
                        chat: "chat".into(),
                        thread: thread.into(),
                        actor: "actor".into(),
                        reply_to: None,
                    },
                    input: Input::Message(format!("/pair {code}")),
                },
            )
            .unwrap();
        }
        for _ in 0..2 {
            poll(&g).await.unwrap();
        }
        let db = g.db().unwrap();
        let notices: (i64, String, String) = db.query_row(
            "SELECT count(*),r.thread,json_extract(o.job,'$.Text') FROM outgoing o JOIN routes r ON o.route=r.id WHERE o.job LIKE '%--interactive%'", [],
            |r| Ok((r.get(0)?, r.get(1)?,r.get(2)?))).unwrap();
        assert_eq!((notices.0, notices.1.as_str()), (1, "on"));
        let observe: String = db
            .query_row(
                "SELECT json_extract(job,'$.Text') FROM outgoing WHERE job LIKE '%run18%'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            observe.contains("Send the next instruction in chat.")
                && !observe.contains("--interactive")
        );
        assert!(
            notices.2.contains("Choose a path")
                && notices
                    .2
                    .contains("tarla --workspace work attach run17 --interactive")
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM cards WHERE action='osc'", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
            0
        );
        assert_eq!(
            db.query_row("SELECT count(*) FROM cards WHERE action='tool'", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap(),
            1
        );
        server.abort();
    }

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
        let g = gateway(core, db);
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
