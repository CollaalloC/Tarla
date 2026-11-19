//! Workspace-scoped channel ingress, durable relay and approval delivery.
mod config;
mod drafts;
mod platform;
mod store;
mod workchat;
mod worker;
use anyhow::{Context, Result};
use axum::{
    body::Bytes,
    extract::{DefaultBodyLimit, Path, State},
    http::{HeaderMap, StatusCode},
    routing::{delete, post},
    Json, Router,
};
pub use config::GatewayConfig;
use reqwest::Method;
use rusqlite::{Connection, OptionalExtension};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};

struct Gateway {
    cfg: GatewayConfig,
    connection: Mutex<Connection>,
    client: reqwest::Client,
    feishu_token: tokio::sync::Mutex<Option<(i64, String)>>,
    instance: String,
    #[cfg(test)]
    test_platform_origin: Option<reqwest::Url>,
}
impl Gateway {
    fn db(&self) -> Result<MutexGuard<'_, Connection>> {
        self.connection
            .lock()
            .map_err(|_| anyhow::anyhow!("gateway database unavailable"))
    }
    fn owner(&self, headers: &HeaderMap) -> WebResult<()> {
        let supplied = headers
            .get("authorization")
            .and_then(|s| s.to_str().ok())
            .and_then(|s| s.strip_prefix("Bearer "));
        if supplied.is_some_and(|t| platform::matches_secret(t, &self.cfg.token)) {
            Ok(())
        } else {
            Err(StatusCode::UNAUTHORIZED)
        }
    }
}
pub async fn router(cfg: GatewayConfig) -> Result<Router> {
    let conn = store::open(&cfg.database)?;
    conn.execute_batch("CREATE TABLE IF NOT EXISTS identity (singleton INTEGER PRIMARY KEY CHECK(singleton=1), id TEXT NOT NULL)")?;
    conn.execute(
        "INSERT OR IGNORE INTO identity VALUES(1,?)",
        [uuid::Uuid::new_v4().simple().to_string()],
    )?;
    let instance = conn
        .query_row("SELECT id FROM identity WHERE singleton=1", [], |r| {
            r.get(0)
        })
        .optional()?
        .context("gateway identity missing")?;
    let g = Arc::new(Gateway {
        cfg,
        connection: Mutex::new(conn),
        client: reqwest::Client::builder()
            .timeout(Duration::from_secs(90))
            .redirect(reqwest::redirect::Policy::none())
            .build()?,
        feishu_token: tokio::sync::Mutex::new(None),
        instance,
        #[cfg(test)]
        test_platform_origin: None,
    });
    worker::start(&g);
    Ok(Router::new()
        .route("/pairings", post(pair).get(list))
        .route("/pairings/{id}", delete(remove))
        .route("/telegram", post(telegram))
        .route("/feishu", post(feishu))
        .route("/slack", post(workchat::slack))
        .route("/discord", post(workchat::discord))
        .layer(DefaultBodyLimit::max(512 * 1024))
        .with_state(g))
}
type WebResult<T> = std::result::Result<T, StatusCode>;
type Data = State<Arc<Gateway>>;
#[derive(Deserialize)]
struct Pair {
    platform: String,
    actor_id: String,
    conversation_id: String,
    #[serde(default)]
    approvals: bool,
}
async fn pair(State(g): Data, headers: HeaderMap, Json(p): Json<Pair>) -> WebResult<Json<Value>> {
    g.owner(&headers)?;
    if !match p.platform.as_str() {
        "telegram" => g.cfg.telegram.is_some(),
        "feishu" => g.cfg.feishu.is_some(),
        "slack" => g.cfg.slack.is_some(),
        "discord" => g.cfg.discord.is_some(),
        _ => false,
    } || p.actor_id.is_empty()
        || p.conversation_id.is_empty()
    {
        return Err(StatusCode::BAD_REQUEST);
    }
    let latest = g
        .core(
            Method::GET,
            &["conversations", &p.conversation_id, "messages"],
            &[("latest", "true".into()), ("limit", "1".into())],
            None,
        )
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let messages = latest.as_array().ok_or(StatusCode::BAD_GATEWAY)?;
    let cursor = messages
        .iter()
        .filter_map(|m| m["seq_no"].as_i64())
        .max()
        .unwrap_or(0);
    let code = store::issue(
        &*g.db().map_err(internal)?,
        &p.platform,
        &p.actor_id,
        &p.conversation_id,
        p.approvals,
        cursor,
    )
    .map_err(internal)?;
    Ok(Json(json!({"code":code,"expires_in":300})))
}
fn internal(_: anyhow::Error) -> StatusCode {
    StatusCode::INTERNAL_SERVER_ERROR
}
async fn list(State(g): Data, h: HeaderMap) -> WebResult<Json<Value>> {
    g.owner(&h)?;
    let bindings = store::bindings(&*g.db().map_err(internal)?).map_err(internal)?;
    Ok(Json(json!(bindings)))
}
async fn remove(State(g): Data, h: HeaderMap, Path(id): Path<String>) -> WebResult<StatusCode> {
    g.owner(&h)?;
    let changed = g
        .db()
        .map_err(internal)?
        .execute("DELETE FROM routes WHERE id=?", [id])
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(if changed == 1 {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    })
}
async fn telegram(State(g): Data, h: HeaderMap, body: Bytes) -> WebResult<Json<Value>> {
    let tg = g.cfg.telegram.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    let secret = h
        .get("x-telegram-bot-api-secret-token")
        .and_then(|s| s.to_str().ok());
    if !secret.is_some_and(|s| platform::matches_secret(s, &tg.secret)) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    let parsed = serde_json::from_slice(&body).map_err(|_| StatusCode::BAD_REQUEST)?;
    if let Some(e) = platform::telegram(parsed).map_err(|_| StatusCode::BAD_REQUEST)? {
        store::ingest(&mut *g.db().map_err(internal)?, e).map_err(internal)?
    }
    Ok(Json(json!({"ok":true})))
}
async fn feishu(State(g): Data, headers: HeaderMap, body: Bytes) -> WebResult<Json<Value>> {
    let cfg = g.cfg.feishu.as_ref().ok_or(StatusCode::NOT_FOUND)?;
    let p = platform::verified_feishu(cfg, &headers, &body, store::now())
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    if p["type"] == "url_verification" {
        return Ok(Json(
            json!({"challenge":p["challenge"].as_str().ok_or(StatusCode::BAD_REQUEST)?}),
        ));
    }
    if let Some(e) = platform::feishu(&p).map_err(|_| StatusCode::BAD_REQUEST)? {
        store::ingest(&mut *g.db().map_err(internal)?, e).map_err(internal)?
    }
    Ok(Json(json!({})))
}
