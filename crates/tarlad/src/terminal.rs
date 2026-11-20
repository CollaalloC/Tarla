use super::*;
use axum::{
    extract::{
        ws::{Message, WebSocketUpgrade},
        Path as Route, State,
    },
    http::HeaderMap,
};
use futures_util::{SinkExt, StreamExt};
use rusqlite::{params, OptionalExtension};
use tarla_protocol::{AttachLease, AttachMode};
use tarla_sidecar::TerminalEvent;
const NEXT_TURN: &str = "This structured agent turn reads instructions from chat. Send the next instruction in this run’s conversation.";
const FINISHED: &str = "This run has finished. Its terminal is available for observation.";
const UNKNOWN: &str =
    "The executor has not confirmed input support. Send a message in this run’s conversation.";

pub fn owner_input(s: &Service, id: &str) -> Result<bool> {
    let saved = s
        .db()?
        .query_row(
            "SELECT interactive FROM service_terminal_input WHERE run_id=?",
            [id],
            |r| r.get::<_, bool>(0),
        )
        .optional()?;
    match saved {
        Some(interactive) => Ok(interactive),
        None => Ok(!handoff::structured_turn(&s.ws.root, id)?),
    }
}

pub fn remember_input(db: &Connection, id: &str, interactive: bool) -> Result<()> {
    db.execute("INSERT INTO service_terminal_input VALUES(?,?) ON CONFLICT(run_id) DO UPDATE SET interactive=excluded.interactive",
        params![id, interactive])?;
    Ok(())
}

pub fn local_input_info(s: &Service, id: &str) -> Result<Value> {
    let run = api::one(s, id)?;
    anyhow::ensure!(
        run["target_node_id"] == s.node(),
        "terminal belongs to another executor"
    );
    Ok(json!({"interactive":owner_input(s,id)?}))
}

pub async fn describe(s: &Service, run: &mut Value) -> Result<()> {
    let id = string(run, "run_id")?;
    let owner = string(run, "target_node_id")?;
    if !run["hc_terminal_id"].is_string() {
        run["terminal_modes"] = json!([]);
        run["terminal_input_hint"] = Value::Null;
        return Ok(());
    }
    let mut interactive = if owner == s.node() {
        Some(owner_input(s, id)?)
    } else {
        s.db()?
            .query_row(
                "SELECT interactive FROM service_terminal_input WHERE run_id=?",
                [id],
                |r| r.get::<_, bool>(0),
            )
            .optional()?
    };
    // Pre-upgrade active runs may already have emitted their only running event.
    // Ask the owning executor once, retaining a usable read-only view offline.
    if interactive.is_none() && s.is_supervisor() {
        if let Ok(Ok(reply)) = tokio::time::timeout(
            Duration::from_secs(1),
            peers::call(s, owner, "terminal_input", &json!({"run_id":id})),
        )
        .await
        {
            if let Some(value) = reply["interactive"].as_bool() {
                remember_input(&s.db()?, id, value)?;
                interactive = Some(value);
            }
        }
    }
    let ended = matches!(
        run["observed_state"].as_str(),
        Some("succeeded" | "failed" | "cancelled")
    );
    let hint = match (interactive, ended) {
        (Some(false), _) => Some(NEXT_TURN),
        (_, true) => Some(FINISHED),
        (None, _) => Some(UNKNOWN),
        _ => None,
    };
    run["terminal_modes"] = if hint.is_some() {
        json!(["observe"])
    } else {
        json!(["observe", "interactive"])
    };
    run["terminal_input_hint"] = json!(hint);
    Ok(())
}

fn check_mode(run: &Value, mode: AttachMode) -> Api<()> {
    if !run["hc_terminal_id"].is_string() {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "This run does not yet have a terminal.".into(),
        ));
    }
    if mode == AttachMode::Interactive {
        if let Some(hint) = run["terminal_input_hint"].as_str() {
            return Err(ApiError(StatusCode::CONFLICT, hint.into()));
        }
    }
    Ok(())
}

pub async fn local_ticket(s: &Service, id: &str, v: &Value) -> Api<Value> {
    let mut run = api::one(s, id)?;
    if run["target_node_id"] != s.node() {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "terminal belongs to another node".into(),
        ));
    }
    let mode =
        serde_json::from_value::<AttachMode>(v["mode"].clone()).map_err(anyhow::Error::from)?;
    describe(s, &mut run).await?;
    check_mode(&run, mode)?;
    let term = string(&run, "hc_terminal_id")?;
    Ok(
        json!({"ticket":AttachLease::issue(s.ws.attach_key.as_bytes(),s.id(),s.node(),id,term,mode,now())?,"mode":mode,"route":"direct","run_id":id,"node_id":s.node(),"hc_terminal_id":term,"expires_in":60}),
    )
}
pub async fn issue(State(s): State<S>, Route(id): Route<String>, Json(v): Json<Value>) -> Api {
    let route = v["route"].as_str().unwrap_or("direct");
    if !matches!(route, "auto" | "direct" | "supervisor") {
        return Err(anyhow!("unknown terminal route").into());
    }
    let mut r = api::one(&s, &id)?;
    if r["target_node_id"] == s.node() {
        return Ok(Json(local_ticket(&s, &id, &v).await?));
    }
    ensure_supervisor(&s)?;
    let mode = serde_json::from_value(v["mode"].clone()).map_err(anyhow::Error::from)?;
    describe(&s, &mut r).await?;
    check_mode(&r, mode)?;
    let target = string(&r, "target_node_id")?;
    let peer = s.peers.get(target).context("executor is not paired")?;
    let public = peer.client_url.as_deref().or_else(|| {
        peer.url
            .starts_with("https://")
            .then_some(peer.url.as_str())
    });
    if route == "supervisor" || (route == "auto" && public.is_none()) {
        return Ok(Json(terminal_relay::issue(&s, &r, &id, mode)?));
    }
    let mut remote = peers::call(&s, target, "ticket", &json!({"run_id":id,"mode":mode})).await?;
    remote["node_url"] = json!(public.unwrap_or(&peer.url));
    Ok(Json(remote))
}
pub async fn socket(
    State(s): State<S>,
    Route(id): Route<String>,
    h: HeaderMap,
    upgrade: WebSocketUpgrade,
) -> Api<Response> {
    let ticket = h
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    if let Some(raw) = ticket.strip_prefix("relay.") {
        return terminal_relay::connect(&s, &id, raw, upgrade).await;
    }
    let lease = AttachLease::verify(ticket, s.ws.attach_key.as_bytes(), s.id(), s.node(), now())
        .map_err(|_| ApiError(StatusCode::UNAUTHORIZED, "invalid terminal lease".into()))?;
    let mut run = api::one(&s, &id)?;
    if lease.run_id != id
        || run["hc_terminal_id"] != lease.hc_terminal_id
        || run["target_node_id"] != s.node()
    {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "terminal scope mismatch".into(),
        ));
    }
    describe(&s, &mut run).await?;
    check_mode(&run, lease.mode)?;
    consume(&s, &lease)?;
    let observe = lease.mode == AttachMode::Observe;
    let (mut reader, mut writer) = fleet::herdr(&s)?
        .attach(&lease.run_id, &lease.hc_terminal_id, observe, 80, 24)
        .await
        .map_err(|e| ApiError(StatusCode::BAD_GATEWAY, e.to_string()))?;
    Ok(upgrade.max_message_size(65536).on_upgrade(move|socket|async move{
        let(mut sink,mut source)=socket.split();let(tx,mut rx)=tokio::sync::mpsc::channel(4);
        let pump=tokio::spawn(async move{while let Ok(event)=reader.next().await{let closed=matches!(&event,TerminalEvent::Closed(_));if tx.send(event).await.is_err()||closed{break}}});
        loop{tokio::select!{
            event=rx.recv()=>match event{
                Some(TerminalEvent::Output(bytes))=>{let len=bytes.len();if sink.send(Message::Binary(bytes.into())).await.is_err()||writer.credit(len).await.is_err(){break}},
                Some(TerminalEvent::Resize(cols,rows))=>if sink.send(Message::Text(json!({"cols":cols,"rows":rows}).to_string().into())).await.is_err(){break},
                _=>break
            },
            inbound=source.next()=>match inbound{
                Some(Ok(Message::Binary(bytes))) if !observe=>if writer.input(&bytes).await.is_err(){break},
                Some(Ok(Message::Text(txt))) if !observe=>{
                    #[derive(serde::Deserialize)]struct Size{cols:u16,rows:u16}
                    let Ok(size)=serde_json::from_str::<Size>(&txt)else{break};if writer.resize(size.cols,size.rows).await.is_err(){break}
                }
                Some(Ok(Message::Ping(x)))=>if sink.send(Message::Pong(x)).await.is_err(){break},
                Some(Ok(Message::Pong(_)))=>{},
                _=>break
            }
        }}
        pump.abort();let _=writer.close().await;let _=sink.close().await;
    }))
}

pub fn consume(s: &Service, lease: &AttachLease) -> Api<()> {
    let mut c = s.db()?;
    let tx = c.transaction()?;
    tx.execute("DELETE FROM service_leases WHERE expires<=?", [now()])?;
    if tx.execute(
        "INSERT OR IGNORE INTO service_leases VALUES(?,?)",
        params![lease.nonce, lease.expires_at],
    )? != 1
    {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "ticket already used".into(),
        ));
    }
    tx.commit()?;

    Ok(())
}
