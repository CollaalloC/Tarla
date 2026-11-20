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

#[derive(serde::Deserialize)]
struct PeerGrant<'a> {
    ticket: &'a str,
    run_id: &'a str,
    node_id: &'a str,
    hc_terminal_id: Option<&'a str>,
    mode: AttachMode,
    expires_in: u64,
}

fn validate_peer_grant(
    value: &Value,
    workspace: &str,
    node: &str,
    id: &str,
    terminal: &str,
    mode: AttachMode,
) -> Api<()> {
    use serde::Deserialize;
    let malformed = || {
        ApiError(
            StatusCode::BAD_GATEWAY,
            "executor returned a malformed terminal ticket".into(),
        )
    };
    let fields = value.as_object().ok_or_else(malformed)?;
    let grant = PeerGrant::deserialize(value).map_err(|_| malformed())?;
    if !(1..=60).contains(&grant.expires_in)
        || (fields.contains_key("hc_terminal_id") && grant.hc_terminal_id.is_none())
    {
        return Err(malformed());
    }
    let same_scope =
        AttachLease::peer_ticket_has_scope(grant.ticket, workspace, node, id, terminal, mode)
            .map_err(|_| malformed())?;
    if !same_scope
        || grant.run_id != id
        || grant.node_id != node
        || grant.mode != mode
        || grant
            .hc_terminal_id
            .is_some_and(|address| address != terminal)
    {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "executor terminal changed; request a new terminal grant".into(),
        ));
    }
    // Previous executors did not confirm input support or repeat the address outside the ticket.
    if mode == AttachMode::Interactive && grant.hc_terminal_id.is_none() {
        return Err(ApiError(StatusCode::CONFLICT, UNKNOWN.into()));
    }
    Ok(())
}

pub(super) async fn peer_ticket(
    s: &Service,
    node: &str,
    id: &str,
    terminal: &str,
    mode: AttachMode,
) -> Api<Value> {
    let grant = peers::call(s, node, "ticket", &json!({"run_id":id,"mode":mode}))
        .await
        .map_err(|error| {
            if error
                .downcast_ref::<reqwest::Error>()
                .and_then(reqwest::Error::status)
                == Some(StatusCode::CONFLICT)
            {
                ApiError(
                    StatusCode::CONFLICT,
                    "the executor no longer offers the requested terminal mode".into(),
                )
            } else {
                ApiError(
                    StatusCode::BAD_GATEWAY,
                    "could not get a terminal ticket from the executor".into(),
                )
            }
        })?;
    validate_peer_grant(&grant, s.id(), node, id, terminal, mode)?;
    Ok(grant)
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
    // A peer may use HTTPS inside the private network. Only client_url
    // explicitly advertises an executor address reachable by clients.
    let public = peer.client_url.as_deref();
    if route == "supervisor" || (route == "auto" && public.is_none()) {
        return Ok(Json(terminal_relay::issue(&s, &r, &id, mode)?));
    }
    let mut remote = peer_ticket(&s, target, &id, string(&r, "hc_terminal_id")?, mode).await?;
    remote["route"] = json!("direct");
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
    if lease.run_id != id {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "terminal scope mismatch".into(),
        ));
    }
    let mut run = api::one(&s, &id)?;
    if run["hc_terminal_id"] != lease.hc_terminal_id || run["target_node_id"] != s.node() {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "terminal scope mismatch".into(),
        ));
    }
    describe(&s, &mut run).await?;
    check_mode(&run, lease.mode)?;
    consume(&mut s.db()?, &lease)?;
    let observe = lease.mode == AttachMode::Observe;
    let (mut reader, mut writer) = fleet::herdr(&s)?
        .attach(&lease.run_id, &lease.hc_terminal_id, observe, 80, 24)
        .await
        .map_err(|e| ApiError(StatusCode::BAD_GATEWAY, e.to_string()))?;
    Ok(upgrade.max_message_size(tarla_sidecar::MAX_TERMINAL_INPUT_BYTES).on_upgrade(move|socket|async move{
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

pub(super) fn consume(c: &mut Connection, lease: &AttachLease) -> Api<()> {
    let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    // A competing redemption can hold the write lock until this ticket expires.
    let at = now();
    if !lease.valid_at(at) {
        return Err(ApiError(
            StatusCode::UNAUTHORIZED,
            "expired terminal ticket".into(),
        ));
    }
    tx.execute("DELETE FROM service_leases WHERE expires<=?", [at])?;
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ticket(terminal: &str, mode: AttachMode, at: i64) -> String {
        AttachLease::issue(&[1; 32], "work", "worker", "run", terminal, mode, at).unwrap()
    }

    #[test]
    fn redemption_rechecks_the_lifetime_and_consumes_once() {
        let mut db = Connection::open_in_memory().unwrap();
        db.execute_batch(
            "CREATE TABLE service_leases(nonce TEXT PRIMARY KEY, expires INTEGER NOT NULL)",
        )
        .unwrap();
        let at = now();
        let verified_at_issue = |issued_at| {
            AttachLease::verify(
                &ticket("t1", AttachMode::Observe, issued_at),
                &[1; 32],
                "work",
                "worker",
                issued_at,
            )
            .unwrap()
        };
        let expired = verified_at_issue(at - 61);
        assert_eq!(
            consume(&mut db, &expired).err().unwrap().0,
            StatusCode::UNAUTHORIZED
        );
        let valid = verified_at_issue(at);
        assert!(consume(&mut db, &valid).is_ok());
        assert_eq!(
            consume(&mut db, &valid).err().unwrap().0,
            StatusCode::UNAUTHORIZED
        );
        let rows: Vec<String> = db
            .prepare("SELECT nonce FROM service_leases")
            .unwrap()
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        assert_eq!(rows, [valid.nonce]);
    }

    #[test]
    fn peer_reply_and_ticket_must_agree_with_requested_terminal() {
        let mode = AttachMode::Observe;
        let modern = json!({"ticket":ticket("t1",mode,now()), "run_id":"run", "node_id":"worker",
                            "mode":"observe", "hc_terminal_id":"t1", "expires_in":60});
        let check = |value: &Value| {
            validate_peer_grant(value, "work", "worker", "run", "t1", mode)
                .err()
                .map(|e| e.0)
        };
        assert_eq!(check(&modern), None);
        let mut legacy = modern.clone();
        legacy.as_object_mut().unwrap().remove("hc_terminal_id");
        assert_eq!(check(&legacy), None);
        let positional = json!([modern["ticket"], "run", "worker", "t1", "observe", 60]);
        assert_eq!(check(&positional), Some(StatusCode::BAD_GATEWAY));
        for (key, value, status) in [
            ("mode", json!("interactive"), StatusCode::CONFLICT),
            ("run_id", json!("elsewhere"), StatusCode::CONFLICT),
            ("node_id", json!("elsewhere"), StatusCode::CONFLICT),
            ("hc_terminal_id", json!("t2"), StatusCode::CONFLICT),
            (
                "ticket",
                json!(ticket("t2", mode, now())),
                StatusCode::CONFLICT,
            ),
            ("ticket", json!("incomplete"), StatusCode::BAD_GATEWAY),
            ("expires_in", json!(0), StatusCode::BAD_GATEWAY),
        ] {
            let mut changed = modern.clone();
            changed[key] = value;
            assert_eq!(check(&changed), Some(status), "{key}");
        }
    }
}
