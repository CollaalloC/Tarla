use super::*;
use axum::{
    extract::{
        ws::{Message, WebSocketUpgrade},
        Path as Route, State,
    },
    http::HeaderMap,
};
use futures_util::{SinkExt, StreamExt};
use rusqlite::params;
use tarla_protocol::{AttachLease, AttachMode};
use tarla_sidecar::TerminalEvent;
pub fn local_ticket(s: &Service, id: &str, v: &Value) -> Result<Value> {
    let run = api::one(s, id)?;
    if run["target_node_id"] != s.node() {
        bail!("terminal belongs to another node")
    }
    let term = string(&run, "hc_terminal_id")?;
    let mode = serde_json::from_value::<AttachMode>(v["mode"].clone())?;
    Ok(
        json!({"ticket":AttachLease::issue(s.ws.attach_key.as_bytes(),s.id(),s.node(),id,term,mode,now())?,"mode":mode,"run_id":id,"node_id":s.node(),"expires_in":60}),
    )
}
pub async fn issue(State(s): State<S>, Route(id): Route<String>, Json(v): Json<Value>) -> Api {
    let r = api::one(&s, &id)?;
    let target = string(&r, "target_node_id")?;
    if target == s.node() {
        return Ok(Json(local_ticket(&s, &id, &v)?));
    }
    ensure_supervisor(&s)?;
    let mut remote =
        peers::call(&s, target, "ticket", &json!({"run_id":id,"mode":v["mode"]})).await?;
    remote["node_url"] = json!(s.peers[target].url);
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
    let lease = AttachLease::verify(ticket, s.ws.attach_key.as_bytes(), s.id(), s.node(), now())
        .map_err(|_| ApiError(StatusCode::UNAUTHORIZED, "invalid terminal lease".into()))?;
    let run = api::one(&s, &id)?;
    if lease.run_id != id
        || run["hc_terminal_id"] != lease.hc_terminal_id
        || run["target_node_id"] != s.node()
    {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "terminal scope mismatch".into(),
        ));
    }
    {
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
    }
    let observe = lease.mode == AttachMode::Observe;
    let (mut reader, mut writer) = fleet::herdr(&s)?
        .attach(&lease.hc_terminal_id, observe, 80, 24)
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
