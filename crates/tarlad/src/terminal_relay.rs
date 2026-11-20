//! A fallback for a paired executor reachable only over the supervisor's private
//! route. The endpoint and terminal always come from the local run ledger.
use super::*;
use axum::{
    extract::ws::{Message, WebSocketUpgrade},
    response::Response,
};
use futures_util::{SinkExt, StreamExt};
use tarla_protocol::{AttachLease, AttachMode};
use tokio_tungstenite::{
    tungstenite::{
        handshake::{client::generate_key, derive_accept_key},
        protocol::{Role, WebSocketConfig},
        Message as Remote,
    },
    WebSocketStream,
};

fn signing_key(s: &Service) -> [u8; 32] {
    let mut digest = Sha256::new();
    digest.update(b"tarla/terminal-relay/v1\0");
    digest.update(s.ws.attach_key.as_bytes());
    digest.finalize().into()
}

pub fn issue(s: &Service, run: &Value, id: &str, mode: AttachMode) -> Result<Value> {
    let node = string(run, "target_node_id")?;
    let terminal = string(run, "hc_terminal_id")?;
    let lease = AttachLease::issue(&signing_key(s), s.id(), node, id, terminal, mode, now())?;
    Ok(
        json!({"run_id":id,"node_id":node,"mode":mode,"route":"supervisor","expires_in":60,"ticket":format!("relay.{lease}")}),
    )
}

pub async fn connect(
    s: &Service,
    id: &str,
    ticket: &str,
    upgrade: WebSocketUpgrade,
) -> Api<Response> {
    ensure_supervisor(s)?;
    let run = api::one(s, id)?;
    let owner = string(&run, "target_node_id")?;
    let peer = s.peers.get(owner).context("executor is not paired")?;
    let grant =
        AttachLease::verify(ticket, &signing_key(s), s.id(), owner, now()).map_err(|_| {
            ApiError(
                StatusCode::UNAUTHORIZED,
                "invalid or expired relay lease".into(),
            )
        })?;
    if grant.run_id != id || run["hc_terminal_id"] != grant.hc_terminal_id || owner == s.node() {
        return Err(ApiError(
            StatusCode::FORBIDDEN,
            "terminal relay scope mismatch".into(),
        ));
    }
    terminal::consume(s, &grant)?;
    let remote = peers::call(s, owner, "ticket", &json!({"run_id":id,"mode":grant.mode})).await?;
    if remote["run_id"] != grant.run_id
        || remote["node_id"] != grant.node_id
        || remote["hc_terminal_id"] != grant.hc_terminal_id
        || remote["mode"] != json!(grant.mode)
    {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "executor terminal changed; request a new terminal grant".into(),
        ));
    }
    let key = generate_key();
    let mut url = reqwest::Url::parse(&peer.url).map_err(anyhow::Error::from)?;
    url.path_segments_mut()
        .map_err(|_| anyhow!("invalid paired executor origin"))?
        .clear()
        .extend(["v1", "terminal", id]);
    let res = s
        .http
        .get(url)
        .version(reqwest::Version::HTTP_11)
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header("Sec-WebSocket-Key", &key)
        .bearer_auth(string(&remote, "ticket")?)
        .send()
        .await
        .map_err(anyhow::Error::from)?;
    if res.status() != 101
        || !res
            .headers()
            .get("upgrade")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
        || res
            .headers()
            .get("sec-websocket-accept")
            .and_then(|v| v.to_str().ok())
            != Some(derive_accept_key(key.as_bytes()).as_str())
    {
        return Err(ApiError(
            StatusCode::BAD_GATEWAY,
            "executor declined terminal attachment".into(),
        ));
    }
    let upstream = res.upgrade().await.map_err(anyhow::Error::from)?;
    let cfg = WebSocketConfig::default()
        .max_message_size(Some(65536))
        .max_frame_size(Some(65536));
    let mut remote = WebSocketStream::from_raw_socket(upstream, Role::Client, Some(cfg)).await;
    Ok(upgrade.max_message_size(65536).on_upgrade(move |mut client|async move {
        loop {
            tokio::select! {
                stream=remote.next()=> {
                    let frame=match stream {
                        Some(Ok(Remote::Binary(b)))=>Message::Binary(b),
                        Some(Ok(Remote::Text(t)))=>Message::Text(t.as_str().into()),
                        Some(Ok(Remote::Ping(_)))=>{if remote.flush().await.is_err(){break}continue},
                        Some(Ok(Remote::Pong(_)))=>continue,
                        _=>break,
                    };
                    if client.send(frame).await.is_err(){break}
                }
                stream=client.next()=> {
                    let frame=match stream{
                        Some(Ok(Message::Binary(b))) if grant.mode==AttachMode::Interactive=>Remote::Binary(b),
                        Some(Ok(Message::Text(t))) if grant.mode==AttachMode::Interactive=>Remote::Text(t.as_str().into()),
                        Some(Ok(Message::Ping(_)))=>{if client.flush().await.is_err(){break}continue},
                        Some(Ok(Message::Pong(_)))=>continue,
                        _=>break,
                    };
                    if remote.send(frame).await.is_err(){break}
                }
            }
        }
        let _=remote.close(None).await;
        let _=client.close().await;
    }))
}
