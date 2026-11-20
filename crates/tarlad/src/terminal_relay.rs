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
    let remote = terminal::peer_ticket(s, owner, id, grant.mode).await?;
    let remote_ticket = string(&remote, "ticket")?;
    // Older executors include the terminal address only inside their ticket.
    let same_terminal = match remote.get("hc_terminal_id") {
        Some(terminal) => terminal == &grant.hc_terminal_id,
        None => grant.matches_peer_ticket_scope(remote_ticket),
    };
    if remote["run_id"] != grant.run_id
        || remote["node_id"] != grant.node_id
        || !same_terminal
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
        .bearer_auth(remote_ticket)
        .send()
        .await
        .map_err(|_| {
            ApiError(
                StatusCode::BAD_GATEWAY,
                "could not connect to the executor terminal".into(),
            )
        })?;
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
    let upstream = res.upgrade().await.map_err(|_| {
        ApiError(
            StatusCode::BAD_GATEWAY,
            "executor terminal connection closed during upgrade".into(),
        )
    })?;
    // Executor output can contain an entire screen repaint in one frame.
    let cfg = WebSocketConfig::default()
        .max_message_size(Some(tarla_sidecar::MAX_HERDR_FRAME_BYTES))
        .max_frame_size(Some(tarla_sidecar::MAX_HERDR_FRAME_BYTES));
    let mut remote = WebSocketStream::from_raw_socket(upstream, Role::Client, Some(cfg)).await;
    let interactive = grant.mode == AttachMode::Interactive;
    Ok(upgrade.max_message_size(tarla_sidecar::MAX_TERMINAL_INPUT_BYTES).on_upgrade(move |mut client| async move {
        loop {
            tokio::select! {
                incoming = remote.next() => {
                    let frame = match incoming {
                        Some(Ok(Remote::Binary(bytes))) => Message::Binary(bytes),
                        Some(Ok(Remote::Text(text))) => Message::Text(text.as_str().into()),
                        Some(Ok(Remote::Ping(_))) => {
                            if remote.flush().await.is_err() {
                                break;
                            }
                            continue;
                        }
                        Some(Ok(Remote::Pong(_))) => continue,
                        _ => break,
                    };
                    if client.send(frame).await.is_err() {
                        break;
                    }
                }
                incoming = client.next() => {
                    let frame = match incoming {
                        Some(Ok(Message::Binary(bytes))) if interactive => Remote::Binary(bytes),
                        Some(Ok(Message::Text(text))) if interactive => Remote::Text(text.as_str().into()),
                        Some(Ok(Message::Ping(_))) => {
                            if client.flush().await.is_err() {
                                break;
                            }
                            continue;
                        }
                        Some(Ok(Message::Pong(_))) => continue,
                        _ => break,
                    };
                    if remote.send(frame).await.is_err() {
                        break;
                    }
                }
            }
        }
        let _ = remote.close(None).await;
        let _ = client.close().await;
    }))
}
