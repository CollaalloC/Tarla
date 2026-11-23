//! Codync-style monotonic `rev` (`since: rev`) incremental sync, SSE push stream, and `/v1/hello` handshake.
use super::*;
use axum::{
    extract::{Query, State},
    response::sse::{Event, KeepAlive, Sse},
};
use serde::Deserialize;
use std::convert::Infallible;

pub const HOST_VERSION: &str = "0.2.0";
pub const MIN_APP_VERSION: &str = "0.2.0";
pub const PROTOCOL_GENERATION: u32 = 2;

#[derive(Debug, Deserialize, Default)]
pub struct SyncQuery {
    pub since: Option<i64>,
    pub limit: Option<usize>,
}

/// `GET /v1/hello` — protocol generation & mobile client compatibility handshake (Spec §4.2).
pub async fn hello(State(s): State<S>) -> Api {
    let latest_rev = tarla_memory::latest_sync_rev(&s.db()?)?;
    Ok(Json(json!({
        "protocol_generation": PROTOCOL_GENERATION,
        "min_app_version": MIN_APP_VERSION,
        "host_version": HOST_VERSION,
        "workspace_id": s.id(),
        "node_id": s.node(),
        "role_mode": s.ws.config.workspace.role_mode,
        "latest_rev": latest_rev,
    })))
}

/// `GET /v1/sync?since=<last_rev>&limit=200` — fast batch catch-up after reconnect or sleep.
pub async fn get_sync(State(s): State<S>, Query(q): Query<SyncQuery>) -> Api {
    let since = q.since.unwrap_or(0);
    if since < 0 {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "since cursor must be >= 0".into(),
        ));
    }
    let limit = q.limit.unwrap_or(200).clamp(1, 500);
    let (latest_rev, has_more, events) =
        tarla_memory::read_sync_since(&s.db()?, s.id(), since, limit)?;
    Ok(Json(json!({
        "workspace_id": s.id(),
        "since_rev": since,
        "latest_rev": latest_rev,
        "has_more": has_more,
        "events": events,
    })))
}

/// `GET /v1/events?since=<last_rev>` — real-time SSE stream of monotonic `sync_ledger` revisions.
pub async fn sse_events(
    State(s): State<S>,
    Query(q): Query<SyncQuery>,
) -> std::result::Result<
    Sse<impl futures_util::Stream<Item = std::result::Result<Event, Infallible>>>,
    ApiError,
> {
    let cursor = q.since.unwrap_or(0);
    if cursor < 0 {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "since cursor must be >= 0".into(),
        ));
    }
    // Validate database access up-front before upgrading to SSE.
    let _ = s.db()?;

    let stream = futures_util::stream::unfold((s, cursor), |(svc, mut last_rev)| async move {
        loop {
            let batch = svc
                .db()
                .and_then(|db| tarla_memory::read_sync_since(&db, svc.id(), last_rev, 100));
            match batch {
                Ok((_, _, entries)) if !entries.is_empty() => {
                    let mut out = Vec::with_capacity(entries.len());
                    for entry in entries {
                        last_rev = entry.rev;
                        let payload = serde_json::to_string(&entry).unwrap_or_else(|_| "{}".into());
                        let ev = Event::default()
                            .id(entry.rev.to_string())
                            .event(entry.event_kind)
                            .data(payload);
                        out.push(Ok(ev));
                    }
                    return Some((futures_util::stream::iter(out), (svc, last_rev)));
                }
                Ok(_) => {}
                Err(_) => return None,
            }
            tokio::select! {
                _ = svc.rev_wake.notified() => {}
                _ = svc.notify.notified() => {}
                _ = tokio::time::sleep(Duration::from_millis(200)) => {}
            }
        }
    });
    use futures_util::StreamExt;
    Ok(Sse::new(stream.flatten()).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keepalive"),
    ))
}
