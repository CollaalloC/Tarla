//! An opted-in desktop app discovers its ephemeral loopback port without sending
//! its bearer token to a stale listener. Use a distinct HMAC domain for the proof.
use super::{Result, S};
use axum::{
    extract::{Path as Route, State},
    http::StatusCode,
    routing::get,
    Json, Router,
};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use std::{
    fs,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
};

pub struct Publication(PathBuf);
impl Drop for Publication {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}
pub fn publish(root: &Path, addr: SocketAddr) -> Result<Publication> {
    anyhow::ensure!(
        addr.ip().is_loopback(),
        "cannot publish a non-loopback socket"
    );
    let dir = root.join("runtime");
    let mut tmp = tempfile::NamedTempFile::new_in(&dir)?;
    serde_json::to_writer(&mut tmp, &json!({"port":addr.port()}))?;
    tmp.flush()?;
    let path = dir.join("desktop-endpoint.json");
    tmp.persist(&path)?;
    Ok(Publication(path))
}
pub fn proof_route(s: S) -> Router {
    Router::new()
        .route("/local/v1/proof/{nonce}", get(proof))
        .with_state(s)
}
async fn proof(State(s): State<S>, Route(nonce): Route<String>) -> Result<Json<Value>, StatusCode> {
    if nonce.len() != 64 || !nonce.bytes().all(|x| x.is_ascii_hexdigit()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut h = Hmac::<Sha256>::new_from_slice(s.ws.token.as_bytes())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    h.update(b"tarla-desktop-v1:");
    h.update(nonce.as_bytes());
    Ok(Json(
        json!({"proof":format!("{:x}",h.finalize().into_bytes())}),
    ))
}
