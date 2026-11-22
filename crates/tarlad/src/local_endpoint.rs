//! An opted-in desktop app discovers its ephemeral loopback port without sending
//! its bearer token to a stale listener. A fresh per-startup `proof_key` in the
//! private endpoint file keeps `api.token` out of unauthenticated challenge routes.
use super::Result;
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
    sync::Arc,
};
use uuid::Uuid;

pub struct Publication(PathBuf);
impl Drop for Publication {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
    }
}

fn fresh_proof_key() -> String {
    format!("{}{}", Uuid::new_v4().simple(), Uuid::new_v4().simple())
}

pub fn enable(root: &Path, addr: SocketAddr) -> Result<(Publication, Router)> {
    anyhow::ensure!(
        addr.ip().is_loopback(),
        "cannot publish a non-loopback socket"
    );
    let key = fresh_proof_key();
    let dir = root.join("runtime");
    let mut tmp = tempfile::NamedTempFile::new_in(&dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        tmp.as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    serde_json::to_writer(&mut tmp, &json!({"port": addr.port(), "proof_key": key}))?;
    tmp.flush()?;
    tmp.as_file().sync_all()?;
    let path = dir.join("desktop-endpoint.json");
    tmp.persist(&path)?;
    let state: Arc<str> = Arc::from(key);
    let router = Router::new()
        .route("/local/v1/proof/{nonce}", get(proof))
        .with_state(state);
    Ok((Publication(path), router))
}

fn compute_proof(secret: &str, nonce: &str) -> Result<String, StatusCode> {
    if nonce.len() != 64 || !nonce.bytes().all(|x| x.is_ascii_hexdigit()) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut h = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    h.update(b"tarla-desktop-v1:");
    h.update(nonce.as_bytes());
    Ok(format!("{:x}", h.finalize().into_bytes()))
}

async fn proof(
    State(proof_key): State<Arc<str>>,
    Route(nonce): Route<String>,
) -> Result<Json<Value>, StatusCode> {
    Ok(Json(json!({"proof": compute_proof(&proof_key, &nonce)?})))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn desktop_endpoint_uses_isolated_ephemeral_proof_key() -> Result<()> {
        let dir = tempfile::tempdir()?;
        fs::create_dir_all(dir.path().join("runtime"))?;
        let api_token = "custom-operator-token-0123456789abcdef";
        let addr: SocketAddr = "127.0.0.1:47878".parse()?;
        let (_pub_guard, _router) = enable(dir.path(), addr)?;
        let record: Value =
            serde_json::from_slice(&fs::read(dir.path().join("runtime/desktop-endpoint.json"))?)?;
        assert_eq!(record["port"], 47878);
        let proof_key = record["proof_key"].as_str().unwrap();
        assert_eq!(proof_key.len(), 64);
        assert_ne!(proof_key, api_token);
        let nonce = "a".repeat(64);
        assert_ne!(
            compute_proof(proof_key, &nonce).unwrap(),
            compute_proof(api_token, &nonce).unwrap()
        );
        Ok(())
    }
}
