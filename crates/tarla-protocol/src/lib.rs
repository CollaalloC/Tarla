//! Contracts shared by the supervisor, executor and authenticated terminal proxy.
use anyhow::{ensure, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD as B64, Engine};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MissionPacket {
    pub run_id: String,
    pub workspace_id: String,
    pub objective: String,
    pub acceptance_criteria: Vec<String>,
    pub distilled_context: Vec<String>,
    pub project_memory_snapshot: String,
    pub required_fact_keys: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RunReceipt {
    pub run_id: String,
    pub final_state: String,
    pub executive_summary: String,
    pub artifact_refs: Vec<String>,
    pub s0_transcript_pointer: String,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AttachMode {
    Observe,
    Interactive,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct AttachLease {
    pub workspace_id: String,
    pub node_id: String,
    pub run_id: String,
    pub hc_terminal_id: String,
    pub mode: AttachMode,
    pub nonce: String,
    pub expires_at: i64,
}

impl AttachLease {
    fn mac(secret: &[u8]) -> Result<Hmac<Sha256>> {
        ensure!(secret.len() >= 32, "attach signing key too short");
        let mut mac = Hmac::new_from_slice(secret)?;
        mac.update(b"tarla/attach/v1\0");
        Ok(mac)
    }
    pub fn issue(
        secret: &[u8],
        workspace: &str,
        node: &str,
        run: &str,
        terminal: &str,
        mode: AttachMode,
        now: i64,
    ) -> Result<String> {
        for field in [workspace, node, run, terminal] {
            ensure!(
                !field.is_empty() && field.len() <= 256,
                "invalid lease scope"
            );
        }
        let lease = Self {
            workspace_id: workspace.into(),
            node_id: node.into(),
            run_id: run.into(),
            hc_terminal_id: terminal.into(),
            mode,
            nonce: Uuid::new_v4().to_string(),
            expires_at: now.checked_add(60).context("invalid clock")?,
        };
        let payload = B64.encode(serde_json::to_vec(&lease)?);
        let mut mac = Self::mac(secret)?;
        mac.update(payload.as_bytes());
        Ok(format!(
            "{}.{}",
            payload,
            B64.encode(mac.finalize().into_bytes())
        ))
    }
    fn parts(ticket: &str) -> Result<(&str, &str)> {
        ensure!(ticket.len() <= 8192, "ticket too large");
        ticket.split_once('.').context("invalid ticket")
    }

    pub fn valid_at(&self, now: i64) -> bool {
        self.expires_at > now && self.expires_at <= now.saturating_add(60)
    }

    /// Authenticate before consulting the ledger for the destination node of a relay.
    pub fn verify_for_workspace(
        ticket: &str,
        secret: &[u8],
        workspace: &str,
        now: i64,
    ) -> Result<Self> {
        let (body, signature) = Self::parts(ticket)?;
        let mut mac = Self::mac(secret)?;
        mac.update(body.as_bytes());
        mac.verify_slice(&B64.decode(signature)?)
            .map_err(|_| anyhow::anyhow!("invalid signature"))?;
        let lease: Self = serde_json::from_slice(&B64.decode(body)?)?;
        ensure!(lease.workspace_id == workspace, "wrong ticket audience");
        ensure!(lease.valid_at(now), "ticket outside validity window");
        Ok(lease)
    }

    /// The executor must atomically consume the verified nonce before bridging any bytes.
    pub fn verify(
        ticket: &str,
        secret: &[u8],
        workspace: &str,
        node: &str,
        now: i64,
    ) -> Result<Self> {
        let lease = Self::verify_for_workspace(ticket, secret, workspace, now)?;
        ensure!(lease.node_id == node, "wrong ticket audience");
        Ok(lease)
    }

    /// Check wire format and compare scope only. The executor authenticates its
    /// own ticket with its private key when redeeming it on either route.
    pub fn peer_ticket_has_scope(
        ticket: &str,
        workspace: &str,
        node: &str,
        run: &str,
        terminal: &str,
        mode: AttachMode,
    ) -> Result<bool> {
        let (body, signature) = Self::parts(ticket)?;
        ensure!(
            B64.decode(signature)?.len() == 32,
            "invalid ticket signature size"
        );
        let peer: Self = serde_json::from_slice(&B64.decode(body)?)?;
        Ok(peer.workspace_id == workspace
            && peer.node_id == node
            && peer.run_id == run
            && peer.hc_terminal_id == terminal
            && peer.mode == mode)
    }

    pub fn matches_peer_ticket_scope(&self, ticket: &str) -> bool {
        Self::peer_ticket_has_scope(
            ticket,
            &self.workspace_id,
            &self.node_id,
            &self.run_id,
            &self.hc_terminal_id,
            self.mode,
        )
        .unwrap_or(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn legacy_peer_ticket_must_name_the_same_terminal_and_mode() {
        let key = [73; 32];
        let issued = |w, n, r, t, m| AttachLease::issue(&key, w, n, r, t, m, 5).unwrap();
        let mode = AttachMode::Observe;
        let t = issued("w", "n", "r", "t1", mode);
        let relay = AttachLease::verify(&t, &key, "w", "n", 5).unwrap();
        assert!(relay.matches_peer_ticket_scope(&issued("w", "n", "r", "t1", mode)));
        for other in [
            issued("v", "n", "r", "t1", mode),
            issued("w", "m", "r", "t1", mode),
            issued("w", "n", "s", "t1", mode),
            issued("w", "n", "r", "t2", mode),
            issued("w", "n", "r", "t1", AttachMode::Interactive),
            "invalid".into(),
        ] {
            assert!(!relay.matches_peer_ticket_scope(&other));
        }
    }
    #[test]
    fn ticket_is_bound_to_audience_key_and_time() {
        let key = [73; 32];
        let t =
            AttachLease::issue(&key, "w", "n", "r", "terminal", AttachMode::Observe, 5).unwrap();
        let lease = AttachLease::verify(&t, &key, "w", "n", 64).unwrap();
        assert_eq!(lease.mode, AttachMode::Observe);
        assert_eq!(lease.hc_terminal_id, "terminal");
        for (w, n, time) in [
            ("other", "n", 5),
            ("w", "other", 5),
            ("w", "n", 65),
            ("w", "n", 0),
        ] {
            assert!(AttachLease::verify(&t, &key, w, n, time).is_err());
        }
        assert!(AttachLease::verify(&t, &[21; 32], "w", "n", 5).is_err());
    }
}
