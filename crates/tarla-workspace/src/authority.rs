//! An incumbent signs each handover. Replicas pin its public key through their
//! existing authenticated pairing. A higher term alone never transfers authority.
use crate::{validate_name, Identity, Workspace};
use anyhow::{anyhow, ensure, Context, Result};
use ring::{
    rand::SystemRandom,
    signature::{self, Ed25519KeyPair, KeyPair},
};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::{fs, io::Write};
use tempfile::NamedTempFile;

pub const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS authority_keys(term INTEGER PRIMARY KEY, node TEXT NOT NULL, public TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS authority_private(singleton INTEGER PRIMARY KEY CHECK(singleton=1), node TEXT NOT NULL, term INTEGER NOT NULL, secret BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS authority_grants(term INTEGER PRIMARY KEY, grant_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS authority_reservation(term INTEGER PRIMARY KEY, grant_json TEXT NOT NULL, successor_secret BLOB NOT NULL);";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Grant {
    pub workspace: String,
    pub issuer: String,
    pub from_term: i64,
    pub successor: String,
    pub to_term: i64,
    pub public: String,
    pub signature: String,
}
impl Grant {
    fn payload(&self) -> Result<Vec<u8>> {
        Ok(serde_json::to_vec(&(
            "tarla/authority/v1",
            &self.workspace,
            &self.issuer,
            self.from_term,
            &self.successor,
            self.to_term,
            &self.public,
        ))?)
    }
    pub fn verify(&self, space: &str, issuer: &str, term: i64, public: &str) -> Result<()> {
        validate_name(&self.successor)?;
        ensure!(
            self.workspace == space
                && self.issuer == issuer
                && self.from_term == term
                && self.successor != issuer
                && Some(self.to_term) == term.checked_add(1),
            "invalid authority succession"
        );
        ensure!(
            hex::decode(&self.public)?.len() == 32,
            "invalid successor key"
        );
        signature::UnparsedPublicKey::new(&signature::ED25519, hex::decode(public)?)
            .verify(&self.payload()?, &hex::decode(&self.signature)?)
            .map_err(|_| anyhow!("authority handover signature mismatch"))
    }
}
fn new_secret() -> Result<Vec<u8>> {
    Ok(Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
        .map_err(|_| anyhow!("could not generate authority key"))?
        .as_ref()
        .to_vec())
}
fn key(bytes: &[u8]) -> Result<Ed25519KeyPair> {
    Ed25519KeyPair::from_pkcs8(bytes).map_err(|_| anyhow!("invalid authority key"))
}
pub fn known(db: &Connection, node: &str, term: i64) -> Result<Option<String>> {
    Ok(db
        .query_row(
            "SELECT public FROM authority_keys WHERE node=?1 AND term=?2",
            params![node, term],
            |r| r.get(0),
        )
        .optional()?)
}
pub fn pin(db: &Connection, node: &str, term: i64, public: &str) -> Result<()> {
    ensure!(
        hex::decode(public)?.len() == 32,
        "invalid authority public key"
    );
    db.execute(
        "INSERT OR IGNORE INTO authority_keys VALUES(?,?,?)",
        params![term, node, public],
    )?;
    ensure!(
        known(db, node, term)?.as_deref() == Some(public),
        "authority key changed at the same term"
    );
    Ok(())
}
pub fn ensure_signer(db: &mut Connection, id: &Identity) -> Result<String> {
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let secret: Option<Vec<u8>> = tx
        .query_row(
            "SELECT secret FROM authority_private WHERE node=? AND term=?",
            params![id.node_id, id.supervisor_term],
            |r| r.get(0),
        )
        .optional()?;
    let secret = match secret {
        Some(s) => s,
        None => {
            let s = new_secret()?;
            tx.execute(
                "INSERT OR REPLACE INTO authority_private VALUES(1,?,?,?)",
                params![id.node_id, id.supervisor_term, s],
            )?;
            s
        }
    };
    let public = hex::encode(key(&secret)?.public_key().as_ref());
    pin(&tx, &id.node_id, id.supervisor_term, &public)?;
    tx.commit()?;
    Ok(public)
}
/// All backups from the same term reserve the same successor and key.
pub fn reserve(db: &mut Connection, id: &Identity, successor: &str) -> Result<(Grant, Vec<u8>)> {
    validate_name(successor)?;
    ensure!(
        successor != id.node_id
            && id.node_id == id.supervisor_node_id
            && id.role_mode != "executor_only",
        "handover requires the incumbent supervisor"
    );
    db.execute_batch(SCHEMA)?;
    ensure_signer(db, id)?;
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    let pending: Option<(String, Vec<u8>)> = tx
        .query_row(
            "SELECT grant_json,successor_secret FROM authority_reservation WHERE term=?",
            [id.supervisor_term],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((raw, secret)) = pending {
        let grant: Grant = serde_json::from_str(&raw)?;
        ensure!(
            grant.successor == successor,
            "a different successor is already reserved for this term"
        );
        return Ok((grant, secret));
    }
    let old: Vec<u8> = tx.query_row(
        "SELECT secret FROM authority_private WHERE node=? AND term=?",
        params![id.node_id, id.supervisor_term],
        |r| r.get(0),
    )?;
    let secret = new_secret()?;
    let mut grant = Grant {
        workspace: id.id.clone(),
        issuer: id.node_id.clone(),
        from_term: id.supervisor_term,
        successor: successor.into(),
        to_term: id.supervisor_term.checked_add(1).context("term overflow")?,
        public: hex::encode(key(&secret)?.public_key().as_ref()),
        signature: String::new(),
    };
    grant.signature = hex::encode(key(&old)?.sign(&grant.payload()?).as_ref());
    tx.execute(
        "INSERT INTO authority_reservation VALUES(?,?,?)",
        params![grant.from_term, serde_json::to_string(&grant)?, secret],
    )?;
    tx.commit()?;
    Ok((grant, secret))
}
pub fn remember(db: &Connection, grant: &Grant) -> Result<()> {
    pin(db, &grant.successor, grant.to_term, &grant.public)?;
    db.execute(
        "INSERT OR IGNORE INTO authority_grants VALUES(?,?)",
        params![grant.to_term, serde_json::to_string(grant)?],
    )?;
    Ok(())
}
pub fn grants(db: &Connection, since: i64) -> Result<Vec<Grant>> {
    let mut stmt =
        db.prepare("SELECT grant_json FROM authority_grants WHERE term>? ORDER BY term LIMIT 128")?;
    let rows = stmt.query_map([since], |r| r.get::<_, String>(0))?;
    rows.map(|r| Ok(serde_json::from_str(&r?)?)).collect()
}
pub fn set_config(ws: &Workspace, supervisor: &str, term: i64, role: &str) -> Result<()> {
    let path = ws.root.join("workspace.toml");
    let mut cfg: toml::Value = fs::read_to_string(&path)?.parse()?;
    let identity = cfg
        .get_mut("workspace")
        .and_then(toml::Value::as_table_mut)
        .context("invalid workspace identity")?;
    identity.insert("supervisor_node_id".into(), supervisor.into());
    identity.insert("supervisor_term".into(), term.into());
    identity.insert("role_mode".into(), role.into());
    let mut tmp = NamedTempFile::new_in(&ws.root)?;
    tmp.write_all(toml::to_string_pretty(&cfg)?.as_bytes())?;
    tmp.as_file().sync_all()?;
    tmp.persist(path)?;
    #[cfg(unix)]
    fs::File::open(&ws.root)?.sync_all()?;
    Ok(())
}
