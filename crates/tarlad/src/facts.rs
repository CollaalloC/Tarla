//! A remote node advertises capability metadata. Only the owning node resolves
//! the fact's private filesystem path and verifies it before execution.
use super::*;
use rusqlite::params;
pub fn configure(db: &mut Connection, config: &toml::Table, node: &str) -> Result<()> {
    let tx = db.transaction()?;
    tx.execute("DELETE FROM workspace_facts WHERE node_id=?", [node])?;
    if let Some(rows) = config.get("facts") {
        for row in rows.as_array().context("expected [[facts]]")? {
            let v = serde_json::to_value(row)?;
            let key = string(&v, "fact_key")?;
            validate_id(key)?;
            let path = Path::new(string(&v, "local_path")?);
            if !path.is_absolute() {
                bail!("fact path must be absolute")
            }
            let metadata = v.get("metadata").cloned().unwrap_or(json!({}));
            let verified_at = if path.exists() { now() } else { 0 };
            tx.execute(
                "INSERT INTO workspace_facts VALUES(?,?,?,?,?)",
                params![
                    node,
                    key,
                    path.to_string_lossy(),
                    metadata.to_string(),
                    verified_at
                ],
            )?;
        }
    }
    tx.commit()?;
    Ok(())
}
pub fn local(db: &Connection, node: &str) -> Result<Vec<Value>> {
    list(db,"SELECT node_id,fact_key,metadata_json,verified_at FROM workspace_facts WHERE node_id=? ORDER BY fact_key LIMIT 256",&[&node])
}
pub fn replace(db: &Connection, node: &str, facts: &[Value]) -> Result<()> {
    if facts.len() > 256 {
        bail!("too many advertised facts")
    }
    db.execute("DELETE FROM service_peer_facts WHERE node_id=?", [node])?;
    for fact in facts {
        let key = string(fact, "fact_key")?;
        validate_id(key)?;
        if fact["node_id"] != node {
            bail!("wrong fact owner")
        }
        let raw = string(fact, "metadata_json")?;
        let _: Value = serde_json::from_str(raw)?;
        if raw.len() > 1024 {
            bail!("fact metadata too large")
        }
        db.execute(
            "INSERT INTO service_peer_facts VALUES(?,?,?,?)",
            params![node, key, raw, fact["verified_at"].as_i64().unwrap_or(0)],
        )?;
    }
    Ok(())
}
pub fn mirror(db: &Connection) -> Result<Vec<Value>> {
    list(db,"SELECT node_id,fact_key,metadata_json,verified_at FROM service_peer_facts ORDER BY node_id,fact_key",&[])
}
