//! The configured incumbent can pin its key on a paired node. A replacement
//! carries a workspace-scoped chain signed by each preceding authority.
use super::*;
use std::sync::atomic::Ordering;
use tarla_workspace::authority as ledger;

pub fn receive(s: &Service, sender: &str, request: &Value) -> Result<Value> {
    let _guard = s
        .authority_change
        .lock()
        .map_err(|_| anyhow!("authority lock poisoned"))?;
    let term = request["term"].as_i64().context("missing authority term")?;
    let public = string(request, "public")?;
    let mut db = s.db()?;
    let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
    if term == s.term() {
        anyhow::ensure!(
            sender == s.supervisor(),
            "only the configured incumbent can pin its key"
        );
        ledger::pin(&tx, sender, term, public)?;
        tx.commit()?;
        return Ok(json!({"term":term}));
    }
    anyhow::ensure!(term > s.term(), "older authority term");
    let chain = request["grants"]
        .as_array()
        .context("missing authority grants")?;
    anyhow::ensure!(
        !chain.is_empty() && chain.len() <= 128,
        "invalid authority chain length"
    );
    let mut owner = s.supervisor().to_owned();
    let mut from = s.term();
    let mut key = ledger::known(&tx, &owner, from)?
        .context("no paired incumbent key; use explicit set-authority")?;
    for raw in chain {
        let grant: ledger::Grant = serde_json::from_value(raw.clone())?;
        grant.verify(s.id(), &owner, from, &key)?;
        ledger::remember(&tx, &grant)?;
        owner = grant.successor;
        from = grant.to_term;
        key = grant.public;
    }
    anyhow::ensure!(
        owner == sender && from == term && key == public && owner != s.node(),
        "invalid announced successor"
    );
    tx.commit()?;
    ledger::set_config(&s.ws, sender, term, "executor_only")?;
    s.retired.store(true, Ordering::Release);
    s.reload.notify_one();
    Ok(json!({"term":term}))
}

pub fn start(s: &S) {
    if !s.is_supervisor() {
        return;
    }
    for node in s.peers.keys() {
        let service = s.clone();
        let peer = node.clone();
        tokio::spawn(async move {
            loop {
                let retry = announce(&service, &peer).await.is_err();
                tokio::time::sleep(Duration::from_secs(if retry { 2 } else { 10 })).await;
            }
        });
    }
}
async fn announce(s: &Service, peer: &str) -> Result<()> {
    let state = peers::call(s, peer, "authority_state", &json!({})).await?;
    let from = state["term"].as_i64().context("missing peer term")?;
    if from > s.term() {
        return Ok(());
    }
    let db = s.db()?;
    let public =
        ledger::known(&db, s.node(), s.term())?.context("missing incumbent signing key")?;
    if from == s.term() && state["public"] == public && state["supervisor"] == s.node() {
        return Ok(());
    }
    let grants = ledger::grants(&db, from)?;
    drop(db);
    peers::call(
        s,
        peer,
        "authority_update",
        &json!({"public":public,"grants":grants}),
    )
    .await?;
    Ok(())
}
