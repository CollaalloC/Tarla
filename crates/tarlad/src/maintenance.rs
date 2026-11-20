use super::*;
use rusqlite::params;

/// A reply arriving during compaction gets a higher sequence and survives the
/// conditional delete. Failed conversations rotate behind the current cursor.
fn compact_pending(s: &Service, after: &str) -> Result<(String, bool)> {
    let mut db = s.db()?;
    let policy = control::context_policy(s, 0)?;
    let mut stmt = db.prepare(
        "SELECT conversation_id,seq_no FROM service_context_pending
         ORDER BY conversation_id<=?1,conversation_id LIMIT 16",
    )?;
    let pending = stmt
        .query_map([after], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);

    let mut processed = 0;
    let mut last = after.to_owned();
    for (id, seq) in pending {
        match tarla_context::maintain_after_turn(&mut db, &id, &policy) {
            Ok(()) => {
                db.execute(
                    "DELETE FROM service_context_pending WHERE conversation_id=?1 AND seq_no=?2",
                    params![id, seq],
                )?;
                processed += 1;
            }
            Err(error) => eprintln!("idle context compaction deferred for {id}: {error}"),
        }
        last = id;
    }
    Ok((last, processed == 16))
}

pub async fn maintain_context(s: S) {
    if !s.is_supervisor() {
        return;
    }
    let mut cursor = String::new();
    loop {
        let service = s.clone();
        let after = cursor.clone();
        match tokio::task::spawn_blocking(move || compact_pending(&service, &after)).await {
            Ok(Ok((last, full))) => {
                cursor = last;
                if full {
                    continue;
                }
            }
            Ok(Err(e)) => eprintln!("idle context scan deferred: {e}"),
            Err(e) => eprintln!("idle context worker interrupted: {e}"),
        }
        tokio::select! {
            _ = s.context_wake.notified() => (),
            _ = tokio::time::sleep(Duration::from_secs(2)) => (),
        }
    }
}
