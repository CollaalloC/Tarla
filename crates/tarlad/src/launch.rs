//! The permanent holder owns the account lock. A durable one-shot permit fences a
//! delayed Herdr request after the executor has reconciled or cancelled its claim.
use super::*;
use rusqlite::{params, OpenFlags, OptionalExtension, TransactionBehavior};
use std::{
    ffi::OsString,
    io::{self, ErrorKind},
    process::Command,
};

pub const SCHEMA: &str = "
    CREATE TABLE IF NOT EXISTS service_launch_permits (
        run_id TEXT PRIMARY KEY REFERENCES service_claims(run_id) ON DELETE CASCADE,
        state TEXT NOT NULL CHECK(state IN ('issued','admitted','withdrawn'))
    );
    CREATE TABLE IF NOT EXISTS service_run_abandonments (
        run_id TEXT PRIMARY KEY REFERENCES runs(run_id), reason TEXT NOT NULL, recorded_at INTEGER NOT NULL
    );";

pub fn issue(db: &mut Connection, id: &str) -> Result<bool> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let changed = tx.execute(
        "UPDATE service_claims SET phase='launching',updated_at=? WHERE run_id=? AND phase='queued'
         AND EXISTS (SELECT 1 FROM runs WHERE run_id=? AND desired_state='running'
                     AND observed_state NOT IN ('failed','cancelled','succeeded'))",
        params![now(), id, id],
    )?;
    if changed != 0 {
        tx.execute(
            "INSERT INTO service_launch_permits VALUES(?,'issued')",
            [id],
        )?;
    }
    tx.commit()?;
    Ok(changed != 0)
}

/// Success proves the command never passed admission, including a late holder
/// waking after this transaction. No such proof is available for legacy claims.
pub fn withdraw(db: &mut Connection, id: &str) -> Result<bool> {
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let safe = tx.execute(
        "UPDATE service_launch_permits SET state='withdrawn'
         WHERE run_id=? AND state IN ('issued','withdrawn')",
        [id],
    )? != 0;
    tx.commit()?;
    Ok(safe)
}

pub fn command(s: &Service, id: &str, argv: Vec<String>) -> Result<Vec<String>> {
    let mut result = Vec::with_capacity(5 + argv.len());
    for p in [
        std::env::current_exe()?,
        s.home.canonicalize()?,
        s.db_path.canonicalize()?,
    ] {
        result.push(
            p.into_os_string()
                .into_string()
                .map_err(|_| anyhow!("non-UTF8 executor path"))?,
        );
    }
    result.insert(1, "--executor-launch-helper".into());
    result.push(id.into());
    result.extend(argv);
    Ok(result)
}

fn allowed(db: &Connection, id: &str, required: &str) -> Result<Option<String>> {
    Ok(db
        .query_row(
            "SELECT r.target_slot_id FROM service_launch_permits p JOIN runs r USING(run_id)
         JOIN service_claims c USING(run_id)
         WHERE p.run_id=? AND p.state=? AND r.desired_state='running'
         AND r.observed_state NOT IN ('failed','cancelled','succeeded')
         AND c.phase IN ('launching','active')",
            params![id, required],
            |r| r.get(0),
        )
        .optional()?)
}

#[cfg(unix)]
static TERMINATION_SIGNAL: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

#[cfg(unix)]
extern "C" fn record_termination_signal(sig: libc::c_int) {
    TERMINATION_SIGNAL.store(sig, std::sync::atomic::Ordering::Relaxed);
}

#[cfg(unix)]
fn install_signal_forwarder() -> io::Result<()> {
    TERMINATION_SIGNAL.store(0, std::sync::atomic::Ordering::Relaxed);
    unsafe {
        let mut action: libc::sigaction = std::mem::zeroed();
        if libc::sigemptyset(&mut action.sa_mask) != 0 {
            return Err(io::Error::last_os_error());
        }
        action.sa_sigaction = record_termination_signal as *const () as libc::sighandler_t;
        action.sa_flags = libc::SA_RESTART;
        for sig in [libc::SIGTERM, libc::SIGHUP, libc::SIGINT] {
            if libc::sigaction(sig, &action, std::ptr::null_mut()) != 0 {
                return Err(io::Error::last_os_error());
            }
        }
    }
    Ok(())
}

#[cfg(unix)]
fn signal_child_tree(pid: libc::pid_t, sig: libc::c_int) {
    unsafe {
        if libc::getpgid(pid) == pid {
            let _ = libc::kill(-pid, sig);
        } else {
            let _ = libc::kill(pid, sig);
        }
    }
}

#[cfg(unix)]
fn wait_for_child(child: &mut std::process::Child) -> io::Result<i32> {
    use std::os::unix::process::ExitStatusExt;
    use std::sync::atomic::Ordering;
    let pid = child.id() as libc::pid_t;
    let mut forwarded = 0;
    let mut escalate_at: Option<std::time::Instant> = None;
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status
                .code()
                .or_else(|| status.signal().map(|s| 128 + s))
                .unwrap_or(1));
        }
        let sig = TERMINATION_SIGNAL.swap(0, Ordering::Relaxed);
        if sig != 0 {
            if forwarded == 0 {
                forwarded = sig;
                signal_child_tree(pid, sig);
                escalate_at = Some(std::time::Instant::now() + Duration::from_secs(3));
            } else {
                signal_child_tree(pid, libc::SIGKILL);
                let _ = child.kill();
            }
        }
        if escalate_at.is_some_and(|deadline| std::time::Instant::now() >= deadline) {
            signal_child_tree(pid, libc::SIGKILL);
            let _ = child.kill();
            escalate_at = None;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
}

#[cfg(not(unix))]
fn install_signal_forwarder() -> std::io::Result<()> {
    Ok(())
}

#[cfg(not(unix))]
fn wait_for_child(child: &mut std::process::Child) -> std::io::Result<i32> {
    Ok(child.wait()?.code().unwrap_or(1))
}

pub fn helper(mut args: impl Iterator<Item = OsString>) -> Result<i32> {
    let home = PathBuf::from(args.next().context("missing private home")?);
    let path = PathBuf::from(args.next().context("missing executor database")?);
    let run = args
        .next()
        .and_then(|s| s.into_string().ok())
        .context("missing run id")?;
    let executable = args.next().context("missing mission executable")?;
    let mut db = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
    db.busy_timeout(Duration::from_secs(10))?;
    let _account = loop {
        let Some(slot) = allowed(&db, &run, "issued")? else {
            return Ok(1);
        };
        match tarla_sidecar::try_lock_slot(&home, &slot) {
            Ok(guard) => break guard,
            Err(e) if e.kind() == ErrorKind::WouldBlock => {
                std::thread::sleep(Duration::from_millis(100))
            }
            Err(e) => return Err(e.into()),
        }
    };
    // Persist the consumed permit before spawning. If we die here an uncertain
    // claim stays fenced. Nobody can automatically admit the same command twice.
    if db.execute(
        "UPDATE service_launch_permits SET state='admitted' WHERE run_id=? AND state='issued'",
        [&run],
    )? != 1
    {
        return Ok(1);
    }
    // Cancellation and process creation must have one order. A canceller that
    // has already committed prevents spawn; a later one sees an existing holder.
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    if allowed(&tx, &run, "admitted")?.is_none() {
        tx.execute(
            "UPDATE service_launch_permits SET state='withdrawn' WHERE run_id=?",
            [&run],
        )?;
        tx.commit()?;
        return Ok(1);
    }
    install_signal_forwarder()?;
    let mut child = Command::new(executable).args(args).spawn()?;
    if let Err(err) = tx.commit() {
        let _ = child.kill();
        let _ = child.wait();
        return Err(err.into());
    }
    drop(db);
    Ok(wait_for_child(&mut child)?)
}

/// A legacy or already-admitted command can have made writes even when the
/// holder disappeared. An owner may close that uncertainty after inspecting the
/// machine; the original run and the owner's evidence remain in the ledger.
pub async fn abandon(
    axum::extract::State(s): axum::extract::State<S>,
    axum::extract::Path(id): axum::extract::Path<String>,
    Json(request): Json<Value>,
) -> Api {
    let reason = string(&request, "reason")?.trim();
    if reason.is_empty() || reason.chars().count() > 280 || request["confirmed_stopped"] != true {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "confirm the process is stopped and include a reason of 1–280 characters".into(),
        ));
    }
    let prior = |db: &Connection| -> rusqlite::Result<Option<String>> {
        db.query_row(
            "SELECT reason FROM service_run_abandonments WHERE run_id=?",
            [&id],
            |r| r.get(0),
        )
        .optional()
    };
    let row = api::one(&s, &id)?;
    if row["target_node_id"] != s.node() {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "abandon must be sent directly to the owning executor".into(),
        ));
    }
    if let Some(saved) = prior(&s.db()?)? {
        if saved != reason {
            return Err(ApiError(
                StatusCode::CONFLICT,
                "run already closed with a different reason".into(),
            ));
        }
        fleet::finish(&s, &id, "cancelled", "", "")?;
        return Ok(Json(
            json!({"run_id":id,"reason":saved,"observed_state":"cancelled"}),
        ));
    }
    if row["observed_state"] != "unconfirmed" {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "only an unconfirmed run can be abandoned".into(),
        ));
    }
    let hc = fleet::herdr(&s)?;
    let found = hc.find_run(&id).await.map_err(|e| {
        ApiError(
            StatusCode::SERVICE_UNAVAILABLE,
            format!("cannot verify the private holder inventory: {e}"),
        )
    })?;
    if found.is_some() {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "this run still has a retained terminal; reconcile or cancel it".into(),
        ));
    }

    let mut db = s.db()?;
    let tx = db.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let n = tx.execute(
        "UPDATE service_claims SET phase='abandoning',updated_at=? WHERE run_id=? AND phase IN ('active','launching')
         AND EXISTS(SELECT 1 FROM runs WHERE run_id=? AND target_node_id=? AND observed_state='unconfirmed')",
        params![now(),id,id,s.node()],
    )?;
    if n == 0 && prior(&tx)?.as_deref() != Some(reason) {
        return Err(ApiError(
            StatusCode::CONFLICT,
            "the run changed while its holder was being checked".into(),
        ));
    }
    tx.execute(
        "INSERT OR IGNORE INTO service_run_abandonments VALUES(?,?,?)",
        params![id, reason, now()],
    )?;
    tx.execute(
        "UPDATE runs SET desired_state='cancelled' WHERE run_id=?",
        [&id],
    )?;
    tx.execute(
        "UPDATE service_launch_permits SET state='withdrawn' WHERE run_id=? AND state='issued'",
        [&id],
    )?;
    tx.commit()?;
    fleet::finish(&s, &id, "cancelled", "", "")?;
    Ok(Json(
        json!({"run_id":id,"observed_state":"cancelled","reason":reason}),
    ))
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::os::unix::process::CommandExt;

    struct RestoreSignals([(libc::c_int, libc::sigaction); 3]);
    impl Drop for RestoreSignals {
        fn drop(&mut self) {
            for (sig, old) in &self.0 {
                unsafe {
                    let _ = libc::sigaction(*sig, old, std::ptr::null_mut());
                }
            }
        }
    }

    #[test]
    fn signal_forwarder_retains_child_until_graceful_exit() {
        let saved = unsafe {
            let mut prev: [(libc::c_int, libc::sigaction); 3] = std::mem::zeroed();
            for (slot, sig) in prev
                .iter_mut()
                .zip([libc::SIGTERM, libc::SIGHUP, libc::SIGINT])
            {
                slot.0 = sig;
                assert_eq!(libc::sigaction(sig, std::ptr::null(), &mut slot.1), 0);
            }
            prev
        };
        let _restore = RestoreSignals(saved);
        install_signal_forwarder().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let ready = dir.path().join("ready");
        let cleaned = dir.path().join("cleaned");
        let mut child = Command::new("/bin/sh")
            .process_group(0)
            .args([
                "-c",
                "trap 'sleep 0.12; : > \"$2\"; exit 0' TERM; : > \"$1\"; while :; do sleep 0.05; done",
                "sh",
            ])
            .arg(&ready)
            .arg(&cleaned)
            .spawn()
            .unwrap();
        while !ready.exists() {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(unsafe { libc::raise(libc::SIGTERM) }, 0);
        let code = wait_for_child(&mut child).unwrap();
        assert_eq!(code, 0);
        assert!(
            cleaned.exists(),
            "wait_for_child returned before child finished TERM cleanup"
        );
    }
}
