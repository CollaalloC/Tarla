//! Explicit authority changes. All paths hold the same exclusion lock as the daemon.
use anyhow::{ensure, Context, Result};
use clap::Subcommand;
use fs2::FileExt;
use rusqlite::{params, Connection, OpenFlags};
use std::{
    fs::{self, File, OpenOptions},
    path::{Path, PathBuf},
};
use tarla_workspace::{
    authority::{self, set_config},
    validate_name, Workspace,
};
use tempfile::NamedTempFile;

#[derive(Subcommand)]
pub enum Operation {
    /// Freeze the current supervisor and export its ledger for a named replacement.
    Handover {
        #[arg(long)]
        to: String,
        #[arg(long)]
        snapshot: PathBuf,
    },
    /// Save a recoverable supervisor ledger while retaining the incumbent.
    Backup {
        #[arg(long)]
        to: String,
        #[arg(long)]
        snapshot: PathBuf,
    },
    /// Import a handover ledger into a freshly initialized supervisor on its designated node.
    Restore {
        #[arg(long)]
        snapshot: PathBuf,
        /// Explicitly activate a recovery backup after taking the old supervisor offline.
        #[arg(long)]
        confirm_old_offline: bool,
    },
    /// Fence old authority locally. Set this on each peer during a manual migration or disaster recovery.
    SetAuthority {
        #[arg(long)]
        node: String,
        #[arg(long)]
        term: i64,
    },
}
fn lock(ws: &Workspace) -> Result<File> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(ws.root.join("runtime/daemon.lock"))?;
    FileExt::try_lock_exclusive(&file)
        .context("stop this workspace's tarlad before changing authority")?;
    Ok(file)
}
fn role(ws: &Workspace, new_supervisor: &str) -> &'static str {
    if ws.config.workspace.node_id != new_supervisor {
        "executor_only"
    } else if ws.config.workspace.role_mode == "supervisor_only" {
        "supervisor_only"
    } else {
        "supervisor_and_executor"
    }
}
pub fn run(home: &Path, id: &str, op: &Operation) -> Result<()> {
    let initial = Workspace::open(home, id)?;
    let _lock = lock(&initial)?;
    let ws = Workspace::open(home, id)?;
    let identity = &ws.config.workspace;
    let db = ws.root.join("state.db");
    match op {
        Operation::SetAuthority { node, term } => {
            validate_name(node)?;
            ensure!(
                *term > identity.supervisor_term,
                "new term must strictly increase"
            );
            // Never promote an executor's partial S1 mirror through a configuration edit.
            if node == &identity.node_id {
                ensure!(
                    identity.node_id == identity.supervisor_node_id || !db.exists(),
                    "promotion requires a restored supervisor ledger; use restore first"
                );
            }
            set_config(&ws, node, *term, role(&ws, node))?;
            println!("Workspace {id} accepts supervisor {node} at term {term}");
        }
        Operation::Handover { to, snapshot } | Operation::Backup { to, snapshot } => {
            let backup = matches!(op, Operation::Backup { .. });
            validate_name(to)?;
            ensure!(
                identity.node_id == identity.supervisor_node_id
                    && identity.role_mode != "executor_only",
                "handover must run on the current supervisor"
            );
            ensure!(
                to != &identity.node_id && !snapshot.exists(),
                "choose a different node and a new snapshot path"
            );
            let new_term = identity
                .supervisor_term
                .checked_add(1)
                .context("term overflow")?;
            let parent = snapshot
                .parent()
                .filter(|v| !v.as_os_str().is_empty())
                .unwrap_or_else(|| Path::new("."));
            let tmp = NamedTempFile::new_in(parent)?;
            let mut c = Connection::open_with_flags(&db, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
            let (grant, successor_secret) = authority::reserve(&mut c, identity, to)?;
            c.execute(
                "VACUUM INTO ?1",
                [tmp.path().to_str().context("invalid snapshot path")?],
            )?;
            drop(c);
            let mut copy = Connection::open(tmp.path())?;
            copy.pragma_update(None, "secure_delete", true)?;
            let tx = copy.transaction()?;
            // Node-private facts, receipt queues and terminal state stay on the old executor.
            tx.execute_batch("DROP TABLE IF EXISTS handover_manifest;
                CREATE TABLE handover_manifest(workspace_id TEXT, source TEXT, destination TEXT, term INTEGER);
                DROP TABLE IF EXISTS handover_activation;
                CREATE TABLE handover_activation(requires_confirmation INTEGER NOT NULL);
                DELETE FROM authority_private; DELETE FROM authority_reservation;
                DELETE FROM outbox_events; DELETE FROM service_claims; DELETE FROM service_peers;
                DELETE FROM service_leases; DELETE FROM workspace_facts;
                UPDATE runs SET s0_log_local_path=NULL;
                UPDATE runs SET observed_state='unconfirmed' WHERE observed_state IN ('running','blocked','dispatch_pending');")?;
            tx.execute(
                "INSERT INTO handover_manifest VALUES(?,?,?,?)",
                params![id, identity.node_id, to, new_term],
            )?;
            // The destination has its own sender sequence. Keep the receiver frontiers,
            // signatures and original execution claim bindings for reconciliation.
            for table in [
                "service_s1_delivery",
                "service_assignment_pages",
                "service_s1_sequence",
                "service_s1_journal",
                "service_launch_permits",
                "service_receipt_drafts",
                "service_proposals",
                "service_telemetry",
            ] {
                let exists: bool = tx.query_row(
                    "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?)",
                    [table],
                    |r| r.get(0),
                )?;
                if exists {
                    tx.execute(&format!("DELETE FROM {table}"), [])?;
                }
            }
            tx.execute(
                "DELETE FROM sqlite_sequence WHERE name='service_s1_sequence'",
                [],
            )?;
            tx.execute_batch(
                "CREATE TABLE IF NOT EXISTS service_restore_sources(node TEXT PRIMARY KEY);
                CREATE TABLE IF NOT EXISTS service_restore_nonexecutors(node TEXT PRIMARY KEY);
                DELETE FROM service_restore_sources; DELETE FROM service_restore_nonexecutors;",
            )?;
            let config: toml::Table =
                fs::read_to_string(ws.root.join("workspace.toml"))?.parse()?;
            if let Some(peers) = config.get("peers").and_then(|p| p.as_array()) {
                for peer in peers {
                    let node = peer
                        .get("node_id")
                        .and_then(|n| n.as_str())
                        .context("invalid paired node in backup")?;
                    if node != to {
                        tx.execute(
                            "INSERT OR IGNORE INTO service_restore_sources VALUES(?)",
                            [node],
                        )?;
                    }
                }
            }
            if identity.role_mode == "supervisor_only" {
                tx.execute(
                    "INSERT INTO service_restore_nonexecutors VALUES(?)",
                    [&identity.node_id],
                )?;
            } else {
                tx.execute(
                    "INSERT OR IGNORE INTO service_restore_sources VALUES(?)",
                    [&identity.node_id],
                )?;
            }
            tx.execute("INSERT INTO handover_activation VALUES(?)", [backup])?;
            authority::remember(&tx, &grant)?;
            tx.execute(
                "INSERT INTO authority_private VALUES(1,?,?,?)",
                params![to, new_term, successor_secret],
            )?;
            tx.commit()?;
            copy.execute_batch("PRAGMA journal_mode=DELETE; VACUUM;")?;
            drop(copy);
            tmp.as_file().sync_all()?;
            // Demote before publishing a snapshot that another host can activate.
            if !backup {
                set_config(&ws, to, new_term, "executor_only")?;
            }
            if let Err(e) = tmp.persist_noclobber(snapshot) {
                if !backup {
                    set_config(
                        &ws,
                        &identity.supervisor_node_id,
                        identity.supervisor_term,
                        &identity.role_mode,
                    )?;
                }
                return Err(e.error.into());
            }
            #[cfg(unix)]
            File::open(parent)?.sync_all()?;
            println!(
                "Saved {id} ledger for {to} at term {new_term} to {}",
                snapshot.display()
            );
        }
        Operation::Restore {
            snapshot,
            confirm_old_offline,
        } => {
            ensure!(
                !db.exists(),
                "restore into a freshly initialized workspace to preserve existing node state"
            );
            let from = Connection::open_with_flags(snapshot, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            let (space, dest, term): (String, String, i64) = from.query_row(
                "SELECT workspace_id,destination,term FROM handover_manifest",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )?;
            ensure!(
                space == id && dest == identity.node_id && term >= identity.supervisor_term,
                "snapshot is for a different workspace, node or term"
            );
            let has_activation:bool=from.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='handover_activation')",[],|r|r.get(0))?;
            if has_activation {
                let confirmation: bool = from.query_row(
                    "SELECT requires_confirmation FROM handover_activation",
                    [],
                    |r| r.get(0),
                )?;
                ensure!(
                    !confirmation || *confirm_old_offline,
                    "recovery backup requires --confirm-old-offline"
                );
            }
            let check: String = from.query_row("PRAGMA quick_check", [], |r| r.get(0))?;
            ensure!(check == "ok", "snapshot integrity check failed");
            let tmp = NamedTempFile::new_in(&ws.root)?;
            from.execute(
                "VACUUM INTO ?1",
                [tmp.path().to_str().context("invalid import path")?],
            )?;
            let imported = Connection::open(tmp.path())?;
            imported.execute_batch("CREATE TABLE IF NOT EXISTS service_restore_barrier(singleton INTEGER PRIMARY KEY); INSERT OR REPLACE INTO service_restore_barrier VALUES(1);")?;
            drop(imported);
            tmp.as_file().sync_all()?;
            // If interrupted before installing the ledger a startup must fail closed.
            // Install the copy first, retaining its old epoch until configuration is switched.
            tmp.persist_noclobber(&db)?;
            if let Err(e) = set_config(&ws, &dest, term, role(&ws, &dest)) {
                fs::remove_file(&db)?;
                return Err(e);
            }
            println!("Restored {id} on {dest} at term {term}");
        }
    };
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restore_preserves_local_executor_capability_unless_supervisor_only() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let old_home = dir.path().join("old");
        let old = Workspace::init(&old_home, "work", "chief", "supervisor_only", "chief")?;
        {
            let mut db = Connection::open(old.root.join("state.db"))?;
            db.execute_batch(authority::SCHEMA)?;
            db.execute_batch(
                "CREATE TABLE outbox_events(id INTEGER PRIMARY KEY);
                 CREATE TABLE service_claims(run_id TEXT PRIMARY KEY);
                 CREATE TABLE service_peers(node_id TEXT PRIMARY KEY);
                 CREATE TABLE service_leases(nonce TEXT PRIMARY KEY);
                 CREATE TABLE workspace_facts(key TEXT PRIMARY KEY);
                 CREATE TABLE runs(run_id TEXT PRIMARY KEY, observed_state TEXT, s0_log_local_path TEXT);
                 CREATE TABLE service_s1_sequence(seq INTEGER PRIMARY KEY AUTOINCREMENT);
                 INSERT INTO service_s1_sequence DEFAULT VALUES;",
            )?;
            authority::ensure_signer(&mut db, &old.config.workspace)?;
        }
        let snap_exec = dir.path().join("to-exec.db");
        run(
            &old_home,
            "work",
            &Operation::Handover {
                to: "desk".into(),
                snapshot: snap_exec.clone(),
            },
        )?;
        let desk_home = dir.path().join("desk");
        Workspace::init(
            &desk_home,
            "work",
            "desk",
            "supervisor_and_executor",
            "desk",
        )?;
        run(
            &desk_home,
            "work",
            &Operation::Restore {
                snapshot: snap_exec,
                confirm_old_offline: false,
            },
        )?;
        let restored = Workspace::open(&desk_home, "work")?;
        assert_eq!(
            restored.config.workspace.role_mode,
            "supervisor_and_executor"
        );
        assert_eq!(restored.config.workspace.supervisor_term, 2);

        let snap_only = dir.path().join("to-only.db");
        run(
            &desk_home,
            "work",
            &Operation::Backup {
                to: "pure".into(),
                snapshot: snap_only.clone(),
            },
        )?;
        let pure_home = dir.path().join("pure");
        Workspace::init(&pure_home, "work", "pure", "supervisor_only", "pure")?;
        run(
            &pure_home,
            "work",
            &Operation::Restore {
                snapshot: snap_only,
                confirm_old_offline: true,
            },
        )?;
        let pure = Workspace::open(&pure_home, "work")?;
        assert_eq!(pure.config.workspace.role_mode, "supervisor_only");
        Ok(())
    }
}
