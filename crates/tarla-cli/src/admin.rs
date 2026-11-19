//! Explicit authority changes. All paths hold the same exclusion lock as the daemon.
use anyhow::{ensure, Context, Result};
use clap::Subcommand;
use fs2::FileExt;
use rusqlite::{params, Connection, OpenFlags};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};
use tarla_workspace::{validate_name, Workspace};
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
    /// Import a handover ledger into a freshly initialized supervisor on its designated node.
    Restore {
        #[arg(long)]
        snapshot: PathBuf,
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
fn set_config(ws: &Workspace, supervisor: &str, term: i64, role: &str) -> Result<()> {
    let path = ws.root.join("workspace.toml");
    let mut cfg: toml::Value = fs::read_to_string(&path)?.parse()?;
    let ident = cfg
        .get_mut("workspace")
        .and_then(toml::Value::as_table_mut)
        .context("invalid identity")?;
    ident.insert("supervisor_node_id".into(), supervisor.into());
    ident.insert("supervisor_term".into(), term.into());
    ident.insert("role_mode".into(), role.into());
    let mut tmp = NamedTempFile::new_in(&ws.root)?;
    tmp.write_all(toml::to_string_pretty(&cfg)?.as_bytes())?;
    tmp.as_file().sync_all()?;
    tmp.persist(path)?;
    #[cfg(unix)]
    File::open(&ws.root)?.sync_all()?;
    Ok(())
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
        Operation::Handover { to, snapshot } => {
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
            let c = Connection::open_with_flags(&db, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            c.execute(
                "VACUUM INTO ?1",
                [tmp.path().to_str().context("invalid snapshot path")?],
            )?;
            drop(c);
            let mut copy = Connection::open(tmp.path())?;
            let tx = copy.transaction()?;
            // Node-private facts, receipt queues and terminal state stay on the old executor.
            tx.execute_batch("DROP TABLE IF EXISTS handover_manifest;
                CREATE TABLE handover_manifest(workspace_id TEXT, source TEXT, destination TEXT, term INTEGER);
                DELETE FROM outbox_events; DELETE FROM service_claims; DELETE FROM service_peers;
                DELETE FROM service_leases; DELETE FROM workspace_facts;
                UPDATE runs SET s0_log_local_path=NULL;
                UPDATE runs SET observed_state='unconfirmed' WHERE observed_state IN ('running','blocked','dispatch_pending');")?;
            tx.execute(
                "INSERT INTO handover_manifest VALUES(?,?,?,?)",
                params![id, identity.node_id, to, new_term],
            )?;
            tx.commit()?;
            copy.execute_batch("PRAGMA journal_mode=DELETE; VACUUM;")?;
            drop(copy);
            tmp.as_file().sync_all()?;
            // Demote before publishing a snapshot that another host can activate.
            set_config(&ws, to, new_term, "executor_only")?;
            if let Err(e) = tmp.persist_noclobber(snapshot) {
                set_config(
                    &ws,
                    &identity.supervisor_node_id,
                    identity.supervisor_term,
                    &identity.role_mode,
                )?;
                return Err(e.error.into());
            }
            #[cfg(unix)]
            File::open(parent)?.sync_all()?;
            println!(
                "Frozen {id} at term {new_term}; restore {} on {to}",
                snapshot.display()
            );
        }
        Operation::Restore { snapshot } => {
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
            let check: String = from.query_row("PRAGMA quick_check", [], |r| r.get(0))?;
            ensure!(check == "ok", "snapshot integrity check failed");
            let tmp = NamedTempFile::new_in(&ws.root)?;
            from.execute(
                "VACUUM INTO ?1",
                [tmp.path().to_str().context("invalid import path")?],
            )?;
            tmp.as_file().sync_all()?;
            // If interrupted before installing the ledger a startup must fail closed.
            // Install the copy first, retaining its old epoch until configuration is switched.
            tmp.persist_noclobber(&db)?;
            if let Err(e) = set_config(&ws, &dest, term, "supervisor_only") {
                fs::remove_file(&db)?;
                return Err(e);
            }
            println!("Restored {id} on {dest} at term {term}");
        }
    };
    Ok(())
}
