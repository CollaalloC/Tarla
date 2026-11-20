//! Restore only the confirmed native transcript inside another local CLI profile.
//! Account credentials and other conversations never participate in this transfer.
use super::*;
use std::fs;
use std::io::ErrorKind;

pub(super) fn session(s: &Service, job: &Value, dest: &fleet::Slot) -> Result<Option<String>> {
    let db = s.db()?;
    let parent=db.query_row("SELECT r.run_id,r.target_slot_id,r.native_session_ref FROM service_resume_origins o
        JOIN runs r ON r.run_id=o.predecessor WHERE o.run_id=? AND r.conversation_id=? AND r.target_node_id=?
        AND r.observed_state IN ('failed','succeeded') AND r.native_session_ref IS NOT NULL",
        params![string(job,"run_id")?,string(job,"conversation_id")?,s.node()],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?))).optional()?;
    let Some((run, old_slot, session)) = parent else {
        return Ok(None);
    };
    let Some(old) = s
        .slots
        .iter()
        .find(|slot| slot.snapshot.slot_id == old_slot)
    else {
        return Ok(None);
    };
    if old.snapshot.driver != dest.snapshot.driver
        || old.workdir != dest.workdir
        || Uuid::parse_str(&session).is_err()
    {
        return Ok(None);
    }
    let old_plan = metadata_path(&s.ws.root, &run).with_extension("launch");
    let Ok(plan) = fs::read(old_plan) else {
        return Ok(None);
    };
    let plan: Launch = serde_json::from_slice(&plan)?;
    if plan.driver != dest.snapshot.driver || plan.workdir.as_ref() != Some(&dest.workdir) {
        return Ok(None);
    }
    let slots = s.home.join("slots").canonicalize()?;
    let src = slots.join(&old_slot);
    let dst = slots.join(&dest.snapshot.slot_id);
    let root = if dest.snapshot.driver == "codex" {
        "sessions"
    } else {
        "projects"
    };
    if src.canonicalize()? != src || dst.canonicalize()? != dst {
        return Ok(None);
    }
    let mut stack = vec![PathBuf::from(root)];
    let mut count = 0;
    while let Some(rel) = stack.pop() {
        let dir = src.join(&rel);
        if !dir.symlink_metadata().is_ok_and(|m| m.is_dir()) {
            continue;
        }
        for entry in fs::read_dir(dir)? {
            count += 1;
            if count > 20000 {
                return Ok(None);
            }
            let entry = entry?;
            let ty = entry.file_type()?;
            let sub = rel.join(entry.file_name());
            if ty.is_dir() && sub.components().count() < 6 {
                stack.push(sub);
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let matches = if root == "sessions" {
                name.starts_with("rollout-") && name.ends_with(&format!("-{session}.jsonl"))
            } else {
                name == format!("{session}.jsonl")
            };
            if ty.is_file() && matches && export(&src, &dst, &sub)? {
                return Ok(Some(session));
            }
        }
    }
    Ok(None)
}

fn export(src: &Path, dst: &Path, rel: &Path) -> Result<bool> {
    let source = src.join(rel);
    if source.canonicalize()? != source {
        return Ok(false);
    }
    let mut dir = dst.to_owned();
    for part in rel
        .parent()
        .context("missing native transcript directory")?
    {
        dir.push(part);
        match fs::create_dir(&dir) {
            Ok(()) => (),
            Err(e) if e.kind() == ErrorKind::AlreadyExists => (),
            Err(e) => return Err(e.into()),
        }
        if !dir.symlink_metadata()?.is_dir() {
            return Ok(false);
        }
    }
    let target = dst.join(rel);
    let mut tmp = tempfile::NamedTempFile::new_in(&dir)?;
    std::io::copy(&mut fs::File::open(&source)?, &mut tmp)?;
    tmp.as_file().sync_all()?;
    match tmp.persist_noclobber(&target) {
        Ok(_) => Ok(true),
        Err(e) if e.error.kind() == ErrorKind::AlreadyExists => {
            if !target.symlink_metadata()?.is_file()
                || target.metadata()?.len() != source.metadata()?.len()
            {
                return Ok(false);
            }
            let digest = |path: &Path| -> Result<_> {
                let mut hash = Sha256::new();
                std::io::copy(&mut fs::File::open(path)?, &mut hash)?;
                Ok(hash.finalize())
            };
            Ok(digest(&source)? == digest(&target)?)
        }
        Err(e) => Err(e.error.into()),
    }
}
