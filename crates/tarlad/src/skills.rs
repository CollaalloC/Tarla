//! User-selected instructions apply to one input and its delegated mission.
//! Discovery reads directory metadata. Replays use the original skill revision.
use super::*;
use rusqlite::{params, OptionalExtension};

pub struct Selection {
    pub task: String,
    pub instructions: String,
}
impl Selection {
    pub fn cost(&self) -> usize {
        tarla_context::estimate("user", &self.instructions)
    }
}
pub fn select(
    ws: &Workspace,
    c: &Connection,
    source: &str,
    input: &str,
) -> Result<Option<Selection>> {
    let text = input.trim();
    let (command, rest) = text.split_once(char::is_whitespace).unwrap_or((text, ""));
    if command != "/skill" {
        return Ok(None);
    }
    let (name, task) = rest
        .trim()
        .split_once(char::is_whitespace)
        .context("usage: /skill <name> <task>")?;
    let task = task.trim();
    if task.is_empty() {
        bail!("usage: /skill <name> <task>")
    }
    tarla_workspace::validate_name(name)?;
    let prior: Option<(String, String)> = c
        .query_row(
            "SELECT name,instructions FROM service_skill_selections WHERE source=?",
            [source],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let instructions = if let Some((old, value)) = prior {
        if old != name {
            bail!("message source already selected another skill")
        }
        value
    } else {
        let file = ws.load_skill(name)?;
        if file.content.trim().is_empty() {
            bail!("selected skill is empty")
        }
        let instructions = format!(
            "Workspace skill selected by the user for this task ({name}):\n{}",
            file.content
        );
        c.execute(
            "INSERT INTO service_skill_selections VALUES(?,?,?)",
            params![source, name, instructions],
        )?;
        instructions
    };
    Ok(Some(Selection {
        task: task.into(),
        instructions,
    }))
}
pub fn available(ws: &Workspace) -> Result<String> {
    let names = ws.skill_names()?;
    Ok(if names.is_empty() {
        "No skills are installed in this workspace.".into()
    } else {
        names.join("\n")
    })
}
