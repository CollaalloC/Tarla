use anyhow::{ensure, Result};
use serde::Serialize;
use tarla_protocol::{MissionPacket, RunReceipt};

fn size(value: &impl Serialize) -> Result<usize> {
    Ok(serde_json::to_vec(value)?.len())
}

/// Copy whole decisions and whole memory entries while the serialized packet fits.
/// The objective, acceptance criteria and fact keys must fit in their entirety.
pub fn bound_mission(mut packet: MissionPacket, budget: usize) -> Result<MissionPacket> {
    let budget = budget.min(1800);
    ensure!(
        !packet.run_id.trim().is_empty()
            && !packet.workspace_id.trim().is_empty()
            && !packet.objective.trim().is_empty(),
        "mission identity and objective required"
    );
    let context = std::mem::take(&mut packet.distilled_context);
    let memory = std::mem::take(&mut packet.project_memory_snapshot);
    let mut remaining = budget
        .checked_sub(size(&packet)?)
        .ok_or_else(|| anyhow::anyhow!("required mission fields exceed budget"))?;
    for decision in context {
        if packet.distilled_context.len() == 5 {
            break;
        }
        if decision.trim().is_empty() || decision.len() > remaining {
            continue;
        }
        let cost = size(&decision)? + usize::from(!packet.distilled_context.is_empty());
        if cost <= remaining {
            packet.distilled_context.push(decision);
            remaining -= cost;
        }
    }
    for entry in memory.lines() {
        if entry.trim().is_empty() || entry.len() > remaining {
            continue;
        }
        let separator_cost = if packet.project_memory_snapshot.is_empty() {
            0
        } else {
            2
        }; // JSON \n
        let cost = size(&entry)? - 2 + separator_cost;
        if cost <= remaining {
            if separator_cost > 0 {
                packet.project_memory_snapshot.push('\n');
            }
            packet.project_memory_snapshot.push_str(entry);
            remaining -= cost;
        }
    }
    Ok(packet)
}

/// Preserve identity/state/S0 pointer, fit the executive summary first and add
/// only complete artifact references. Counts escaped quotes, newlines and UTF-8.
pub fn bound_receipt(mut receipt: RunReceipt, budget: usize) -> Result<RunReceipt> {
    let budget = budget.min(500);
    ensure!(
        !receipt.run_id.trim().is_empty() && !receipt.s0_transcript_pointer.trim().is_empty(),
        "receipt requires a run id and local transcript pointer"
    );
    ensure!(
        matches!(
            receipt.final_state.as_str(),
            "succeeded" | "failed" | "cancelled" | "blocked"
        ),
        "unknown receipt state"
    );
    let summary = std::mem::take(&mut receipt.executive_summary);
    ensure!(!summary.trim().is_empty(), "receipt requires a summary");
    let artifacts = std::mem::take(&mut receipt.artifact_refs);
    let space = budget
        .checked_sub(size(&receipt)?)
        .ok_or_else(|| anyhow::anyhow!("required receipt fields exceed budget"))?;
    let prefix = super::clip(&summary, space);
    if prefix.len() == summary.len() && size(&prefix)? - 2 <= space {
        receipt.executive_summary = summary;
    } else {
        let mark = "…";
        ensure!(space > mark.len(), "no room for bounded receipt summary");
        let mut remaining = space - mark.len();
        for ch in prefix.chars() {
            let cost = size(&ch)? - 2;
            if cost > remaining {
                break;
            }
            receipt.executive_summary.push(ch);
            remaining -= cost;
        }
        ensure!(
            !receipt.executive_summary.is_empty(),
            "no room for bounded receipt summary"
        );
        receipt.executive_summary.push_str(mark);
    }
    let mut remaining = budget - size(&receipt)?;
    for artifact in artifacts {
        if artifact.trim().is_empty() || artifact.len() > remaining {
            continue;
        }
        let cost = size(&artifact)? + usize::from(!receipt.artifact_refs.is_empty());
        if cost <= remaining {
            receipt.artifact_refs.push(artifact);
            remaining -= cost;
        }
    }
    Ok(receipt)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn escaped_utf8_and_structural_fields_count_toward_budget() -> Result<()> {
        let packet = MissionPacket {
            run_id: "r".into(),
            workspace_id: "w".into(),
            objective: "Read RFC".into(),
            acceptance_criteria: vec!["full criterion".into()],
            distilled_context: vec![
                "x".repeat(3000),
                "选定路径".into(),
                "a".into(),
                "b".into(),
                "c".into(),
                "d".into(),
                "e".into(),
            ],
            project_memory_snapshot: "\nquoted \"keys\"\n".repeat(80),
            required_fact_keys: vec!["checkout".into()],
        };
        let bounded = bound_mission(packet.clone(), 240)?;
        assert!(size(&bounded)? <= 240);
        assert_eq!(bounded.acceptance_criteria, packet.acceptance_criteria);
        assert!(bounded.distilled_context.len() <= 5);
        assert!(bound_mission(packet, 20).is_err());
        let receipt = RunReceipt {
            run_id: "r".into(),
            final_state: "failed".into(),
            executive_summary: "文件 \"x\"\n".repeat(100),
            artifact_refs: vec!["https://example.invalid/a".into()],
            s0_transcript_pointer: "node:n/run:r/transcript.jsonl".into(),
        };
        let bounded = bound_receipt(receipt, 240)?;
        assert!(size(&bounded)? <= 240);
        assert!(bounded.executive_summary.ends_with('…'));
        assert_eq!(
            bounded.s0_transcript_pointer,
            "node:n/run:r/transcript.jsonl"
        );
        Ok(())
    }
}
