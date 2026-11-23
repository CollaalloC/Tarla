use anyhow::{ensure, Result};
use serde::Serialize;
use tarla_protocol::{MissionPacket, RunReceipt};

/// Wire-size estimate including UTF-8 and JSON framing, independent of provider tokenizers.
pub const BYTES_PER_TOKEN: usize = 4;

const MAX_RECEIPT_TOKENS: usize = 500;
pub const MAX_RECEIPT_BYTES: usize = MAX_RECEIPT_TOKENS * BYTES_PER_TOKEN;

fn size(value: &impl Serialize) -> Result<usize> {
    Ok(serde_json::to_vec(value)?.len())
}

/// Copy whole decisions and whole memory entries while the serialized packet fits.
/// The objective, acceptance criteria and fact keys must fit in their entirety.
pub fn bound_mission(mut packet: MissionPacket, max_tokens: usize) -> Result<MissionPacket> {
    let budget = max_tokens.min(1800).saturating_mul(BYTES_PER_TOKEN);
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

/// Preserve identity/state/S0 pointer, reserve room for up to the first two complete
/// artifact references, fit the executive summary, and append any remaining artifacts.
pub fn bound_receipt(mut receipt: RunReceipt, max_tokens: usize) -> Result<RunReceipt> {
    let budget = max_tokens
        .min(MAX_RECEIPT_TOKENS)
        .saturating_mul(BYTES_PER_TOKEN);
    ensure!(
        !receipt.run_id.trim().is_empty() && !receipt.s0_transcript_pointer.trim().is_empty(),
        "receipt requires a run id and local transcript pointer"
    );
    ensure!(
        matches!(
            receipt.final_state.as_str(),
            "succeeded"
                | "failed"
                | "cancelled"
                | "canceled"
                | "aborted"
                | "blocked"
                | "blocked_approval"
        ),
        "unknown receipt state"
    );
    let summary = std::mem::take(&mut receipt.executive_summary);
    ensure!(!summary.trim().is_empty(), "receipt requires a summary");
    let artifacts: Vec<String> = std::mem::take(&mut receipt.artifact_refs)
        .into_iter()
        .filter(|a| !a.trim().is_empty())
        .collect();
    let base_cost = size(&receipt)?;
    ensure!(base_cost < budget, "required receipt fields exceed budget");
    let mark = "…";
    let full_summary_cost = size(&summary)? - 2;
    let first_ch = summary.chars().next().expect("non-empty summary");
    let min_summary_cost = full_summary_cost.min(size(&first_ch)? - 2 + mark.len());
    ensure!(
        budget - base_cost >= min_summary_cost,
        "no room for bounded receipt summary"
    );

    // Reserve guaranteed space for up to 2 artifact references so a verbose summary
    // never drops primary deliverable paths, while keeping room for a readable summary.
    let summary_floor = min_summary_cost.max((budget - base_cost) / 2);
    let mut reserved_budget = (budget - base_cost).saturating_sub(summary_floor);
    let mut deferred = Vec::new();
    for artifact in artifacts {
        if receipt.artifact_refs.len() < 2 {
            let cost = size(&artifact)? + usize::from(!receipt.artifact_refs.is_empty());
            if cost <= reserved_budget {
                receipt.artifact_refs.push(artifact);
                reserved_budget -= cost;
                continue;
            }
        }
        deferred.push(artifact);
    }

    let space = budget
        .checked_sub(size(&receipt)?)
        .ok_or_else(|| anyhow::anyhow!("required receipt fields exceed budget"))?;
    let prefix = super::clip(&summary, space);
    if prefix.len() == summary.len() && size(&prefix)? - 2 <= space {
        receipt.executive_summary = summary;
    } else {
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
    for artifact in deferred {
        if artifact.len() > remaining {
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
        let bounded = bound_mission(packet.clone(), 60)?;
        assert!(size(&bounded)? <= 60 * BYTES_PER_TOKEN);
        assert_eq!(bounded.acceptance_criteria, packet.acceptance_criteria);
        assert!(bounded.distilled_context.len() <= 5);
        assert!(bound_mission(packet, 5).is_err());
        let receipt = RunReceipt {
            run_id: "r".into(),
            final_state: "failed".into(),
            executive_summary: "文件 \"x\"\n".repeat(100),
            artifact_refs: vec![
                "results/report.csv".into(),
                "results/diff.patch".into(),
                "results/extra.log".into(),
            ],
            s0_transcript_pointer: "node:n/run:r/transcript.jsonl".into(),
        };
        let bounded = bound_receipt(receipt, 80)?;
        assert!(size(&bounded)? <= 80 * BYTES_PER_TOKEN);
        assert!(bounded.executive_summary.ends_with('…'));
        assert_eq!(
            bounded.artifact_refs,
            vec!["results/report.csv", "results/diff.patch"]
        );
        assert_eq!(
            bounded.s0_transcript_pointer,
            "node:n/run:r/transcript.jsonl"
        );
        Ok(())
    }

    #[test]
    fn chinese_receipt_summary_and_primary_artifacts_fit_default_500_token_budget() -> Result<()> {
        let chinese_summary =
            "已完成潮汐校准模块编译与回归验证，所有十六项传感器边界测试全部通过。".repeat(8);
        assert!(chinese_summary.chars().count() >= 250);
        let receipt = RunReceipt {
            run_id: "0193a5e0-1234-7000-8000-abcdef012345".into(),
            final_state: "succeeded".into(),
            executive_summary: chinese_summary.clone(),
            artifact_refs: vec![
                "artifacts/calibration-summary.json".into(),
                "artifacts/regression.diff".into(),
            ],
            s0_transcript_pointer:
                "node:office-mini/run:0193a5e0-1234-7000-8000-abcdef012345/transcript.jsonl".into(),
        };
        let bounded = bound_receipt(receipt, 500)?;
        assert!(size(&bounded)? <= 500 * BYTES_PER_TOKEN);
        assert_eq!(bounded.executive_summary, chinese_summary);
        assert_eq!(bounded.artifact_refs.len(), 2);
        Ok(())
    }
}
