//! Deterministic workspace-scoped scheduling. The daemon performs final binding in its
//! durable claim transaction. A lost heartbeat is never a handoff boundary.
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AgentRole {
    Architect,
    Implementer,
    Reviewer,
    Explorer,
    Distiller,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BillingTier {
    PrepaidSubscription,
    FreeQuota,
    LocalZeroCost,
    PayAsYouGoApi,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotHealth {
    Healthy,
    CoolingDown { resets_at_unix: i64 },
    AuthExpired { message: String },
    NodeOffline,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AgentSlotSnapshot {
    pub workspace_id: String,
    pub node_id: String,
    pub slot_id: String,
    pub driver: String,
    pub model_family: String,
    #[serde(default)]
    pub account_id: String,
    pub supported_roles: Vec<AgentRole>,
    #[serde(default)]
    pub node_capabilities: Vec<String>,
    pub billing_tier: BillingTier,
    pub window_used_pct: f32,
    pub window_resets_at: Option<i64>,
    #[serde(default)]
    pub weekly_used_pct: f32,
    #[serde(default)]
    pub weekly_resets_at: Option<i64>,
    #[serde(default)]
    pub active_runs: u32,
    pub health: SlotHealth,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRequirement {
    pub workspace_id: String,
    pub role: AgentRole,
    #[serde(default)]
    pub required_caps: Vec<String>,
    pub exclude_model_family: Option<String>,
}

fn usage_at(used: f32, reset: Option<i64>, now: i64) -> Option<f32> {
    if !(0.0..=100.0).contains(&used) {
        return None;
    }
    Some(if reset.is_some_and(|r| r <= now) {
        0.0
    } else {
        used
    })
}

fn score(req: &TaskRequirement, slot: &AgentSlotSnapshot, now: i64) -> Option<i64> {
    let health_ok = match slot.health {
        SlotHealth::Healthy => true,
        SlotHealth::CoolingDown { resets_at_unix } => now >= resets_at_unix,
        _ => false,
    };
    let rolling = usage_at(slot.window_used_pct, slot.window_resets_at, now)?;
    let weekly = usage_at(slot.weekly_used_pct, slot.weekly_resets_at, now)?;
    if !health_ok
        || req.workspace_id != slot.workspace_id
        || rolling.max(weekly) >= 95.0
        || !req
            .required_caps
            .iter()
            .all(|cap| slot.node_capabilities.contains(cap))
        || req
            .exclude_model_family
            .as_ref()
            .is_some_and(|f| f == &slot.model_family)
    {
        return None;
    }
    let role = if slot.supported_roles.first() == Some(&req.role) {
        400
    } else if slot.supported_roles.contains(&req.role) {
        250
    } else {
        50
    };
    let cost = match slot.billing_tier {
        BillingTier::PrepaidSubscription => 300,
        BillingTier::LocalZeroCost => 260,
        BillingTier::FreeQuota => 220,
        BillingTier::PayAsYouGoApi => 80,
    };
    let urgency = slot
        .window_resets_at
        .filter(|r| *r > now)
        .map(|r| (168 - (r.saturating_sub(now) / 3600).min(168)) / 2)
        .unwrap_or(0);
    Some(
        role + cost + urgency + (100.0 - rolling.max(weekly)) as i64
            - 120 * i64::from(slot.active_runs),
    )
}

fn best<'a>(
    req: &TaskRequirement,
    slots: impl Iterator<Item = &'a AgentSlotSnapshot>,
    now: i64,
    prefer: impl Fn(&AgentSlotSnapshot) -> bool,
) -> Option<&'a AgentSlotSnapshot> {
    slots
        .filter_map(|s| Some((s, (prefer(s), score(req, s, now)?))))
        .max_by(|(a, sa), (b, sb)| {
            sa.cmp(sb)
                .then_with(|| (&b.node_id, &b.slot_id).cmp(&(&a.node_id, &a.slot_id)))
        })
        .map(|(s, _)| s)
}

pub fn select_best_slot<'a>(
    req: &TaskRequirement,
    slots: &'a [AgentSlotSnapshot],
    now: i64,
) -> Option<&'a AgentSlotSnapshot> {
    best(req, slots.iter(), now, |_| false)
}

/// Only a completed, acknowledged turn may be resumed on a different slot. The caller
/// persists the checkpoint + compare-and-swap of the old binding before starting it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnBoundary {
    Running,
    Unconfirmed,
    Checkpointed,
}

#[derive(Debug)]
pub struct Handoff<'a> {
    pub slot: &'a AgentSlotSnapshot,
    /// True when the destination can consume the same driver account's protected session data.
    pub resume_native: bool,
}

pub fn select_handoff<'a>(
    req: &TaskRequirement,
    old: &AgentSlotSnapshot,
    slots: &'a [AgentSlotSnapshot],
    boundary: TurnBoundary,
    now: i64,
) -> Option<Handoff<'a>> {
    if boundary != TurnBoundary::Checkpointed || req.workspace_id != old.workspace_id {
        return None;
    }
    let compatible = |s: &AgentSlotSnapshot| s.node_id == old.node_id && s.driver == old.driver;
    let slot = best(
        req,
        slots.iter().filter(|s| {
            (s.node_id.as_str(), s.slot_id.as_str()) != (old.node_id.as_str(), old.slot_id.as_str())
        }),
        now,
        compatible,
    )?;
    // Distinct subscription profiles have isolated session stores. Pass a bounded
    // handoff packet unless the destination really shares this account's session.
    let resume_native =
        compatible(slot) && !old.account_id.is_empty() && slot.account_id == old.account_id;
    Some(Handoff {
        slot,
        resume_native,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    fn slot(id: &str) -> AgentSlotSnapshot {
        AgentSlotSnapshot {
            workspace_id: "work".into(),
            node_id: "pc".into(),
            slot_id: id.into(),
            driver: "codex".into(),
            model_family: "gpt".into(),
            account_id: id.into(),
            supported_roles: vec![AgentRole::Implementer],
            node_capabilities: vec!["gpu".into()],
            billing_tier: BillingTier::PrepaidSubscription,
            window_used_pct: 10.,
            window_resets_at: Some(1000),
            weekly_used_pct: 20.,
            weekly_resets_at: None,
            active_runs: 0,
            health: SlotHealth::Healthy,
        }
    }
    fn req() -> TaskRequirement {
        TaskRequirement {
            workspace_id: "work".into(),
            role: AgentRole::Implementer,
            required_caps: vec!["gpu".into()],
            exclude_model_family: None,
        }
    }
    #[test]
    fn isolates_and_orders() {
        let mut other = slot("0");
        other.workspace_id = "life".into();
        let mut full = slot("1");
        full.weekly_used_pct = 99.;
        let slots = [other, full, slot("b"), slot("a")];
        assert_eq!(select_best_slot(&req(), &slots, 0).unwrap().slot_id, "a");
        let mut reviewer = req();
        reviewer.exclude_model_family = Some("gpt".into());
        assert!(select_best_slot(&reviewer, &slots, 0).is_none());
    }
    #[test]
    fn cooldown_and_invalid_metrics() {
        let mut s = slot("a");
        s.health = SlotHealth::CoolingDown {
            resets_at_unix: 1000,
        };
        s.window_used_pct = 99.;
        assert!(select_best_slot(&req(), &[s.clone()], 999).is_none());
        assert!(select_best_slot(&req(), &[s.clone()], 1000).is_some());
        s.weekly_used_pct = f32::NAN;
        assert!(select_best_slot(&req(), &[s], 1000).is_none());
    }
    #[test]
    fn checkpoint_required() {
        let old = slot("old");
        let alternatives = [slot("new")];
        assert!(
            select_handoff(&req(), &old, &alternatives, TurnBoundary::Unconfirmed, 0).is_none()
        );
        let out =
            select_handoff(&req(), &old, &alternatives, TurnBoundary::Checkpointed, 0).unwrap();
        assert_eq!(out.slot.slot_id, "new");
        assert!(!out.resume_native);
    }
}
