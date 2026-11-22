//! Tarla V2.0 declarative `botflow.yml` graph, ROS 2 `Action` seven-state machine,
//! and SQLite-backed event router with debounce coalescing and hop/room circuit breakers.
pub mod router;
pub mod spec;
pub mod state;
pub mod yaml;

pub use router::{
    admit_room_round, claim_next_inbox, extract_bot_mentions, publish_event, EventEnvelope,
    InboxDelivery, PublishOutcome,
};
pub use spec::{
    command_basename, BotConstraints, BotDef, BotInputPort, BotflowNode, BotflowPolicy,
    BotflowSpec, HerdrCoreDef, NodeTemplateDef, QueuePolicy, ResolvedTarget,
};
pub use state::{
    is_terminal_run_state, ActionState, ObservedActionView, APPROVAL_HOLD_TIMEOUT_SECS,
};

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::BTreeSet;
    use std::path::Path;

    const SPEC_V02_YAML: &str = r#"
version: "0.2"
workspace: work
policy:
  max_event_hops: 6
  max_room_rounds: 3
  max_room_replies: 10
  result_gate_max_chars: 8000
  mission_packet_max_bytes: 7200
  run_receipt_max_bytes: 2000
nodes:
  iphone-pro:
    role: master_control
    platform: ios
  mac-studio:
    role: supervisor_and_executor
    herdr_cores:
      master_core:
        session: tarla-work-master_core
      code_core:
        session: tarla-work-code_core
      ops_core:
        session: tarla-work-ops_core
    templates:
      codex-master:
        core: master_core
        command: "/opt/homebrew/bin/codex"
        workdir: "/Users/collaalloc/Projects/Tarla"
      claude-arch:
        core: code_core
        command: "/opt/homebrew/bin/claude"
        env_profile: "claude-work-max"
        workdir: "/Users/collaalloc/Projects/Tarla"
      codex-impl:
        core: code_core
        command: "/opt/homebrew/bin/codex"
        env_profile: "codex-pro-main"
        workdir: "/Users/collaalloc/Projects/Tarla"
      gemini-rev:
        core: code_core
        command: "/opt/homebrew/bin/gemini"
        workdir: "/Users/collaalloc/Projects/Tarla"
bots:
  - id: chief
    name: "随身总控 Master"
    control_authority: iphone-pro
    targets: ["mac-studio/codex-master", "mac-studio/claude-arch"]
    idle_reap_secs: 1800
    inputs:
      user_intent:
        source: sys/user_message
        queue_policy: backpressure
      final_verdict:
        source: reviewer/approved
        queue_policy: backpressure
    outputs:
      - spec_ready
  - id: implementer
    name: "核心研发 Bot"
    targets: ["mac-studio/codex-impl", "mac-studio/claude-arch"]
    checkpoint_files: true
    verify_before_done: "mac-studio/gemini-rev"
    inputs:
      task_spec:
        source: chief/spec_ready
        queue_policy: backpressure
      fix_request:
        source: reviewer/changes_requested
        queue_policy: coalesce_latest
        debounce_ms: 2000
    outputs:
      - patch_ready
  - id: reviewer
    name: "异构代码审查 Bot"
    targets: ["mac-studio/gemini-rev", "mac-studio/claude-arch"]
    constraints:
      different_command_from_upstream: true
    inputs:
      patch:
        source: implementer/patch_ready
        queue_policy: coalesce_latest
        debounce_ms: 1500
    outputs:
      - approved
      - changes_requested
"#;

    #[test]
    fn parses_v02_botflow_yaml_and_enforces_heterogeneous_reviewer_command() {
        let spec = BotflowSpec::from_yaml_str(SPEC_V02_YAML).unwrap();
        assert_eq!(spec.bots.len(), 3);
        assert_eq!(spec.policy.max_event_hops, 6);

        // If gemini-rev is unhealthy/excluded and upstream used claude, reviewer cannot pick claude-arch.
        let excluded = BTreeSet::from(["mac-studio/gemini-rev".to_string()]);
        assert!(spec
            .select_target_for_bot("reviewer", Some("/opt/homebrew/bin/claude"), &excluded)
            .unwrap()
            .is_none());
        // If upstream used codex, claude-arch satisfies different_command_from_upstream.
        let chosen = spec
            .select_target_for_bot("reviewer", Some("/opt/homebrew/bin/codex"), &excluded)
            .unwrap()
            .unwrap();
        assert_eq!(chosen.template_name, "claude-arch");
    }

    #[test]
    fn coalesces_ten_rapid_patches_into_one_and_trips_sixth_hop_breaker() {
        let spec = BotflowSpec::from_yaml_str(SPEC_V02_YAML).unwrap();
        let mut db = tarla_memory::open(Path::new(":memory:"), "work").unwrap();

        for idx in 1..=10 {
            let (_, outcomes) = publish_event(
                &mut db,
                &spec,
                "implementer/patch_ready",
                Some("corr-1"),
                1,
                "hlc-1",
                json!({"iteration": idx}),
                10_000 + idx * 100,
            )
            .unwrap();
            assert_eq!(outcomes.len(), 1);
            if idx == 1 {
                assert!(matches!(outcomes[0], PublishOutcome::Queued { .. }));
            } else {
                assert!(matches!(
                    outcomes[0],
                    PublishOutcome::Coalesced {
                        coalesced_count,
                        ..
                    } if coalesced_count == idx
                ));
            }
        }

        // Before debounce_ms (1500ms after last publish at 11_000 -> 12_500) nothing is delivered.
        assert!(claim_next_inbox(&mut db, "reviewer", 12_000, false, false)
            .unwrap()
            .is_none());
        // Human interactive lease (Issue #75) also withholds delivery.
        assert!(claim_next_inbox(&mut db, "reviewer", 13_000, true, false)
            .unwrap()
            .is_none());

        let delivered = claim_next_inbox(&mut db, "reviewer", 13_000, false, false)
            .unwrap()
            .unwrap();
        assert_eq!(delivered.coalesced_count, 10);
        assert_eq!(delivered.payload["payload"]["iteration"], 10);
        assert!(claim_next_inbox(&mut db, "reviewer", 13_000, false, false)
            .unwrap()
            .is_none());

        // Hop 6 trips the circuit breaker immediately and writes an approval alert.
        let (_, breaker) = publish_event(
            &mut db,
            &spec,
            "reviewer/changes_requested",
            Some("corr-loop"),
            6,
            "hlc-6",
            json!({"reason": "infinite ping-pong"}),
            20_000,
        )
        .unwrap();
        assert!(matches!(
            breaker[0],
            PublishOutcome::TrippedHopBreaker { hop_count: 6, .. }
        ));
        assert!(
            claim_next_inbox(&mut db, "implementer", 30_000, false, false)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn ros2_seven_state_transitions_and_room_anti_loop_limits() {
        let mut s = ActionState::Accepted;
        assert!(s.transition(ActionState::Succeeded).is_err());
        s = s.on_osc_status("working", true).unwrap();
        assert_eq!(s, ActionState::Executing);
        s = s.on_osc_status("blocked", true).unwrap();
        assert_eq!(s, ActionState::BlockedApproval);
        s = s.transition(ActionState::Executing).unwrap();
        // With verify_before_done active, OSC `done` stays in Executing until verifier passes.
        assert_eq!(
            s.on_osc_status("done", true).unwrap(),
            ActionState::Executing
        );
        assert_eq!(
            s.on_osc_status("done", false).unwrap(),
            ActionState::Succeeded
        );
        assert!(ActionState::Succeeded
            .transition(ActionState::Aborted)
            .is_err());

        let spec = BotflowSpec::from_yaml_str(SPEC_V02_YAML).unwrap();
        let mut db = tarla_memory::open(Path::new(":memory:"), "work").unwrap();
        let all = vec!["chief".into(), "implementer".into(), "reviewer".into()];
        assert_eq!(
            admit_room_round(&mut db, &spec, "room-1", 1, "Build feature", &all, 1).unwrap(),
            all
        );
        // Round 2 without @mention wakes nobody.
        assert!(
            admit_room_round(&mut db, &spec, "room-1", 2, "Looks good!", &all, 2)
                .unwrap()
                .is_empty()
        );
        // Round 2 with @reviewer wakes only reviewer.
        assert_eq!(
            admit_room_round(
                &mut db,
                &spec,
                "room-1",
                2,
                "Please check @reviewer",
                &all,
                2
            )
            .unwrap(),
            vec!["reviewer".to_string()]
        );
        // Round 4 exceeds max_room_rounds = 3.
        assert!(admit_room_round(&mut db, &spec, "room-1", 4, "Ping @chief", &all, 4).is_err());
    }
}
