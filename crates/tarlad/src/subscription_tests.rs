use super::*;
use std::{fs, os::unix::fs::PermissionsExt};
use tarla_supervisor::{backends_from_config_with_slots, BackendConfig, BrainError};

#[tokio::test]
async fn registered_subscription_pool_rereads_quota_health_runs_and_account_lock() {
    let tmp = tempfile::tempdir().unwrap();
    let exe = tmp.path().join("provider");
    fs::write(
        &exe,
        r#"#!/bin/sh
set -eu
test "$1" = "-p"
test "$PWD" != "$CLAUDE_CONFIG_DIR"
cat >/dev/null
account=${CLAUDE_CONFIG_DIR##*/}
printf '%s\n' "{\"result\":\"{\\\"text\\\":\\\"$account\\\"}\"}"
"#,
    )
    .unwrap();
    fs::set_permissions(&exe, fs::Permissions::from_mode(0o700)).unwrap();
    let trap = tmp.path().join("must-never-start");
    let marker = tmp.path().join("ran-unsupported-or-paid");
    fs::write(&trap, format!("#!/bin/sh\ntouch '{}'\n", marker.display())).unwrap();
    fs::set_permissions(&trap, fs::Permissions::from_mode(0o700)).unwrap();
    let mut slots = vec![];
    for (id, driver, used, billing) in [
        ("codex", "codex", 0, "prepaid_subscription"),
        ("gemini", "gemini-cli", 0, "free_quota"),
        ("paid", "claude", 0, "pay_as_you_go_api"),
        ("one", "claude", 10, "prepaid_subscription"),
        ("two", "claude_code", 20, "prepaid_subscription"),
    ] {
        tarla_sidecar::credential_environment(tmp.path(), id, driver).unwrap();
        let executable = if id == "one" || id == "two" {
            &exe
        } else {
            &trap
        };
        slots.push(Slot {
            snapshot: serde_json::from_value(json!({"workspace_id":"w", "node_id":"local", "slot_id":id,
                "driver":driver, "model_family":driver, "supported_roles":[], "billing_tier":billing,
                "health":"healthy", "window_used_pct":used})).unwrap(),
            argv: vec![executable.display().to_string(), "--executor-only-argument".into()],
            workdir: tmp.path().to_owned(),
        });
    }
    let path = tmp.path().join("state.db");
    let db = tarla_memory::open(&path, "w").unwrap();
    db.execute_batch(MIGRATIONS).unwrap();
    let registry = subscription_registry(tmp.path(), &path, &slots).unwrap();
    let config = BTreeMap::from([(
        "group".into(),
        BackendConfig::SlotPool {
            slots: slots.iter().map(|s| s.snapshot.slot_id.clone()).collect(),
        },
    )]);
    let pools =
        backends_from_config_with_slots(&["slot_borrow:group".into()], &config, Some(&registry))
            .unwrap();
    let decide = || pools[0].generate_turn(&[], &[]);
    assert_eq!(decide().await.unwrap().text, "one");
    db.execute(
        "INSERT INTO service_telemetry VALUES ('one','{\"weekly_used_pct\":70}',0)",
        [],
    )
    .unwrap();
    assert_eq!(decide().await.unwrap().text, "two");
    let lock = tarla_sidecar::try_lock_slot(tmp.path(), "two").unwrap();
    assert_eq!(decide().await.unwrap().text, "one");
    drop(lock);
    db.execute("INSERT INTO runs(run_id,workspace_id,conversation_id,supervisor_term,target_node_id,target_slot_id,desired_state,observed_state,mission_packet_json,created_at,updated_at) VALUES ('r','w','c',1,'local','two','running','running','{}',0,0)", []).unwrap();
    assert_eq!(decide().await.unwrap().text, "one");
    db.execute("UPDATE runs SET observed_state='succeeded'", [])
        .unwrap();
    db.execute("INSERT INTO service_telemetry VALUES ('two','{\"health\":{\"auth_expired\":{\"message\":\"reauth\"}}}',0)", []).unwrap();
    assert_eq!(decide().await.unwrap().text, "one");
    db.execute(
        "UPDATE service_telemetry SET report='{\"window_used_pct\":99}' WHERE slot_id='one'",
        [],
    )
    .unwrap();
    assert!(
        matches!(decide().await, Err(BrainError::Unavailable(message)) if message.contains("codex") && message.contains("gemini"))
    );
    assert!(!marker.exists());

    let single = backends_from_config_with_slots(
        &["slot_borrow:one".into()],
        &BTreeMap::new(),
        Some(&registry),
    )
    .unwrap();
    db.execute("UPDATE service_telemetry SET report='{\"window_used_pct\":99,\"window_resets_at\":1}' WHERE slot_id='one'",[]).unwrap();
    assert_eq!(single[0].generate_turn(&[], &[]).await.unwrap().text, "one");
    assert!(backends_from_config_with_slots(
        &["slot_borrow:typo".into()],
        &config,
        Some(&registry)
    )
    .is_err());
}
