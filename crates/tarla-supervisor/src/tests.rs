use super::*;
use std::{
    collections::VecDeque,
    io::{BufRead, BufReader, Read, Write},
    net::TcpListener,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct Plane {
    bypass: Option<String>,
    replies: Mutex<Vec<String>>,
    calls: Mutex<Vec<ControlTool>>,
}
#[async_trait]
impl ControlPlane for Plane {
    async fn short_circuit(&self, _: &SupervisorTrigger) -> Result<Option<String>> {
        Ok(self.bypass.clone())
    }
    async fn context(&self, trigger: &SupervisorTrigger) -> Result<Vec<ChatMessage>> {
        let body = match trigger {
            SupervisorTrigger::UserMessage { text, .. } => text.clone(),
            SupervisorTrigger::RunCompleted { receipt, .. } => receipt.to_string(),
        };
        Ok(vec![ChatMessage::User(body)])
    }
    async fn call_tool(&self, _: &str, name: ControlTool, _: Value) -> Result<Value> {
        self.calls.lock().unwrap().push(name);
        Ok(if name == ControlTool::DispatchMission {
            json!({"run_id":"run-34"})
        } else {
            json!({"slots":1})
        })
    }
    async fn deliver(&self, _: &str, reply: &str) -> Result<()> {
        self.replies.lock().unwrap().push(reply.into());
        Ok(())
    }
}
type Turns = VecDeque<Result<BrainTurnOutput, BrainError>>;
struct Script(Mutex<Turns>);
fn script(
    turns: impl IntoIterator<Item = Result<BrainTurnOutput, BrainError>>,
) -> Box<dyn BrainBackend> {
    Box::new(Script(Mutex::new(turns.into_iter().collect())))
}
#[async_trait]
impl BrainBackend for Script {
    async fn generate_turn(
        &self,
        _: &[ChatMessage],
        _: &[ToolSpec],
    ) -> Result<BrainTurnOutput, BrainError> {
        self.0
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected model invocation")
    }
}
fn user() -> SupervisorTrigger {
    SupervisorTrigger::UserMessage {
        conversation_id: "one".into(),
        text: "hello".into(),
    }
}
fn decision(name: ControlTool, args: Value, id: &str) -> BrainTurnOutput {
    BrainTurnOutput {
        text: String::new(),
        tool_calls: vec![ToolCall {
            id: id.into(),
            name,
            arguments: args,
        }],
    }
}
fn reply(s: &str) -> BrainTurnOutput {
    BrainTurnOutput {
        text: s.into(),
        tool_calls: vec![],
    }
}

#[tokio::test]
async fn pinned_users_bypass_models_and_receipts_wake_supervisor() {
    let plane = Plane {
        bypass: Some("forwarded".into()),
        ..Default::default()
    };
    let engine = SupervisorEngine::new(vec![script([Ok(reply("work complete"))])], 5);
    assert_eq!(
        engine.handle_trigger(&plane, &user()).await.unwrap(),
        "forwarded"
    );
    let event = SupervisorTrigger::RunCompleted {
        conversation_id: "one".into(),
        receipt: json!({"final_state":"succeeded"}),
    };
    assert_eq!(
        engine.handle_trigger(&plane, &event).await.unwrap(),
        "work complete"
    );
    assert_eq!(plane.replies.lock().unwrap().len(), 2);
}

#[tokio::test]
async fn dispatch_ack_stops_before_next_model_call() {
    let turn = decision(
        ControlTool::DispatchMission,
        json!({"objective":"review","acceptance_criteria":["give a report"]}),
        "call",
    );
    let engine = SupervisorEngine::new(vec![script([Ok(turn)])], 5);
    let plane = Plane::default();
    assert!(engine
        .handle_trigger(&plane, &user())
        .await
        .unwrap()
        .contains("run-34"));
    assert_eq!(*plane.calls.lock().unwrap(), [ControlTool::DispatchMission]);
}

#[tokio::test]
async fn malformed_batches_and_limits_are_checked_before_side_effects() {
    let mut turn = decision(ControlTool::ListFleetAndRuns, json!({}), "a");
    turn.tool_calls
        .extend(decision(ControlTool::InspectRunLog, json!({"limit":2}), "b").tool_calls);
    let plane = Plane::default();
    for budget in [1, 5] {
        let engine = SupervisorEngine::new(vec![script([Ok(turn.clone())])], budget);
        assert!(engine.handle_trigger(&plane, &user()).await.is_err());
        assert!(plane.calls.lock().unwrap().is_empty());
    }
    let tool = ToolSpec {
        name: ControlTool::MemoryManage,
        description: "",
        parameters: json!({"properties":{}}),
    };
    assert!(tool.validate(&json!({"undocumented":1})).is_err());
    assert!(
        serde_json::from_value::<ToolCall>(json!({"id":"bad","name":"bash","arguments":{}}))
            .is_err()
    );
}

#[tokio::test]
async fn exhausted_budget_never_executes_extra_tools() {
    let engine = SupervisorEngine::new(
        vec![script([
            Ok(decision(ControlTool::ListFleetAndRuns, json!({}), "1")),
            Ok(decision(ControlTool::ListFleetAndRuns, json!({}), "2")),
        ])],
        1,
    );
    let plane = Plane::default();
    assert!(engine.handle_trigger(&plane, &user()).await.is_err());
    assert_eq!(plane.calls.lock().unwrap().len(), 1);
}

struct Fixture {
    url: String,
    requests: Arc<Mutex<Vec<Value>>>,
    handle: std::thread::JoinHandle<()>,
}
fn fixture(responses: Vec<(&'static str, &'static str, String)>) -> Fixture {
    let server = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/api", server.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(vec![]));
    let seen = requests.clone();
    let handle = std::thread::spawn(move || {
        for (status, mime, payload) in responses {
            let (mut socket, _) = server.accept().unwrap();
            let mut reader = BufReader::new(socket.try_clone().unwrap());
            let mut line = String::new();
            let mut len = 0usize;
            loop {
                line.clear();
                reader.read_line(&mut line).unwrap();
                if line == "\r\n" {
                    break;
                }
                if let Some(n) = line.to_ascii_lowercase().strip_prefix("content-length: ") {
                    len = n.trim().parse().unwrap()
                }
            }
            let mut body = vec![0; len];
            reader.read_exact(&mut body).unwrap();
            seen.lock()
                .unwrap()
                .push(serde_json::from_slice(&body).unwrap());
            write!(socket,"HTTP/1.1 {status}\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",payload.len()).unwrap();
            for bytes in payload.as_bytes().chunks(7) {
                socket.write_all(bytes).unwrap();
            }
        }
    });
    Fixture {
        url,
        requests,
        handle,
    }
}
fn sse(events: &[Value], done: bool) -> String {
    let mut body = events
        .iter()
        .map(|v| format!("data: {v}\r\n\r\n"))
        .collect::<String>();
    if done {
        body.push_str("data: [DONE]\r\n\r\n");
    }
    body
}
fn openai(text: &str) -> String {
    sse(
        &[
            json!({"choices":[{"delta":{"content":text},"finish_reason":null}]}),
            json!({"choices":[{"delta":{},"finish_reason":"stop"}]}),
        ],
        true,
    )
}
fn http(dialect: http::Dialect, url: &str) -> Box<dyn BrainBackend> {
    Box::new(http::HttpBrain::new(dialect, url, "fixture", None, None).unwrap())
}

#[tokio::test]
async fn http_429_and_partial_stream_fall_back_only_before_tool_execution() {
    let first = fixture(vec![(
        "429 Too Many Requests",
        "application/json",
        "{}".into(),
    )]);
    let second = fixture(vec![(
        "200 OK",
        "text/event-stream",
        sse(
            &[
                json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"a","function":{"name":"memory_manage","arguments":"{}"}}]}}]}),
            ],
            false,
        ),
    )]);
    let third = fixture(vec![("200 OK", "text/event-stream", openai("服务已恢复"))]);
    let engine = SupervisorEngine::new(
        [&first, &second, &third]
            .into_iter()
            .map(|f| http(http::Dialect::Openai, &f.url))
            .collect(),
        5,
    );
    let plane = Plane::default();
    assert_eq!(
        engine.handle_trigger(&plane, &user()).await.unwrap(),
        "服务已恢复"
    );
    assert!(plane.calls.lock().unwrap().is_empty());
    for f in [first, second, third] {
        f.handle.join().unwrap();
        let req = f.requests.lock().unwrap();
        assert_eq!(req[0]["tools"].as_array().unwrap().len(), 6);
    }
}

#[tokio::test]
async fn anthropic_fragments_round_trip_to_control_plane_and_tool_results() {
    let tool = sse(
        &[
            json!({"type":"content_block_start","index":0,"content_block":{"type":"tool_use","id":"x","name":"list_fleet_and_runs","input":{}}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"{"}}),
            json!({"type":"content_block_delta","index":0,"delta":{"type":"input_json_delta","partial_json":"}"}}),
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),
            json!({"type":"message_stop"}),
        ],
        false,
    );
    let final_turn = sse(
        &[
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"one slot"}}),
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
            json!({"type":"message_stop"}),
        ],
        false,
    );
    let server = fixture(vec![
        ("200 OK", "text/event-stream", tool),
        ("200 OK", "text/event-stream", final_turn),
    ]);
    let engine = SupervisorEngine::new(vec![http(http::Dialect::Anthropic, &server.url)], 5);
    let plane = Plane::default();
    assert_eq!(
        engine.handle_trigger(&plane, &user()).await.unwrap(),
        "one slot"
    );
    server.handle.join().unwrap();
    let log = server.requests.lock().unwrap();
    assert_eq!(log[1]["messages"][1]["content"][0]["type"], "tool_use");
    assert_eq!(log[1]["messages"][2]["content"][0]["tool_use_id"], "x");
    assert_eq!(
        *plane.calls.lock().unwrap(),
        [ControlTool::ListFleetAndRuns]
    );
}

#[tokio::test]
async fn openai_fragmented_tool_arguments_execute_once() {
    let stream = sse(
        &[
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"a","function":{"name":"dispatch_mission","arguments":"{\"objective\":\"a\","}}]}}]}),
            json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"acceptance_criteria\":[\"b\"]}"}}]},"finish_reason":"tool_calls"}]}),
        ],
        true,
    );
    let server = fixture(vec![("200 OK", "text/event-stream", stream)]);
    let engine = SupervisorEngine::new(vec![http(http::Dialect::Openai, &server.url)], 5);
    assert!(engine
        .handle_trigger(&Plane::default(), &user())
        .await
        .unwrap()
        .contains("run-34"));
    server.handle.join().unwrap();
}

#[cfg(unix)]
#[tokio::test]
async fn subscription_slot_isolated_credentials_and_stateless_stdin() {
    use std::{fs, os::unix::fs::PermissionsExt};
    let dir = tempfile::tempdir().unwrap();
    let credential_dir = dir.path().join("account");
    fs::create_dir(&credential_dir).unwrap();
    let exe = dir.path().join("test-claude");
    fs::write(
        &exe,
        r#"#!/bin/sh
set -eu
test -d "$CLAUDE_CONFIG_DIR"
test -z "${ANTHROPIC_API_KEY-}"
test "$PWD" != "$CLAUDE_CONFIG_DIR"
printf '%s\n' "$@" > "$CLAUDE_CONFIG_DIR/args"
cat > "$CLAUDE_CONFIG_DIR/input"
printf '%s\n' '{"result":"{\"text\":\"via slot\",\"tool_calls\":[]}"}'
"#,
    )
    .unwrap();
    fs::set_permissions(&exe, fs::Permissions::from_mode(0o700)).unwrap();
    let mut configs = BTreeMap::new();
    configs.insert(
        "account".into(),
        BackendConfig::ClaudeSlot {
            executable: exe,
            credential_dir: credential_dir.clone(),
            model: None,
        },
    );
    let engine = SupervisorEngine::new(
        backends_from_config(&["slot_borrow:account".into()], &configs).unwrap(),
        5,
    );
    let reply = engine
        .handle_trigger(&Plane::default(), &user())
        .await
        .unwrap();
    assert_eq!(reply, "via slot");
    let args = fs::read_to_string(credential_dir.join("args")).unwrap();
    assert!(args.contains("--tools\n\n"));
    assert!(args.contains("--safe-mode"));
    assert!(args.contains("--disallowedTools\nmcp__*\n"));
    assert!(args.contains("--strict-mcp-config\n--mcp-config\n{\"mcpServers\":{}}\n"));
    assert!(args.contains("--max-turns\n1\n"));
    assert!(args.contains("--no-session-persistence"));
    assert!(!args.contains("hello"));
    let prompt: Value =
        serde_json::from_slice(&fs::read(credential_dir.join("input")).unwrap()).unwrap();
    assert_eq!(prompt["messages"][0]["content"], "hello");
    assert_eq!(prompt["tools"].as_array().unwrap().len(), 6);
}

#[tokio::test]
async fn executor_receipt_cannot_authorize_another_dispatch() {
    let turn = decision(
        ControlTool::DispatchMission,
        json!({"objective":"new","acceptance_criteria":["new"]}),
        "x",
    );
    let engine = SupervisorEngine::new(vec![script([Ok(turn)])], 5);
    let plane = Plane::default();
    let event = SupervisorTrigger::RunCompleted {
        conversation_id: "one".into(),
        receipt: json!({}),
    };
    assert!(engine.handle_trigger(&plane, &event).await.is_err());
    assert!(plane.calls.lock().unwrap().is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn subscription_slot_requires_safe_mode_and_keeps_diagnostics_local() {
    use std::{fs, os::unix::fs::PermissionsExt};
    let dir = tempfile::tempdir().unwrap();
    let cred_ok = dir.path().join("compat");
    let cred_err = dir.path().join("broken");
    fs::create_dir(&cred_ok).unwrap();
    fs::create_dir(&cred_err).unwrap();

    let compat_exe = dir.path().join("claude-no-safe-mode");
    fs::write(
        &compat_exe,
        r#"#!/bin/sh
set -eu
for arg in "$@"; do
  if [ "$arg" = "--safe-mode" ]; then
    head -c 20000 /dev/zero >&2
    echo "error: unknown option '--safe-mode'" >&2
    exit 2
  fi
done
cat >/dev/null
printf '%s\n' '{"result":"{\"text\":\"recovered without safe-mode\",\"tool_calls\":[]}"}'
"#,
    )
    .unwrap();
    fs::set_permissions(&compat_exe, fs::Permissions::from_mode(0o700)).unwrap();

    let broken_exe = dir.path().join("claude-expired");
    fs::write(
        &broken_exe,
        r#"#!/bin/sh
cat >/dev/null
echo "OAuth session expired for work profile" >&2
exit 1
"#,
    )
    .unwrap();
    fs::set_permissions(&broken_exe, fs::Permissions::from_mode(0o700)).unwrap();

    let mut configs = BTreeMap::new();
    configs.insert(
        "broken".into(),
        BackendConfig::ClaudeSlot {
            executable: broken_exe,
            credential_dir: cred_err,
            model: None,
        },
    );
    let err_engine = SupervisorEngine::new(
        backends_from_config(&["slot_borrow:broken".into()], &configs).unwrap(),
        5,
    );
    let err_msg = err_engine
        .handle_trigger(&Plane::default(), &user())
        .await
        .unwrap_err()
        .to_string();
    assert!(err_msg.contains("check daemon diagnostics"), "{err_msg}");
    assert!(!err_msg.contains("OAuth session expired"));

    configs.insert(
        "compat".into(),
        BackendConfig::ClaudeSlot {
            executable: compat_exe,
            credential_dir: cred_ok,
            model: None,
        },
    );
    let engine = SupervisorEngine::new(
        backends_from_config(
            &["slot_borrow:broken".into(), "slot_borrow:compat".into()],
            &configs,
        )
        .unwrap(),
        5,
    );
    let err = engine
        .handle_trigger(&Plane::default(), &user())
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("lacks required --safe-mode"), "{err}");
}
