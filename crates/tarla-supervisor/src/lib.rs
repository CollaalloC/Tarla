//! Event-driven control loop. The daemon implements workspace-scoped persistence and dispatch.
mod http;
mod slot;
mod tools;
mod visible;
pub use slot::{SubscriptionRegistry, SubscriptionSlot};
pub use tools::{control_tools, ControlTool, ToolSpec};

use anyhow::{bail, ensure, Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashSet},
    path::PathBuf,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "role", content = "content", rename_all = "snake_case")]
pub enum ChatMessage {
    System(String),
    User(String),
    Assistant(String),
    AssistantTools(BrainTurnOutput),
    Tool { call_id: String, result: Value },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BrainTurnOutput {
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub tool_calls: Vec<ToolCall>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ToolCall {
    pub id: String,
    pub name: ControlTool,
    pub arguments: Value,
}

/// Unavailable backends may be retried with the next explicitly configured provider.
/// Invalid requests and malformed decisions fail before executing any new tool.
#[derive(Debug)]
pub enum BrainError {
    Unavailable(String),
    Invalid(String),
}

impl std::fmt::Display for BrainError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unavailable(message) | Self::Invalid(message) => f.write_str(message),
        }
    }
}
impl std::error::Error for BrainError {}

#[async_trait]
pub trait BrainBackend: Send + Sync {
    async fn generate_turn(
        &self,
        messages: &[ChatMessage],
        tools: &[ToolSpec],
    ) -> Result<BrainTurnOutput, BrainError>;
    /// Backends with public text deltas may replace the current draft. A default
    /// implementation keeps providers which only return complete decisions working.
    async fn generate_turn_with_preview(
        &self,
        messages: &[ChatMessage],
        tools: &[ToolSpec],
        _preview: &(dyn for<'chunk> Fn(&'chunk str) + Send + Sync),
    ) -> Result<BrainTurnOutput, BrainError> {
        self.generate_turn(messages, tools).await
    }
}

#[derive(Debug, Clone)]
pub enum SupervisorTrigger {
    UserMessage {
        conversation_id: String,
        text: String,
    },
    RunCompleted {
        conversation_id: String,
        receipt: Value,
    },
}

impl SupervisorTrigger {
    pub fn conversation_id(&self) -> &str {
        match self {
            Self::UserMessage {
                conversation_id, ..
            }
            | Self::RunCompleted {
                conversation_id, ..
            } => conversation_id,
        }
    }
}

#[async_trait]
pub trait ControlPlane: Send + Sync {
    /// Ephemeral public assistant text for this specific input. An empty string clears it.
    fn preview(&self, _text: &str) {}
    /// Handles deterministic user actions and pinned conversations. Return the reply when handled.
    async fn short_circuit(&self, trigger: &SupervisorTrigger) -> Result<Option<String>>;
    /// Includes the current trigger exactly once, in addition to the frozen prefix and history.
    async fn context(&self, trigger: &SupervisorTrigger) -> Result<Vec<ChatMessage>>;
    async fn call_tool(
        &self,
        conversation_id: &str,
        name: ControlTool,
        args: Value,
    ) -> Result<Value>;
    /// Persist the answer and queue delivery to the originating channels.
    async fn deliver(&self, conversation_id: &str, reply: &str) -> Result<()>;
}

pub fn dispatch_acknowledgement(run_id: &str) -> String {
    format!("已提交任务 {run_id}。")
}

pub struct SupervisorEngine {
    backends: Vec<Box<dyn BrainBackend>>,
    max_tool_steps: usize,
    max_window_bytes: usize,
}

impl SupervisorEngine {
    pub fn new(backends: Vec<Box<dyn BrainBackend>>, max_tool_steps: usize) -> Self {
        Self {
            backends,
            max_tool_steps: max_tool_steps.min(5),
            max_window_bytes: 24_000,
        }
    }
    /// Upper bound on UTF-8 content bytes during internal tool rounds.
    pub fn with_window_budget(mut self, budget: usize) -> Self {
        self.max_window_bytes = budget;
        self
    }

    async fn turn(
        &self,
        messages: &[ChatMessage],
        tools: &[ToolSpec],
        plane: &dyn ControlPlane,
    ) -> Result<BrainTurnOutput> {
        ensure!(
            !self.backends.is_empty(),
            "no supervisor backend configured"
        );
        let mut errors = Vec::new();
        for backend in &self.backends {
            plane.preview("");
            match backend
                .generate_turn_with_preview(messages, tools, &|text| plane.preview(text))
                .await
            {
                Ok(turn) => return Ok(turn),
                Err(BrainError::Unavailable(message)) => {
                    plane.preview("");
                    errors.push(message)
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(BrainError::Unavailable(format!(
            "all configured supervisor backends are unavailable ({})",
            errors.join(", ")
        ))
        .into())
    }

    pub async fn handle_trigger(
        &self,
        plane: &dyn ControlPlane,
        trigger: &SupervisorTrigger,
    ) -> Result<String> {
        let id = trigger.conversation_id();
        if matches!(trigger, SupervisorTrigger::UserMessage { .. }) {
            if let Some(reply) = plane.short_circuit(trigger).await? {
                plane.deliver(id, &reply).await?;
                return Ok(reply);
            }
        }
        let mut messages = plane.context(trigger).await?;
        let mut tools = control_tools();
        if matches!(trigger, SupervisorTrigger::RunCompleted { .. }) {
            // An executor receipt can wake a reporter but cannot authorize another mission.
            tools.retain(|t| {
                matches!(
                    t.name,
                    ControlTool::ListFleetAndRuns
                        | ControlTool::InspectRunLog
                        | ControlTool::SessionRecall
                )
            });
        }
        let mut spent = 0;
        let mut seen_calls = HashSet::new();
        let reply = loop {
            let size = messages
                .iter()
                .map(content_bytes)
                .fold(0usize, usize::saturating_add);
            let allowed = if spent < self.max_tool_steps {
                &tools[..]
            } else {
                &[]
            };
            ensure!(
                size.saturating_add(tool_overhead(allowed)) <= self.max_window_bytes,
                "supervisor context exceeds active budget"
            );
            let next = self.turn(&messages, allowed, plane).await?;
            if next.tool_calls.is_empty() {
                ensure!(!next.text.trim().is_empty(), "empty supervisor reply");
                break next.text;
            }
            plane.preview("");
            ensure!(
                next.tool_calls.len() <= self.max_tool_steps - spent,
                "supervisor tool budget exceeded"
            );
            // Validate the whole batch before performing its first side effect.
            for call in &next.tool_calls {
                ensure!(
                    !call.id.is_empty() && seen_calls.insert(call.id.clone()),
                    "duplicate or empty tool call ID"
                );
                let spec = allowed
                    .iter()
                    .find(|s| s.name == call.name)
                    .context("unknown control tool")?;
                spec.validate(&call.arguments)?;
                ensure!(
                    call.name != ControlTool::DispatchMission || next.tool_calls.len() == 1,
                    "dispatch_mission must be its own decision"
                );
            }
            let dispatch = next
                .tool_calls
                .first()
                .is_some_and(|c| c.name == ControlTool::DispatchMission);
            spent += next.tool_calls.len();
            messages.push(ChatMessage::AssistantTools(next.clone()));
            let mut dispatch_id = None;
            for call in next.tool_calls {
                let result = plane.call_tool(id, call.name, call.arguments).await?;
                if dispatch {
                    dispatch_id = Some(
                        result["run_id"]
                            .as_str()
                            .context("dispatch did not return a run ID")?
                            .to_owned(),
                    );
                } else {
                    let remaining = self
                        .max_window_bytes
                        .saturating_sub(messages.iter().map(content_bytes).sum());
                    let remaining = remaining.saturating_sub(tool_overhead(&tools));
                    let cost = result.to_string().len() + call.id.len() + 32;
                    let bounded = if cost <= remaining {
                        result
                    } else {
                        json!({"error":"Result exceeds the remaining window. Request a smaller page."})
                    };
                    messages.push(ChatMessage::Tool {
                        call_id: call.id,
                        result: bounded,
                    });
                }
            }
            if let Some(run_id) = dispatch_id {
                break dispatch_acknowledgement(&run_id);
            }
        };
        plane.deliver(id, &reply).await?;
        Ok(reply)
    }
}
fn content_bytes(message: &ChatMessage) -> usize {
    let bytes = match message {
        ChatMessage::System(s) | ChatMessage::Assistant(s) | ChatMessage::User(s) => s.len(),
        ChatMessage::AssistantTools(turn) => {
            turn.text.len()
                + turn
                    .tool_calls
                    .iter()
                    .map(|c| c.id.len() + c.name.as_str().len() + c.arguments.to_string().len())
                    .sum::<usize>()
        }
        ChatMessage::Tool { call_id, result } => call_id.len() + result.to_string().len(),
    };
    bytes.saturating_add(match message {
        ChatMessage::Assistant(_) | ChatMessage::AssistantTools(_) => 25,
        ChatMessage::System(_) => 22,
        ChatMessage::User(_) => 20,
        ChatMessage::Tool { .. } => 32,
    })
}

fn tool_overhead(tools: &[ToolSpec]) -> usize {
    // Conservative allowance for provider-specific tool wrappers and role framing.
    serde_json::to_vec(tools)
        .expect("static schemas serialize")
        .len()
        + 64 * tools.len()
        + 96
}

/// Reserve this many tokens before asking the context assembler for a user turn.
/// Counts UTF-8 bytes conservatively across the supported model tokenizers.
pub fn control_window_reserve() -> usize {
    tool_overhead(&control_tools())
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum BackendConfig {
    Openai {
        endpoint: String,
        model: String,
        api_key_env: Option<String>,
        socks_proxy: Option<String>,
    },
    Anthropic {
        endpoint: String,
        model: String,
        api_key_env: Option<String>,
        socks_proxy: Option<String>,
    },
    SlotPool {
        slots: Vec<String>,
    },
    ClaudeSlot {
        executable: PathBuf,
        credential_dir: PathBuf,
        model: Option<String>,
    },
}

pub fn backends_from_config(
    chain: &[String],
    configs: &BTreeMap<String, BackendConfig>,
) -> Result<Vec<Box<dyn BrainBackend>>> {
    backends_from_config_with_slots(chain, configs, None)
}

/// Resolve named HTTP backends, legacy isolated Claude accounts and registered local slot pools.
pub fn backends_from_config_with_slots(
    chain: &[String],
    configs: &BTreeMap<String, BackendConfig>,
    registry: Option<&SubscriptionRegistry>,
) -> Result<Vec<Box<dyn BrainBackend>>> {
    let mut resolved: Vec<Box<dyn BrainBackend>> = Vec::new();
    for reference in chain {
        let (kind, key) = reference
            .split_once(':')
            .context("expected http:<name> or slot_borrow:<name>")?;
        if kind == "slot_borrow" {
            let pool = match configs.get(key) {
                Some(BackendConfig::SlotPool { slots }) => Some(slots.clone()),
                None => Some(vec![key.to_owned()]),
                _ => None,
            };
            if let Some(ids) = pool {
                resolved.push(
                    registry
                        .context("registered subscription slots unavailable")?
                        .pool(&ids)?,
                );
                continue;
            }
        }
        let backend: Box<dyn BrainBackend> = match (
            kind,
            configs
                .get(key)
                .with_context(|| format!("unknown brain backend {key}"))?,
        ) {
            (
                "http",
                BackendConfig::Openai {
                    endpoint,
                    model,
                    api_key_env,
                    socks_proxy,
                },
            ) => Box::new(http::HttpBrain::new(
                http::Dialect::Openai,
                endpoint,
                model,
                api_key_env.clone(),
                socks_proxy.as_deref(),
            )?),
            (
                "http",
                BackendConfig::Anthropic {
                    endpoint,
                    model,
                    api_key_env,
                    socks_proxy,
                },
            ) => Box::new(http::HttpBrain::new(
                http::Dialect::Anthropic,
                endpoint,
                model,
                api_key_env.clone(),
                socks_proxy.as_deref(),
            )?),
            (
                "slot_borrow",
                BackendConfig::ClaudeSlot {
                    executable,
                    credential_dir,
                    model,
                },
            ) => Box::new(slot::ClaudeSlot::new(
                executable,
                credential_dir,
                model.clone(),
            )?),
            _ => bail!("brain backend kind mismatch for {reference}"),
        };
        resolved.push(backend);
    }
    Ok(resolved)
}
#[cfg(test)]
mod tests;
