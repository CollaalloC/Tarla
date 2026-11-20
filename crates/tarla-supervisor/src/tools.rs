use anyhow::{ensure, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ControlTool {
    ListFleetAndRuns,
    DispatchMission,
    InspectRunLog,
    SessionRecall,
    MemoryManage,
    BindChannel,
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolSpec {
    pub name: ControlTool,
    pub description: &'static str,
    pub parameters: Value,
}

impl ControlTool {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ListFleetAndRuns => "list_fleet_and_runs",
            Self::DispatchMission => "dispatch_mission",
            Self::InspectRunLog => "inspect_run_log",
            Self::SessionRecall => "session_recall",
            Self::MemoryManage => "memory_manage",
            Self::BindChannel => "bind_channel",
        }
    }
}

pub fn control_tools() -> Vec<ToolSpec> {
    use ControlTool::*;
    let s = || json!({"type":"string","minLength":1});
    let list = || json!({"type":"array","items":s()});
    let n = || json!({"type":"integer","minimum":0});
    let tools = [
        (ListFleetAndRuns, "List workspace nodes, slot health and recent runs.", json!({}), vec![]),
        (DispatchMission, "Queue one bounded mission; returns immediately with its run ID. Use only for work requested by the user.", json!({
            "objective":s(),"acceptance_criteria":list(),"distilled_context":{"type":"array","maxItems":5,"items":s()},
            "required_fact_keys":list(),"role":{"type":"string","enum":["architect","implementer","reviewer","explorer","distiller"]},
            "required_caps":list(),"exclude_model_family":s(),
            "scope":{"type":"string","description":"Memory scope: global or project:<slug>. Includes global memories."},
            "author_run_id":{"type":"string","description":"For a review, the conversation run that authored the work being reviewed. Required if the author is ambiguous."}
        }), vec!["objective","acceptance_criteria"]),
        (InspectRunLog, "Read a bounded page of an executor log for a workspace run.", json!({
            "run_id":s(),"offset":n(),"limit":n(),"query":s()
        }), vec!["run_id"]),
        (SessionRecall, "Search this archive or read exact UTF-8 pages using seq_no or source. Continue from next_offset_bytes.", json!({
            "query":s(),"dag_node_id":s(),"after_seq":n(),"limit":n(),"seq_no":n(),"source":{"type":"string","enum":["profile","frozen_memory"]},"offset_bytes":n(),"max_bytes":n()
        }), vec![]),
        (MemoryManage, "Find, propose, update or archive a workspace memory. Updates require the version previously read.", json!({
            "operation":{"type":"string","enum":["create","update","archive","search"]},
            "memory_id":s(),"scope":s(),"content":s(),"base_version":n(),"query":s()
        }), vec!["operation"]),
        (BindChannel, "Change this conversation's binding. Pinned mode requires a node, slot, and workdir.", json!({
            "mode":{"type":"string","enum":["orchestrator","pinned"]},"node_id":s(),"slot_id":s(),"workdir":s()
        }), vec!["mode"]),
    ];
    tools.into_iter().map(|(name,description,properties,required)| ToolSpec {
        name,description,parameters:json!({"type":"object","properties":properties,"required":required,"additionalProperties":false})
    }).collect()
}

impl ToolSpec {
    pub(crate) fn validate(&self, args: &Value) -> Result<()> {
        let object = args
            .as_object()
            .ok_or_else(|| anyhow::anyhow!("tool arguments must be an object"))?;
        let schema = &self.parameters;
        for key in schema["required"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
        {
            ensure!(object.contains_key(key), "missing tool argument {key}");
        }
        for (key, value) in object {
            let property = &schema["properties"][key];
            let valid = match property["type"].as_str() {
                Some("integer") => value.as_u64().is_some(),
                Some("string") => value.as_str().is_some_and(|s| !s.trim().is_empty()),
                Some("array") => value.as_array().is_some_and(|a| {
                    a.iter()
                        .all(|v| v.as_str().is_some_and(|s| !s.trim().is_empty()))
                        && a.len() <= property["maxItems"].as_u64().unwrap_or(128) as usize
                }),
                _ => false,
            };
            ensure!(valid, "invalid tool argument {key}");
            if let Some(allowed) = property["enum"].as_array() {
                ensure!(allowed.contains(value), "unsupported value for {key}");
            }
        }
        if self.name == ControlTool::BindChannel && args["mode"] == "pinned" {
            ensure!(
                ["node_id", "slot_id", "workdir"]
                    .iter()
                    .all(|k| object.contains_key(*k)),
                "pinned binding requires node_id, slot_id and workdir"
            );
        }
        if self.name == ControlTool::MemoryManage {
            if let Some("update" | "archive") = args["operation"].as_str() {
                ensure!(
                    args["memory_id"].is_string() && args["base_version"].is_u64(),
                    "update and archive require memory_id and base_version"
                );
            }
            if matches!(args["operation"].as_str(), Some("create" | "update")) {
                ensure!(args["content"].is_string(), "memory content required");
            }
        }
        Ok(())
    }
}
