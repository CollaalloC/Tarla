//! Declarative `botflow.yml` (v0.2) schema, graph validation, and target resolution.
use crate::yaml::parse_yaml_to_json;
use anyhow::{bail, ensure, Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

fn default_hops() -> u8 {
    6
}
fn default_room_rounds() -> u32 {
    3
}
fn default_room_replies() -> u32 {
    10
}
fn default_result_gate() -> usize {
    8000
}
fn default_mission_bytes() -> usize {
    7200
}
fn default_receipt_bytes() -> usize {
    2000
}
fn default_ring_cap() -> usize {
    16
}
fn default_idle_reap() -> u64 {
    1800
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BotflowPolicy {
    #[serde(default = "default_hops")]
    pub max_event_hops: u8,
    #[serde(default = "default_room_rounds")]
    pub max_room_rounds: u32,
    #[serde(default = "default_room_replies")]
    pub max_room_replies: u32,
    #[serde(default = "default_result_gate")]
    pub result_gate_max_chars: usize,
    #[serde(default = "default_mission_bytes")]
    pub mission_packet_max_bytes: usize,
    #[serde(default = "default_receipt_bytes")]
    pub run_receipt_max_bytes: usize,
    #[serde(default = "default_ring_cap")]
    pub drop_oldest_capacity: usize,
}

impl Default for BotflowPolicy {
    fn default() -> Self {
        Self {
            max_event_hops: default_hops(),
            max_room_rounds: default_room_rounds(),
            max_room_replies: default_room_replies(),
            result_gate_max_chars: default_result_gate(),
            mission_packet_max_bytes: default_mission_bytes(),
            run_receipt_max_bytes: default_receipt_bytes(),
            drop_oldest_capacity: default_ring_cap(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct HerdrCoreDef {
    pub session: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct NodeTemplateDef {
    pub core: String,
    pub command: String,
    #[serde(default)]
    pub env_profile: Option<String>,
    pub workdir: String,
    #[serde(default)]
    pub ready: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BotflowNode {
    pub role: String,
    #[serde(default)]
    pub platform: Option<String>,
    #[serde(default)]
    pub herdr_cores: BTreeMap<String, HerdrCoreDef>,
    #[serde(default)]
    pub templates: BTreeMap<String, NodeTemplateDef>,
}

#[derive(Debug, Copy, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueuePolicy {
    #[default]
    Backpressure,
    CoalesceLatest,
    DropOldest,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BotInputPort {
    pub source: String,
    #[serde(default)]
    pub queue_policy: QueuePolicy,
    #[serde(default)]
    pub debounce_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct BotConstraints {
    #[serde(default)]
    pub different_command_from_upstream: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BotDef {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub control_authority: Option<String>,
    pub targets: Vec<String>,
    #[serde(default = "default_idle_reap")]
    pub idle_reap_secs: u64,
    #[serde(default)]
    pub checkpoint_files: bool,
    #[serde(default)]
    pub verify_before_done: Option<String>,
    #[serde(default)]
    pub constraints: BotConstraints,
    #[serde(default)]
    pub inputs: BTreeMap<String, BotInputPort>,
    #[serde(default)]
    pub outputs: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BotflowSpec {
    pub version: String,
    pub workspace: String,
    #[serde(default)]
    pub policy: BotflowPolicy,
    pub nodes: BTreeMap<String, BotflowNode>,
    pub bots: Vec<BotDef>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTarget {
    pub target_ref: String,
    pub node_id: String,
    pub template_name: String,
    pub core_group: String,
    pub command: String,
    pub env_profile: Option<String>,
    pub workdir: String,
    pub ready: Option<String>,
}

fn valid_ident(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

impl BotflowSpec {
    pub fn from_yaml_str(raw: &str) -> Result<Self> {
        let json_val = parse_yaml_to_json(raw)?;
        let spec: Self = serde_json::from_value(json_val).context("invalid botflow.yml schema")?;
        spec.validate()?;
        Ok(spec)
    }

    pub fn resolve_target(&self, target_ref: &str) -> Result<ResolvedTarget> {
        let Some((node_id, tpl_name)) = target_ref.split_once('/') else {
            bail!("target `{target_ref}` must use `<node>/<template>` format");
        };
        let node = self.nodes.get(node_id).with_context(|| {
            format!("target `{target_ref}` references unknown node `{node_id}`")
        })?;
        ensure!(
            node.role != "master_control",
            "target `{target_ref}` cannot execute on master_control node `{node_id}`"
        );
        let tpl = node.templates.get(tpl_name).with_context(|| {
            format!("target `{target_ref}` references unknown template `{tpl_name}` on `{node_id}`")
        })?;
        ensure!(
            node.herdr_cores.contains_key(&tpl.core),
            "template `{target_ref}` references undeclared herdr_core `{}`",
            tpl.core
        );
        ensure!(
            !tpl.command.trim().is_empty() && !tpl.workdir.trim().is_empty(),
            "template `{target_ref}` must specify command and workdir"
        );
        Ok(ResolvedTarget {
            target_ref: target_ref.to_owned(),
            node_id: node_id.to_owned(),
            template_name: tpl_name.to_owned(),
            core_group: tpl.core.clone(),
            command: tpl.command.clone(),
            env_profile: tpl.env_profile.clone(),
            workdir: tpl.workdir.clone(),
            ready: tpl.ready.clone(),
        })
    }

    pub fn bot(&self, bot_id: &str) -> Option<&BotDef> {
        self.bots.iter().find(|b| b.id == bot_id)
    }

    /// Select the best healthy target for `bot_id`, honoring `different_command_from_upstream`
    /// and skipping targets listed in `excluded_targets` (e.g. ≥95% quota or crashed `!acted`).
    pub fn select_target_for_bot(
        &self,
        bot_id: &str,
        upstream_command: Option<&str>,
        excluded_targets: &BTreeSet<String>,
    ) -> Result<Option<ResolvedTarget>> {
        let bot = self
            .bot(bot_id)
            .with_context(|| format!("unknown bot `{bot_id}`"))?;
        let enforce_diff = bot.constraints.different_command_from_upstream;
        for target_ref in &bot.targets {
            if excluded_targets.contains(target_ref) {
                continue;
            }
            let resolved = self.resolve_target(target_ref)?;
            if enforce_diff {
                if let Some(up_cmd) = upstream_command {
                    if command_basename(&resolved.command) == command_basename(up_cmd) {
                        continue;
                    }
                }
            }
            return Ok(Some(resolved));
        }
        Ok(None)
    }

    pub fn validate(&self) -> Result<()> {
        ensure!(
            self.version == "0.2",
            "unsupported botflow.yml version `{}`; expected `0.2`",
            self.version
        );
        ensure!(
            valid_ident(&self.workspace),
            "invalid botflow workspace identifier `{}`",
            self.workspace
        );
        ensure!(
            self.policy.max_event_hops >= 1 && self.policy.max_event_hops <= 32,
            "policy.max_event_hops must be in 1..=32"
        );
        ensure!(
            self.policy.max_room_rounds >= 1 && self.policy.max_room_replies >= 1,
            "room round/reply limits must be positive"
        );

        for (node_id, node) in &self.nodes {
            ensure!(valid_ident(node_id), "invalid node id `{node_id}`");
            ensure!(
                matches!(
                    node.role.as_str(),
                    "master_control"
                        | "supervisor_and_executor"
                        | "executor_only"
                        | "supervisor_only"
                ),
                "node `{node_id}` has invalid role `{}`",
                node.role
            );
            for core_name in node.herdr_cores.keys() {
                ensure!(
                    valid_ident(core_name),
                    "node `{node_id}` has invalid herdr_core name `{core_name}`"
                );
            }
            for (tpl_name, tpl) in &node.templates {
                ensure!(
                    valid_ident(tpl_name),
                    "node `{node_id}` has invalid template name `{tpl_name}`"
                );
                ensure!(
                    node.herdr_cores.contains_key(&tpl.core),
                    "template `{node_id}/{tpl_name}` references unknown core `{}`",
                    tpl.core
                );
            }
        }

        ensure!(
            !self.bots.is_empty(),
            "botflow.yml must declare at least one bot"
        );
        let mut declared_outputs: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for bot in &self.bots {
            ensure!(valid_ident(&bot.id), "invalid bot id `{}`", bot.id);
            ensure!(
                !declared_outputs.contains_key(&bot.id),
                "duplicate bot id `{}`",
                bot.id
            );
            ensure!(
                !bot.targets.is_empty(),
                "bot `{}` must declare at least one target",
                bot.id
            );
            if let Some(ctrl) = &bot.control_authority {
                ensure!(
                    self.nodes.contains_key(ctrl),
                    "bot `{}` control_authority references unknown node `{ctrl}`",
                    bot.id
                );
            }
            let primary = self.resolve_target(&bot.targets[0])?;
            for t in &bot.targets[1..] {
                self.resolve_target(t)?;
            }
            if let Some(verifier_ref) = &bot.verify_before_done {
                let verifier = self.resolve_target(verifier_ref)?;
                ensure!(
                    verifier.target_ref != primary.target_ref,
                    "bot `{}` verify_before_done must be distinct from primary target",
                    bot.id
                );
            }
            let mut ports = BTreeSet::new();
            for out_port in &bot.outputs {
                ensure!(
                    valid_ident(out_port),
                    "bot `{}` has invalid output port `{out_port}`",
                    bot.id
                );
                ports.insert(out_port.clone());
            }
            declared_outputs.insert(bot.id.clone(), ports);
        }

        for bot in &self.bots {
            for (in_port, cfg) in &bot.inputs {
                ensure!(
                    valid_ident(in_port),
                    "bot `{}` has invalid input port `{in_port}`",
                    bot.id
                );
                let Some((src_ns, src_port)) = cfg.source.split_once('/') else {
                    bail!(
                        "bot `{}` input `{in_port}` source `{}` must be `<bot>/<port>` or `sys/<topic>`",
                        bot.id,
                        cfg.source
                    );
                };
                if src_ns == "sys" {
                    ensure!(
                        valid_ident(src_port),
                        "invalid builtin sys source `{}`",
                        cfg.source
                    );
                } else {
                    let Some(up_ports) = declared_outputs.get(src_ns) else {
                        bail!(
                            "bot `{}` input `{in_port}` references unknown upstream bot `{src_ns}`",
                            bot.id
                        );
                    };
                    ensure!(
                        up_ports.contains(src_port),
                        "bot `{}` input `{in_port}` references undeclared output `{}`",
                        bot.id,
                        cfg.source
                    );
                }
            }
        }
        Ok(())
    }
}

pub fn command_basename(cmd: &str) -> &str {
    cmd.rsplit(['/', '\\']).next().unwrap_or(cmd)
}
