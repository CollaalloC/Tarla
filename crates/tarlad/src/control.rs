use super::*;
use async_trait::async_trait;
use rusqlite::{params, OptionalExtension};
use tarla_memory::{MemoryBackend, Mutation, SqliteMemory};
use tarla_supervisor::{ChatMessage, ControlPlane, ControlTool, SupervisorTrigger};

pub fn ensure_conversation(c: &Connection, s: &Service, id: &str) -> Result<()> {
    validate_id(id)?;
    c.execute("INSERT INTO conversations(conversation_id,workspace_id,title,created_at,updated_at) VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING",params![id,s.id(),id,now(),now()])?;
    Ok(())
}
pub fn set_binding(s: &Service, id: &str, v: &Value) -> Result<Value> {
    handoff::bind(s, id, v)
}
pub async fn search_memories(
    s: &Service,
    query: &str,
    limit: usize,
) -> Result<Vec<tarla_memory::Memory>> {
    if let Some(provider) = s.hosted_index.as_ref().filter(|_| !query.trim().is_empty()) {
        let snapshot = tarla_memory::Projection::read(&mut s.db()?, s.id())?;
        let remote = async {
            provider.publish(&snapshot).await?;
            provider.search(&snapshot, query, limit).await
        }
        .await;
        match remote {
            Ok(results) => return Ok(results),
            Err(error) => {
                eprintln!("hosted memory search unavailable, using local S2 index: {error}")
            }
        }
    }
    if let Some(provider) = s.memory_index.clone().filter(|_| !query.trim().is_empty()) {
        let path = s.db_path.clone();
        let (workspace, node, query) = (s.id().to_owned(), s.node().to_owned(), query.to_owned());
        return tokio::task::spawn_blocking(move || {
            let mut db = Connection::open(path)?;
            db.busy_timeout(Duration::from_secs(5))?;
            provider.search(&mut db, &workspace, &node, &query, limit)
        })
        .await?;
    }
    SqliteMemory::new(&mut s.db()?, s.id(), s.node()).search(query, limit)
}
pub async fn publish_memories(s: S) {
    let Some(provider) = s.hosted_index.as_ref() else {
        return;
    };
    loop {
        let result: Result<()> = async {
            let cursor = SqliteMemory::new(&mut s.db()?, s.id(), s.node()).sync_cursor()?;
            if !provider.is_current(cursor).await {
                let snapshot = tarla_memory::Projection::read(&mut s.db()?, s.id())?;
                provider.publish(&snapshot).await?;
            }
            Ok(())
        }
        .await;
        if let Err(e) = result {
            eprintln!("hosted S2 synchronization deferred: {e}");
            tokio::time::sleep(Duration::from_secs(15)).await;
        } else {
            tokio::select! {_=s.notify.notified()=>(),_=tokio::time::sleep(Duration::from_secs(5))=>()}
        }
    }
}
pub async fn memories(s: &Service, v: &Value) -> Result<Value> {
    let op = v["operation"].as_str().unwrap_or("update");
    if op == "search" {
        return Ok(json!(
            search_memories(s, v["query"].as_str().unwrap_or(""), 100).await?
        ));
    }
    let mut db = s.db()?;
    let mut m = SqliteMemory::new(&mut db, s.id(), s.node());
    if !s.is_supervisor() {
        bail!("global memory writes require the designated supervisor")
    }
    if !matches!(op, "create" | "update" | "archive") {
        bail!("unsupported memory operation")
    }
    let req = Mutation {
        proposal_id: v["proposal_id"]
            .as_str()
            .map(String::from)
            .unwrap_or_else(uid),
        memory_id: string(v, "memory_id")?.into(),
        scope: v["scope"].as_str().unwrap_or("global").into(),
        content: v["content"].as_str().unwrap_or_default().into(),
        base_version: v["base_version"]
            .as_i64()
            .context("base_version required")?,
        source_node_id: s.node().into(),
        source_run_id: None,
        is_deleted: op == "archive",
        hlc_timestamp: None,
    };
    let reply = m.propose_mutation(&req)?;
    peers::mutation_committed(s, &reply);
    s.notify.notify_waiters();
    Ok(json!(reply))
}

pub fn context_policy(s: &Service, skill_cost: usize) -> Result<tarla_context::ContextPolicy> {
    let p = &s.ws.config.context_policy;
    let available = p
        .supervisor_active_budget_tokens
        .checked_sub(tarla_supervisor::control_window_reserve())
        .and_then(|n| n.checked_sub(skill_cost))
        .context("selected skill and control tools exceed the supervisor budget")?;
    // Keep room for a bounded archive page and the assistant tool call that requested it.
    Ok(tarla_context::ContextPolicy {
        supervisor_active_budget_tokens: available - (available / 4).min(1536),
        compaction_high_watermark_pct: p.compaction_high_watermark_pct,
        protected_verbatim_turns: p.protected_verbatim_turns,
    })
}

pub struct Plane<'a> {
    s: &'a Service,
    source: String,
    skill: Option<skills::Selection>,
    draft: Option<drafts::Live<'a>>,
    completion: Option<(&'a str, &'a str)>,
}
impl Plane<'_> {
    fn effect(&self, action: &str) -> String {
        effect_key(&self.source, action)
    }
}
#[async_trait]
impl ControlPlane for Plane<'_> {
    fn preview(&self, text: &str) {
        if let Some(d) = &self.draft {
            d.write(text)
        }
    }
    async fn short_circuit(&self, trigger: &SupervisorTrigger) -> Result<Option<String>> {
        let SupervisorTrigger::UserMessage {
            conversation_id: id,
            text,
        } = trigger
        else {
            return Ok(None);
        };
        let answer = match text.trim() {
            "/help" => "Commands: /skills, /skill <name> <task>, /runs, /memories, /dispatch <goal>, /switch [node/slot], @slot [message], /unbind".into(),
            "/skills" => skills::available(&self.s.ws)?,
            "/runs" => serde_json::to_string(&api::run_list(self.s)?)?,
            "/memories" => memories(self.s, &json!({"operation":"search"}))
                .await?
                .to_string(),
            "/unbind" => set_binding(self.s, id, &json!({"mode":"orchestrator"}))?.to_string(),
            t if t.starts_with("/dispatch ") => fleet::dispatch_selected(
                self.s,
                id,
                &json!({"goal":t[10..].trim()}),
                Some(self.effect("dispatch")),
                None,
                Some(fleet::Acknowledgement::Command(&self.source)),
            )?
            .to_string(),
            _=> return handoff::pinned(self.s,id,text,&self.source,self.effect("dispatch"),self.skill.as_ref()),
        };
        Ok(Some(answer))
    }
    async fn context(&self, trigger: &SupervisorTrigger) -> Result<Vec<ChatMessage>> {
        let id = match trigger {
            SupervisorTrigger::UserMessage {
                conversation_id, ..
            }
            | SupervisorTrigger::RunCompleted {
                conversation_id, ..
            } => conversation_id,
        };
        let mut c = self.s.db()?;
        let frozen: bool = c.query_row(
            "SELECT EXISTS(SELECT 1 FROM conversation_prefix WHERE conversation_id=?1)",
            [id],
            |r| r.get(0),
        )?;
        let prefix = if frozen {
            String::new()
        } else {
            SqliteMemory::new(&mut c, self.s.id(), self.s.node())
                .prefetch_frozen_snapshot("global", self.s.ws.config.memory.prefetch_token_budget)?
        };
        let policy = context_policy(
            self.s,
            self.skill.as_ref().map_or(0, skills::Selection::cost),
        )?;
        let history = tarla_context::assemble(&mut c, id, &self.s.ws.soul, &prefix, &policy)?;
        let mut messages: Vec<_> = history
            .into_iter()
            .map(|m| match m.role.as_str() {
                "system" => ChatMessage::System(m.content),
                "assistant" => ChatMessage::Assistant(m.content),
                _ => ChatMessage::User(m.content),
            })
            .collect();
        if let Some(skill) = &self.skill {
            messages.push(ChatMessage::User(skill.instructions.clone()));
        }
        Ok(messages)
    }
    async fn call_tool(&self, id: &str, tool: ControlTool, mut args: Value) -> Result<Value> {
        match tool {
            ControlTool::ListFleetAndRuns => {
                Ok(json!({"slots":fleet::snapshots(self.s)?,"runs":api::run_list(self.s)?}))
            }
            ControlTool::DispatchMission => fleet::dispatch_selected(
                self.s,
                id,
                &args,
                Some(self.effect("dispatch")),
                self.skill.as_ref(),
                Some(fleet::Acknowledgement::Supervisor(&self.source)),
            ),
            ControlTool::InspectRunLog => {
                api::read_log(
                    self.s,
                    string(&args, "run_id")?,
                    args["offset"].as_u64().unwrap_or(0),
                    args["limit"].as_u64().unwrap_or(40),
                    args["query"].as_str().unwrap_or(""),
                )
                .await
            }
            ControlTool::SessionRecall => tarla_context::recall_tool(&self.s.db()?, id, &args),
            ControlTool::MemoryManage => {
                if args["operation"] == "create" {
                    if args["memory_id"].is_null() {
                        args["memory_id"] = json!(self.effect(&format!("memory-id/{args}")));
                    }
                    if args["base_version"].is_null() {
                        args["base_version"] = json!(0);
                    }
                }
                args["proposal_id"] = json!(self.effect(&format!("memory/{args}")));
                memories(self.s, &args).await
            }
            ControlTool::BindChannel => set_binding(self.s, id, &args),
        }
    }
    async fn deliver(&self, id: &str, reply: &str) -> Result<()> {
        let mut db = self.s.db()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tarla_context::append_in(
            &tx,
            self.s.id(),
            id,
            ("supervisor", &self.source),
            "assistant",
            reply,
            self.completion.map(|(_, run)| run),
        )?;
        if let Some((event, _)) = self.completion {
            tx.execute(
                "UPDATE service_wakes SET delivered=1 WHERE event_id=? AND conversation_id=?",
                params![event, id],
            )?;
        }
        tx.commit()?;
        self.s.context_wake.notify_one();
        Ok(())
    }
}
fn effect_key(source: &str, action: &str) -> String {
    format!("{:x}", Sha256::digest(format!("{source}\n{action}")))
}

pub fn record_run_reply(
    tx: &rusqlite::Transaction<'_>,
    workspace: &str,
    conversation: &str,
    source: &str,
    run: &str,
    reply: &str,
) -> Result<()> {
    tarla_context::append_in(
        tx,
        workspace,
        conversation,
        ("supervisor", source),
        "assistant",
        reply,
        Some(run),
    )?;
    Ok(())
}

fn message_result(id: &str, reply: String, run: Option<String>) -> Value {
    let run = run.or_else(|| {
        serde_json::from_str::<Value>(&reply)
            .ok()
            .and_then(|value| value["run_id"].as_str().map(String::from))
    });
    let mut out = json!({"conversation_id":id, "reply":reply});
    if let Some(run) = run {
        out["run_id"] = json!(run);
    }
    out
}

pub async fn message(s: &Service, id: &str, payload: &Value) -> Result<Value> {
    validate_id(id)?;
    let text = string(payload, "text")?;
    let channel = payload["channel"].as_str().unwrap_or("cli");
    validate_id(channel)?;
    let source = payload["source_msg_id"]
        .as_str()
        .map(String::from)
        .unwrap_or_else(uid);
    if source.is_empty() || source.len() > 512 || channel == "supervisor" || channel == "receipt" {
        bail!("invalid message source")
    }
    let _lock = s.lock(id).await;
    let key;
    {
        let mut c = s.db()?;
        c.execute(
            "INSERT OR IGNORE INTO service_responses VALUES(?,?,?,?,NULL)",
            params![channel, source, id, text],
        )?;
        let (old_id, old_text): (String, String) = c.query_row(
            "SELECT conversation_id,text FROM service_responses WHERE channel=? AND source=?",
            params![channel, source],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )?;
        if old_id != id || old_text != text {
            bail!("message source id already used for different content")
        }
        key = drafts::reply_key(&c, id, channel, &source)?;
        let replay = c
            .query_row(
                "SELECT content,run_id FROM messages WHERE channel_type='supervisor' AND conversation_id=? AND source_msg_id=?",
                params![id,key],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_,Option<String>>(1)?)),
            )
            .optional()?;
        if let Some((reply, run)) = replay {
            return Ok(message_result(id, reply, run));
        }
        // Older databases could commit the mission before archiving its answer.
        // Recover the exact response shape without asking a model to recreate the decision.
        let rid = effect_key(&key, "dispatch");
        let committed = c.query_row("SELECT r.target_node_id,r.target_slot_id,p.run_id IS NOT NULL FROM runs r LEFT JOIN service_pin_epochs p USING(run_id) WHERE r.conversation_id=? AND r.run_id=?",
            params![id,rid], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,bool>(2)?))).optional()?;
        if let Some((node, slot, pinned)) = committed {
            let reply = if pinned || text.trim().starts_with("/dispatch ") {
                json!({"run_id":rid,"target_node_id":node,"target_slot_id":slot}).to_string()
            } else {
                tarla_supervisor::dispatch_acknowledgement(&rid)
            };
            let tx = c.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
            record_run_reply(&tx, s.id(), id, &key, &rid, &reply)?;
            tx.commit()?;
            s.context_wake.notify_one();
            return Ok(message_result(id, reply, Some(rid)));
        }
        tarla_context::append(&mut c, s.id(), id, channel, &source, "user", text)?;
    }
    let skill = skills::select(&s.ws, &s.db()?, &key, text)?;
    let plane = Plane {
        s,
        source: key,
        draft: Some(s.drafts.begin(id, channel, &source)),
        completion: None,
        skill,
    };
    let reply = s
        .brain
        .handle_trigger(
            &plane,
            &SupervisorTrigger::UserMessage {
                conversation_id: id.into(),
                text: text.into(),
            },
        )
        .await?;
    let run = s.db()?.query_row(
        "SELECT run_id FROM messages WHERE conversation_id=? AND channel_type='supervisor' AND source_msg_id=?",
        params![id, plane.source], |r| r.get::<_,Option<String>>(0))?;
    s.context_wake.notify_one();
    Ok(message_result(id, reply, run))
}

pub async fn deliver_receipts(s: S) {
    use futures_util::{stream::FuturesUnordered, StreamExt};
    use std::collections::BTreeSet;

    if !s.is_supervisor() {
        return;
    }
    let mut active = BTreeSet::new();
    let mut workers = FuturesUnordered::new();
    let mut cursor = String::new();
    loop {
        if active.len() < 16 {
            let pending = s.db().and_then(|db| {
                list(
                    &db,
                    "SELECT DISTINCT conversation_id FROM service_wakes WHERE delivered=0
                 ORDER BY conversation_id<=?,conversation_id",
                    &[&cursor],
                )
            });
            match pending {
                Ok(rows) => {
                    for row in rows {
                        let Some(id) = row["conversation_id"].as_str() else {
                            continue;
                        };
                        if active.insert(id.to_owned()) {
                            cursor = id.to_owned();
                            let service = s.clone();
                            let conversation = id.to_owned();
                            workers.push(async move {
                                if let Err(e) = deliver_next_receipt(&service, &conversation).await
                                {
                                    eprintln!(
                                        "receipt notification deferred for {conversation}: {e}"
                                    );
                                    tokio::time::sleep(Duration::from_secs(2)).await;
                                }
                                conversation
                            });
                            if active.len() == 16 {
                                break;
                            }
                        }
                    }
                }
                Err(e) => eprintln!("receipt scan deferred: {e}"),
            }
        }
        tokio::select! {
            Some(id) = workers.next(), if !workers.is_empty() => { active.remove(&id); },
            _ = s.receipt_wake.notified() => (),
            _ = tokio::time::sleep(Duration::from_secs(2)) => (),
        }
    }
}

async fn deliver_next_receipt(s: &Service, id: &str) -> Result<()> {
    let _lock = s.lock(id).await;
    let db = s.db()?;
    let wake = db.query_row("SELECT event_id,receipt FROM service_wakes WHERE conversation_id=? AND delivered=0 ORDER BY rowid LIMIT 1",
        [id], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).optional()?;
    let Some((event, raw)) = wake else {
        return Ok(());
    };
    let receipt: tarla_protocol::RunReceipt = serde_json::from_str(&raw)?;
    let source = format!("wake:{event}");
    let existing = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM messages WHERE channel_type='supervisor' AND conversation_id=? AND source_msg_id=?)",
        params![id,source], |r| r.get::<_,bool>(0))?;
    if existing {
        // Previous daemon versions archived the report before marking the wake delivered.
        db.execute(
            "UPDATE service_wakes SET delivered=1 WHERE event_id=? AND conversation_id=?",
            params![event, id],
        )?;
        return Ok(());
    }
    {
        let mut db = s.db()?;
        let tx = db.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
        tarla_context::append_in(
            &tx,
            s.id(),
            id,
            ("receipt", &event),
            "run_receipt",
            &raw,
            Some(&receipt.run_id),
        )?;
        tx.commit()?;
    }
    let plane = Plane {
        s,
        source,
        skill: None,
        draft: None,
        completion: Some((&event, &receipt.run_id)),
    };
    let trigger = SupervisorTrigger::RunCompleted {
        conversation_id: id.into(),
        receipt: serde_json::to_value(&receipt)?,
    };
    let reported = tokio::time::timeout(
        Duration::from_secs(30),
        s.brain.handle_trigger(&plane, &trigger),
    )
    .await;
    if !matches!(reported, Ok(Ok(_))) {
        let fallback = format!(
            "Run {} {}. {}",
            receipt.run_id, receipt.final_state, receipt.executive_summary
        );
        plane.deliver(id, &fallback).await?;
    }
    Ok(())
}
