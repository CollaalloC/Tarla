use super::*;
use async_trait::async_trait;
use rusqlite::{params,OptionalExtension};
use tarla_memory::{MemoryBackend,Mutation,SqliteMemory};
use tarla_supervisor::{ControlPlane,ControlTool,ChatMessage,SupervisorTrigger};

pub fn ensure_conversation(c:&Connection,s:&Service,id:&str)->Result<()>{
    validate_id(id)?;
    c.execute("INSERT INTO conversations(conversation_id,workspace_id,title,created_at,updated_at) VALUES(?,?,?,?,?) ON CONFLICT DO NOTHING",params![id,s.id(),id,now(),now()])?; Ok(())
}
pub fn set_binding(s:&Service,id:&str,v:&Value)->Result<Value>{
    let c=s.db()?; ensure_conversation(&c,s,id)?;
    match string(v,"mode")? {
        "orchestrator"=>{c.execute("UPDATE conversations SET binding_mode='orchestrator',pinned_node_id=NULL,pinned_slot_id=NULL,pinned_workdir=NULL WHERE conversation_id=?", [id])?;}
        "pinned"=>{
            let node=string(v,"node_id")?;let slot=string(v,"slot_id")?;let path=string(v,"workdir")?;
            if !fleet::snapshots(s)?.iter().any(|p|p.node_id==node && p.slot_id==slot){bail!("unknown slot")}
            if node==s.node() && !s.slots.iter().any(|p|p.snapshot.slot_id==slot && p.workdir.to_string_lossy()==path){bail!("pinned workdir must match configured slot workdir")}
            c.execute("UPDATE conversations SET binding_mode='pinned',pinned_node_id=?,pinned_slot_id=?,pinned_workdir=? WHERE conversation_id=?",params![node,slot,path,id])?;
        }
        _=>bail!("unsupported binding mode")
    };Ok(json!({"mode":v["mode"]}))
}
pub async fn search_memories(s:&Service,query:&str,limit:usize)->Result<Vec<tarla_memory::Memory>> {
    if let Some(provider)=s.memory_index.clone().filter(|_|!query.trim().is_empty()) {
        let path=s.db_path.clone();
        let (workspace,node,query)=(s.id().to_owned(),s.node().to_owned(),query.to_owned());
        return tokio::task::spawn_blocking(move || {
            let mut db=Connection::open(path)?;
            db.busy_timeout(Duration::from_secs(5))?;
            provider.search(&mut db,&workspace,&node,&query,limit)
        }).await?;
    }
    SqliteMemory::new(&mut s.db()?,s.id(),s.node()).search(query,limit)
}
pub async fn memories(s:&Service,v:&Value)->Result<Value>{
    let op=v["operation"].as_str().unwrap_or("update");
    if op=="search"{return Ok(json!(search_memories(s,v["query"].as_str().unwrap_or(""),100).await?))}
    let mut db=s.db()?; let mut m=SqliteMemory::new(&mut db,s.id(),s.node());
    if !s.is_supervisor(){bail!("global memory writes require the designated supervisor")}
    if !matches!(op,"create"|"update"|"archive"){bail!("unsupported memory operation")}
    let req=Mutation{proposal_id:v["proposal_id"].as_str().map(String::from).unwrap_or_else(uid),memory_id:string(v,"memory_id")?.into(),scope:v["scope"].as_str().unwrap_or("global").into(),content:v["content"].as_str().unwrap_or_default().into(),base_version:v["base_version"].as_i64().context("base_version required")?,source_node_id:s.node().into(),source_run_id:None,is_deleted:op=="archive",hlc_timestamp:None};
    let reply=m.propose_mutation(&req)?; s.notify.notify_waiters();Ok(json!(reply))
}

pub struct Plane<'a>{s:&'a Service,source:String}
impl Plane<'_>{
    fn effect(&self,action:&str)->String{
        format!("{:x}",Sha256::digest(format!("{}\n{action}",self.source).as_bytes()))
    }
}
#[async_trait]
impl ControlPlane for Plane<'_> {
    async fn short_circuit(&self,trigger:&SupervisorTrigger)->Result<Option<String>>{
        let SupervisorTrigger::UserMessage{conversation_id:id,text}=trigger else{return Ok(None)};
        let answer=match text.trim(){
            "/help"=>"Commands: /runs, /memories, /dispatch <goal>, /unbind".into(),
            "/runs"=>serde_json::to_string(&api::run_list(self.s)?)?,
            "/memories"=>memories(self.s,&json!({"operation":"search"})).await?.to_string(),
            "/unbind"=>set_binding(self.s,id,&json!({"mode":"orchestrator"}))?.to_string(),
            t if t.starts_with("/dispatch ")=>fleet::dispatch(self.s,id,&json!({"goal":t[10..].trim()}),Some(self.effect("dispatch")))?.to_string(),
            _=>{
                let c=self.s.db()?;
                let binding=c.query_row("SELECT pinned_node_id,pinned_slot_id,pinned_workdir FROM conversations WHERE conversation_id=? AND binding_mode='pinned'",[id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?))).optional()?;
                if let Some((node,slot,workdir))=binding{
                    fleet::dispatch(self.s,id,&json!({"goal":text,"node_id":node,"slot_id":slot,"pinned_workdir":workdir}),Some(self.effect("dispatch")))?.to_string()
                }else{return Ok(None)}
            }
        };Ok(Some(answer))
    }
    async fn context(&self,trigger:&SupervisorTrigger)->Result<Vec<ChatMessage>>{
        let id=match trigger{SupervisorTrigger::UserMessage{conversation_id,..}|SupervisorTrigger::RunCompleted{conversation_id,..}=>conversation_id};
        let mut c=self.s.db()?;
        let prefix=SqliteMemory::new(&mut c,self.s.id(),self.s.node()).prefetch_frozen_snapshot("global",self.s.ws.config.memory.prefetch_token_budget)?;
        let p=&self.s.ws.config.context_policy;
        let history=tarla_context::assemble(&mut c,id,&self.s.ws.soul,&prefix,&tarla_context::ContextPolicy{
            supervisor_active_budget_tokens:p.supervisor_active_budget_tokens.checked_sub(tarla_supervisor::control_window_reserve()).context("supervisor budget too small for control tools")?,compaction_high_watermark_pct:p.compaction_high_watermark_pct,protected_verbatim_turns:p.protected_verbatim_turns
        })?;
        Ok(history.into_iter().map(|m| match m.role.as_str(){"system"=>ChatMessage::System(m.content),"assistant"=>ChatMessage::Assistant(m.content),_=>ChatMessage::User(m.content)}).collect())
    }
    async fn call_tool(&self,id:&str,tool:ControlTool,mut args:Value)->Result<Value>{
        match tool{
            ControlTool::ListFleetAndRuns=>Ok(json!({"slots":fleet::snapshots(self.s)?,"runs":api::run_list(self.s)?})),
            ControlTool::DispatchMission=>fleet::dispatch(self.s,id,&args,Some(self.effect("dispatch"))),
            ControlTool::InspectRunLog=>api::read_log(self.s,string(&args,"run_id")?,args["offset"].as_u64().unwrap_or(0),args["limit"].as_u64().unwrap_or(40),args["query"].as_str().unwrap_or("")).await,
            ControlTool::SessionRecall=>Ok(json!(tarla_context::recall(&self.s.db()?,id,args["dag_node_id"].as_str(),args["query"].as_str().unwrap_or(""),args["after_seq"].as_i64().unwrap_or(0),args["limit"].as_u64().unwrap_or(10).min(50) as usize)?)),
            ControlTool::MemoryManage=>{if args["operation"]=="create"{
                    if args["memory_id"].is_null(){args["memory_id"]=json!(self.effect(&format!("memory-id/{args}")));}
                    if args["base_version"].is_null(){args["base_version"]=json!(0);}
                }
                args["proposal_id"]=json!(self.effect(&format!("memory/{args}")));memories(self.s,&args).await}
            ControlTool::BindChannel=>set_binding(self.s,id,&args)
        }
    }
    async fn deliver(&self,id:&str,reply:&str)->Result<()>{
        tarla_context::append(&mut self.s.db()?,self.s.id(),id,"supervisor",&self.source,"assistant",reply)?;Ok(())
    }
}
pub async fn message(s:&Service,id:&str,payload:&Value)->Result<String>{
    validate_id(id)?;let text=string(payload,"text")?; let channel=payload["channel"].as_str().unwrap_or("cli");validate_id(channel)?;
    let source=payload["source_msg_id"].as_str().map(String::from).unwrap_or_else(uid);
    if source.is_empty()||source.len()>512||channel=="supervisor"||channel=="receipt"{bail!("invalid message source")}
    let _lock=s.lock(id).await;
    let key=format!("in:{channel}:{source}");
    {
        let mut c=s.db()?;
        c.execute("INSERT OR IGNORE INTO service_responses VALUES(?,?,?,?,NULL)",params![channel,source,id,text])?;
        let (old_id,old_text):(String,String)=c.query_row("SELECT conversation_id,text FROM service_responses WHERE channel=? AND source=?",params![channel,source],|r|Ok((r.get(0)?,r.get(1)?)))?;
        if old_id!=id||old_text!=text{bail!("message source id already used for different content")}
        let replay=c.query_row("SELECT content FROM messages WHERE channel_type='supervisor' AND source_msg_id=?",[&key],|r|r.get::<_,String>(0)).optional()?;
        if let Some(v)=replay{return Ok(v)}
        tarla_context::append(&mut c,s.id(),id,channel,&source,"user",text)?;
    }
    let plane=Plane{s,source:key};
    s.brain.handle_trigger(&plane,&SupervisorTrigger::UserMessage{conversation_id:id.into(),text:text.into()}).await
}

pub async fn deliver_receipts(s:S){
    loop{
        if s.is_supervisor(){let result=next_wake(&s).await;if let Err(e)=result{eprintln!("receipt notification deferred: {e}")}}
        tokio::select!{_=s.notify.notified()=>(),_=tokio::time::sleep(Duration::from_secs(2))=>()}
    }
}
async fn next_wake(s:&Service)->Result<()>{
    let entries=list(&s.db()?,"SELECT * FROM service_wakes WHERE delivered=0 ORDER BY rowid LIMIT 32",&[])?;
    for v in entries{
        let id=string(&v,"conversation_id")?;let event=string(&v,"event_id")?;let raw=string(&v,"receipt")?;
        let _lock=s.lock(id).await;
        let source=format!("wake:{event}"); let c=s.db()?;
        let existing=c.query_row("SELECT 1 FROM messages WHERE channel_type='supervisor' AND source_msg_id=?",[&source],|r|r.get::<_,i32>(0)).optional()?.is_some();
        if !existing{
            let receipt:Value=serde_json::from_str(raw)?;
            tarla_context::append(&mut s.db()?,s.id(),id,"receipt",event,"run_receipt",raw)?;
            let plane=Plane{s,source};
            let trigger=SupervisorTrigger::RunCompleted{conversation_id:id.into(),receipt:receipt.clone()};
            if s.brain.handle_trigger(&plane,&trigger).await.is_err(){
                let fallback=format!("Run {} {}. {}",receipt["run_id"].as_str().unwrap_or(""),receipt["final_state"].as_str().unwrap_or("completed"),receipt["executive_summary"].as_str().unwrap_or(""));
                plane.deliver(id,&fallback).await?;
            }
        }
        c.execute("UPDATE service_wakes SET delivered=1 WHERE event_id=?",[event])?;
    };Ok(())
}
