CREATE TABLE IF NOT EXISTS service_epoch(singleton INTEGER PRIMARY KEY CHECK(singleton=1),supervisor TEXT NOT NULL, term INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS service_peers(node_id TEXT PRIMARY KEY, seen INTEGER NOT NULL, slots TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_claims(run_id TEXT PRIMARY KEY, phase TEXT NOT NULL, terminal TEXT, updated_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS service_inbox(event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_wakes(event_id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, receipt TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS service_pending_wakes ON service_wakes(conversation_id) WHERE delivered=0;
CREATE INDEX IF NOT EXISTS service_wake_runs ON service_wakes(conversation_id,json_extract(receipt,'$.run_id'));
CREATE TABLE IF NOT EXISTS service_responses(channel TEXT NOT NULL, source TEXT NOT NULL, conversation_id TEXT NOT NULL, text TEXT NOT NULL, reply TEXT, PRIMARY KEY(channel,source));
CREATE TABLE IF NOT EXISTS service_frozen(conversation_id TEXT PRIMARY KEY, content TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_leases(nonce TEXT PRIMARY KEY, expires INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS service_run_paths(run_id TEXT PRIMARY KEY,workdir TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_receipt_drafts(run_id TEXT PRIMARY KEY,receipt TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_proposals(id TEXT PRIMARY KEY,proposal TEXT NOT NULL,result TEXT);
CREATE TABLE IF NOT EXISTS service_telemetry(slot_id TEXT PRIMARY KEY,report TEXT NOT NULL,updated_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS service_forwards(source TEXT PRIMARY KEY,run_id TEXT NOT NULL,sent INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS service_binding_epochs(conversation_id TEXT PRIMARY KEY, epoch TEXT NOT NULL, checkpoint TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_pin_epochs(run_id TEXT PRIMARY KEY,epoch TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_handoffs(source TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,result TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_slot_paths(node_id TEXT NOT NULL,slot_id TEXT NOT NULL,workdir TEXT NOT NULL,PRIMARY KEY(node_id,slot_id));
CREATE TABLE IF NOT EXISTS service_peer_facts(node_id TEXT NOT NULL,fact_key TEXT NOT NULL,metadata_json TEXT NOT NULL,verified_at INTEGER NOT NULL,PRIMARY KEY(node_id,fact_key));
CREATE TABLE IF NOT EXISTS service_requirements(run_id TEXT PRIMARY KEY,role TEXT NOT NULL,model_family TEXT NOT NULL,request_json TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_osc(run_id TEXT PRIMARY KEY,approval_id TEXT NOT NULL UNIQUE,fingerprint TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_skill_selections(source TEXT PRIMARY KEY,name TEXT NOT NULL,instructions TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_reply_origins(reply_key TEXT PRIMARY KEY,conversation_id TEXT NOT NULL,channel TEXT NOT NULL,source TEXT NOT NULL,UNIQUE(channel,source));
CREATE TABLE IF NOT EXISTS service_reply_origin_migration(done INTEGER PRIMARY KEY);

CREATE TABLE IF NOT EXISTS service_context_pending(conversation_id TEXT PRIMARY KEY, seq_no INTEGER NOT NULL);
CREATE TRIGGER IF NOT EXISTS service_schedule_context AFTER INSERT ON messages
WHEN NEW.sender_role='assistant'
BEGIN
    INSERT INTO service_context_pending(conversation_id,seq_no) VALUES(NEW.conversation_id,NEW.seq_no)
    ON CONFLICT(conversation_id) DO UPDATE SET seq_no=excluded.seq_no;
END;

CREATE TABLE IF NOT EXISTS service_rate_limits(run_id TEXT PRIMARY KEY, resets_at INTEGER NOT NULL, checkpoint TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_turn_transfers(
    run_id TEXT PRIMARY KEY, target TEXT NOT NULL DEFAULT '', prompt TEXT, manual_source TEXT,
    quota INTEGER NOT NULL DEFAULT 0, checkpoint TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'pending', successor TEXT, last_error TEXT
);
CREATE INDEX IF NOT EXISTS service_pending_transfers ON service_turn_transfers(state) WHERE state='pending';
CREATE TABLE IF NOT EXISTS service_resume_origins(run_id TEXT PRIMARY KEY, predecessor TEXT NOT NULL);

-- Captured on the authority before launch and carried with the assignment.
CREATE TABLE IF NOT EXISTS service_lesson_baselines(
    run_id TEXT PRIMARY KEY REFERENCES runs(run_id), scope TEXT NOT NULL, base_version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS service_distillations(
    run_id TEXT PRIMARY KEY REFERENCES runs(run_id), receipt_event TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending' CHECK(state IN ('pending','queued','no_finding')),
    attempts INTEGER NOT NULL DEFAULT 0, retry_at INTEGER NOT NULL DEFAULT 0);
CREATE INDEX IF NOT EXISTS service_distill_waiting ON service_distillations(run_id) WHERE state='pending';
CREATE TRIGGER IF NOT EXISTS service_distill_completed AFTER INSERT ON outbox_events
WHEN NEW.event_type='run_receipt' AND json_extract(NEW.payload_json,'$.final_state')='succeeded'
BEGIN
    INSERT OR IGNORE INTO service_distillations(run_id,receipt_event) VALUES(NEW.run_id,NEW.event_id);
END;
-- Receipts in the pre-upgrade offline outbox are still eligible.
INSERT OR IGNORE INTO service_distillations(run_id,receipt_event)
SELECT run_id,event_id FROM outbox_events WHERE acked_at IS NULL AND event_type='run_receipt'
AND json_extract(payload_json,'$.final_state')='succeeded';
