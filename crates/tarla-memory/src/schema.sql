CREATE TABLE conversations (
    conversation_id TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    title           TEXT,
    binding_mode    TEXT NOT NULL DEFAULT 'orchestrator',
    pinned_node_id  TEXT,
    pinned_slot_id  TEXT,
    pinned_workdir  TEXT,
    active_tokens   INTEGER NOT NULL DEFAULT 0,
    created_at      INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE TABLE messages (
    seq_no          INTEGER PRIMARY KEY AUTOINCREMENT,
    conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
    channel_type    TEXT NOT NULL,
    source_msg_id   TEXT NOT NULL,
    sender_role     TEXT NOT NULL,
    content         TEXT NOT NULL,
    token_estimate  INTEGER NOT NULL DEFAULT 0,
    compacted_into  TEXT,
    run_id          TEXT,
    created_at      INTEGER NOT NULL,
    UNIQUE(channel_type, source_msg_id)
);

CREATE TABLE context_dag_nodes (
    dag_node_id     TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES conversations(conversation_id),
    depth_level     INTEGER NOT NULL DEFAULT 1,
    start_seq_no    INTEGER NOT NULL,
    end_seq_no      INTEGER NOT NULL,
    parent_dag_id   TEXT REFERENCES context_dag_nodes(dag_node_id),
    summary_text    TEXT NOT NULL,
    key_entities    TEXT NOT NULL,
    token_estimate  INTEGER NOT NULL,
    created_at      INTEGER NOT NULL
);

CREATE TABLE memories (
    memory_id       TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    scope           TEXT NOT NULL,
    content         TEXT NOT NULL,
    content_hash    TEXT NOT NULL,
    version         INTEGER NOT NULL DEFAULT 1,
    hlc_timestamp   TEXT NOT NULL,
    updated_by      TEXT NOT NULL,
    is_deleted      INTEGER NOT NULL DEFAULT 0,
    sync_seq        INTEGER NOT NULL,
    updated_at      INTEGER NOT NULL
);

CREATE INDEX idx_memories_sync_seq ON memories(workspace_id, sync_seq);

CREATE TABLE memory_candidates (
    candidate_id    TEXT PRIMARY KEY,
    memory_id       TEXT REFERENCES memories(memory_id),
    workspace_id    TEXT NOT NULL,
    proposed_text   TEXT NOT NULL,
    base_version    INTEGER NOT NULL,
    hlc_timestamp   TEXT NOT NULL,
    source_node_id  TEXT NOT NULL,
    source_run_id   TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      INTEGER NOT NULL
);

CREATE TABLE workspace_facts (
    node_id         TEXT NOT NULL,
    fact_key        TEXT NOT NULL,
    local_path      TEXT NOT NULL,
    metadata_json   TEXT NOT NULL,
    verified_at     INTEGER NOT NULL,
    PRIMARY KEY (node_id, fact_key)
);

CREATE TABLE runs (
    run_id              TEXT PRIMARY KEY,
    workspace_id        TEXT NOT NULL,
    conversation_id     TEXT NOT NULL,
    supervisor_term     INTEGER NOT NULL,
    target_node_id      TEXT NOT NULL,
    target_slot_id      TEXT NOT NULL,
    desired_state       TEXT NOT NULL,
    observed_state      TEXT NOT NULL,
    mission_packet_json TEXT NOT NULL,
    hc_terminal_id      TEXT,
    native_session_ref  TEXT,
    s0_log_local_path   TEXT,
    memory_snapshot_ver INTEGER,
    receipt_summary     TEXT,
    artifacts_json      TEXT,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL
);

CREATE TABLE outbox_events (
    event_id        TEXT PRIMARY KEY,
    run_id          TEXT,
    event_type      TEXT NOT NULL,
    payload_json    TEXT NOT NULL,
    created_at      INTEGER NOT NULL,
    acked_at        INTEGER
);

CREATE TABLE approvals (
    approval_id     TEXT PRIMARY KEY,
    workspace_id    TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    node_id         TEXT NOT NULL,
    tool_or_cmd     TEXT NOT NULL,
    risk_reason     TEXT NOT NULL,
    resolution      TEXT NOT NULL DEFAULT 'pending',
    resolved_by     TEXT,
    created_at      INTEGER NOT NULL,
    resolved_at     INTEGER
);
CREATE TABLE workspace_identity (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    workspace_id TEXT NOT NULL
);

CREATE INDEX idx_messages_conversation ON messages(conversation_id,seq_no);
CREATE INDEX idx_context_roots ON context_dag_nodes(conversation_id,parent_dag_id,start_seq_no);
CREATE INDEX idx_outbox_pending ON outbox_events(acked_at,event_id);
CREATE INDEX idx_runs_by_node ON runs(target_node_id,observed_state);
CREATE TABLE conversation_prefix (
    conversation_id TEXT PRIMARY KEY REFERENCES conversations(conversation_id),
    frozen_memory TEXT NOT NULL
);
CREATE TRIGGER immutable_message BEFORE UPDATE OF seq_no,conversation_id,channel_type,
    source_msg_id,sender_role,content,token_estimate,run_id,created_at ON messages BEGIN
    SELECT RAISE(ABORT,'original messages are immutable');
END;
CREATE VIRTUAL TABLE message_search USING fts5(content,content='messages',content_rowid='seq_no');
CREATE TRIGGER index_message AFTER INSERT ON messages BEGIN
    INSERT INTO message_search(rowid,content) VALUES(new.seq_no,new.content);
END;

-- Each revision is durable, including deletions and superseded revisions.
CREATE TABLE memory_changes (
    sync_seq INTEGER PRIMARY KEY CHECK(sync_seq>0),
    memory_id TEXT NOT NULL,
    revision_json TEXT NOT NULL
);
CREATE TABLE memory_proposals (
    proposal_id TEXT PRIMARY KEY,
    request_json TEXT NOT NULL,
    result_json TEXT NOT NULL
);
CREATE TABLE memory_candidate_details (
    candidate_id TEXT PRIMARY KEY REFERENCES memory_candidates(candidate_id),
    request_json TEXT NOT NULL
);
CREATE TABLE memory_clock (
    singleton INTEGER PRIMARY KEY CHECK(singleton=1),
    physical_ms INTEGER NOT NULL,
    logical INTEGER NOT NULL
);
CREATE VIRTUAL TABLE memory_search USING fts5(content,content='memories',content_rowid='rowid');
CREATE TRIGGER index_memory AFTER INSERT ON memories BEGIN
    INSERT INTO memory_search(rowid,content) VALUES(new.rowid,new.content);
END;
CREATE TRIGGER reindex_memory AFTER UPDATE ON memories BEGIN
    INSERT INTO memory_search(memory_search,rowid,content) VALUES('delete',old.rowid,old.content);
    INSERT INTO memory_search(rowid,content) VALUES(new.rowid,new.content);
END;
