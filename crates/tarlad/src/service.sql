CREATE TABLE IF NOT EXISTS service_epoch(singleton INTEGER PRIMARY KEY CHECK(singleton=1),supervisor TEXT NOT NULL, term INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS service_peers(node_id TEXT PRIMARY KEY, seen INTEGER NOT NULL, slots TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_claims(run_id TEXT PRIMARY KEY, phase TEXT NOT NULL, terminal TEXT, updated_at INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS service_inbox(event_id TEXT PRIMARY KEY, run_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_wakes(event_id TEXT PRIMARY KEY, conversation_id TEXT NOT NULL, receipt TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0);
CREATE TABLE IF NOT EXISTS service_responses(channel TEXT NOT NULL, source TEXT NOT NULL, conversation_id TEXT NOT NULL, text TEXT NOT NULL, reply TEXT, PRIMARY KEY(channel,source));
CREATE TABLE IF NOT EXISTS service_frozen(conversation_id TEXT PRIMARY KEY, content TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_leases(nonce TEXT PRIMARY KEY, expires INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS service_run_paths(run_id TEXT PRIMARY KEY,workdir TEXT NOT NULL);
