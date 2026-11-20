-- Sender history remains available after acknowledgement. Unlike SQLite rowids,
-- these assigned sequence numbers survive VACUUM and administrative snapshots.
CREATE TABLE IF NOT EXISTS service_s1_sequence(
 seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE
);
INSERT INTO service_s1_sequence(event_id)
SELECT o.event_id FROM outbox_events o WHERE NOT EXISTS
 (SELECT 1 FROM service_s1_sequence x WHERE x.event_id=o.event_id) ORDER BY o.rowid;
CREATE TRIGGER IF NOT EXISTS service_sequence_outbox AFTER INSERT ON outbox_events BEGIN
 INSERT INTO service_s1_sequence(event_id) VALUES(NEW.event_id);
END;
CREATE TABLE IF NOT EXISTS service_s1_delivery(supervisor TEXT PRIMARY KEY,seq INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS service_s1_frontier(node TEXT PRIMARY KEY,seq INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS service_s1_received(node TEXT NOT NULL,seq INTEGER NOT NULL,event_id TEXT NOT NULL UNIQUE,digest TEXT NOT NULL,PRIMARY KEY(node,seq));
CREATE TABLE IF NOT EXISTS service_s1_signer(singleton INTEGER PRIMARY KEY CHECK(singleton=1),key BLOB NOT NULL);
INSERT INTO service_s1_signer SELECT 1,randomblob(32) WHERE NOT EXISTS(SELECT 1 FROM service_s1_signer);
CREATE TABLE IF NOT EXISTS service_s1_proofs(run_id TEXT PRIMARY KEY,certificate TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_s1_claims(run_id TEXT PRIMARY KEY,claim_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_assignment_pages(peer TEXT PRIMARY KEY,after_run TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_event_digests(event_id TEXT PRIMARY KEY,digest TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS service_restore_barrier(singleton INTEGER PRIMARY KEY CHECK(singleton=1));
CREATE TABLE IF NOT EXISTS service_restore_pending(node TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS service_restore_sources(node TEXT PRIMARY KEY);
CREATE TABLE IF NOT EXISTS service_restore_nonexecutors(node TEXT PRIMARY KEY);
