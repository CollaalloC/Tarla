"""Private bridge to a user-installed Mnemosyne SDK. All data travels over stdin."""
import contextlib
import json
import sqlite3
import sys
import tempfile
from pathlib import Path


def main():
    from mnemosyne import Mnemosyne

    request = json.load(sys.stdin)
    path = Path(request["db"])
    cursor = request["cursor"]
    workspace = request["workspace"]
    # Only Tarla-owned projections can be refreshed. Copying a fresh projection
    # via SQLite backup makes interrupted rebuilds restartable and preserves WAL safety.
    if request["rebuild"]:
        with tempfile.TemporaryDirectory(dir=path.parent) as stage:
            staged = Path(stage) / "index.db"
            engine = Mnemosyne(db_path=staged, session_id=workspace)
            mapping = []
            for identity, version, text in request["records"]:
                remote = engine.remember(text, source="tarla", dedupe=False)
                # Mnemosyne filters some content. The authoritative S2 row remains local.
                if remote:
                    mapping.append((str(remote), identity, version))
            with contextlib.closing(sqlite3.connect(staged)) as source:
                source.execute("CREATE TABLE tarla_projection(workspace TEXT PRIMARY KEY, cursor INTEGER, history_hash TEXT)")
                source.execute("INSERT INTO tarla_projection VALUES(?,?,?)", (workspace, cursor, request["history_hash"]))
                source.execute("CREATE TABLE tarla_members(remote_id TEXT PRIMARY KEY, memory_id TEXT, version INTEGER)")
                source.executemany("INSERT INTO tarla_members VALUES(?,?,?)", mapping)
                source.commit()
                with contextlib.closing(sqlite3.connect(path)) as target:
                    source.backup(target)
            # Close handles before TemporaryDirectory cleanup on Windows.
            from mnemosyne import close_thread_connection
            close_thread_connection(staged, allow_main_thread=True)
            del engine

    engine = Mnemosyne(db_path=path, session_id=workspace)
    hits = engine.recall(request["query"], top_k=request["limit"], source="tarla")
    identities = []
    with contextlib.closing(sqlite3.connect(path)) as conn:
        for hit in hits:
            record = conn.execute("SELECT memory_id,version FROM tarla_members WHERE remote_id=?",
                                  (str(hit.get("id", "")),)).fetchone()
            if record:
                identities.append(record)
    return identities


if __name__ == "__main__":
    # Do not mix provider startup diagnostics with the JSON protocol.
    with contextlib.redirect_stdout(sys.stderr):
        answer = main()
    json.dump(answer, sys.stdout)
