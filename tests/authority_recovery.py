"""Restore an explicitly authorized backup with its worker online and its old primary offline."""
import contextlib
import os
from pathlib import Path
import secrets
import sqlite3
import subprocess
import tempfile
from unittest.mock import patch

from acceptance import BIN, Node, until


def pair(node, peers):
    with (node.dir / "workspace.toml").open("a") as cfg:
        for other in peers:
            key = "TARLA_PAIR_" + "_".join(sorted((node.name, other.name)))
            cfg.write(f'\n[[peers]]\nnode_id="{other.name}"\nurl="{other.url}"\ntoken_env="{key}"\n')


def accepted(node, name, term):
    health = node.api("/health")
    return health["supervisor_node_id"] == name and health["supervisor_term"] == term


def run(root):
    old = Node(root / "old", "chief", "supervisor_only")
    worker = Node(root / "worker", "worker", "executor_only")
    new = Node(root / "new", "backup", "executor_only")
    nodes = [old, worker, new]
    keys = {}
    for i, node in enumerate(nodes):
        pair(node, [other for other in nodes if other != node])
        for other in nodes[i + 1:]:
            keys["TARLA_PAIR_" + "_".join(sorted((node.name, other.name)))] = secrets.token_hex(32)
    with patch.dict(os.environ, keys), contextlib.ExitStack() as cleanup:
        for node in nodes:
            cleanup.callback(node.stop)
        old.start("")
        worker.start("")
        def pinned():
            with sqlite3.connect(worker.dir / "state.db") as db:
                return db.execute("SELECT 1 FROM authority_keys WHERE node='chief' AND term=1").fetchone()
        until(pinned, "worker pins the incumbent key")
        record = {"memory_id":"migration-fixture", "scope":"global","content":"before backup","base_version":0}
        assert old.api("/v1/memories",record)["status"]=="applied"
        until(lambda: any(x["content"]=="before backup" for x in worker.api("/v1/memories")), "first record mirrored")
        old.stop()
        snapshot=root / "restore.db"
        old.cli("supervisor","backup","--to","backup","--snapshot",str(snapshot))
        refused=subprocess.run([BIN/"tarla","--home",new.home,"--workspace","work","supervisor","restore","--snapshot",snapshot],capture_output=True)
        assert refused.returncode and not (new.dir/"state.db").exists()
        # A recovery backup leaves its incumbent operational at the previous term.
        old.start("")
        assert accepted(old, "chief", 1)
        old.stop()
        new.cli("supervisor","restore","--snapshot",str(snapshot),"--confirm-old-offline")
        new.start("")
        assert accepted(new,"backup",2)
        until(lambda: accepted(worker,"backup",2),"worker adopts the signed successor without operator restart")
        old.start("")
        until(lambda: accepted(old,"backup",2),"returning incumbent automatically downgrades")
        assert old.api("/health")["role_mode"]=="executor_only"
        assert new.api("/v1/memories",{**record,"content":"after restore","base_version":1})["status"]=="applied"
        for replica in (old,worker):
            until(lambda: any(x["content"]=="after restore" for x in replica.api("/v1/memories")), "new authority replicated")
            replica.stop()
            replica.start("")
            assert accepted(replica,"backup",2)
    print("PASS explicit recovery, paired authority change, returning supervisor demotion and persistent restart")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as folder:
        run(Path(folder))
