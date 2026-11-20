"""Rejoin an older designated authority using the real headless CLI, HTTP and SQLite mirrors."""
from contextlib import ExitStack
from pathlib import Path
from tempfile import TemporaryDirectory
import os
import secrets
from unittest.mock import patch
import sqlite3
from urllib.error import HTTPError
from acceptance import Node, until


def put(node, key, text, base=0, op="update"):
    return node.api("/v1/memories", {"memory_id":key, "scope":"global", "content":text,
                                     "base_version":base, "operation":op})


def records(node):
    return {r["memory_id"]:r for r in node.api("/v1/memories?limit=256")}


def pair(node, peer, token_env="TARLA_PAIR_TEST"):
    node.cli("pair", peer.name, peer.url, "--token-env", token_env)


def scenario(root, divergent):
    secret = secrets.token_hex(32)
    a = Node(root/"a","chief","supervisor_only")
    b = Node(root/"b","worker","executor_only")
    c = Node(root/"c","replacement","executor_only")
    pair(a,b)
    pair(b,a)
    pair(b,c,"TARLA_RECOVERY_PAIR")
    pair(c,b,"TARLA_RECOVERY_PAIR")
    with ExitStack() as cleanup:
        cleanup.enter_context(patch.dict(os.environ, {"TARLA_RECOVERY_PAIR":secrets.token_hex(32)}))
        for node in [a,b,c]:
            cleanup.callback(node.stop)
        a.start(secret)
        b.start(secret)
        put(a, "style", "base")
        put(a, "obsolete", "archive this")
        until(lambda: len(records(b))==2,"initial offline mirror")
        # The backup retains the incumbent. Changes below did not exist in its ledger.
        a.stop()
        backup=root/"historical.db"
        a.cli("supervisor","backup","--to",c.name,"--snapshot",str(backup))
        a.start(secret)
        if divergent:
            put(a,"style","old branch edit",1)
        else:
            for i in range(38):
                put(a,f"note-{i:02}",f"old branch lesson {i}")
            put(a,"obsolete","",1,"archive")
        expected = records(a)
        until(lambda: records(b)==expected,"mirror of post-backup source revisions")
        a.stop()
        if divergent:
            # The first proposal cannot be accepted at its claimed version.
            assert put(b,"future-base","not ready",99)["status"]=="queued"
            assert put(b,"offline","queued behind the future base")["status"]=="queued"
        b.stop()
        c.cli("supervisor","restore","--snapshot",str(backup),"--confirm-old-offline")
        c.start(secret)
        assert len(records(c))==2
        if divergent:
            # Both histories now end at sequence 3 but contain distinct decisions.
            put(c,"style","new authority edit",1)
        b.cli("supervisor","set-authority","--node",c.name,"--term","2")
        b.start(secret)
        if divergent:
            until(lambda: records(b)["style"]["content"]=="new authority edit","equal-height fork rejoin")
            action=until(lambda: next((x for x in c.api("/v1/actions")
                         if x["kind"]=="memory_conflict" and x["proposed_text"]=="old branch edit"),None),
                         "divergent revision presented for human review")
            assert action["current_version"]==2
            until(lambda: "offline" in records(c) and "offline" in records(b),
                  "unrelated queued mutation delivered despite unavailable base")
            put(c,"style","review changed",2)
            try:
                c.api("/v1/actions/"+action["id"],{"resolution":"approved","expected_version":2})
                raise AssertionError("outdated review accepted")
            except HTTPError:
                pass
            c.api("/v1/actions/"+action["id"],{"resolution":"approved","expected_version":3})
            until(lambda: records(b)["style"]["version"]==4,"approved recovery candidate replicated")
        else:
            until(lambda: len(records(c))==len(expected),"multi-page archived lessons recovered")
            until(lambda: records(c)==records(b),"newly sequenced replicas converge")
            assert {k:v["content"] for k,v in records(c).items()}=={k:v["content"] for k,v in expected.items()}
        assert records(c)==records(b)
        def ledger(node):
            with sqlite3.connect(node.dir/"state.db") as db:
                return db.execute("SELECT sync_seq,history_hash,revision_json FROM memory_changes ORDER BY sync_seq").fetchall()
        assert ledger(c)==ledger(b)
        before=records(b)
        c.stop()
        b.stop()
        b.start(secret)
        assert records(b)==before
        assert records(b)["style"]["content"]==("old branch edit" if divergent else "base")
        print("PASS historical backup, offline restart, and converged S2 history"+(" after equal-cursor conflict review" if divergent else " after multi-page recovery with deletion"))


if __name__=="__main__":
    with TemporaryDirectory() as directory:
        for name,divergent in [("prefix",False),("fork",True)]:
            scenario(Path(directory)/name,divergent)
