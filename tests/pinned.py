"""Exercise two remote CLI turns, switch, slot credentials and per-account continuation.
Set TARLA_TEST_HC to the upstream herdr-core binary. Driver fixtures emit real-shaped JSONL.
"""
import contextlib
import json
import os
from pathlib import Path
import secrets
import tempfile
from urllib.error import HTTPError
from urllib.request import Request

from acceptance import HTTP, Node, install_test_slot, until


def fixtures(node, hc):
    work = install_test_slot(node, hc)
    for name in ["codex", "claude"]:
        exe = work / name
        exe.write_text("""#!/usr/bin/env python3
import json, os, pathlib, sys, time, uuid
args=sys.argv[1:]
packet=json.loads(args[-1])
codex=pathlib.Path(sys.argv[0]).name=="codex"
key="CODEX_HOME" if codex else "CLAUDE_CONFIG_DIR"
profile=pathlib.Path(os.environ[key])
session=profile / "fixture-id"
if session.exists():
    expected=session.read_text()
    assert ("resume" if codex else "--resume") in args and expected in args, args
else:
    expected=str(uuid.uuid4())
    session.write_text(expected)
if packet["objective"]=="wait": time.sleep(2)
with open("calls", "a") as f: print(json.dumps([key,args,expected]),file=f)
if codex:
    assert args[0]=="exec" and "--json" in args
    print(json.dumps({"type":"thread.started","thread_id":expected}), flush=True)
    print(json.dumps({"type":"item.completed","item":{"type":"agent_message","text":"Codex checked."}}))
else:
    assert "--print" in args and "--output-format" in args
    print(json.dumps({"type":"result","session_id":expected,"result":"Claude checked."}))
""")
        exe.chmod(0o700)
        argv = [str(exe), "exec"] if name == "codex" else [str(exe)]
        with (node.dir / "workspace.toml").open("a") as f:
            f.write(f'\n[[slots]]\nslot_id="{name}"\ndriver="{name}"\nmodel_family="{name}"\n'
                    f'workdir={json.dumps(str(work))}\nargv={json.dumps(argv)}\n')
    return work


def check(tmp, hc):
    chief = Node(tmp / "chief", "chief", "supervisor_only")
    remote = Node(tmp / "worker", "worker", "executor_only")
    for host, peer in [(chief, remote), (remote, chief)]:
        with (host.dir / "workspace.toml").open("a") as f:
            f.write(f'\n[[peers]]\nnode_id="{peer.name}"\nurl="{peer.url}"\ntoken_env="TARLA_PAIR_TEST"\n')
    work = fixtures(remote, hc)
    secret = secrets.token_hex(32)
    with contextlib.ExitStack() as cleanup:
        for host in [chief, remote]:
            cleanup.callback(host.stop)
            host.start(secret)
        until(lambda: any(s["slot_id"]=="codex" for s in chief.api("/v1/slots")), "remote Codex discovery")
        def chat(text, source):
            return chief.api("/v1/conversations/pinned/messages", {"text":text,"source_msg_id":source,"channel":"app"})
        def wait_turn(run):
            obj = until(lambda: (r if (r:=chief.api("/v1/runs/"+run))["observed_state"] in ("succeeded","failed") else None), "remote pinned completion")
            assert obj["observed_state"]=="succeeded", obj
        bind = Request(chief.url+"/v1/conversations/pinned/binding",method="PUT",
             data=json.dumps({"mode":"pinned","node_id":"worker","slot_id":"codex","workdir":str(work)}).encode(),
             headers={"Content-Type":"application/json","Authorization":"Bearer "+chief.token})
        HTTP.open(bind).close()
        one=chat("wait","c1")["run_id"]
        try:
            chat("/switch claude","busy")
            raise AssertionError("switched active turn")
        except HTTPError as e:
            assert e.code in (400,409) and b"still active" in e.read()
        wait_turn(one)
        wait_turn(chat("continue","c2")["run_id"])
        token=remote.api("/v1/slots/codex/telemetry-token",{})["token"]
        payload={"health":"healthy","window_used_pct":97,"node_id":"worker","slot_id":"codex"}
        try:
            remote.api("/telemetry/v1/slots/claude",payload,token)
            raise AssertionError("wrong-slot credential accepted")
        except HTTPError as e:
            assert e.code==401
        remote.api("/telemetry/v1/slots/codex",payload,token)
        until(lambda: any(s["slot_id"]=="codex" and s["window_used_pct"]==97 for s in chief.api("/v1/slots")), "peer dynamic telemetry")
        switched=json.loads(chat("/switch claude","c3")["reply"])
        assert switched["slot_id"]=="claude" and json.loads(chat("/switch claude","c3")["reply"])==switched
        wait_turn(chat("@claude finish","c4")["run_id"])
        wait_turn(chat("continue","c5")["run_id"])
        calls=[json.loads(line) for line in (work/"calls").read_text().splitlines()]
        assert len(calls)==4 and calls[0][2]==calls[1][2] and calls[2][2]==calls[3][2]
        assert calls[0][2]!=calls[2][2]
        assert "Codex checked" in str(json.loads(calls[2][1][-1])["distilled_context"])
        print("PASS remote native session reuse, fenced handoff, durable checkpoint and scoped telemetry")


if __name__=="__main__":
    with tempfile.TemporaryDirectory() as tmp:
        check(Path(tmp), os.environ["TARLA_TEST_HC"])
