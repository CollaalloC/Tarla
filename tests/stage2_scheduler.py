"""Public dispatch selection across a supervisor and a registered executor."""
import contextlib
import json
import os
from pathlib import Path
import secrets
import signal
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.error import HTTPError

from acceptance import Node, until


def pair(root):
    chief = Node(root/"chief", "chief", "supervisor_and_executor")
    worker = Node(root/"worker", "worker", "executor_only")
    for host, peer in [(chief,worker), (worker,chief)]:
        with (host.dir/"workspace.toml").open("a") as f:
            f.write(f'\n[[peers]]\nnode_id="{peer.name}"\nurl="{peer.url}"\ntoken_env="TARLA_PAIR_TEST"\n')
    return chief,worker


def slot(node, name, caps, root, tier="prepaid_subscription"):
    with (node.dir/"workspace.toml").open("a") as f:
        f.write(f'\n[[slots]]\nslot_id="{name}"\nmodel_family="{name}"\ndriver="local"\n'
                f'supported_roles=["implementer","reviewer"]\nbilling_tier="{tier}"\n'
                f'node_capabilities={json.dumps(caps)}\nargv=["unused-after-preflight"]\n'
                f'workdir={json.dumps(str(root))}\n')


def check(root):
    chief,worker=pair(root)
    slot(chief,"paid",["generic"],root,"pay_as_you_go_api")
    for name in ("alpha","bravo"):
        slot(worker,name,["generic","remote"],root)
    secret=secrets.token_hex(32)
    with contextlib.ExitStack() as cleanup:
        for host in (chief,worker):
            cleanup.callback(host.stop)
            host.start(secret)
        def remote_slots():
            return {s["slot_id"]:s for s in chief.api("/v1/slots") if s["node_id"]=="worker"}
        until(lambda: len(remote_slots())==2,"peer slot discovery")
        tokens={slot:worker.api(f"/v1/slots/{slot}/telemetry-token",{})["token"] for slot in remote_slots()}
        def report(slot, **fields):
            worker.api(f"/telemetry/v1/slots/{slot}", fields, tokens[slot])
            until(lambda: all(remote_slots()[slot][k]==v for k,v in fields.items()),"replicated quota sample")
        def submit(conv,cap,**args):
            return chief.api("/v1/runs", {"conversation_id":conv,"goal":"check public scheduler",
                "required_caps":[cap],"required_fact_keys":["preflight_missing"],**args})
        def reject(fn, text):
            try: fn()
            except HTTPError as err: assert text in err.read().decode()
            else: raise AssertionError("dispatch unexpectedly accepted")
        # Pause the worker just after registration so pending claims have not entered its next heartbeat.
        os.kill(worker.proc.pid,signal.SIGSTOP)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                submissions=list(pool.map(lambda i: submit("load","remote",request_id=str(i)),range(2)))
            assert {s["target_slot_id"] for s in submissions}=={"alpha","bravo"},submissions
            assert all(s["target_node_id"]=="worker" for s in submissions)
        finally: os.kill(worker.proc.pid,signal.SIGCONT)
        until(lambda: all(chief.api("/v1/runs/"+s["run_id"])["observed_state"]=="failed" for s in submissions),
              "remote preflight receipts")
        until(lambda: all(s["active_runs"]==0 for s in remote_slots().values()),"refreshed remote load")
        assert submit("scored","generic")["target_slot_id"]=="alpha"  # prepaid beats paid; stable tie.
        reject(lambda: submit("no-cap","unavailable"),"eligible")
        report("alpha",weekly_used_pct=95)
        report("bravo",window_used_pct=99)
        assert submit("cutoff","generic")["target_node_id"]=="chief"
        reject(lambda: submit("remote-cutoff","remote"),"eligible")
        report("alpha",weekly_resets_at=int(time.time())-1)
        assert submit("reset","remote")["target_slot_id"]=="alpha"
        report("alpha",weekly_used_pct=99)  # Fresh usage must override the preceding expired reset.
        reject(lambda: submit("new-window","remote"),"eligible")
        report("alpha",weekly_used_pct=0)
        report("bravo",window_used_pct=0)
        author=submit("review","remote",slot_id="alpha")["run_id"]
        assert submit("review","remote",role="reviewer",author_run_id=author,exclude_model_family="unrelated")["target_slot_id"]=="bravo"
        worker.stop()
        until(lambda: remote_slots()["alpha"]["health"]=="node_offline","stale peer filtered",timeout=22)
        reject(lambda: submit("offline","remote"),"eligible")
        assert submit("offline","generic")["target_node_id"]=="chief"
        worker.start(secret)
        until(lambda: remote_slots()["alpha"]["health"]=="healthy","peer rejoined")
        assert submit("recovered","remote")["target_node_id"]=="worker"


if __name__=="__main__":
    with tempfile.TemporaryDirectory(prefix="stage2-slots-") as tmp:
        check(Path(tmp))
    print("PASS native two-node scheduler, concurrent reservations, stale and capped slots, reset and reviewer provenance")
