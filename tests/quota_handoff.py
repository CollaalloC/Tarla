"""Two real daemons and private PTYs exercise native and portable turn-boundary recovery."""
import contextlib
import json
import os
from pathlib import Path
import secrets
import sys
import tempfile
import time

from acceptance import install_test_slot, until
from stage2_scheduler import pair


DRIVER = """#!/usr/bin/env python3
import json, os, pathlib, sys, time, uuid
profile=pathlib.Path(os.environ["CODEX_HOME"])
name=profile.name
args=sys.argv[1:]
packet=json.loads(args[-1])
rid=packet["run_id"]
root=pathlib.Path.cwd()
if name=="limited":
    session=str(uuid.uuid4())
    log=profile/"sessions"/"2026"/"01"/"20"/("rollout-t-"+session+".jsonl")
    log.parent.mkdir(parents=True,exist_ok=True)
    log.write_text("LOCAL_SESSION_ONLY")
    (profile/"auth.json").write_text("SOURCE_ACCOUNT_ONLY")
    print(json.dumps({"type":"thread.started","thread_id":session}),flush=True)
    print(json.dumps({"type":"item.completed","item":{"type":"agent_message","text":"Completed the initial inspection."}}),flush=True)
    print("S0_ONLY_MARKER",flush=True)
    print(json.dumps({"type":"turn.failed","error":{"code":"rate_limit_exceeded","resets_at":int(time.time())+3600}}),flush=True)
    (root/(rid+".ready")).write_text(session)
    while not (root/(rid+".allow")).exists(): time.sleep(.08)
    sys.exit(1)
else:
    assert name=="spare" and "resume" in args
    session=args[args.index("--json")+1]
    assert uuid.UUID(session) and not (profile/"auth.json").exists()
    logs=list((profile/"sessions").rglob("*"+session+".jsonl"))
    assert len(logs)==1 and logs[0].read_text()=="LOCAL_SESSION_ONLY"
    (root/(rid+".packet")).write_text(json.dumps(packet))
    print(json.dumps({"type":"thread.started","thread_id":session}))
    print(json.dumps({"type":"item.completed","item":{"type":"agent_message","text":"Spare completed."}}))
"""


def configure(node, name, driver, argv, folder):
    with (node.dir/"workspace.toml").open("a") as f:
        f.write(f'\n[[slots]]\nslot_id="{name}"\ndriver="{driver}"\nmodel_family="{driver}"\n'
                f'node_capabilities=["handoff-test"]\nsupported_roles=["implementer"]\n'
                f'workdir={json.dumps(str(folder))}\nargv={json.dumps(argv)}\n')


def exercise(root, hc):
    chief,remote=pair(root)
    local,work=install_test_slot(chief,hc),install_test_slot(remote,hc)
    driver=work/"codex"
    driver.write_text(DRIVER)
    driver.chmod(0o700)
    for name in ("limited","spare"):
        configure(remote,name,"codex",[str(driver),"exec"],work)
    portable=local/"portable.py"
    portable.write_text("import pathlib,json,sys\np=json.loads(sys.argv[-1]); pathlib.Path(p['run_id']+'.packet').write_text(json.dumps(p))\n")
    configure(chief,"portable","local",[sys.executable,str(portable)],local)
    secret=secrets.token_hex(32)
    with contextlib.ExitStack() as cleanup:
        for host in (chief,remote):
            cleanup.callback(host.stop)
            host.start(secret)
        until(lambda: len(chief.api("/v1/slots"))==5,"both slot registries")
        def run(scenario):
            req={"conversation_id":scenario,"request_id":"source","goal":"Inspect then finish "+scenario,
                 "acceptance_criteria":["finish once"],"required_caps":["handoff-test"],
                 "node_id":"worker","slot_id":"limited"}
            parent=chief.api("/v1/runs",req)["run_id"]
            ready=work/(parent+".ready")
            until(ready.exists,"native driver reported rate limit and remains live")
            # Neither the visible 429 nor a supervisor restart can move a live attempt.
            assert len([r for r in chief.api("/v1/runs") if r["conversation_id"]==scenario])==1
            chief.stop()
            chief.start(secret)
            assert chief.api("/v1/runs",req)["run_id"]==parent
            (work/(parent+".allow")).touch()
            try:
                successor=until(lambda: chief.api("/v1/runs/"+parent)["successor_run_id"],"bounded continuation",timeout=35)
            except AssertionError as error:
                raise AssertionError((str(error),chief.api("/v1/runs/"+parent),chief.api("/v1/slots"))) from error
            record=until(lambda:(r if (r:=chief.api("/v1/runs/"+successor))["observed_state"] in ("failed","succeeded") else None),"successor receipt",timeout=30)
            assert record["observed_state"]=="succeeded",record
            assert record["conversation_id"]==scenario and record["predecessor_run_id"]==parent
            assert record["target_slot_id"]==("spare" if scenario=="resume" else "portable"),record
            folder=work if scenario=="resume" else local
            packet=json.loads((folder/(successor+".packet")).read_text())
            assert packet["objective"]==req["goal"] and packet["acceptance_criteria"]==["finish once"]
            assert parent in packet["distilled_context"][0] and "initial inspection" in packet["distilled_context"][0]
            assert "S0_ONLY_MARKER" not in str(packet) and "LOCAL_SESSION_ONLY" not in str(packet)
            assert len([r for r in chief.api("/v1/runs") if r["conversation_id"]==scenario])==2
            src=chief.api("/v1/runs/"+parent)
            assert src["observed_state"]=="failed" and src["target_slot_id"]=="limited"
            assert any(isinstance(slot["health"],dict) and "cooling_down" in slot["health"]
                       for slot in chief.api("/v1/slots") if slot["slot_id"]=="limited")
        run("resume")
        for name,fields in [("limited",{"health":"healthy"}),("spare",{"weekly_used_pct":100})]:
            token=remote.api(f"/v1/slots/{name}/telemetry-token",{})["token"]
            remote.api(f"/telemetry/v1/slots/{name}",fields,token)
            until(lambda: any(slot["slot_id"]==name and all(slot[k]==v for k,v in fields.items())
                  for slot in chief.api("/v1/slots")),"replicated profile availability")
        run("portable")


if __name__=="__main__":
    with tempfile.TemporaryDirectory(prefix="quota-handoff-") as tmp:
        exercise(Path(tmp), os.environ["TARLA_TEST_HC"])
    print("PASS confirmed 429 boundary, same-machine account resume, cross-node portable checkpoint and restart")
