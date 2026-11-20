"""Run the installed Flutter client twice on a supplied emulator against three real host daemons.

Set TARLA_TEST_HC to the matching host's herdr-core. This fixture publishes only two
supervisor URLs and their workspace bearer tokens to the first disposable app process.
"""
import argparse
import contextlib
import json
import os
from pathlib import Path
import secrets
import sqlite3
import subprocess
import sys
import tempfile
import threading
from http.server import ThreadingHTTPServer

from acceptance import BIN, Model, Node, ROOT, install_test_slot, until


APP = ROOT / "apps/tarla-app"
OBJECTIVE = "Mobile terminal relay verification"
FIRST = "Original mobile archive entry"
LIFE = "Personal archive from the life workspace"


def write_brain(node, port):
    config = node.dir / "workspace.toml"
    content = config.read_text().replace('fallback_chain = []','fallback_chain = ["http:mobile"]')
    config.write_text(content + f'\n[supervisor.brain.backends.mobile]\nkind="openai"\n'
        f'model="fixture"\nendpoint="http://127.0.0.1:{port}/model"\n')


def pair(left, right):
    with (left.dir / "workspace.toml").open("a") as config:
        config.write(f'\n[[peers]]\nnode_id="{right.name}"\nurl="{right.url}"\ntoken_env="TARLA_PAIR_TEST"\n')


def seed(root, stack, hc):
    model = ThreadingHTTPServer(("127.0.0.1",0),Model)
    threading.Thread(target=model.serve_forever,daemon=True).start()
    stack.callback(model.server_close)
    stack.callback(model.shutdown)
    work = Node(root/"w","chief","supervisor_only")
    worker = Node(root/"e","desk","executor_only")
    life = Node(root/"l","chief","supervisor_only",workspace="life")
    write_brain(work,model.server_port)
    write_brain(life,model.server_port)
    pair(work,worker)
    pair(worker,work)
    project = install_test_slot(worker,hc)
    (project/"job.py").write_text(
        "import sys,time\nfrom pathlib import Path\n"
        "print('MOBILE-PTY-READY',flush=True)\n"
        "line=input().strip()\nPath('mobile-input').write_text(line)\n"
        "print('MOBILE-INPUT-ACCEPTED' if line=='mobile-input-proof' else 'BAD-INPUT',flush=True)\n"
        "time.sleep(4)\n"
    )
    pairing = secrets.token_hex(32)
    for node in (work,worker,life):
        stack.callback(node.stop)
        node.start(pairing)
    for index in range(34):
        work.api("/v1/conversations/mobile-archive/messages",{
            "text": FIRST if index==0 else f"Archive message {index}",
            "channel":"cli","source_msg_id":f"record-{index}"})
    life.api("/v1/conversations/personal-archive/messages",{"text":LIFE,"source_msg_id":"personal"})
    until(lambda: any(s["node_id"]=="desk" for s in work.api("/v1/slots")),"paired mobile fixture executor")
    run = work.api("/v1/runs", {"conversation_id":"mobile-task","objective":OBJECTIVE,
        "node_id":"desk","slot_id":"fixture","acceptance_criteria":["Record the terminal input."]})["run_id"]
    until(lambda: work.api("/v1/runs/"+run).get("hc_terminal_id"), "real waiting PTY",timeout=45)
    # Preserve proof after the short-lived replay-protection table's normal expiry.
    for node in (work,worker):
        with sqlite3.connect(node.dir/"state.db") as db:
            db.executescript("CREATE TABLE mobile_used_leases(nonce TEXT PRIMARY KEY);"
                "CREATE TRIGGER mobile_lease AFTER INSERT ON service_leases BEGIN "
                "INSERT INTO mobile_used_leases VALUES(NEW.nonce); END;")
    assert work.api("/v1/runs/"+run+"/attach",{"mode":"observe","route":"auto"})["route"]=="supervisor"
    return work,worker,life,project


def flutter(device, filename, defines=None):
    command=["flutter","test","--no-pub","--timeout","8m","-d",device]
    if defines is not None:
        command.append("--dart-define-from-file="+str(defines))
    command.append("integration_test/"+filename)
    subprocess.run(command,cwd=APP,check=True,timeout=1000)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("platform",choices=("android","ios"))
    parser.add_argument("device")
    args=parser.parse_args()
    hc=os.environ.get("TARLA_TEST_HC")
    if not hc or not Path(hc).is_file() or not (BIN/"tarlad").is_file():
        parser.error("build tarlad and tarla, then set TARLA_TEST_HC to host herdr-core")
    # A short home also fits Darwin's Unix-domain socket path limit.
    with tempfile.TemporaryDirectory(prefix="tm-",dir="/tmp") as tmp, contextlib.ExitStack() as cleanup:
        root=Path(tmp)
        work,worker,life,project=seed(root,cleanup,hc)
        if args.platform=="android":
            for node in (work,life):
                port=f"tcp:{node.port}"
                subprocess.run(["adb","-s",args.device,"reverse",port,port],check=True)
                cleanup.callback(subprocess.run,["adb","-s",args.device,"reverse","--remove",port],check=False)
        config=root/"ephemeral.json"
        config.write_text(json.dumps({"WORK_URL":work.url,"WORK_TOKEN":work.token,
            "LIFE_URL":life.url,"LIFE_TOKEN":life.token}))
        config.chmod(0o600)
        flutter(args.device,"native_pairing_test.dart",config)
        until(lambda:(project/"mobile-input").is_file(), "input recorded in real executor")
        assert (project/"mobile-input").read_text()=="mobile-input-proof"
        for node in (work,worker):
            with sqlite3.connect(node.dir/"state.db") as db:
                (used,)=db.execute("SELECT count(*) FROM mobile_used_leases").fetchone()
                assert used>=2, f"{node.name} consumed {used} leases for two UI sessions"
        config.unlink()
        if args.platform=="android":
            subprocess.run(["adb","-s",args.device,"shell","am","force-stop","dev.tarla.client"],check=True)
        else:
            # Flutter may have terminated the first process after reporting its result.
            subprocess.run(["xcrun","simctl","terminate",args.device,"dev.tarla.client"],check=False)
        flutter(args.device,"native_restore_test.dart")
        print(f"PASS {args.platform} fresh-process secure restore, two real workspace archives, "
              "supervisor-relayed observe/input and two executor leases")


if __name__=="__main__":
    main()
