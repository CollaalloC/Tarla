"""Black-box acceptance using two actual daemons, private homes, and a local model fixture."""
import base64
import contextlib
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError, URLError
from urllib.request import Request, ProxyHandler, build_opener

ROOT = Path(__file__).resolve().parents[1]
BIN = Path(os.environ.get("TARLA_BIN_DIR", ROOT / "target/debug"))
HTTP = build_opener(ProxyHandler({}))
SPACE = "work"


class Model(BaseHTTPRequestHandler):
    calls = 0

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/unavailable":
            self.send_error(429)
            return
        assert body["stream"] and "messages" in body
        type(self).calls += 1
        content = "Recorded the decision."
        response = json.dumps({"choices": [{"message": {"role": "assistant", "content": content},
                                             "finish_reason": "stop"}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, *_):
        pass


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def until(fn, label, timeout=25):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        try:
            value = fn()
            if value:
                return value
        except (URLError, ConnectionRefusedError, ConnectionResetError):
            pass
        time.sleep(0.15)
    raise AssertionError("timed out: " + label)


class Node:
    def __init__(self, path, name, role, workspace=SPACE):
        self.workspace = workspace
        self.home, self.name, self.port = path, name, free_port()
        self.url = f"http://127.0.0.1:{self.port}"
        self.dir = self.home / "workspaces" / self.workspace
        self.proc = None
        self.cli("init", "--node", name, "--role", role, "--supervisor", "chief")
        self.token = (self.dir / "runtime/api.token").read_text().strip()

    def cli(self, *args):
        return subprocess.check_output([BIN / "tarla", "--home", self.home,
                                        "--workspace", self.workspace, "--url", self.url, *args], text=True)

    def configure(self, peer, port):
        config = self.dir / "workspace.toml"
        cfg = config.read_text().replace('fallback_chain = []',
            'fallback_chain = ["http:busy", "http:local"]')
        for backend, route in [("busy", "unavailable"), ("local", "model")]:
            cfg += f'\n[supervisor.brain.backends.{backend}]\nkind="openai"\nmodel="fixture"\nendpoint="http://127.0.0.1:{port}/{route}"\n'
        cfg += f'\n[[peers]]\nnode_id="{peer.name}"\nurl="{peer.url}"\ntoken_env="TARLA_PAIR_TEST"\n'
        config.write_text(cfg)

    def start(self, secret):
        self.log = (self.home / "server.log").open("a")
        self.proc = subprocess.Popen([BIN / "tarlad", "--home", self.home,
            "--workspace", self.workspace, "--listen", f"127.0.0.1:{self.port}"],
            env={**os.environ, "TARLA_PAIR_TEST": secret}, stdout=self.log, stderr=self.log)
        def ready():
            if self.proc.poll() is not None:
                raise AssertionError((self.home / "server.log").read_text())
            return self.api("/health")
        return until(ready, "daemon startup")

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(timeout=8)
        if self.proc:
            self.log.close()
            self.proc = None

    def api(self, path, data=None, token=None):
        req = Request(self.url + path, data=None if data is None else json.dumps(data).encode(),
                      headers={"Authorization": "Bearer " + (token or self.token),
                               "Content-Type": "application/json"})
        with HTTP.open(req, timeout=8) as r:
            return json.load(r)


def install_test_slot(node, binary):
    base = node.home / "bin"
    dest = base / "herdr-core" / "fixture" / "hc"
    dest.parent.mkdir(parents=True)
    shutil.copy2(binary, dest)
    (base / "toolchain-manifest.json").write_text(json.dumps({"tools": {"herdr-core": {
        "file":"hc","version":"fixture","sha256":hashlib.sha256(dest.read_bytes()).hexdigest()}}}))
    work = node.home / "fixture-worktree"
    work.mkdir()
    (work / "job.py").write_text(
        "import json,sys,time\nfrom pathlib import Path\n"
        "m=json.loads(sys.argv[1])\n"
        "with open('launches', 'a') as f: print(m['run_id'],file=f,flush=True)\n"
        "print('ready', flush=True)\n"
        "if m['objective']=='terminal check':\n"
        "    value=input().strip()\n"
        "    with open('terminal-result','w') as f: f.write(value)\n"
        "    print('terminal acknowledged',flush=True)\n"
        "elif m['objective']=='exercise restart':\n"
        "    for _ in range(900):\n"
        "        if Path('release-after-restart').is_file(): break\n"
        "        time.sleep(.1)\n"
        "    else: raise SystemExit(1)\n"
        "else: time.sleep(5)\n"
        "print('fixture job completed', flush=True)\n")
    with (node.dir/"workspace.toml").open("a") as cfg:
        cfg.write('\n[[slots]]\nslot_id="fixture"\ndriver="local"\nmodel_family="fixture"\n'
                  + "argv=" + json.dumps([sys.executable,"-u","job.py"]) + "\n"
                  + "workdir=" + json.dumps(str(work)) + "\n")
    return work


def assert_remote_execution(chief, worker, work, secret):
    until(lambda: any(x["slot_id"]=="fixture" for x in chief.api("/v1/slots")), "remote slot discovery")
    dispatch = chief.api("/v1/runs", {"conversation_id":"jobs","goal":"exercise restart",
                                      "acceptance":["produce one completion"]})
    run_id = dispatch["run_id"]
    path = "/v1/runs/" + run_id
    marker = work/"launches"
    until(lambda: marker.exists() and run_id in marker.read_text(),"real executor launch")
    until(lambda: chief.api(path).get("hc_terminal_id"), "replicated live terminal")
    grant = chief.api(path + "/attach", {"mode": "observe"})
    # Keep a viewer connected through SIGTERM. Its HTTP connection must not
    # retain the workspace lock or stop the independently held native worker.
    with websocket(worker, "/v1/terminal/" + run_id, grant["ticket"]) as status:
        assert status == 101
        chief.stop()
        worker.stop()
        worker.start(secret)
        chief.start(secret)
    (work / "release-after-restart").touch()
    until(lambda: chief.api(path)["observed_state"]=="succeeded","task receipt after restart",45)
    until(lambda: any(m["sender_role"]=="assistant" for m in chief.api("/v1/conversations/jobs/messages")),
          "RunCompleted wake")
    assert marker.read_text().splitlines()==[run_id]
    print("PASS real private herdr-core, cross-node mission, executor and supervisor restart, exactly one launch, active completion")

@contextlib.contextmanager
def websocket(node, path, ticket):
    # Exercise the same HTTP Upgrade used by the App; no workspace API credential is sent.
    key = base64.b64encode(os.urandom(16)).decode()
    with socket.create_connection(("127.0.0.1",node.port),timeout=5) as conn:
        headers = [f"GET {path} HTTP/1.1", f"Host: 127.0.0.1:{node.port}",
                   "Connection: Upgrade", "Upgrade: websocket", "Sec-WebSocket-Version: 13",
                   f"Sec-WebSocket-Key: {key}", f"Authorization: Bearer {ticket}", "", ""]
        conn.sendall("\r\n".join(headers).encode())
        with conn.makefile("rb") as reply:
            yield int(reply.readline().split()[1])


def websocket_status(node, path, ticket):
    with websocket(node, path, ticket) as status:
        return status


def assert_remote_terminal(chief, worker, work):
    rid = chief.api("/v1/runs",{"conversation_id":"jobs","goal":"terminal check"})["run_id"]
    path = "/v1/runs/" + rid
    until(lambda: chief.api(path).get("hc_terminal_id"),"terminal on paired executor")
    observe = chief.api(path + "/attach",{"mode":"observe"})
    assert observe["node_url"] == worker.url and observe["node_id"] == worker.name
    ws_path = "/v1/terminal/" + rid
    ticket = observe["ticket"]
    assert websocket_status(chief,ws_path,ticket) == 401
    assert websocket_status(worker,ws_path,ticket) == 101
    assert websocket_status(worker,ws_path,ticket) == 401
    command = [BIN/"tarla","--home",chief.home,"--workspace",SPACE,"--url",chief.url,
               "attach",rid,"--interactive"]
    client = subprocess.Popen(command,stdin=subprocess.PIPE,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
    try:
        client.stdin.write(b"paired terminal input\r")
        client.stdin.flush()
        until(lambda: (work/"terminal-result").is_file(),"interactive input via scoped CLI")
        assert (work/"terminal-result").read_text() == "paired terminal input"
        until(lambda: chief.api(path)["observed_state"]=="succeeded","terminal task exit")
    finally:
        client.stdin.close()
        try: client.wait(timeout=5)
        except subprocess.TimeoutExpired:
            client.terminate()
            client.wait(timeout=5)
    assert client.returncode==0, client.stderr.read().decode()
    print("PASS remote terminal ticket scope, single redemption, authenticated CLI interactive attach")

def test_nodes(path, model):
    chief = Node(path / "primary", "chief", "supervisor_only")
    worker = Node(path / "secondary", "worker", "executor_only")
    chief.configure(worker, model.server_port)
    worker.configure(chief, model.server_port)
    hc = os.environ.get("TARLA_TEST_HC")
    work = install_test_slot(worker,hc) if hc else None
    secret = secrets.token_hex(32)
    with contextlib.ExitStack() as cleanup:
        for n in [chief,worker]:
            cleanup.callback(n.stop)
            n.start(secret)
        try:
            chief.api("/v1/memories", token=worker.token)
            raise AssertionError("foreign workspace token accepted")
        except HTTPError as err:
            assert err.code in (401,403)
        endpoint = "/v1/conversations/demo/messages"
        message = {"text": "Remember the calibration fixture.", "channel": "cli", "source_msg_id": "unique"}
        result = chief.api(endpoint, message)
        assert result["reply"] == "Recorded the decision.", result
        calls = Model.calls
        assert chief.api(endpoint, message) == result
        assert Model.calls == calls
        assert len(chief.api(endpoint)) == 2
        assert "dispatch" in chief.cli("chat", "commands", "/help")
        memory = {"memory_id":"pref","scope":"global","content":"285 mm","base_version":0}
        assert chief.api("/v1/memories",memory)["status"] == "applied"
        until(lambda: any(m["content"]=="285 mm" for m in worker.api("/v1/memories")), "S2 replication")
        chief.stop()
        assert worker.api("/v1/memories")[0]["content"]=="285 mm"
        offline=worker.api("/v1/memories",{"memory_id":"offline","content":"field result",
                      "scope":"global","base_version":0,"proposal_id":"offline-1"})
        assert offline["status"]=="queued", offline
        chief.start(secret)
        for origin in [chief,worker]:
            until(lambda: any(m["memory_id"]=="offline" and m["version"]==1
                              for m in origin.api("/v1/memories")),"offline proposal replay")
        assert len(chief.api(endpoint)) == 2 and Model.calls == calls
        conflict=chief.api("/v1/memories",{**memory, "content":"stale"})
        assert conflict["status"]=="conflict", conflict
        assert any("pref" in str(item) for item in chief.api("/v1/actions"))
        if work is not None:
            assert_remote_execution(chief,worker,work,secret)
            assert_remote_terminal(chief,worker,work)
        snap = path / "handover.db"
        frozen = subprocess.run([BIN/"tarla","--home",chief.home,"--workspace",SPACE,
                                 "supervisor","handover","--to","backup","--snapshot",snap],
                                capture_output=True)
        assert frozen.returncode!=0 and not snap.exists(), "active supervisor must hold exclusive authority lock"
        chief.stop()
        chief.cli("supervisor","handover","--to","backup","--snapshot",str(snap))
        replacement = Node(path/"backup","backup","executor_only")
        replacement.cli("supervisor","restore","--snapshot",str(snap))
        cleanup.callback(replacement.stop)
        replacement.start(secret)
        info=replacement.api("/health")
        assert info["supervisor_node_id"]=="backup" and info["supervisor_term"]==2, info
        assert any(m["content"]=="285 mm" for m in replacement.api("/v1/memories"))
        assert len(replacement.api(endpoint))==2
        print("PASS live migration lock, offline supervisor handover, restored history, incremented authority term")
        print("PASS CLI, model failover, message replay, workspace authentication, peer replication, offline search, restart, memory conflict")


def main():
    with tempfile.TemporaryDirectory(prefix="tarla-accept-") as directory:
        model=ThreadingHTTPServer(("127.0.0.1",0),Model)
        thread=threading.Thread(target=model.serve_forever,daemon=True)
        thread.start()
        try:
            test_nodes(Path(directory),model)
        finally:
            model.shutdown()
            model.server_close()
            thread.join()

if __name__=="__main__":
    main()
