"""Signed IM ingress, per-conversation ordering and retry after provider recovery."""
import base64
import contextlib
import hashlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import shlex
import sqlite3
import subprocess
import tempfile
import threading
import time
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.request import Request

from acceptance import BIN, HTTP, Node, install_test_slot, until

KEY = "local-fixture-encryption-key"


class Provider(BaseHTTPRequestHandler):
    def do_POST(self):
        payload = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        text = payload["messages"][-1]["content"]
        if text == "slow":
            self.server.started.set()
            assert self.server.release.wait(40)
        if text == "interrupted" and not self.server.interrupted.is_set():
            self.server.interrupted.set()
            assert self.server.disconnected.wait(40)
        if text == "flaky" and not self.server.healthy.is_set():
            self.send_error(429 if self.path == "/primary" else 503)
            return
        result = json.dumps({"choices":[{"message":{"role":"assistant","content":"reply " + text},
                                        "finish_reason":"stop"}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(result)))
        self.end_headers()
        try:
            self.wfile.write(result)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def log_message(self, *_):
        pass


def telegram(node, event, text):
    body = {"update_id":event, "message":{"message_thread_id":11,"chat":{"id":7},
                                         "from":{"id":7},"text":text}}
    request = Request(node.url + "/gateway/telegram", data=json.dumps(body).encode(),
                      headers={"Content-Type":"application/json","X-Telegram-Bot-Api-Secret-Token":"fixture"})
    with HTTP.open(request) as response:
        assert json.load(response)["ok"]


def feishu(node, event, thread, text):
    record = {"header":{"event_id":event,"token":"verify","app_id":"fixture-app",
                        "event_type":"im.message.receive_v1"},
              "event":{"sender":{"sender_type":"user","sender_id":{"open_id":"actor"}},
                       "message":{"message_id":event,"chat_id":"group","thread_id":thread,
                                  "root_id":thread,"message_type":"text","content":json.dumps({"text":text})}}}
    iv = secrets.token_bytes(16)
    cipher = subprocess.run(["openssl","enc","-aes-256-cbc","-K",hashlib.sha256(KEY.encode()).hexdigest(),
                             "-iv",iv.hex()], input=json.dumps(record).encode(), capture_output=True, check=True)
    raw = json.dumps({"encrypt":base64.b64encode(iv+cipher.stdout).decode()}).encode()
    timestamp = str(int(time.time()))
    nonce = secrets.token_hex(8)
    signature = hashlib.sha256((timestamp + nonce + KEY).encode() + raw).hexdigest()
    request = Request(node.url + "/gateway/feishu", data=raw,
                      headers={"Content-Type":"application/json","X-Lark-Request-Timestamp":timestamp,
                               "X-Lark-Request-Nonce":nonce,"X-Lark-Signature":signature})
    with HTTP.open(request) as response:
        assert response.status == 200


def history(node, conv):
    return [(m["sender_role"], m["content"]) for m in node.api(f"/v1/conversations/{conv}/messages")]


def completed(node, conv, text):
    return until(lambda: ("assistant", "reply " + text) in history(node, conv), f"archived {text}")


def terminal_notice(node, work):
    run = node.api("/v1/runs", {"conversation_id":"shared","goal":"interactive terminal question"})["run_id"]
    action = until(lambda: next((a for a in node.api("/v1/actions")
                                if a["kind"] == "terminal_input" and a["run_id"] == run), None),
                   "actual Herdr OSC 7501 terminal input")
    with contextlib.closing(sqlite3.connect(node.dir / "gateway.db")) as db:
        def notice():
            return db.execute("SELECT json_extract(job,'$.Text') FROM outgoing o JOIN terminal_notices t "
                              "ON t.route=o.route JOIN routes r ON r.id=o.route WHERE t.action=? "
                              "AND r.platform='telegram' AND job LIKE ?", (action["id"], "%"+run+"%")).fetchone()
        (text,) = until(notice, "paired Telegram terminal notice")
        assert not db.execute("SELECT 1 FROM cards WHERE action=?", (action["id"],)).fetchone()
        assert db.execute("SELECT count(*) FROM terminal_notices WHERE action=?", (action["id"],)).fetchone() == (1,)
    command = shlex.split(text.splitlines()[-1])
    assert command == ["tarla","--workspace","work","attach",run,"--interactive","--via-supervisor"], command
    process = subprocess.Popen([BIN/"tarla","--home",node.home,"--url",node.url,*command[1:]],
                               stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        process.stdin.write(b"paired reply\r")
        process.stdin.flush()
        until(lambda: (work/"answer").is_file(), "terminal instruction reaches actual blocked process")
        assert (work/"answer").read_text() == "paired reply"
        until(lambda: node.api("/v1/runs/"+run)["observed_state"]=="succeeded","interactive run completes")
    finally:
        process.stdin.close()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.terminate()
            process.wait(timeout=5)
    assert process.returncode == 0, process.stderr.read().decode()
    print("PASS real OSC 7501 prompt, one scoped IM notice, advertised CLI command resolves the live terminal")


def check(root):
    server = ThreadingHTTPServer(("127.0.0.1", 0), Provider)
    server.started, server.release, server.healthy = (threading.Event() for _ in range(3))
    server.interrupted, server.disconnected = threading.Event(), threading.Event()
    node = Node(root, "chief", "supervisor_and_executor")
    cfg = node.dir / "workspace.toml"
    content = cfg.read_text().replace("fallback_chain = []", 'fallback_chain = ["http:primary","http:fallback"]')
    for name in ("primary","fallback"):
        content += (f'\n[supervisor.brain.backends.{name}]\nkind="openai"\nmodel="fixture"\n'
                    f'endpoint="http://127.0.0.1:{server.server_port}/{name}"\n')
    cfg.write_text(content + '\n[gateway.telegram]\nenabled=true\n[gateway.feishu]\nenabled=true\n'
                   'app_id="fixture-app"\nencrypt_key_env="FIXTURE_ENCRYPT"\n')
    hc = os.environ.get("TARLA_TEST_HC")
    if hc:
        work = install_test_slot(node, hc)
        (work / "job.py").write_text(
            "from pathlib import Path\n"
            "print('\\x1b]7501;state=blocked:kind=permission:id=a/b:msg=QXBwbHk/\\x07', end='', flush=True)\n"
            "Path('answer').write_text(input().strip())\n"
        )

    with contextlib.ExitStack() as cleanup:
        thread = threading.Thread(target=server.serve_forever)
        thread.start()
        cleanup.callback(thread.join)
        cleanup.callback(server.server_close)
        cleanup.callback(server.shutdown)
        cleanup.callback(server.release.set)
        cleanup.callback(server.disconnected.set)
        if hc:
            runtime = node.dir / "runtime"
            cleanup.callback(subprocess.run, [hc,"server","stop"], capture_output=True, timeout=10,
                             env={**os.environ,"HC_SESSION":f"tarla-{node.workspace}","HC_SOCKET":str(runtime/"hc.sock"),
                                  "XDG_RUNTIME_DIR":str(runtime/"hc-runtime"),"XDG_STATE_HOME":str(runtime/"hc-state"),
                                  "XDG_CONFIG_HOME":str(runtime/"hc-config")})
        cleanup.callback(node.stop)
        cleanup.enter_context(patch.dict(os.environ, {
            "TARLA_TELEGRAM_BOT_TOKEN":"42:LOCAL", "TARLA_TELEGRAM_WEBHOOK_SECRET":"fixture",
            "TARLA_FEISHU_APP_SECRET":"app-secret", "TARLA_FEISHU_VERIFICATION_TOKEN":"verify",
            "FIXTURE_ENCRYPT":KEY, "HTTPS_PROXY":"http://127.0.0.1:1","https_proxy":"http://127.0.0.1:1",
            "NO_PROXY":"127.0.0.1,localhost","no_proxy":"127.0.0.1,localhost",
        }))
        node.start("")
        def pair(platform, actor, conv, approvals=False):
            return "/pair " + node.api("/gateway/pairings", {"platform":platform,"actor_id":actor,
                                       "conversation_id":conv,"approvals":approvals})["code"]
        telegram(node, 1, pair("telegram","7","shared", approvals=True))
        feishu(node, "pair-shared", "same", pair("feishu","actor","shared"))
        feishu(node, "pair-free", "other", pair("feishu","actor","free"))
        telegram(node, 2, "slow")
        assert server.started.wait(10)
        feishu(node, "one", "same", "next")
        feishu(node, "two", "other", "fast")
        completed(node, "free", "fast")
        assert ("user","next") not in history(node,"shared")
        server.release.set()
        completed(node,"shared","next")

        try:
            node.api("/v1/conversations/probe/messages", {"text":"flaky","source_msg_id":"diagnostic"})
            raise AssertionError("exhausted provider unexpectedly answered")
        except HTTPError as error:
            assert error.code == 503, (error.code,error.read())
        telegram(node, 3, "flaky")
        feishu(node, "three", "same", "following")
        with contextlib.closing(sqlite3.connect(node.dir / "gateway.db")) as db:
            until(lambda: db.execute("SELECT tries FROM inbox WHERE platform='telegram' AND event='3' AND done=0"
                                     ).fetchone()[0] > 0, "provider failure scheduled a durable retry")
        assert ("user","following") not in history(node,"shared")
        node.stop()
        node.start("")
        telegram(node, 3, "flaky")
        feishu(node, "three", "same", "following")
        server.healthy.set()
        completed(node,"shared","following")
        telegram(node, 4, "interrupted")
        assert server.interrupted.wait(10)
        feishu(node, "four", "same", "after restart")
        # Simulate a crash, since normal shutdown waits for the deliberately
        # suspended request to finish.
        node.proc.kill()
        node.proc.wait(timeout=5)
        node.stop()
        node.start("")
        server.disconnected.set()
        completed(node, "shared", "after restart")
        expected = [(role, text if role=="user" else "reply "+text)
                    for text in ("slow","next","flaky","following","interrupted","after restart")
                    for role in ("user","assistant")]
        assert history(node, "shared") == expected, history(node,"shared")
        assert history(node, "free") == [("user","fast"),("assistant","reply fast")]
        print("PASS signed Telegram/Feishu overlap, FIFO through 429/503 and restart, one archived reply per source")
        if hc:
            terminal_notice(node, work)


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="tg-") as directory:
        check(Path(directory))
