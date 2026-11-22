"""Use real private PTYs, two daemons and the CLI to verify direct and relayed terminal grants."""
import argparse
import base64
import contextlib
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import socket
import sqlite3
import subprocess
import tempfile
import threading
import time
from urllib.error import HTTPError

from acceptance import BIN, Node, free_port, install_test_slot, until
from pinned import fixtures

WORKER = """import json, pathlib, select, sys, time
run = json.loads(sys.argv[1])["run_id"]
target = pathlib.Path(run + ".input")
target.touch()
print("ready-" + run, flush=True)
end = time.monotonic() + 180
while not pathlib.Path("finish").exists():
    if time.monotonic() > end: raise SystemExit(1)
    if select.select([sys.stdin], [], [], .1)[0]:
        text = sys.stdin.readline()
        if not text: break
        with target.open("a") as stream: stream.write(text)
"""


@contextlib.contextmanager
def upgrade(node, run, bearer):
    key = base64.b64encode(secrets.token_bytes(16)).decode()
    with socket.create_connection(("127.0.0.1", node.port), timeout=8) as conn:
        conn.settimeout(10)
        conn.sendall((f"GET /v1/terminal/{run} HTTP/1.1\r\nHost: 127.0.0.1:{node.port}\r\n"
                      f"Connection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\n"
                      f"Sec-WebSocket-Key: {key}\r\nAuthorization: Bearer {bearer}\r\n\r\n").encode())
        with conn.makefile("rb") as stream:
            status = int(stream.readline().split()[1])
            headers = {}
            for raw in stream:
                if raw == b"\r\n":
                    break
                name, value = raw.decode().split(":", 1)
                headers[name.lower()] = value.strip()
            if status == 101:
                expected = hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()
                assert headers["sec-websocket-accept"] == base64.b64encode(expected).decode()
            yield status, conn, stream


def ws_status(node, run, token):
    with upgrade(node, run, token) as (status, _, _):
        return status


def denied(fn, expected):
    try:
        fn()
    except HTTPError as e:
        assert e.code == expected, (e.code, expected, e.read())
    else:
        raise AssertionError(f"expected HTTP {expected}")


def ticket(node, run, mode="observe", route="direct", token=None):
    value = node.api(f"/v1/runs/{run}/attach", {"mode": mode, "route": route}, token)
    assert value["expires_in"] == 60 and value["run_id"] == run and value["mode"] == mode
    return value


def send_masked(conn, opcode, payload):
    mask = secrets.token_bytes(4)
    conn.sendall(bytes([opcode, 128 | len(payload)]) + mask +
                 bytes(x ^ mask[i % 4] for i, x in enumerate(payload)))


def check_observer(node, run, grant, marker):
    with upgrade(node, run, grant["ticket"]) as (status, conn, stream):
        assert status == 101
        send_masked(conn, 0x81, b'{"cols":120,"rows":40}')
        send_masked(conn, 0x82, marker.encode() + b"\r")
        # Either the initial paint or the close frame may be first. Both WebSocket
        # endpoints close on an observer input attempt, leaving the live job alone.
        while True:
            head = stream.read(2)
            if not head:
                break
            assert len(head) == 2 and head[1] < 128
            size = head[1] & 0x7f
            if size == 126:
                size = int.from_bytes(stream.read(2), "big")
            elif size == 127:
                size = int.from_bytes(stream.read(8), "big")
            stream.read(size)
            if head[0] & 15 == 8:
                break
    assert ws_status(node, run, grant["ticket"]) == 401


@contextlib.contextmanager
def cli(node, run, relay=False):
    args = [BIN / "tarla", "--home", node.home, "--workspace", node.workspace, "--url", node.url,
            "attach", run, "--interactive"]
    if relay:
        args.append("--via-supervisor")
    p = subprocess.Popen(args, env={**os.environ, "TARLA_TOKEN": node.token}, stdin=subprocess.PIPE,
                         stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    try:
        yield p
        p.stdin.write(b"\x1d")
        p.stdin.flush()
        p.stdin.close()
        assert p.wait(timeout=8) == 0, p.stderr.read().decode()
    finally:
        if p.poll() is None:
            p.terminate()
            p.wait(timeout=8)


def send(p, message, log):
    p.stdin.write(message.encode() + b"\r")
    p.stdin.flush()
    def observed():
        if p.poll() is not None:
            raise AssertionError(p.stderr.read().decode())
        return log.exists() and message in log.read_text().splitlines()
    until(observed, "terminal delivered " + message)


class ExecutorFixture(BaseHTTPRequestHandler):
    """Emulate the previous and current ticket response formats on a loopback WebSocket."""
    protocol_version = "HTTP/1.1"

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path != "/peer/v1/ticket":
            self.send_error(404)
            return
        assert self.headers["Authorization"] == "Bearer " + self.server.pair_secret
        if self.server.ticket_status != 200:
            self.send_error(self.server.ticket_status)
            return
        grant = {"workspace_id": body["workspace_id"], "node_id": "worker", "run_id": body["run_id"],
                 "mode": body["mode"], "hc_terminal_id": self.server.terminal, "nonce": secrets.token_hex(16),
                 "expires_at": int(time.time()) + 60}
        payload = base64.urlsafe_b64encode(json.dumps(grant).encode()).rstrip(b"=")
        signature = hmac.digest(self.server.signing_key, b"tarla/attach/v1\0" + payload, "sha256")
        token = (payload + b"." + base64.urlsafe_b64encode(signature).rstrip(b"=")).decode()
        self.server.issued.add(token)
        response = {"run_id": grant["run_id"], "node_id": "worker", "mode": grant["mode"],
                    "expires_in": 60, "ticket": token}
        if self.server.current_protocol:
            response["hc_terminal_id"] = grant["hc_terminal_id"]
        response = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def do_GET(self):
        token = self.headers.get("Authorization", "").removeprefix("Bearer ")
        if token not in self.server.issued or self.path != "/v1/terminal/legacy-run":
            self.send_error(401)
            return
        self.server.issued.remove(token)
        key = self.headers["Sec-WebSocket-Key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
        self.send_response(101)
        self.send_header("Connection", "Upgrade")
        self.send_header("Upgrade", "websocket")
        self.send_header("Sec-WebSocket-Accept", base64.b64encode(hashlib.sha1(key.encode()).digest()).decode())
        self.end_headers()
        if self.server.current_protocol:
            # The CLI sends one masked binary keystroke. Echo it and end the session.
            self.connection.settimeout(5)
            assert self.rfile.read(2) == b"\x82\x81"
            mask = self.rfile.read(4)
            key = self.rfile.read(1)[0] ^ mask[0]
            self.wfile.write(b"\x82\x01" + bytes([key]))
            self.wfile.write(b"\x88\x00")
        else:
            time.sleep(0.08)
            payload = self.server.repaint
            with contextlib.suppress(BrokenPipeError, ConnectionResetError):
                self.wfile.write(b"\x82\x7f" + len(payload).to_bytes(8, "big") + payload)
                self.wfile.write(b"\x88\x00")
        self.close_connection = True

    def log_message(self, *_):
        pass


def check_idle_cli_close(chief, rid):
    args = [BIN / "tarla", "--home", chief.home, "--workspace", chief.workspace, "--url", chief.url,
            "attach", rid, "--interactive", "--via-supervisor"]
    with subprocess.Popen(args, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE) as proc:
        try:
            proc.stdin.write(b"x")
            proc.stdin.flush()
            # Keep stdin open after the keystroke. Only the peer initiates closure.
            assert proc.wait(timeout=5) == 0, proc.stderr.read().decode()
            assert proc.stdout.read() == b"x"
        finally:
            if proc.poll() is None:
                proc.kill()


def check_routes(root):
    chief = Node(root, "chief", "supervisor_only")
    rid = "legacy-run"
    public = "https://worker.example.invalid"
    with contextlib.ExitStack() as stack:
        peer = stack.enter_context(ThreadingHTTPServer(("127.0.0.1", 0), ExecutorFixture))
        peer.terminal, peer.issued = "t1b", set()
        peer.current_protocol = False
        peer.ticket_status = 200
        peer.repaint = b"screen repaint\r\n" * 8192  # one valid output frame larger than the input limit
        peer.pair_secret, peer.signing_key = secrets.token_hex(32), secrets.token_bytes(32)
        threading.Thread(target=peer.serve_forever, daemon=True).start()
        stack.callback(peer.shutdown)
        private = f"http://127.0.0.1:{peer.server_port}"
        chief.cli("pair", "worker", private, "--token-env", "TARLA_PAIR_TEST", "--client-url", public)
        config = chief.dir / "workspace.toml"
        original = config.read_text()
        stack.callback(chief.stop)
        chief.start(peer.pair_secret)
        with contextlib.closing(sqlite3.connect(chief.dir / "state.db")) as db, db:
            db.execute("""INSERT INTO runs(run_id,workspace_id,conversation_id,supervisor_term,target_node_id,
                target_slot_id,desired_state,observed_state,mission_packet_json,hc_terminal_id,created_at,updated_at)
                VALUES(?,?,'chat',1,'worker','slot','running','running','{}','t1b',0,0)""", (rid, chief.workspace))
        direct = ticket(chief, rid, route="auto")
        assert direct["route"] == "direct" and direct["node_url"] == public
        denied(lambda: ticket(chief, rid, mode="interactive", route="supervisor"), 409)
        # Even a previously cached capability cannot upgrade a legacy observer.
        with contextlib.closing(sqlite3.connect(chief.dir / "state.db")) as db, db:
            db.execute("INSERT INTO service_terminal_input VALUES(?,1)", (rid,))
        denied(lambda: ticket(chief, rid, mode="interactive", route="direct"), 409)
        interactive = ticket(chief, rid, mode="interactive", route="supervisor")
        assert ws_status(chief, rid, interactive["ticket"]) == 409
        grant = ticket(chief, rid, route="supervisor")
        with upgrade(chief, rid, grant["ticket"]) as (status, conn, stream):
            assert status == 101
            send_masked(conn, 0x81, b'{"cols":120,"rows":40}')
            assert stream.read(2) == b"\x82\x7f"
            assert int.from_bytes(stream.read(8), "big") == len(peer.repaint)
            assert stream.read(len(peer.repaint)) == peer.repaint
        assert ws_status(chief, rid, grant["ticket"]) == 401
        denied_obs = ticket(chief, rid, route="supervisor")
        with upgrade(chief, rid, denied_obs["ticket"]) as (status, conn, stream):
            assert status == 101
            send_masked(conn, 0x82, b"denied\r")
            head = stream.read(2)
            assert not head or (head[0] & 15) == 8
        peer.terminal = "t2"
        assert ws_status(chief, rid, ticket(chief, rid, route="supervisor")["ticket"]) == 409
        peer.current_protocol, peer.terminal = True, "t1b"
        check_idle_cli_close(chief, rid)
        for status, expected in [(409, 409), (503, 502)]:
            peer.ticket_status = status
            denied(lambda: ticket(chief, rid, route="direct"), expected)
            relay = ticket(chief, rid, route="supervisor")
            assert ws_status(chief, rid, relay["ticket"]) == expected
        chief.stop()
        # This peer is private HTTPS, with no public origin and no reachable transport.
        offline = f"https://127.0.0.1:{free_port()}"
        config.write_text(original.replace(f'client_url = "{public}"', "").replace(private, offline))
        chief.start(peer.pair_secret)
        grant = ticket(chief, rid, route="auto")
        assert grant["route"] == "supervisor" and "node_url" not in grant
        denied(lambda: ticket(chief, rid, route="direct"), 502)
        assert ws_status(chief, rid, grant["ticket"]) == 502
    print("PASS private HTTPS relay selection, explicit client origin, legacy observer, scoped ticket, remote CLI close with idle stdin and offline errors")


def run(root):
    hc = os.environ["TARLA_TEST_HC"]
    chief = Node(root / "a", "chief", "supervisor_and_executor")
    worker = Node(root / "b", "worker", "executor_only")
    secret = secrets.token_hex(32)
    for node, peer in [(chief, worker), (worker, chief)]:
        node.cli("pair", peer.name, peer.url, "--token-env", "TARLA_PAIR_TEST")
    local = install_test_slot(chief, hc)
    remote = fixtures(worker, hc)
    for folder in (local, remote):
        (folder / "job.py").write_text(WORKER)
    with contextlib.ExitStack() as cleanup:
        for n, folder in [(chief, local), (worker, remote)]:
            rt = n.dir / "runtime"
            env = {**os.environ, "HC_SESSION": f"tarla-{n.workspace}", "HC_SOCKET": str(rt / "hc.sock"),
                   "XDG_CONFIG_HOME": str(rt / "hc-config"), "XDG_RUNTIME_DIR": str(rt / "hc-runtime"),
                   "XDG_STATE_HOME": str(rt / "hc-state")}
            cleanup.callback(subprocess.run, [hc, "server", "stop"], env=env, timeout=15, capture_output=True)
            cleanup.callback((folder / "finish").touch)
            cleanup.callback(n.stop)
            n.start(secret)
        cleanup.callback((remote / "release").touch)
        until(lambda: any(x["node_id"] == worker.name for x in chief.api("/v1/slots")), "executor discovery")
        def start(node):
            run = chief.api("/v1/runs", {"conversation_id":"terminal", "objective":"interactive fixture",
                            "node_id":node.name, "slot_id":"fixture"})["run_id"]
            until(lambda: chief.api("/v1/runs/" + run)["hc_terminal_id"], "real scoped holder")
            detail = chief.api("/v1/runs/" + run)
            assert detail["terminal_modes"] == ["observe","interactive"] and detail["terminal_input_hint"] is None
            return run
        local_run = start(chief)
        rid = start(worker)
        log = remote / (rid + ".input")
        direct = ticket(chief, rid)
        relay = ticket(chief, rid, route="supervisor")
        assert direct["route"] == "direct" and direct["node_url"] == worker.url
        assert relay["route"] == "supervisor" and "node_url" not in relay
        expired = [(chief, ticket(chief, rid, route="supervisor")["ticket"]),
                   (worker, ticket(worker, rid)["ticket"])]
        deadline = time.monotonic() + 61
        for server, other in ((chief,worker), (worker,chief)):
            denied(lambda: ticket(server, rid, token=other.token), 401)
            assert ws_status(server, rid, server.token) == 401
        assert ws_status(chief, rid, direct["ticket"]) == 401
        assert ws_status(worker, rid, relay["ticket"]) in (401, 403, 409)
        assert ws_status(chief, local_run, relay["ticket"]) != 101
        check_observer(chief, rid, relay, "denied-relay")
        check_observer(worker, rid, direct, "denied-direct")
        with cli(chief, local_run) as native:
            send(native, "local-marker", local / (local_run + ".input"))
        with cli(chief, rid, relay=True) as mirrored:
            send(mirrored, "relay-marker", log)
        with cli(chief, rid) as connected:
            send(connected, "direct-before-offline", log)
            chief.stop()
            send(connected, "direct-while-offline", log)
        with cli(worker, rid) as fallback:
            send(fallback, "paired-owner-fallback", log)
        assert not any(line.startswith("denied-") for line in log.read_text().splitlines())
        chief.start(secret)
        until(lambda: any(x["node_id"] == worker.name for x in chief.api("/v1/slots")), "reconnect supervisor")
        for driver in ("codex","claude"):
            conv = "native-" + driver
            chief.cli("bind", conv, worker.name, driver, str(remote))
            run = chief.api(f"/v1/conversations/{conv}/messages", {"text":"wait","source_msg_id":conv,"channel":"cli"})["run_id"]
            until(lambda: chief.api("/v1/runs/" + run)["hc_terminal_id"], "structured holder")
            for server in (chief, worker):
                info = server.api("/v1/runs/" + run)
                assert info["terminal_modes"] == ["observe"] and "conversation" in info["terminal_input_hint"]
                denied(lambda: ticket(server,run,mode="interactive"), 409)
            denied(lambda: ticket(chief,run,mode="interactive",route="supervisor"),409)
            assert ws_status(worker,run,ticket(worker,run)["ticket"]) == 101
        (remote / "release").touch()
        time.sleep(max(0, deadline - time.monotonic()))
        for server, grant in expired:
            assert ws_status(server, rid, grant) == 401
        print("PASS native observe-only input policy, direct and relayed CLI PTYs, scoped bearer, single-use grants, offline executor and 60s expiry")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--routes-only", action="store_true", help="exercise paired HTTP and WebSocket routes without a PTY")
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="s3pty-") as temp:
        check_routes(Path(temp) / "routes")
        if not args.routes_only:
            run(Path(temp))
