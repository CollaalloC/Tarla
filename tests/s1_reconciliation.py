"""Two-daemon S1 recovery after lost replies, late events and supervisor rollback.

Run with TARLA_TEST_HC to execute a successful real PTY mission. Without it,
the executor generates a genuine startup failure and the same durable receipt.
"""
import contextlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import secrets
import sqlite3
import subprocess
import sys
import tempfile
import threading
from urllib.error import HTTPError, URLError
from urllib.request import Request

from acceptance import HTTP, Node, install_test_slot, until


def rows(node, sql, *args):
    with contextlib.closing(sqlite3.connect(node.dir / "state.db")) as db:
        return db.execute(sql, args).fetchall()


def backup(source, destination):
    with contextlib.closing(sqlite3.connect(source)) as src:
        with contextlib.closing(sqlite3.connect(destination)) as dst:
            src.backup(dst)


class Link(ThreadingHTTPServer):
    def __init__(self, target, peer, secret):
        super().__init__(("127.0.0.1", 0), Handler)
        self.url = f"http://127.0.0.1:{self.server_port}"
        self.target = target
        self.peer = peer
        self.secret = secret
        self.mode = "hold"
        self.lock = threading.Lock()
        self.receipt = None
        self.earlier = None
        self.allow_pull = threading.Event()
        self.dropped = threading.Event()
        self.marker = "PRIVATE_S0_" + secrets.token_hex(16)
        self.leaked = threading.Event()

    def send(self, path, payload):
        request = Request(
            self.target.url + path,
            data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json", "Authorization": "Bearer " + self.secret,
                     "x-tarla-peer": self.peer},
        )
        try:
            with HTTP.open(request, timeout=5) as response:
                return response.status, response.read()
        except HTTPError as error:
            return error.code, error.read()

    def set_mode(self, mode):
        with self.lock:
            self.mode = mode


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def do_POST(self):
        raw = self.rfile.read(int(self.headers["Content-Length"]))
        link = self.server
        if (self.headers.get("Authorization") != "Bearer " + link.secret or
                self.headers.get("x-tarla-peer") != link.peer):
            self.send_error(401)
            return
        if link.marker.encode() in raw:
            link.leaked.set()
        payload = json.loads(raw)
        mode = "pass"
        if self.path == "/peer/v1/s1_pull" and not link.allow_pull.is_set():
            self.send_error(503)
            return
        if self.path == "/peer/v1/sync" and payload.get("events"):
            with link.lock:
                if any(e["event_type"] == "run_receipt" for e in payload["events"]):
                    mode = link.mode
                    link.receipt = payload
                    if mode == "drop":
                        link.mode = "hold"
                elif link.earlier is None:
                    # Let the initial claim reach the supervisor. Its acknowledgement
                    # authorizes the worker to start the original command.
                    link.earlier = payload
        if mode == "hold":
            self.send_error(503)
            return
        try:
            code, body = link.send(self.path, payload)
        except (URLError, TimeoutError):
            self.send_error(503)
            return
        if link.marker.encode() in body:
            link.leaked.set()
        if mode == "drop" and code == 200:
            # Supervisor committed. Close the socket before any acknowledgement reaches
            # the executor, then hold all retries until the test inspects both ledgers.
            link.dropped.set()
            self.close_connection = True
            return
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def pair(node, peer, url):
    with (node.dir / "workspace.toml").open("a") as conf:
        conf.write(f'\n[[peers]]\nnode_id="{peer.name}"\nurl="{url}"\ntoken_env="TARLA_PAIR_TEST"\n')


def slot(worker, hc, marker):
    if hc:
        work = install_test_slot(worker, hc)
        (work / "job.py").write_text(
            "import json, sys\n"
            "with open('launches', 'a') as out: print(json.loads(sys.argv[1])['run_id'], file=out)\n"
            f"print({marker!r}, flush=True)\n"
        )
        return work
    with (worker.dir / "workspace.toml").open("a") as conf:
        conf.write('\n[[slots]]\nslot_id="fixture"\ndriver="local"\nmodel_family="fixture"\n'
                   f'workdir={json.dumps(str(worker.home))}\nargv={json.dumps([sys.executable,"-V"])}\n')


def single_wake(chief, run):
    def delivered():
        return rows(chief, "SELECT delivered FROM service_wakes WHERE json_extract(receipt,'$.run_id')=?", run) == [(1,)]
    until(delivered, "a single delivered completion wake")
    messages = rows(chief, "SELECT sender_role,count(*) FROM messages WHERE run_id=? GROUP BY sender_role", run)
    assert dict(messages).get("run_receipt") == dict(messages).get("assistant") == 1, messages


def wait_terminal(node, run, state):
    def completed():
        try:
            row = node.api("/v1/runs/" + run)
            return row if row["observed_state"] == state else None
        except HTTPError as err:
            if err.code != 404:
                raise
    return until(completed, f"{node.name} to recover {run} as {state}")


def acknowledged(worker, eid):
    return rows(worker, "SELECT acked_at IS NOT NULL FROM outbox_events WHERE event_id=?", eid) == [(1,)]


def check(root, hc):
    chief = Node(root / "a", "chief", "supervisor_only")
    worker = Node(root / "b", "worker", "executor_only")
    token = secrets.token_hex(32)
    link = Link(chief, worker.name, token)
    reverse = Link(worker, chief.name, token)
    reverse.marker = link.marker
    work = slot(worker, hc, link.marker)
    pair(worker, chief, link.url)
    pair(chief, worker, reverse.url)
    before_dispatch = root / "before-dispatch.db"
    before_receipt = root / "before-receipt.db"
    with contextlib.ExitStack() as cleanup:
        for transport in (link, reverse):
            listener = threading.Thread(target=transport.serve_forever)
            listener.start()
            cleanup.callback(transport.server_close)
            cleanup.callback(listener.join)
            cleanup.callback(transport.shutdown)
        if hc:
            runtime = worker.dir / "runtime"
            cleanup.callback(subprocess.run, [hc, "server", "stop"], capture_output=True, timeout=10,
                             env={**os.environ, "HC_SESSION":"tarla", "HC_SOCKET":str(runtime/"hc.sock"),
                                  "XDG_RUNTIME_DIR":str(runtime/"hc-runtime"), "XDG_STATE_HOME":str(runtime/"hc-state"),
                                  "XDG_CONFIG_HOME":str(runtime/"hc-config")})
        for node in (chief, worker):
            cleanup.callback(node.stop)
            node.start(token)
        until(lambda: any(s["slot_id"] == "fixture" for s in chief.api("/v1/slots")), "paired worker discovery")
        backup(chief.dir / "state.db", before_dispatch)
        dispatch = {"conversation_id":"recovery","goal":"check S1 recovery",
                    "node_id":"worker","slot_id":"fixture","request_id":"once"}
        run = chief.api("/v1/runs", dispatch)["run_id"]
        packet = until(lambda: link.receipt, "executor's unacknowledged completion")
        seq = [event["seq"] for event in packet["events"]]
        assert seq[-1] > 1 and seq == list(range(seq[0], seq[-1] + 1)), seq
        receipt = next(e for e in packet["events"] if e["event_type"] == "run_receipt" and e["run_id"] == run)
        assert receipt["seq"] == seq[-1], packet["events"]
        backup(chief.dir / "state.db", before_receipt)
        state = "succeeded" if hc else "failed"
        owned = wait_terminal(worker, run, state)
        assert not acknowledged(worker, receipt["event_id"])

        assert chief.api("/v1/runs/" + run)["observed_state"] not in ("succeeded","failed","cancelled")

        link.set_mode("drop")
        assert link.dropped.wait(15), "receipt transaction was never committed behind a lost response"
        first = wait_terminal(chief, run, state)
        single_wake(chief, run)
        assert not acknowledged(worker, receipt["event_id"])
        assert link.earlier is not None
        for replay in (link.earlier, packet):
            code, body = link.send("/peer/v1/sync", replay)
            assert code in (200, 409), (code, body)
        single_wake(chief, run)
        link.set_mode("pass")
        until(lambda: acknowledged(worker, receipt["event_id"]), "executor persisted receipt acknowledgement")

        for snapshot in (before_receipt, before_dispatch):
            worker.stop()
            chief.stop()
            backup(snapshot, chief.dir / "state.db")
            chief.start(token)
            assert not rows(chief, "SELECT 1 FROM service_wakes WHERE json_extract(receipt,'$.run_id')=?", run)
            if snapshot == before_dispatch:
                assert not rows(chief, "SELECT 1 FROM runs WHERE run_id=?", run)
                # Deliver the authentic final event before any of its predecessors,
                # including the signed claim. The acknowledgement cannot skip the gap.
                code, body = link.send("/peer/v1/sync", {**packet, "events":[receipt]})
                if code == 200:
                    assert json.loads(body)["s1_cursor"] == 0, body
                else:
                    assert code in (400, 409, 422), (code, body)
                assert not rows(chief, "SELECT 1 FROM service_wakes WHERE json_extract(receipt,'$.run_id')=?", run)
                reverse.allow_pull.set()
            worker.start(token)
            recovered = wait_terminal(chief, run, state)
            single_wake(chief, run)
            for key in ("run_id","conversation_id","target_node_id","target_slot_id","receipt_summary"):
                assert recovered[key] == first[key] == owned[key], (key, recovered)
            assert chief.api("/v1/runs", dispatch)["run_id"] == run
            assert rows(chief, "SELECT s0_log_local_path FROM runs WHERE run_id=?", run) == [(None,)]
            assert not list(chief.dir.rglob("transcript.jsonl"))
            assert rows(worker, "SELECT phase FROM service_claims WHERE run_id=?", run) == [("finished",)]
        (local_path,), = rows(worker, "SELECT s0_log_local_path FROM runs WHERE run_id=?", run)
        transcript = Path(local_path)
        assert transcript.is_file() and worker.dir in transcript.parents
        if hc:
            assert link.marker in transcript.read_text()
            assert (work / "launches").read_text().splitlines() == [run]
        assert not link.leaked.is_set() and not reverse.leaked.is_set()
        print("PASS lost acknowledgement, reordered/duplicate S1 events and two supervisor rollbacks")
        print(f"PASS {state} receipt and one wake restored, S0 retained only on executor")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="ts-") as tmp:
        root = Path(tmp)
        try:
            check(root, os.environ.get("TARLA_TEST_HC"))
        except Exception:
            for name in ("a", "b"):
                log = root / name / "server.log"
                if log.exists():
                    print(f"Last messages from node {name}:\n{log.read_text()[-1500:]}", file=sys.stderr)
            raise
