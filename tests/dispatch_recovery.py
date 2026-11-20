"""Crash consistency of model dispatch, author-bound reviews and isolated receipt delivery."""
import contextlib
import http.client
import json
import sqlite3
import tempfile
import threading
from collections import Counter
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError

from acceptance import Node, until


class Model(BaseHTTPRequestHandler):
    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        specs = {tool["function"]["name"]: tool["function"] for tool in request["tools"]}
        if "dispatch_mission" not in specs:
            if any(m["content"] == "slow-receipt" for m in request["messages"]):
                self.server.slow_started.set()
                self.server.release_slow.wait(timeout=12)
            return self.send_error(503)  # Force the durable receipt reporting fallback.
        properties = specs["dispatch_mission"]["parameters"]["properties"]
        assert "scope" in properties and "author_run_id" in properties
        prompt = request["messages"][-1]["content"]
        self.server.calls[prompt] += 1
        args = {"objective": prompt, "acceptance_criteria": ["report the result"],
                "scope": "project:alpha", "required_caps": ["author-a"],
                "required_fact_keys": ["missing-preflight-fact"]}
        args.update(self.server.overrides.get(prompt, {}))
        answer = {"content": "", "tool_calls": [{"id": "dispatch", "type": "function",
                  "function": {"name": "dispatch_mission", "arguments": json.dumps(args)}}]}
        body = json.dumps({"choices": [{"message": answer, "finish_reason": "tool_calls"}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *_):
        pass


def rejected(fn, expected):
    try:
        fn()
    except HTTPError as error:
        body = error.read().decode()
        assert expected in body, body
    else:
        raise AssertionError(f"expected rejection: {expected}")


def exercise(root, model):
    chief = Node(root / "home", "chief", "supervisor_and_executor")
    config = chief.dir / "workspace.toml"
    settings = config.read_text().replace("fallback_chain = []", 'fallback_chain = ["http:local"]')
    settings += ('\n[supervisor.brain.backends.local]\nkind="openai"\nmodel="fixture"\n'
                 f'endpoint="http://127.0.0.1:{model.server_port}/model"\n')
    for name in ("a", "b"):
        settings += (f'\n[[slots]]\nslot_id="{name}"\ndriver="local"\nmodel_family="{name}"\n'
                     'supported_roles=["implementer","reviewer"]\n'
                     f'node_capabilities=["author-{name}"]\nargv=["unused-preflight"]\n'
                     f'workdir={json.dumps(str(root))}\n')
    config.write_text(settings)

    def path(conv):
        return f"/v1/conversations/{conv}/messages"

    def payload(text, source=None):
        return {"channel": "cli", "source_msg_id": source or text, "text": text}

    def chat(conv, text, source=None):
        return chief.api(path(conv), payload(text, source))

    def sql(statement, params=()):
        with sqlite3.connect(chief.dir / "state.db") as db:
            return db.execute(statement, params).fetchall()

    def reports(conversation, run):
        return [m for m in chief.api(path(conversation)) if m["sender_role"] == "assistant"
                and m["source_msg_id"].startswith("wake:") and m["run_id"] == run]

    try:
        chief.start("unused")
        for scope, fact in [("global", "global-setting"), ("project:alpha", "alpha-setting"),
                            ("project:other", "other-setting")]:
            chief.api("/v1/memories", {"memory_id": fact, "scope": scope, "content": fact,
                                       "base_version": 0})

        # SQLite aborts between inserting a run and inserting its S1 acknowledgement.
        # Neither the run, its local claim nor a dispatchable mission may remain.
        sql("""CREATE TRIGGER abort_ack BEFORE INSERT ON messages
            WHEN NEW.source_msg_id='in:cli:atomic' AND NEW.sender_role='assistant'
            BEGIN SELECT RAISE(ABORT, 'simulated acknowledgement interruption'); END""")
        rejected(lambda: chat("primary", "dispatch-once", "atomic"), "acknowledgement interruption")
        assert sql("SELECT count(*) FROM runs") == [(0,)]
        assert sql("SELECT count(*) FROM service_claims") == [(0,)]
        sql("DROP TRIGGER abort_ack")

        # Leave the HTTP reply unread, wait for the SQLite commit, then kill tarlad.
        conn = http.client.HTTPConnection("127.0.0.1", chief.port, timeout=7)
        args = payload("dispatch-once", "atomic")
        conn.request("POST", path("primary"), json.dumps(args),
                     {"Content-Type": "application/json", "Authorization": "Bearer " + chief.token})
        try:
            rows = until(lambda: sql("SELECT run_id,content FROM messages WHERE source_msg_id='in:cli:atomic'"),
                         "atomic run and acknowledgement", timeout=4)
        except AssertionError:
            response = conn.getresponse()
            raise AssertionError((response.status, response.read(), dict(model.calls)))
        first, reply = rows[0]
        chief.proc.kill()
        chief.proc.wait(timeout=5)
        conn.close()
        chief.stop()
        chief.start("unused")
        assert chat("primary", "dispatch-once", "atomic") == {"conversation_id": "primary",
                                                              "run_id": first, "reply": reply}
        assert model.calls["dispatch-once"] == 2  # Aborted attempt plus committed attempt.
        assert sql("SELECT count(*) FROM runs") == [(1,)]
        rejected(lambda: chat("primary", "different", "atomic"), "different content")
        packet = json.loads(sql("SELECT mission_packet_json FROM runs WHERE run_id=?", (first,))[0][0])
        memory = packet["project_memory_snapshot"]
        assert "alpha-setting" in memory and "global-setting" in memory and "other-setting" not in memory

        # Simulate a database upgraded from the older two-commit dispatch implementation.
        sql("DELETE FROM messages WHERE source_msg_id='in:cli:atomic'")
        assert chat("primary", "dispatch-once", "atomic")["reply"] == reply
        assert model.calls["dispatch-once"] == 2
        assert sql("SELECT count(*) FROM messages WHERE source_msg_id='in:cli:atomic'") == [(1,)]

        model.overrides["second-author"] = {"required_caps": ["author-b"]}
        second = chat("primary", "second-author")["run_id"]
        model.overrides["review-first"] = {"role": "reviewer", "required_caps": [],
                                           "author_run_id": first, "exclude_model_family": "unrelated"}
        review = chat("primary", "review-first")["run_id"]
        assert chief.api("/v1/runs/" + review)["target_slot_id"] == "b"
        model.overrides["unspecified-review"] = {"role": "reviewer", "required_caps": []}
        rejected(lambda: chat("primary", "unspecified-review"), "specify author_run_id")
        model.overrides["foreign-review"] = {"role": "reviewer", "required_caps": [], "author_run_id": second}
        rejected(lambda: chat("foreign", "foreign-review"), "author run in this conversation")

        # A stalled receipt model in one conversation cannot hold up another user's report.
        slow = chat("left", "slow-receipt")["run_id"]
        assert model.slow_started.wait(timeout=10)
        fast = chat("right", "fast-receipt")["run_id"]
        completed = until(lambda: reports("right", fast), "independent completion", timeout=6)
        assert fast in completed[0]["content"] and "failed" in completed[0]["content"]
        assert not reports("left", fast) and not reports("left", slow)
        model.release_slow.set()
        until(lambda: reports("left", slow), "slow conversation completion")
        sql("""INSERT INTO outbox_events(event_id,run_id,event_type,payload_json,created_at)
               SELECT 'duplicate-finished',run_id,event_type,payload_json,created_at FROM outbox_events
               WHERE run_id=? AND event_type='run_receipt' LIMIT 1""", (fast,))
        until(lambda: sql("SELECT acked_at FROM outbox_events WHERE event_id='duplicate-finished'")[0][0],
              "duplicate receipt acknowledgement")
        assert len(reports("right", fast)) == 1
        assert sql("SELECT count(*) FROM service_wakes WHERE json_extract(receipt,'$.run_id')=?", (fast,)) == [(1,)]
    finally:
        model.release_slow.set()
        chief.stop()


def main():
    with tempfile.TemporaryDirectory(prefix="dispatch-recovery-") as work, contextlib.ExitStack() as cleanup:
        model = ThreadingHTTPServer(("127.0.0.1", 0), Model)
        model.calls, model.overrides = Counter(), {}
        model.slow_started, model.release_slow = threading.Event(), threading.Event()
        cleanup.callback(model.server_close)
        threading.Thread(target=model.serve_forever, daemon=True).start()
        cleanup.callback(model.shutdown)
        exercise(Path(work), model)
    print("PASS atomic dispatch, crash replay, scoped missions, actual author exclusion, isolated completion retries")


if __name__ == "__main__":
    main()
