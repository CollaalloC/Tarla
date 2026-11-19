"""Exercise workspace skills through the daemon's real chat, supervisor and pinned routes."""
import contextlib
import json
from pathlib import Path
import sqlite3
import sys
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.error import HTTPError
from urllib.request import Request

from acceptance import HTTP, Node


class Model(BaseHTTPRequestHandler):
    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.server.requests.append(body["messages"])
        if self.server.defer:
            self.send_error(503)
            return
        message = {"role": "assistant", "content": "Completed."}
        if self.server.dispatch:
            message.update(content=None, tool_calls=[{
                "id": "call-id", "type": "function",
                "function": {"name": "dispatch_mission", "arguments": json.dumps({
                    "objective": "verify fixture", "acceptance_criteria": ["report the result"]})}
            }])
        reply = json.dumps({"choices": [{"message": message, "finish_reason":
                            "tool_calls" if self.server.dispatch else "stop"}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(reply)))
        self.end_headers()
        self.wfile.write(reply)

    def log_message(self, *_):
        pass


def install(node, name, content):
    path = node.dir / "skills" / name / "SKILL.md"
    path.parent.mkdir(exist_ok=True)
    path.write_text(content)
    return path


def check(root, model):
    n = Node(root / "main", "chief", "supervisor_and_executor")
    separate = Node(root / "other", "chief", "supervisor_only")
    install(separate, "private", "second workspace secret")
    selected = install(n, "calibrate", "Measure using fixture delta-125.")
    install(n, "elsewhere", "never load omega-993.")
    # Discovery should still work with an unselected skill that cannot be
    # decoded as UTF-8. Lazy loading only opens the explicitly requested file.
    install(n, "bad-file", "unused").write_bytes(b"\xff")
    conf = n.dir / "workspace.toml"
    conf.write_text(conf.read_text().replace("fallback_chain = []", 'fallback_chain = ["http:fixture"]')
        + f'\n[supervisor.brain.backends.fixture]\nkind="openai"\nmodel="fixture"\n'
          f'endpoint="http://127.0.0.1:{model.server_port}/model"\n'
          f'\n[[slots]]\nslot_id="local"\ndriver="local"\nmodel_family="local"\n'
          f'workdir={json.dumps(str(root))}\nargv={json.dumps([sys.executable])}\n')
    with contextlib.ExitStack() as cleanup:
        cleanup.callback(n.stop)
        n.start("unused")
        def chat(text, source, conv="check"):
            return n.api(f"/v1/conversations/{conv}/messages", {"text":text,"source_msg_id":source})
        def rejected(fn, expected):
            try:
                fn()
            except HTTPError as error:
                assert expected in error.read().decode(), error
                return
            raise AssertionError("request unexpectedly succeeded")
        assert chat("/skills", "list")["reply"].splitlines() == ["bad-file","calibrate","elsewhere"]
        assert model.requests == []
        rejected(lambda:chat("/skill private task", "absent"), "unknown workspace skill")
        rejected(lambda:chat("/skill ../other task", "escape"), "safe path component")
        model.defer = True
        rejected(lambda:chat("/skill calibrate verify", "retry"), "unavailable")
        selected.write_text("New revision fixture-563.")
        model.defer = False
        assert chat("/skill calibrate verify", "retry")["reply"] == "Completed."
        before = model.requests[-1]
        assert "delta-125" in before[-1]["content"] and "fixture-563" not in str(before)
        assert "omega-993" not in str(before) and "second workspace secret" not in str(before)
        assert chat("normal request", "ordinary")["reply"] == "Completed."
        after = model.requests[-1]
        assert before[0] == after[0] and "delta-125" not in str(after)
        model.dispatch = True
        chat("/skill calibrate verify fixture", "delegation", "delegate")
        model.dispatch = False
        def packet(conv):
            with sqlite3.connect(n.dir/"state.db") as db:
                return json.loads(db.execute("SELECT mission_packet_json FROM runs WHERE conversation_id=?",(conv,)).fetchone()[0])
        assert "fixture-563" in packet("delegate")["distilled_context"][0]
        request = Request(n.url + "/v1/conversations/pin/binding", method="PUT",
            headers={"Authorization":"Bearer "+n.token,"Content-Type":"application/json"},
            data=json.dumps({"mode":"pinned","node_id":"chief","slot_id":"local","workdir":str(root)}).encode())
        HTTP.open(request).close()
        assert chat("/skill calibrate sample", "pin", "pin")["run_id"]
        pinned = packet("pin")
        assert pinned["objective"] == "sample" and "fixture-563" in pinned["distilled_context"][0]
        install(n,"oversize","x"*2200)
        model.dispatch = True
        rejected(lambda:chat("/skill oversize run", "too-big","over"), "does not fit")
        with sqlite3.connect(n.dir/"state.db") as db:
            assert not db.execute("SELECT 1 FROM runs WHERE conversation_id='over'").fetchone()
        print("PASS skill discovery, explicit workspace selection, revision replay, stable prefix, supervisor and pinned missions, oversize rejection")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory, ThreadingHTTPServer(("127.0.0.1",0),Model) as model:
        model.requests,model.defer,model.dispatch=[],False,False
        thread=threading.Thread(target=model.serve_forever,daemon=True)
        thread.start()
        try:
            check(Path(directory),model)
        finally:
            model.shutdown()
            thread.join()
