"""Real webhook, supervisor HTTP stream and gateway SQLite queues; no IM credentials."""
import contextlib
import json
import sqlite3
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from urllib.request import Request

from acceptance import HTTP, Node, until


class StreamingModel(BaseHTTPRequestHandler):
    def do_POST(self):
        self.rfile.read(int(self.headers["Content-Length"]))
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()

        def event(payload):
            self.wfile.write(b"data: " + json.dumps(payload).encode() + b"\n\n")
            self.wfile.flush()

        event({"choices": [{"delta": {"content": "First visible words"}, "finish_reason": None}]})
        self.server.finish.wait(35)
        event({"choices": [{"delta": {"content": " and done."}, "finish_reason": None}]})
        event({"choices": [{"delta": {}, "finish_reason": "stop"}]})
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()

    def log_message(self, *_):
        pass


def webhook(node, event, text):
    req = Request(
        node.url + "/gateway/telegram",
        data=json.dumps({"update_id": event, "message": {
            "text": text, "from": {"id": 7}, "chat": {"id": 7}
        }}).encode(),
        headers={"Content-Type": "application/json",
                 "X-Telegram-Bot-Api-Secret-Token": "local-fixture"},
    )
    with HTTP.open(req, timeout=5) as response:
        assert json.load(response) == {"ok": True}


def main():
    with tempfile.TemporaryDirectory() as directory, contextlib.ExitStack() as cleanup:
        server = ThreadingHTTPServer(("127.0.0.1", 0), StreamingModel)
        server.finish = threading.Event()
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        cleanup.callback(thread.join)
        cleanup.callback(server.server_close)
        cleanup.callback(server.shutdown)
        cleanup.callback(server.finish.set)

        node = Node(Path(directory), "chief", "supervisor_only")
        cfg = node.dir / "workspace.toml"
        cfg.write_text(cfg.read_text().replace("fallback_chain = []", 'fallback_chain = ["http:fixture"]')
                       + f'\n[supervisor.brain.backends.fixture]\nkind="openai"\nmodel="fixture"\n'
                         f'endpoint="http://127.0.0.1:{server.server_port}/chat"\n'
                         '\n[gateway.telegram]\nenabled=true\n')
        # Keep provider delivery disconnected so we can inspect the durable final
        # independently of public infrastructure. Adapter unit tests verify HTTP edits.
        with patch.dict("os.environ", {
            "TARLA_TELEGRAM_BOT_TOKEN": "42:LOCAL_ONLY",
            "TARLA_TELEGRAM_WEBHOOK_SECRET": "local-fixture",
            "HTTPS_PROXY": "http://127.0.0.1:1", "https_proxy": "http://127.0.0.1:1",
            "NO_PROXY": "127.0.0.1,localhost", "no_proxy": "127.0.0.1,localhost",
        }):
            cleanup.callback(node.stop)
            node.start("")
        code = node.api("/gateway/pairings", {"platform": "telegram", "actor_id": "7",
                                             "conversation_id": "planning"})["code"]
        webhook(node, 1, "/pair " + code)
        webhook(node, 2, "Stream a reply")
        db = sqlite3.connect(node.dir / "gateway.db")
        cleanup.callback(db.close)
        until(lambda: db.execute(
            "SELECT count(*) FROM live_drafts WHERE source=? AND latest LIKE ? AND closed=0",
            ("in:telegram:2", "%First visible words%")).fetchone()[0], "IM preview before completion")
        history = "/v1/conversations/planning/messages"
        assert all(m["sender_role"] != "assistant" for m in node.api(history))
        server.finish.set()
        final = "First visible words and done."
        until(lambda: any(m["content"] == final for m in node.api(history)), "committed reply")
        until(lambda: any(json.loads(row[0]).get("Final", {}).get("text") == final
                          for row in db.execute("SELECT job FROM outgoing")), "durable final IM delivery")
        print("PASS authenticated Telegram webhook, live draft before model completion, committed IM outbox")


if __name__ == "__main__":
    main()
