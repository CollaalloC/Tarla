"""Stage 1 archive, actual OpenAI-compatible request, dispatch, and receipt budget acceptance.

Requires freshly built tarla and tarlad. The loopback model is deterministic and exercises
the real tool round-trip; no inference credentials or hosted model are required.
"""
import json
import sqlite3
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from acceptance import Node, until


TURNS = 156
WINDOW = 12000
MISSION = 1000
RECEIPT = 340
DECISION = "最终决定将潮汐校准周期固定为四十七天。"
DETAIL = "精确编号【琉璃七九】"
LONG = "起始说明。" + "传感器尚在收集现场数据。" * 430 + DECISION + DETAIL + "。" + "月底整理其他记录。" * 440 + "尾部问题请复核。"
OFFSET = len(LONG[:LONG.index(DETAIL)].encode())
ARCHIVE_TURN = 47
CONVERSATION = "/v1/conversations/history/messages"
PROFILE = "你是潮汐项目主管。"


class LocalModel(BaseHTTPRequestHandler):
    count = 0
    peak = 0
    page = None
    summaries = []
    long_preview = None
    normal_window = None
    prefix = None

    def do_POST(self):
        data = self.rfile.read(int(self.headers["Content-Length"]))
        body = json.loads(data)
        turn = body["messages"]
        cls = type(self)
        cls.count += 1
        cls.peak = max(cls.peak, len(data))
        assert len(data) <= WINDOW, len(data)
        if cls.prefix is None:
            cls.prefix = turn[0]["content"]
        if turn[-1]["content"] in ("测量", "空闲压缩探针"):
            assert turn[0]["content"].startswith("这个新设定只用于新会话。")
        else:
            assert turn[0]["content"] == cls.prefix and PROFILE in cls.prefix
        cls.summaries = [m["content"] for m in turn if m["content"].startswith("[history ")]
        latest = turn[-1]
        reply = {"role": "assistant", "content": "现场记录已收到。"}
        finish = "stop"
        if latest["role"] == "tool":
            cls.page = json.loads(latest["content"])
            reply["content"] = cls.page["content"]
        elif latest["content"] == "空闲压缩探针":
            assert not cls.summaries
            reply["content"] = "这段回执将越过水位线。" * 300
        elif latest["content"] == "回读原始编号。":
            reply.update(content="", tool_calls=[{
                "id": "read-exact", "type": "function", "function": {
                    "name": "session_recall",
                    "arguments": json.dumps({"seq_no": 2 * ARCHIVE_TURN + 1,
                                             "offset_bytes": OFFSET, "max_bytes": len(DETAIL.encode())})
                }
            }])
            finish = "tool_calls"
        elif "尾部问题请复核。" in latest["content"]:
            cls.long_preview = latest["content"]
        elif latest["content"].startswith("第155次"):
            cls.normal_window = [m["content"] for m in turn]
        payload = json.dumps({"choices": [{"message": reply, "finish_reason": finish}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_):
        pass


def normal(i):
    return f"第{i}次采样。" + "雨后海边记录需与当天数据核对。" * 5


def run(path, port):
    node = Node(path / "chief", "chief", "supervisor_and_executor")
    config = node.dir / "workspace.toml"
    cfg = config.read_text().replace('fallback_chain = []', 'fallback_chain = ["http:fixture"]')
    for key, old, new in [("supervisor_active_budget_tokens", 24000, WINDOW),
                          ("mission_packet_max_tokens", 1800, MISSION),
                          ("run_receipt_max_tokens", 500, RECEIPT)]:
        assert f"{key} = {old}" in cfg
        cfg = cfg.replace(f"{key} = {old}", f"{key} = {new}")
    cfg += f'\n[supervisor.brain.backends.fixture]\nkind="openai"\nmodel="local"\nendpoint="http://127.0.0.1:{port}/chat"\n'
    cfg += f'\n[[slots]]\nslot_id="local"\ndriver="local"\nmodel_family="fixture"\nargv=["/bin/true"]\nworkdir={json.dumps(str(path))}\n'
    config.write_text(cfg)
    (node.dir / "SOUL.md").write_text(PROFILE)
    endpoint = node.dir / "state.db"
    try:
        node.start("unused")
        expected = {}
        for i in range(TURNS):
            original = LONG if i == ARCHIVE_TURN else normal(i)
            expected[f"turn-{i}"] = original
            answer = node.api(CONVERSATION, {"text": original, "source_msg_id": f"turn-{i}"})
            assert answer["reply"] == "现场记录已收到。", answer
            if i == 1:
                node.api("/v1/memories", {"memory_id":"changed","scope":"global","content":"新增记录不改变已冻结前缀","base_version":0})
            if i == 100:
                node.stop()
                (node.dir/"SOUL.md").write_text("这个新设定只用于新会话。")
                node.start("unused")
        assert LocalModel.long_preview.startswith("起始说明。")
        assert f"[archive seq_no={2 * ARCHIVE_TURN + 1} " in LocalModel.long_preview
        assert LocalModel.long_preview.endswith("尾部问题请复核。")
        assert DECISION in "\n".join(LocalModel.summaries), LocalModel.summaries
        for i in range(TURNS - 8, TURNS):
            assert normal(i) in LocalModel.normal_window
        answer = node.api(CONVERSATION, {"text": "回读原始编号。", "source_msg_id": "lookup"})
        assert answer["reply"] == DETAIL and LocalModel.page["offset_bytes"] == OFFSET, answer
        assert LocalModel.page["total_bytes"] == len(LONG.encode())
        assert LocalModel.page["next_offset_bytes"] == OFFSET + len(DETAIL.encode())
        # Check every original through the public paginated history route, including the giant input.
        rows = []
        after = 0
        while result := node.api(f"{CONVERSATION}?after_seq={after}&limit=64"):
            rows.extend(result)
            after = result[-1]["seq_no"]
        actual = {m["source_msg_id"]: m["content"] for m in rows if m["source_msg_id"] in expected}
        assert actual == expected and len(rows) == 2 * (TURNS + 1)
        with sqlite3.connect(endpoint) as db:
            depth, = db.execute("SELECT max(depth_level) FROM context_dag_nodes").fetchone()
            assert depth >= 2
        # Exercise actual dispatch+receipt path following the long conversation. A missing
        # local fact terminates the assigned run on preflight without an external program.
        required = "潮汐校准"
        run = node.api("/v1/runs", {"conversation_id": "history", "objective": "请复核潮汐校准周期",
            "acceptance_criteria": ["记录周期"], "distilled_context": ["冗余" * 1700],
            "required_fact_keys": [required]})["run_id"]
        def received():
            with sqlite3.connect(endpoint) as db:
                return db.execute("SELECT content FROM messages WHERE conversation_id='history' AND sender_role='run_receipt' ORDER BY seq_no DESC LIMIT 1").fetchone()
        receipt, = until(received, "archived executor receipt")
        with sqlite3.connect(endpoint) as db:
            packet, = db.execute("SELECT mission_packet_json FROM runs WHERE run_id=?", (run,)).fetchone()
        assert len(packet.encode()) <= MISSION
        decisions = json.loads(packet)["distilled_context"]
        assert 1 <= len(decisions) <= 5 and any(DECISION in item for item in decisions), decisions
        assert len(receipt.encode()) <= RECEIPT
        receipt_obj = json.loads(receipt)
        assert receipt_obj["run_id"] == run and receipt_obj["final_state"] == "failed"
        assert run in receipt_obj["s0_transcript_pointer"]
        idle_conv = "/v1/conversations/idle-only/messages"
        for i in range(8):
            node.api(idle_conv, {"text": "测量", "source_msg_id": f"idle-{i}"})
        node.api(idle_conv, {"text": "空闲压缩探针", "source_msg_id":"idle-pressure"})
        def folded_after_reply():
            with sqlite3.connect(endpoint) as db:
                return db.execute("SELECT count(*) FROM context_dag_nodes WHERE conversation_id='idle-only'").fetchone()[0] > 0
        until(folded_after_reply, "idle compaction without another user turn")
        print(f"PASS {TURNS} real-daemon turns; {len(LONG.encode())}-byte Chinese original, "
              f"DAG depth {depth}, {LocalModel.count} loopback model requests, maximum wire "
              f"body {LocalModel.peak}/{WINDOW} bytes; mission {len(packet.encode())}/{MISSION}, "
              f"receipt {len(receipt.encode())}/{RECEIPT}")
    finally:
        node.stop()


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as directory, ThreadingHTTPServer(("127.0.0.1", 0), LocalModel) as server:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            run(Path(directory), server.server_port)
        finally:
            server.shutdown()
            thread.join()
