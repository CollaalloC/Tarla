"""Verify transient replies with real HTTP requests and fragmented provider SSE streams."""
import contextlib
import json
import secrets
import tempfile
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request

from acceptance import HTTP, Node, until


class Provider(BaseHTTPRequestHandler):
    def event(self, data):
        # Write actual UTF-8, cutting multibyte text and JSON/event delimiters across writes.
        line = "data: " + (data if isinstance(data,str) else json.dumps(data,ensure_ascii=False)) + "\r\n\r\n"
        raw = line.encode()
        for i in range(0,len(raw),3):
            self.wfile.write(raw[i:i+3])
            self.wfile.flush()

    def hold(self, label):
        self.server.ready[label].set()
        if not self.server.release[label].wait(20):
            raise RuntimeError("test did not release " + label)

    def do_POST(self):
        incoming=json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        turns=incoming["messages"]
        self.send_response(200)
        self.send_header("Content-Type","text/event-stream")
        self.end_headers()
        if self.path == "/first":
            if turns[0]["role"]=="system" and any(m.get("content")=="tool-path" for m in turns):
                if any(m.get("role")=="tool" for m in turns):
                    self.event({"choices":[{"delta":{"content":"Final after tool."},"finish_reason":"stop"}]})
                    self.event("[DONE]")
                    return
                args=json.dumps({"query":"private-tool-search"})
                for step,arg in enumerate([args[:5],args[5:]]):
                    call={"index":0,"function":{"arguments":arg}}
                    if step==0:
                        call.update(id="tool-1",type="function");call["function"]["name"]="session_recall"
                    self.event({"choices":[{"delta":{"tool_calls":[call],"reasoning_content":"private reasoning"}}]})
                self.hold("tool")
                self.event({"choices":[{"delta":{},"finish_reason":"tool_calls"}]})
                self.event("[DONE]")
                return
            self.event({"choices":[{"delta":{"reasoning_content":"never publish reasoning","content":"Early <thi"}}]})
            self.event({"choices":[{"delta":{"content":"nk>never publish inline</think>draft"}}]})
            self.hold("early")
            self.event({"error":{"message":"retry provider"}})
        else:
            self.event({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":""}})
            self.event({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"private fallback thinking"}})
            self.hold("reset")
            self.event({"type":"content_block_stop","index":0})
            self.event({"type":"content_block_start","index":1,"content_block":{"type":"text","text":"答"}})
            self.event({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"案中"}})
            self.hold("later")
            self.event({"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"文"}})
            self.event({"type":"content_block_stop","index":1})
            self.event({"type":"message_delta","delta":{"stop_reason":"end_turn"}})
            self.event({"type":"message_stop"})

    def log_message(self,*_):
        pass


def check(root,provider):
    node=Node(root,"chief","supervisor_only")
    cfg=node.dir/"workspace.toml"
    cfg.write_text(cfg.read_text().replace('fallback_chain = []','fallback_chain = ["http:first","http:second"]')
      + f'\n[supervisor.brain.backends.first]\nkind="openai"\nmodel="fixture"\nendpoint="http://127.0.0.1:{provider.server_port}/first"\n'
      + f'\n[supervisor.brain.backends.second]\nkind="anthropic"\nmodel="fixture"\nendpoint="http://127.0.0.1:{provider.server_port}/second"\n')
    secret=secrets.token_hex(32)
    def chat(source,text):
        return node.api("/v1/conversations/current/messages",{"channel":"telegram","source_msg_id":source,"text":text})
    def draft(source="42",channel="telegram",conv="current",token=None):
        return node.api(f"/v1/conversations/{conv}/draft?"+urlencode({"channel":channel,"source_msg_id":source}),token=token)
    node.start(secret)
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            try:
                req=pool.submit(chat,"42","start a visible draft")
                assert provider.ready["early"].wait(10)
                early=until(lambda:(d if (d:=draft())["text"]=="Early draft" else None),"first partial")
                assert early["status"]=="streaming"
                assert draft(conv="elsewhere")["status"]=="idle"
                assert draft(channel="app")["status"]=="idle"
                assert draft(source="another")["status"]=="idle"
                try:
                    draft(token="wrong")
                    raise AssertionError("unauthorized draft read")
                except HTTPError as e:
                    assert e.code==401
                guarded=Request(node.url+"/v1/conversations/current/messages",data=json.dumps({"text":"wrong workspace"}).encode(),
                    headers={"Authorization":"Bearer "+node.token,"Content-Type":"application/json","X-Tarla-Workspace":"other"})
                try:
                    HTTP.open(guarded)
                    raise AssertionError("mismatched workspace accepted")
                except HTTPError as e:
                    assert e.code==409
                provider.release["early"].set()
                assert provider.ready["reset"].wait(10)
                reset=until(lambda:(d if (d:=draft())["text"]=="" else None),"abandoned text removed")
                assert reset["revision"]>early["revision"]
                provider.release["reset"].set()
                assert provider.ready["later"].wait(10)
                later=until(lambda:(d if (d:=draft())["text"]=="答案中" else None),"fallback partial")
                assert later["revision"]>reset["revision"] and not req.done()
                provider.release["later"].set()
                assert req.result(timeout=10)["reply"]=="答案中文"
                final=draft()
                assert final["status"]=="completed" and final["text"]=="答案中文"
                assert [x["sender_role"] for x in node.api("/v1/conversations/current/messages")]==["user","assistant"]
                req=pool.submit(chat,"43","tool-path")
                assert provider.ready["tool"].wait(10)
                assert draft("43")["text"]==""
                provider.release["tool"].set()
                assert req.result(timeout=10)["reply"]=="Final after tool."
            finally:
                for release in provider.release.values():
                    release.set()
        node.stop()
        node.start(secret)
        assert draft()==final
        assert chat("42","start a visible draft")["reply"]=="答案中文"
        print("PASS fragmented OpenAI/Anthropic drafts, hidden fields, provider reset, committed replay, workspace auth and origin isolation")
    finally:
        node.stop()


if __name__=="__main__":
    with tempfile.TemporaryDirectory() as tmp, ThreadingHTTPServer(("127.0.0.1",0),Provider) as server:
        server.ready={k:threading.Event() for k in ("early","reset","later","tool")}
        server.release={k:threading.Event() for k in server.ready}
        thread=threading.Thread(target=server.serve_forever,daemon=True)
        thread.start()
        try:
            check(Path(tmp),server)
        finally:
            server.shutdown()
            thread.join()
