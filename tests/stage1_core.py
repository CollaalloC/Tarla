"""Exercise all six control tools, an asynchronous local mission and host isolation."""
import contextlib
import json
import os
import subprocess
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from unittest.mock import patch
from urllib.error import HTTPError

from acceptance import BIN, Node, install_test_slot, until


class Model(BaseHTTPRequestHandler):
    def do_POST(self):
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        if self.path == "/busy":
            self.server.fallbacks += 1
            return self.send_error(429)
        tools = {x["function"]["name"] for x in request["tools"]}
        message = request["messages"][-1]
        if "dispatch_mission" not in tools:
            assert tools == {"inspect_run_log", "session_recall", "list_fleet_and_runs"}
            answer = {"content": "Completion received for the local mission."}
        elif message["role"] == "tool":
            name = message["tool_call_id"]
            self.server.results[name] = json.loads(message["content"])
            answer = {"content": f"Finished {name}."}
        else:
            assert tools == set(self.server.calls)
            name = message["content"]
            args = self.server.calls[name]
            answer = {"content": "", "tool_calls": [{"id": name, "type": "function",
                      "function": {"name": name, "arguments": json.dumps(args)}}]}
        response = json.dumps({"choices": [{"message": {"role": "assistant", **answer},
                              "finish_reason": "tool_calls" if "tool_calls" in answer else "stop"}]}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, *_):
        pass


def exercise(root, model, hc, host_environment):
    home = root / "tarla"
    work = Node(home, "chief", "supervisor_and_executor")
    other = Node(home, "chief", "supervisor_and_executor", workspace="life")
    config = work.dir / "workspace.toml"
    settings = config.read_text().replace("fallback_chain = []", 'fallback_chain = ["http:busy", "http:local"]')
    for name in ("busy", "local"):
        settings += (f'\n[supervisor.brain.backends.{name}]\nkind="openai"\nmodel="fixture"\n'
                     f'endpoint="http://127.0.0.1:{model.server_port}/{name}"\n')
    config.write_text(settings)
    project = install_test_slot(work, hc)
    (project / "job.py").write_text(
        "from pathlib import Path\nimport json,sys,time\n"
        "packet=json.loads(sys.argv[1]); Path('packet').write_text(json.dumps(packet))\n"
        "print('stage-one-log-marker',flush=True)\n"
        "for _ in range(600):\n"
        "    if Path('finish').exists(): break\n"
        "    time.sleep(.1)\n"
        "else: raise SystemExit(1)\n")
    model.results = {}
    model.fallbacks = 0
    model.calls = {
        "list_fleet_and_runs": {},
        "memory_manage": {"operation": "create", "memory_id": "fixture", "scope": "global",
                          "content": "Record the stage-one fixture."},
        "session_recall": {"query": "memory_manage"},
        "dispatch_mission": {"objective": "Report the fixture state", "acceptance_criteria": ["print marker"]},
        "inspect_run_log": {},
        "bind_channel": {"mode": "pinned", "node_id": "chief", "slot_id": "fixture", "workdir": str(project)},
    }
    def chat(name):
        return work.cli("chat", "daily", name, "--source-id", "stage1-" + name)
    with contextlib.ExitStack() as cleanup:
        # tarlad uses its private socket even when an existing host daemon is selected.
        cleanup.callback(other.stop)
        cleanup.callback(work.stop)
        work.start("unused")
        other.start("unused")
        assert "fixture" in work.cli("slots")
        for action in ("list_fleet_and_runs", "memory_manage", "session_recall"):
            assert "Finished " + action in chat(action)
        assert "fixture" in str(model.results["list_fleet_and_runs"])
        assert "memory_manage" in str(model.results["session_recall"])
        assert work.api("/v1/memories")[0]["memory_id"] == "fixture"
        assert other.api("/v1/memories") == []
        swapped = subprocess.run([BIN / "tarla", "--home", home, "--workspace", "work",
                                  "--url", other.url, "remember", "misrouted", "wrong workspace",
                                  "--base-version", "0"], env={**os.environ, "TARLA_TOKEN": other.token},
                                 capture_output=True, text=True)
        assert swapped.returncode != 0 and "workspace" in swapped.stderr
        assert other.api("/v1/memories") == []
        try:
            other.api("/v1/memories", token=work.token)
        except HTTPError as err:
            assert err.code == 401
        else:
            raise AssertionError("a work credential was accepted in life")

        ack = chat("dispatch_mission")
        rid = work.api("/v1/runs")[0]["run_id"]
        assert rid in ack
        assert rid in chat("dispatch_mission")
        assert len(work.api("/v1/runs")) == 1
        until(lambda: (project / "packet").exists(), "local holder started")
        assert work.api("/v1/runs/" + rid)["observed_state"] != "succeeded"
        packet = json.loads((project / "packet").read_text())
        assert packet["workspace_id"] == "work" and packet["run_id"] == rid
        assert packet["acceptance_criteria"] == ["print marker"]
        assert "stage-one fixture" in str(packet["project_memory_snapshot"])
        assert other.api("/v1/runs") == []
        (project / "finish").touch()
        until(lambda: any(m["content"] == "Completion received for the local mission."
                          for m in work.api("/v1/conversations/daily/messages")), "unprompted receipt")
        assert work.api("/v1/runs/" + rid)["observed_state"] == "succeeded"
        model.calls["inspect_run_log"] = {"run_id": rid, "query": "stage-one-log-marker"}
        assert "Finished inspect_run_log" in chat("inspect_run_log")
        assert "stage-one-log-marker" in json.dumps(model.results["inspect_run_log"])
        assert "Finished bind_channel" in chat("bind_channel")
        assert work.api("/v1/conversations")[0]["pinned_slot_id"] == "fixture"
        assert model.fallbacks > 0
        assert Path(work.dir / "runtime/hc.sock").is_socket()
        assert subprocess.check_output([hc, "ls", "--all", "--json"], env=host_environment) == model.host_before


def main():
    hc = os.environ["TARLA_TEST_HC"]
    with tempfile.TemporaryDirectory(prefix="s1-") as tmp, contextlib.ExitStack() as cleanup:
        root = Path(tmp)
        native = root / "host"
        native.mkdir()
        env = {"HC_SOCKET": str(native / "hc.sock"), "HC_SESSION": "host"}
        for var in ("XDG_RUNTIME_DIR", "XDG_CONFIG_HOME", "XDG_STATE_HOME"):
            directory = native / var
            directory.mkdir(mode=0o700)
            env[var] = str(directory)
        cleanup.enter_context(patch.dict(os.environ, env))
        host = dict(os.environ)
        subprocess.run([hc, "server", "start"], env=host, check=True)
        cleanup.callback(subprocess.run, [hc, "server", "stop"], env=host, check=False)
        model = ThreadingHTTPServer(("127.0.0.1", 0), Model)
        cleanup.callback(model.server_close)
        threading.Thread(target=model.serve_forever, daemon=True).start()
        cleanup.callback(model.shutdown)
        model.host_before = subprocess.check_output([hc, "ls", "--all", "--json"], env=host)
        exercise(root, model, hc, host)
    print("PASS single-node CLI, six model tools, async receipt, isolated work/life and concurrent host Herdr")


if __name__ == "__main__":
    main()
