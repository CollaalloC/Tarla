"""Three Linux network namespaces, one real job, one offline and rejoining mirror.

CI runs this test in a fresh outer namespace, with no host interfaces or routes.
"""
import contextlib
import json
import os
from pathlib import Path
import secrets
import subprocess
import tempfile
import time
from urllib.parse import quote

from acceptance import BIN, Node, install_test_slot, until


def ip(*args):
    return subprocess.run(["ip", *args], check=True, capture_output=True)


class Host(Node):
    def __init__(self, root, name, role, number, cleanup):
        super().__init__(root / name, name, role)
        self.ns = "tarla-" + name + "-" + str(os.getpid())
        self.addr = f"198.18.0.{number}"
        self.link = f"eth{number}"
        self.url = f"http://{self.addr}:{self.port}"
        ip("netns", "add", self.ns)
        cleanup.callback(ip, "netns", "del", self.ns)
        ip("link", "add", self.link, "type", "veth", "peer", "name", "peer" + str(number))
        ip("link", "set", "peer" + str(number), "netns", self.ns)
        ip("-n", self.ns, "link", "set", "lo", "up")
        ip("-n", self.ns, "addr", "add", self.addr + "/24", "dev", "peer" + str(number))
        ip("-n", self.ns, "link", "set", "peer" + str(number), "up")
        ip("link", "set", self.link, "master", "testbr")
        ip("link", "set", self.link, "up")

    def start(self, keys):
        self.log = (self.home / "server.log").open("a")
        self.proc = subprocess.Popen(["ip","netns","exec",self.ns,str(BIN / "tarlad"),"--home",self.home,
            "--workspace","work","--listen",f"{self.addr}:{self.port}"],
            env={**os.environ, **keys}, stdout=self.log, stderr=self.log)
        until(lambda: self.api("/health"), "listen " + self.name)

    def peer(self, target, variable):
        with (self.dir / "workspace.toml").open("a") as stream:
            stream.write(f'\n[[peers]]\nnode_id="{target.name}"\nurl="{target.url}"\ntoken_env="{variable}"\n')


def run(root, hc):
    ip("link","add","testbr","type","bridge")
    with contextlib.ExitStack() as cleanup:
        cleanup.callback(ip,"link","del","testbr")
        ip("addr","add","198.18.0.1/24","dev","testbr")
        ip("link","set","testbr","up")
        chief=Host(root,"chief","supervisor_only",2,cleanup)
        worker=Host(root,"worker","executor_only",3,cleanup)
        mirror=Host(root,"mirror","executor_only",4,cleanup)
        keys={}
        for peer in (worker, mirror):
            variable="PAIR_" + peer.name.upper()
            keys[variable]=secrets.token_hex(32)
            chief.peer(peer,variable)
            peer.peer(chief,variable)
        project=install_test_slot(worker,hc)
        fact="The fixture drift was caused by an inverted calibration offset."
        private="stage-two-private-transcript-marker"
        (project/"job.py").write_text(
            "import json,sys,time\nfrom pathlib import Path\n"
            "run=json.loads(sys.argv[1])['run_id']\n"
            "with open('launches','a') as f: print(run,file=f,flush=True)\n"
            f"print({private!r},flush=True)\n"
            "for _ in range(500):\n"
            "    if Path('finish').exists(): break\n"
            "    time.sleep(.05)\n"
            "else: raise SystemExit(1)\n"
            f"print({fact!r},flush=True)\n")
        for node in (chief,worker,mirror):
            cleanup.callback(node.stop)
            node.start(keys)
        until(lambda:any(s["node_id"]=="worker" for s in chief.api("/v1/slots")),"remote slot registered")
        req={"conversation_id":"calibration","objective":"Explain the calibration drift",
             "request_id":"network-host-probe","target_node_id":"worker","target_slot_id":"fixture"}
        rid=chief.api("/v1/runs",req)["run_id"]
        until(lambda:(project/"launches").exists() and rid in (project/"launches").read_text(),"worker executing")
        # An actual link partition hides the mirror from both other network stacks.
        ip("link","set",mirror.link,"down")
        chief.stop()
        (project/"finish").touch()
        until(lambda:worker.api("/v1/runs/"+rid)["observed_state"]=="succeeded","offline worker receipt")
        chief.start(keys)
        lookup="/v1/memories?query="+quote("calibration")
        def learned(peer):
            return next((m for m in peer.api(lookup) if fact in m["content"]),None)
        lesson=until(lambda:learned(chief),"supervisor accepted automatic lesson")
        assert chief.api("/v1/runs",req)["run_id"]==rid
        before=time.monotonic()
        ip("link","set",mirror.link,"up")
        synced=until(lambda:learned(mirror),"formerly partitioned mirror caught up")
        elapsed=round((time.monotonic()-before)*1000)
        assert synced["memory_id"]==lesson["memory_id"] and synced["version"]==lesson["version"]
        assert (project/"launches").read_text().splitlines()==[rid]
        log=chief.api(f"/v1/runs/{rid}/log?query={private}")
        assert any(private in line for line in log["lines"])
        worker_log=list(worker.dir.rglob("transcript.jsonl"))
        assert worker_log and not list(chief.dir.rglob("transcript.jsonl")) and not list(mirror.dir.rglob("transcript.jsonl"))
        chief.stop()
        assert learned(mirror)["memory_id"]==lesson["memory_id"]
        print(f"PASS three independent network stacks, offline execution, same run, S0 owner-only, automatic lesson mirrored after link recovery in {elapsed} ms, offline local lookup")


if __name__=="__main__":
    with tempfile.TemporaryDirectory(prefix="s2-") as root:
        run(Path(root),os.environ["TARLA_TEST_HC"])
