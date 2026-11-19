"""Run real tarlad peers over separate workspace-owned EasyTier overlays."""
import contextlib
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile

from acceptance import Node, until


def configure(node, peer, local_ip, peer_ip, seeds):
    bin_dir = node.home / "bin"
    executable = bin_dir / "easytier" / "2.7.0" / "easytier-core"
    executable.parent.mkdir(parents=True)
    shutil.copy2(os.environ["TARLA_TEST_EASYTIER"], executable)
    (bin_dir / "toolchain-manifest.json").write_text(json.dumps({"tools": {"easytier": {
        "version": "2.7.0", "file": executable.name,
        "sha256": hashlib.sha256(executable.read_bytes()).hexdigest()}}}))
    cfg = node.dir / "workspace.toml"
    body = (cfg.read_text()
            .replace("enable_easytier = false", "enable_easytier = true")
            .replace('easytier_network_name = ""', 'easytier_network_name = "tarla-mesh-test"')
            .replace('easytier_network_secret_ref = ""',
                     'easytier_network_secret_ref = "env:TARLA_TEST_NETWORK_KEY"'
                     + f'\neasytier_ipv4 = "{local_ip}/24"\neasytier_peers = ' + json.dumps(seeds)))
    body += f'\n[[peers]]\nnode_id = "{peer.name}"\nurl = "http://{peer_ip}:{peer.port}"\n'
    cfg.write_text(body + 'token_env = "TARLA_PAIR_TEST"\n')


def info(node):
    endpoint = json.loads((node.dir / "runtime" / "easytier.json").read_text())
    result = subprocess.run([os.environ["TARLA_TEST_EASYTIER_CLI"], "-p", endpoint["rpc"],
                             "-o", "json", "node", "info"], capture_output=True, timeout=5)
    if result.returncode == 0:
        # Upstream includes private config in this output. Return only network addresses.
        record = json.loads(result.stdout)
        return record.get("ipv4_addr"), record.get("listeners", [])


def run(tmp):
    a = Node(tmp / "a", "chief", "supervisor_only")
    b = Node(tmp / "b", "worker", "executor_only")
    ip_a, ip_b = "10.74.63.1", "10.74.63.2"
    secret = secrets.token_hex(32)
    os.environ["TARLA_TEST_NETWORK_KEY"] = secrets.token_hex(32)
    configure(a, b, ip_a, ip_b, [])
    with contextlib.ExitStack() as cleanup:
        cleanup.callback(a.stop)
        a.start(secret)
        reported, listeners = until(lambda: info(a), "primary EasyTier registered")
        assert reported == ip_a + "/24", "static primary overlay address"
        seed = next(url for url in listeners if url.startswith("tcp://")).replace("0.0.0.0", "127.0.0.1")
        configure(b, a, ip_b, ip_a, [seed])
        cleanup.callback(b.stop)
        b.start(secret)
        reported, _ = until(lambda: info(b), "secondary EasyTier registered")
        assert reported == ip_b + "/24", "static executor overlay address"
        memory = {"memory_id": "mesh", "scope": "global", "content": "live overlay", "base_version": 0}
        assert a.api("/v1/memories", memory)["status"] == "applied"
        until(lambda: any(m["content"] == "live overlay" for m in b.api("/v1/memories")), "replication over overlay")
        identity = b.dir / "runtime/easytier-instance"
        before = identity.read_text()
        b.stop()
        a.api("/v1/memories", {**memory, "content": "offline delta", "base_version": 1})
        b.start(secret)
        assert identity.read_text() == before
        until(lambda: any(m["content"] == "offline delta" for m in b.api("/v1/memories")), "resync after restart")
        print("PASS two authenticated tarlad daemons exchanged memory over real isolated EasyTier processes, including executor restart")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="tarla-real-mesh-") as tmp:
        run(Path(tmp))
