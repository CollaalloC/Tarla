"""Use two stock tailscaled nodes, private tarlad APIs and a disposable Headscale CA."""
import contextlib
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import ssl
import subprocess
import tempfile
from time import monotonic
from unittest.mock import patch
from urllib.error import URLError
from urllib.request import HTTPSHandler, ProxyHandler, build_opener

from acceptance import Node, free_port, until
from overlay import record, recover_child, request, unreachable


def control_config(path, prefix, api_ports):
    path.mkdir(mode=0o700)
    policy = path / "policy.json"
    policy.write_text(json.dumps({"acls": [{"action": "accept", "src": ["*"],
                    "dst": [f"*:{port}" for port in api_ports], "proto": "tcp"}]}))
    ca, ca_key = path / "ca.pem", path / "ca.key"
    cert, key = path / "server.pem", path / "server.key"
    csr, extensions = path / "server.csr", path / "certificate.conf"
    extensions.write_text("subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n")
    for cmd in [
        ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=Tarla test CA",
         "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,keyCertSign,cRLSign",
         "-keyout", ca_key, "-out", ca],
        ["req", "-new", "-newkey", "rsa:2048", "-nodes", "-subj", "/CN=127.0.0.1", "-keyout", key, "-out", csr],
        ["x509", "-req", "-in", csr, "-CA", ca, "-CAkey", ca_key, "-CAcreateserial", "-days", "1",
         "-extfile", extensions, "-out", cert],
    ]:
        subprocess.run(["openssl", *cmd], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    url = "https://127.0.0.1:" + str(free_port())
    data = {
        "server_url": url, "listen_addr": url.removeprefix("https://"),
        "metrics_listen_addr": f"127.0.0.1:{free_port()}", "grpc_listen_addr": f"127.0.0.1:{free_port()}",
        "tls_cert_path": str(cert), "tls_key_path": str(key), "disable_check_updates": True,
        "noise": {"private_key_path": str(path / "noise.key")},
        "database": {"type": "sqlite", "sqlite": {"path": str(path / "control.db")}},
        "prefixes": {"v4": prefix, "v6": "fd7a:115c:a1e0::/48", "allocation": "sequential"},
        "unix_socket": str(path / "control.sock"), "dns": {"magic_dns": False, "override_local_dns": False, "base_domain": "tarla.test"},
        "policy": {"mode": "file", "path": str(policy)}, "logtail": {"enabled": False},
        "derp": {"server": {"enabled": True, "region_id": 999, "region_code": "tarla",
                            "region_name": "Tarla local relay", "private_key_path": str(path / "relay.key"),
                            "stun_listen_addr": f"127.0.0.1:{free_port()}", "ipv4": "127.0.0.1",
                            "automatically_add_embedded_derp_region": True},
                 "urls": [], "paths": [], "auto_update_enabled": False},
    }
    cfg = path / "config.yaml"
    cfg.write_text(json.dumps(data))
    return url, cfg, ca


def install(node):
    exe = node.home / "bin/tailscale/1.82.0/tailscaled"
    exe.parent.mkdir(parents=True, exist_ok=True)
    if not exe.exists():
        shutil.copy2(os.environ["TARLA_TEST_TAILSCALED"], exe)
    (node.home / "bin/toolchain-manifest.json").write_text(json.dumps({"tools": {"tailscale": {
        "version": "1.82.0", "file": "tailscaled", "sha256": hashlib.sha256(exe.read_bytes()).hexdigest()}}}))
    cfg = node.dir / "workspace.toml"
    cfg.write_text(cfg.read_text().replace("enable_tailscale = false", "enable_tailscale = true"))


def terminate(proc):
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def tailnet(root, space, prefix, cleanup):
    path = root / ("control-" + space)
    nodes = [Node(root / "a", "chief", "supervisor_only", workspace=space),
             Node(root / "b", "worker", "executor_only", workspace=space)]
    url, cfg, ca = control_config(path, prefix, [n.port for n in nodes])
    head = [os.environ["TARLA_TEST_HEADSCALE"], "--config", cfg]
    subprocess.run([*head, "configtest"], check=True)
    https = build_opener(ProxyHandler({}), HTTPSHandler(context=ssl.create_default_context(cafile=ca)))
    log = cleanup.enter_context((path / "headscale.log").open("w"))
    service = subprocess.Popen([*head, "serve"], stdout=log, stderr=log)
    cleanup.callback(terminate, service)

    def health():
        if service.poll() is not None:
            raise RuntimeError("Headscale failed to start: " + (path / "headscale.log").read_text()[-1600:])
        try:
            with https.open(url + "/health", timeout=3) as response:
                return response.status == 200
        except URLError:
            return False

    until(health, "private Headscale control plane " + space)
    user = json.loads(subprocess.check_output([*head, "-o", "json", "users", "create", "tarla"]))
    secret = secrets.token_hex(32)
    addresses = []
    with patch.dict(os.environ, {"SSL_CERT_FILE": str(ca)}):
        for n in nodes:
            install(n)
            cleanup.callback(n.stop)
            n.start(secret)
            cli = [os.environ["TARLA_TEST_TAILSCALE_CLI"], "--socket=" + str(n.dir / "runtime/ts.sock")]
            preauth = json.loads(subprocess.check_output(
                [*head, "-o", "json", "preauthkeys", "create", "--user", str(user["id"]), "--expiration", "10m"]))
            key = path / (n.name + ".authkey")
            with open(key, "x", opener=lambda name, flags: os.open(name, flags, 0o600)) as f:
                f.write(preauth["key"])
            try:
                subprocess.run([*cli, "up", "--auth-key=file:" + str(key), "--login-server=" + url,
                                "--accept-dns=false", "--accept-routes=false", "--hostname=" + n.name,
                                "--timeout=45s"], timeout=50, check=True)
            finally:
                key.unlink()
            addresses.append(subprocess.check_output([*cli, "ip", "-4"], text=True).strip())
        assert addresses[0] != addresses[1]
        for n in nodes:
            n.stop()
        for n, peer, peer_ip in zip(nodes, nodes[::-1], addresses[::-1]):
            n.cli("pair", peer.name, f"http://{peer_ip}:{peer.port}", "--token-env", "TARLA_PAIR_TEST")
            n.start(secret)
    chief, worker = nodes
    change = {"memory_id": "tailnet", "scope": "global", "content": space + " enrolled", "base_version": 0}
    assert chief.api("/v1/memories", change)["status"] == "applied"
    until(lambda: any(row["content"] == change["content"] for row in worker.api("/v1/memories")),
          "authenticated S2 through the " + space + " tailnet", timeout=60)
    return chief, worker, addresses[0]


def run(path):
    with contextlib.ExitStack() as cleanup:
        a, b, ip_a = tailnet(path, "work", "100.70.0.0/24", cleanup)
        c, d, ip_c = tailnet(path, "life", "100.75.0.0/24", cleanup)
        assert len({record(n, "tailscale")["socks5"] for n in (a,b,c,d)}) == 4
        assert request(b, "tailscale", ip_a, a.port, a.token, "work")[0] == 200
        assert request(b, "tailscale", ip_a, a.port, c.token)[0] == 401
        assert request(d, "tailscale", ip_c, c.port, c.token, "work")[0] == 409
        unreachable(d, "tailscale", ip_a, a.port)
        unreachable(b, "tailscale", ip_c, c.port)
        unreachable(b, "tailscale", ip_a, c.port)  # work's policy excludes life's co-hosted API
        started = recover_child(b, "tailscale", b.home / "bin/tailscale/1.82.0/tailscaled")
        a.api("/v1/memories", {"memory_id":"tailnet", "scope":"global", "base_version":1, "content":"resumed"})
        until(lambda: any(row["content"] == "resumed" for row in b.api("/v1/memories")),
              "reconnect without a new enrollment or tarlad restart", timeout=75)
        assert [row["content"] for row in d.api("/v1/memories")] == ["life enrolled"]
        print(f"PASS four userspace daemons on two independent Headscale authorities; authenticated traffic, ACL isolation, managed child recovery {monotonic()-started:.2f}s")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="tarla-ts-") as tmp:
        run(Path(tmp))
