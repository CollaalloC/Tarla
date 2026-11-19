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
from urllib.request import HTTPSHandler, ProxyHandler, build_opener

from acceptance import Node, free_port, until


def control_config(path):
    ca, ca_key = path / "ca.pem", path / "ca.key"
    cert, key = path / "server.pem", path / "server.key"
    csr, extensions = path / "server.csr", path / "certificate.conf"
    extensions.write_text("subjectAltName=IP:127.0.0.1\nextendedKeyUsage=serverAuth\n")
    for cmd in [
        ["req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=Tarla test CA",
         "-addext", "basicConstraints=critical,CA:TRUE", "-keyout", ca_key, "-out", ca],
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
        "prefixes": {"v4": "100.64.0.0/10", "v6": "fd7a:115c:a1e0::/48", "allocation": "sequential"},
        "unix_socket": str(path / "control.sock"), "dns": {"magic_dns": False, "override_local_dns": False, "base_domain": "tarla.test"},
        "policy": {"mode": "file"}, "logtail": {"enabled": False},
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
    exe.parent.mkdir(parents=True)
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


def run(path):
    url, cfg, ca = control_config(path)
    head = [os.environ["TARLA_TEST_HEADSCALE"], "--config", cfg]
    subprocess.run([*head, "configtest"], check=True)
    https = build_opener(ProxyHandler({}), HTTPSHandler(context=ssl.create_default_context(cafile=ca)))
    with contextlib.ExitStack() as cleanup:
        log = cleanup.enter_context((path / "headscale.log").open("w"))
        service = subprocess.Popen([*head, "serve"], stdout=log, stderr=log)
        cleanup.callback(terminate, service)

        def health():
            if service.poll() is not None:
                raise RuntimeError("Headscale failed to start: " + (path / "headscale.log").read_text()[-1600:])
            with https.open(url + "/health", timeout=3) as response:
                return response.status == 200
        until(health, "private Headscale control plane")
        user = json.loads(subprocess.check_output([*head, "-o", "json", "users", "create", "tarla"]))
        user_id = str(user["id"])
        secret = secrets.token_hex(32)
        nodes = [Node(path / "a", "chief", "supervisor_only"), Node(path / "b", "worker", "executor_only")]
        os.environ["SSL_CERT_FILE"] = str(ca)
        addresses = []
        for n in nodes:
            install(n)
            cleanup.callback(n.stop)
            n.start(secret)
            cli = [os.environ["TARLA_TEST_TAILSCALE_CLI"], "--socket=" + str(n.dir / "runtime/ts.sock")]
            preauth = json.loads(subprocess.check_output(
                [*head, "-o", "json", "preauthkeys", "create", "--user", user_id, "--expiration", "10m"]))
            # Pass this single-use enrollment credential in an owner-only file.
            key = path / (n.name + ".authkey")
            key.write_text(preauth["key"])
            key.chmod(0o600)
            subprocess.run([*cli, "up", "--auth-key=file:" + str(key), "--login-server=" + url,
                            "--accept-dns=false", "--accept-routes=false", "--hostname=" + n.name,
                            "--timeout=45s"], timeout=50, check=True)
            key.unlink()
            addr = subprocess.check_output([*cli, "ip", "-4"], text=True).strip()
            addresses.append(addr)
        assert addresses[0] != addresses[1]
        for n in nodes:
            n.stop()
        for n, peer, peer_ip in zip(nodes, nodes[::-1], addresses[::-1]):
            n.cli("pair", peer.name, f"http://{peer_ip}:{peer.port}", "--token-env", "TARLA_PAIR_TEST")
            n.start(secret)
        chief, worker = nodes
        change = {"memory_id": "tailnet", "scope": "global", "content": "two enrolled userspace nodes", "base_version": 0}
        assert chief.api("/v1/memories", change)["status"] == "applied"
        until(lambda: any(row["content"] == change["content"] for row in worker.api("/v1/memories")),
              "authenticated S2 synchronization through the self-hosted tailnet", timeout=60)
        print("PASS two managed Tailscale nodes enrolled with private Headscale, restored identities and replicated S2 through VPN IPs")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="tarla-ts-") as tmp:
        run(Path(tmp))
