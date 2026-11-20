"""HTTP probes through the published workspace SOCKS5 endpoint, without host-route fallback."""
import http.client
import ipaddress
import json
import os
from pathlib import Path
import signal
import socket
import struct
from time import monotonic

from acceptance import until


def record(node, backend):
    return json.loads((node.dir / "runtime" / (backend + ".json")).read_text())


def receive(sock, size):
    data = bytearray()
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise ConnectionError("proxy stream closed")
        data.extend(chunk)
    return bytes(data)


def request(node, backend, ip, port, token, workspace=None, timeout=4):
    addr, proxy_port = record(node, backend)["socks5"].rsplit(":", 1)
    assert addr == "127.0.0.1"
    with socket.create_connection((addr, int(proxy_port)), timeout) as sock:
        sock.settimeout(timeout)
        sock.sendall(b"\x05\x01\x00")
        if receive(sock, 2) != b"\x05\x00":
            raise ConnectionError("proxy did not accept SOCKS5")
        sock.sendall(b"\x05\x01\x00\x01" + ipaddress.IPv4Address(ip).packed + struct.pack("!H", port))
        version, status, _, addr_type = receive(sock, 4)
        if version != 5 or status:
            raise ConnectionError("workspace proxy did not reach the destination")
        size = {1: 4, 4: 16}.get(addr_type)
        if size is None:
            if addr_type != 3:
                raise ConnectionError("bad SOCKS5 reply")
            size = receive(sock, 1)[0]
        receive(sock, size + 2)
        client = http.client.HTTPConnection(ip, port, timeout=timeout)
        client.sock = sock
        headers = {"Authorization": "Bearer " + token}
        if workspace is not None:
            headers["X-Tarla-Workspace"] = workspace
        try:
            client.request("GET", "/v1/memories", headers=headers)
            result = client.getresponse()
            return result.status, result.read()
        finally:
            client.close()


def unreachable(src, backend, ip, port):
    try:
        status, _ = request(src, backend, ip, port, "fixture", timeout=2)
    except (OSError, http.client.RemoteDisconnected):
        return
    raise AssertionError(f"foreign workspace unexpectedly answered HTTP {status} on {ip}:{port}")


def recover_child(node, backend, binary):
    old = record(node, backend)
    pid = old["pid"]
    assert Path(f"/proc/{pid}/exe").resolve() == binary.resolve(), "expected fixture-owned transport process"
    started = monotonic()
    os.kill(pid, signal.SIGKILL)
    fresh = until(lambda: (r if (r := record(node, backend)).get("pid") not in (None, pid)
                           and r.get("status") == "ready" else None), "managed transport restart", timeout=40)
    assert fresh["socks5"] == old["socks5"] and fresh["owner"] == old["owner"]
    assert node.proc.poll() is None
    return started
