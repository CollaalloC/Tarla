"""Install pinned Linux x86_64 network sidecars for real CI compatibility tests."""
import hashlib
import io
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
import zipfile


def fetch(url, expected_digest):
    archive = subprocess.check_output(["curl", "--fail", "--location", "--silent", "--show-error",
                                       "--retry", "2", "--max-time", "90",
                                       "--proto", "=https", "--proto-redir", "=https", url])
    if hashlib.sha256(archive).hexdigest() != expected_digest:
        raise ValueError("Network sidecar release checksum mismatch")
    return io.BytesIO(archive)


def install(root, binary, content, environment, name):
    path = root / binary
    path.write_bytes(content)
    path.chmod(0o700)
    print(f"{name}={path}", file=environment)


def main():
    root = Path(tempfile.mkdtemp(prefix="tarla-test-mesh-"))
    et = fetch("https://github.com/EasyTier/EasyTier/releases/download/v2.7.0/easytier-linux-x86_64-v2.7.0.zip",
               "442238912065537c280869089217303e3d2813e9c5d2a6b14deaf25667065aa2")
    ts = fetch("https://pkgs.tailscale.com/stable/tailscale_1.82.0_amd64.tgz",
               "c4ad89554f2289c6aefedc261148c5c08c6db78354ae7b3726608b8c8910cc2b")
    control = fetch("https://github.com/juanfont/headscale/releases/download/v0.26.1/headscale_0.26.1_linux_amd64",
                    "5012577e6fc5d4234aab7b4be0d6e271ea1a4ec38521a8aa472f80ea1fe81cba")
    with Path(sys.argv[1]).open("a") as environment, zipfile.ZipFile(et) as z:
        install(root, "headscale", control.read(), environment, "TARLA_TEST_HEADSCALE")
        for binary, name in [("easytier-core", "TARLA_TEST_EASYTIER"),
                             ("easytier-cli", "TARLA_TEST_EASYTIER_CLI")]:
            install(root, binary, z.read("easytier-linux-x86_64/" + binary), environment, name)
        with tarfile.open(fileobj=ts, mode="r:gz") as archive:
            for binary, name in [("tailscaled", "TARLA_TEST_TAILSCALED"),
                                 ("tailscale", "TARLA_TEST_TAILSCALE_CLI")]:
                path = "tailscale_1.82.0_amd64/" + binary
                install(root, binary, archive.extractfile(path).read(), environment, name)


if __name__ == "__main__":
    main()
