"""Provision pinned upstream Herdr for Linux x86_64 interoperability tests."""
import hashlib
import io
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile

# Publish-time SHA-256 from releases.herdr.dev/core/0.1.19/manifest.json
URL = "https://releases.herdr.dev/core/0.1.19/herdr-core-linux-x86_64.tar.gz"
ARCHIVE_SHA256 = "4adcfb423d52f24c5cc4a1e2cd2fcad1f62d28bb77876ac472c29082a3eaa424"


def main():
    archive = subprocess.check_output(["curl", "--fail", "--location", "--silent", "--show-error",
                                       "--retry", "2", "--max-time", "90",
                                       "--proto", "=https", "--proto-redir", "=https", URL])
    if hashlib.sha256(archive).hexdigest() != ARCHIVE_SHA256:
        raise ValueError("Upstream herdr-core archive checksum mismatch")
    out = Path(tempfile.mkdtemp(prefix="tarla-hc-")) / "herdr-core"
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:gz") as bundle:
        members = [f for f in bundle if f.isfile() and Path(f.name).name == "herdr-core"]
        if len(members) != 1:
            raise ValueError("Unexpected upstream herdr-core archive")
        out.write_bytes(bundle.extractfile(members[0]).read())
    out.chmod(0o700)
    if len(sys.argv) > 1:
        with open(sys.argv[1], "a") as environment:
            print(f"TARLA_TEST_HC={out}", file=environment)
            print(f"TARLA_TEST_HERDR={out}", file=environment)
    else:
        print(out)


if __name__ == "__main__":
    main()
