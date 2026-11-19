"""Install a checksum-verified Herdr release for Linux x86_64 integration tests."""
import hashlib
import io
import json
from pathlib import Path
import sys
import tarfile
import tempfile
import urllib.request

VERSION = "0.1.19"
manifest = f"https://releases.herdr.dev/core/{VERSION}/manifest.json"
with urllib.request.urlopen(manifest,timeout=30) as response:
    data=json.load(response)
assert data["version"] == VERSION
def find_build(item):
    if isinstance(item,dict):
        if "linux-x86_64" in item:
            return item["linux-x86_64"]
        for value in item.values():
            if result:=find_build(value):
                return result
build=find_build(data)
assert build and build["url"].startswith("https://")
with urllib.request.urlopen(build["url"],timeout=90) as response:
    archive=response.read()
assert hashlib.sha256(archive).hexdigest()==build["sha256"]
out=Path(tempfile.mkdtemp(prefix="tarla-hc-"))/"herdr-core"
with tarfile.open(fileobj=io.BytesIO(archive),mode="r:gz") as bundle:
    matches=[f for f in bundle.getmembers() if f.isfile() and Path(f.name).name=="herdr-core"]
    assert len(matches)==1
    out.write_bytes(bundle.extractfile(matches[0]).read())
out.chmod(0o700)
if len(sys.argv)>1:
    with open(sys.argv[1],"a") as f:
        print(f"TARLA_TEST_HC={out}",file=f)
        print(f"TARLA_TEST_HERDR={out}",file=f)
else:
    print(out)
