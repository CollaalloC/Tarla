"""Validate the signed bundle layout and the bundled daemon's actual discovery API."""
import hashlib
import hmac
import json
import os
from pathlib import Path
import plistlib
import secrets
import subprocess
import sys
import tempfile
import time
import urllib.request

app = Path(sys.argv[1]).resolve()
contents = app / "Contents"
label = "dev.tarla.client.local.tarlad"
info = plistlib.loads((contents / "Info.plist").read_bytes())
assert info["CFBundleIdentifier"] == "dev.tarla.client.local"
assert info["LSMinimumSystemVersion"] == "13.0"
plist = plistlib.loads((contents / "Library/LaunchAgents" / (label + ".plist")).read_bytes())
assert plist["Label"] == label and info["CFBundleIdentifier"] in plist["AssociatedBundleIdentifiers"]
daemon = app / plist["BundleProgram"]
cli = contents / "Helpers/tarla"
for path in (app,cli,daemon):
    subprocess.run(["codesign","--verify","--strict","--verbose=2",str(path)],check=True)
def architectures(path):
    return set(subprocess.check_output(["lipo", "-archs", str(path)]).split())

app_arch = architectures(contents / "MacOS" / info["CFBundleExecutable"])
assert architectures(daemon) == app_arch and architectures(cli) == app_arch
ent = subprocess.run(["codesign","-d","--entitlements","-",str(app)],capture_output=True,check=True).stdout
if ent.strip():
    assert not plistlib.loads(ent).get("com.apple.security.app-sandbox",False)

with tempfile.TemporaryDirectory() as d:
    env={**os.environ,"HOME":d}
    for name in ("TARLA_HOME","TARLA_WORKSPACE","TARLA_LISTEN"):
        env.pop(name,None)
    subprocess.run([str(cli),"--workspace","desktop-local","init","--node","mac-desktop"],env=env,check=True)
    runtime=Path(d)/".tarla/workspaces/desktop-local/runtime"
    token=(runtime/"api.token").read_text().strip()
    p=subprocess.Popen([str(daemon),*plist["ProgramArguments"][1:]],env=env)
    try:
        deadline=time.monotonic()+15
        while not (runtime/"desktop-endpoint.json").is_file():
            if p.poll() is not None or time.monotonic()>deadline: raise RuntimeError("daemon startup failed")
            time.sleep(.1)
        port=json.loads((runtime/"desktop-endpoint.json").read_text())["port"]
        base=f"http://127.0.0.1:{port}"
        nonce=secrets.token_hex(32)
        with urllib.request.urlopen(base+"/local/v1/proof/"+nonce,timeout=5) as response:
            mac=json.load(response)["proof"]
        assert hmac.compare_digest(mac,hmac.new(token.encode(),("tarla-desktop-v1:"+nonce).encode(),hashlib.sha256).hexdigest())
        req=urllib.request.Request(base+"/v1/conversations",headers={"Authorization":"Bearer "+token})
        with urllib.request.urlopen(req,timeout=5) as response:
            assert isinstance(json.load(response),list)
    finally:
        p.terminate()
        p.wait(timeout=5)
