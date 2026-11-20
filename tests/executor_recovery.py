"""Exercise a single local executor. Set TARLA_TEST_HC to include real PTY restart."""
import contextlib
import fcntl
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from urllib.error import HTTPError

from acceptance import BIN, Node, install_test_slot, until


def add_slot(node, name, work, argv):
    with (node.dir / "workspace.toml").open("a") as config:
        config.write(
            f'\n[[slots]]\nslot_id={json.dumps(name)}\ndriver="local"\n'
            f'model_family="fixture"\nworkdir={json.dumps(str(work))}\nargv={json.dumps(argv)}\n'
        )


def dispatch(node, slot, goal):
    response = node.api("/v1/runs", {"conversation_id": "recovery", "slot_id": slot, "goal": goal})
    return response["run_id"]


def await_state(node, run, state):
    return until(
        lambda: row if (row := node.api(f"/v1/runs/{run}"))["observed_state"] == state else None,
        f"{run} to become {state}",
    )


def reject_abandon(node, run, payload):
    try:
        node.api(f"/v1/runs/{run}/abandon", payload)
    except HTTPError as err:
        assert err.code in (400, 409, 422), (err.code, err.read())
        return
    raise AssertionError(f"accepted unexpected abandonment for {run}")


@contextlib.contextmanager
def occupied_profile(node, slot):
    directory = node.home / "slots" / slot
    directory.mkdir(mode=0o700, exist_ok=True)
    with (directory / ".tarla-slot.lock").open("a+b") as handle:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def backlog_and_cancel(root):
    node = Node(root, "chief", "supervisor_and_executor")
    for slot in ("busy", "free"):
        add_slot(node, slot, root, [sys.executable, "-c", "pass"])
    with occupied_profile(node, "busy"), contextlib.ExitStack() as cleanup:
        cleanup.callback(node.stop)
        node.start("")
        backlog = [dispatch(node, "busy", f"queued {n}") for n in range(130)]
        cancelled = backlog[-1]
        node.api(f"/v1/runs/{cancelled}/cancel", {})
        independent = dispatch(node, "free", "independent progress")

        # The busy slot is locked throughout. There is intentionally no Herdr in
        # this workspace, so a failed startup proves the free job reached execution.
        await_state(node, cancelled, "cancelled")
        result = await_state(node, independent, "failed")
        assert "herdr" in result["receipt_summary"].lower(), result
        with sqlite3.connect(node.dir / "state.db") as db:
            queued, = db.execute(
                "SELECT count(*) FROM service_claims WHERE phase='queued'"
            ).fetchone()
        assert queued == len(backlog) - 1, queued
        print("PASS cancelled and independent jobs progress beyond 128 queued claims on a busy profile")


def one_shot_launch(root):
    node = Node(root, "chief", "supervisor_and_executor")
    add_slot(node, "account", root, [sys.executable, "-c", "pass"])
    db_path = node.dir / "state.db"
    with occupied_profile(node, "account"), contextlib.ExitStack() as cleanup:
        cleanup.callback(node.stop)
        node.start("")
        current = dispatch(node, "account", "one admission")
        cancelled = dispatch(node, "account", "late launch")
        node.stop()
        # The command holder can arrive after the executor dies. Reproduce the
        # persisted state at that boundary without requiring a Unix socket.
        with sqlite3.connect(db_path) as db:
            for run in (current, cancelled):
                db.execute("UPDATE service_claims SET phase='launching' WHERE run_id=?", (run,))
                db.execute("INSERT INTO service_launch_permits VALUES(?,'issued')", (run,))
            db.execute("UPDATE runs SET desired_state='cancelled' WHERE run_id=?", (cancelled,))

    log = root / "executions"
    release = root / "release"
    program = (
        "import pathlib, sys, time\n"
        "with open(sys.argv[1], 'a') as log: print(sys.argv[2], file=log)\n"
        "while not pathlib.Path(sys.argv[3]).exists(): time.sleep(.05)\n"
    )
    def command(run):
        return [BIN / "tarlad", "--executor-launch-helper", root, db_path, run,
                sys.executable, "-c", program, log, run, release]

    with contextlib.ExitStack() as cleanup:
        children = [cleanup.enter_context(subprocess.Popen(command(run)))
                    for run in (current, current, cancelled)]
        cleanup.callback(release.touch)
        until(lambda: log.exists() and log.read_text().strip(), "admitted process")
        try:
            with occupied_profile(node, "account"):
                raise AssertionError("admitted command released its account before exit")
        except BlockingIOError:
            pass
        release.touch()
        assert sorted(p.wait(timeout=6) for p in children[:2]) == [0, 1]
        assert children[2].wait(timeout=6) == 1
    assert log.read_text().splitlines() == [current]
    print("PASS duplicate and late-cancelled holder commands are fenced while the executor is offline")


def recover_holder(root, hc):
    node = Node(root, "chief", "supervisor_and_executor")
    work = install_test_slot(node, hc)
    for slot in ("ambiguous", "gap"):
        add_slot(node, slot, work, [sys.executable, "-u", "job.py"])
    (work / "job.py").write_text(
        "import json, pathlib, sys, time\n"
        "packet = json.loads(sys.argv[1])\n"
        "with open('launches', 'a') as log: print(packet['run_id'], file=log)\n"
        "if packet['objective'] == 'hold':\n"
        "    deadline = time.monotonic() + 90\n"
        "    while not pathlib.Path('release').exists():\n"
        "        if time.monotonic() >= deadline: raise TimeoutError('fixture cleanup')\n"
        "        time.sleep(.05)\n"
    )
    marker = work / "launches"
    release = work / "release"
    with contextlib.ExitStack() as cleanup:
        runtime = node.dir / "runtime"
        cleanup.callback(
            subprocess.run, [hc, "server", "stop"], timeout=10, capture_output=True,
            env={**os.environ, "HC_SESSION": "tarla", "HC_SOCKET": str(runtime / "hc.sock"),
                 "XDG_CONFIG_HOME": str(runtime / "hc-config"),
                 "XDG_STATE_HOME": str(runtime / "hc-state"),
                 "XDG_RUNTIME_DIR": str(runtime / "hc-runtime")},
        )
        cleanup.callback(release.touch)
        cleanup.callback(node.stop)
        with occupied_profile(node, "ambiguous"), occupied_profile(node, "gap"):
            node.start("")
            running = dispatch(node, "fixture", "hold")
            until(lambda: marker.exists() and running in marker.read_text(), "first native process launch")
            await_state(node, running, "running")
            queued = dispatch(node, "fixture", "next")
            legacy = dispatch(node, "ambiguous", "unknown legacy launch")
            gap = dispatch(node, "gap", "cancelled before admission")
            node.stop()
            try:
                with occupied_profile(node, "fixture"):
                    raise AssertionError("live holder released its account while the daemon was offline")
            except BlockingIOError:
                pass

            # Recreate a persisted pre-upgrade crash between committing launching
            # and recording the Herdr terminal. No permit or holder exists for it.
            # The queued run also sorts before the active run by normal timestamps.
            with sqlite3.connect(node.dir / "state.db") as db:
                db.execute("UPDATE service_claims SET phase='launching' WHERE run_id=?", (legacy,))
                db.execute("UPDATE service_claims SET updated_at=0 WHERE run_id=?", (queued,))
                db.execute("UPDATE runs SET desired_state='cancelled' WHERE run_id=?", (gap,))
                db.execute("UPDATE service_claims SET phase='launching' WHERE run_id=?", (gap,))
                db.execute("INSERT INTO service_launch_permits(run_id,state) VALUES(?,'issued')", (gap,))

        node.start("")
        await_state(node, legacy, "unconfirmed")
        await_state(node, gap, "cancelled")
        assert marker.read_text().splitlines() == [running]
        reason = "Verified the original process is gone on this host."
        confirmation = {"confirmed_stopped": True, "reason": reason}
        reject_abandon(node, running, confirmation)
        reject_abandon(node, queued, confirmation)
        successor = dispatch(node, "ambiguous", "first turn after manual resolution")

        # Cancel requests cannot convert unknown prior execution into a confirmed
        # cancellation. A second restart must preserve the fence.
        node.api(f"/v1/runs/{legacy}/cancel", {})
        node.stop()
        node.start("")
        release.touch()
        await_state(node, running, "succeeded")
        await_state(node, queued, "succeeded")
        row = node.api(f"/v1/runs/{legacy}")
        assert (row["desired_state"], row["observed_state"]) == ("cancelled", "unconfirmed"), row
        assert marker.read_text().splitlines() == [running, queued]
        reject_abandon(node, legacy, {"reason": reason})
        answer = json.loads(node.cli("abandon", legacy, "--confirm-stopped", "--reason", reason))
        assert (answer["run_id"], answer["observed_state"]) == (legacy, "cancelled"), answer
        await_state(node, successor, "succeeded")
        node.stop()
        node.start("")
        settled = node.api(f"/v1/runs/{legacy}")
        assert settled["observed_state"] == "cancelled" and reason in str(settled), settled
        assert marker.read_text().splitlines() == [running, queued, successor]
        print("PASS reconnected live holder, exclusive slot and no replay of ambiguous launch after two restarts")
        print("PASS explicit abandonment persists the operator reason and releases only the affected slot")


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="tr-") as folder:
        root = Path(folder)
        backlog_and_cancel(root / "backlog")
        one_shot_launch(root / "permit")
        if hc := os.environ.get("TARLA_TEST_HC"):
            recover_holder(root / "restart", hc)
