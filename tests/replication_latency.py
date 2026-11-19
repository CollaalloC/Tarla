"""Real paired-daemon S2 visibility benchmark; prints observed latency, never sets a WAN SLA."""
import argparse
import contextlib
import json
import math
import os
from pathlib import Path
import random
import secrets
import socket
import sqlite3
import sys
import tempfile
import threading
import time
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.parse import quote
from urllib.request import Request

from acceptance import HTTP, Model, Node, ThreadingHTTPServer, install_test_slot, until


def ms(start, end):
    return (end - start) / 1e6


def summary(samples):
    ordered = sorted(samples)
    def quantile(q):
        return round(ordered[math.ceil(len(ordered)*q)-1], 2)
    return {"n": len(ordered), "p50_ms": quantile(.5), "p95_ms": quantile(.95),
            "max_ms": quantile(1)}


def visible(node, key, version, interval):
    start = time.perf_counter()
    while time.perf_counter() - start < 15:
        rows = node.api("/v1/memories?query=" + quote(key))
        if any(row["memory_id"] == key and row["version"] == version for row in rows):
            return time.perf_counter_ns()
        time.sleep(interval)
    raise AssertionError(f"memory {key} v{version} still absent")


def mutation(key, version):
    return {"proposal_id": f"{key}-{version}", "memory_id": key, "scope": "global",
            "base_version": version-1, "content": f"{key} revision {version}: the scanner reference distance is 285 mm."}


def assert_hint_denied(target, sender, bearer, term, expected):
    request = Request(target.url+"/peer/v1/memory_changed",
        data=json.dumps({"workspace_id":"work", "term":term, "cursor":1}).encode(),
        headers={"Authorization":"Bearer "+bearer, "x-tarla-peer":sender,
                 "Content-Type":"application/json"})
    try:
        with HTTP.open(request,timeout=5):
            raise AssertionError("unexpectedly accepted unauthorized S2 cursor hint")
    except HTTPError as error:
        assert error.code == expected, error.code


def measure(chief, worker, args, receipt_dir):
    rng = random.Random(args.seed)
    secret = secrets.token_hex(32)
    chief.start(secret)
    worker.start(secret)
    until(lambda: any(n["node_id"] == "worker" and n["online"] for n in chief.api("/v1/nodes")),
          "peer registration")
    term = chief.api("/health")["supervisor_term"]
    assert_hint_denied(worker, chief.name, worker.token, term, 401)
    assert_hint_denied(worker, chief.name, secret, term+1, 409)
    assert_hint_denied(chief, worker.name, secret, term, 403)
    key = "replication-benchmark"
    interval = args.read_interval_ms / 1000
    acknowledged, end_to_end = [], []
    total = args.warmup + args.samples
    for version in range(1, total+1):
        time.sleep(rng.uniform(0, args.jitter_ms/1000))
        start = time.perf_counter_ns()
        result = chief.api("/v1/memories", mutation(key, version))
        ack = time.perf_counter_ns()
        assert result["status"] == "applied"
        end = visible(worker, key, version, interval)
        if version > args.warmup:
            acknowledged.append(ms(ack, end))
            end_to_end.append(ms(start, end))
    output = {"ack_to_observed": summary(acknowledged), "request_to_observed": summary(end_to_end),
              "warmup":args.warmup,"poll_interval_ms":args.read_interval_ms,"jitter_max_ms":args.jitter_ms,"seed":args.seed}

    # An executor accepts a proposal locally, submits it to the supervisor,
    # then receives the newly sequenced version back into its own durable mirror.
    executor_ready, executor_request = [], []
    for version in range(1, total+1):
        time.sleep(rng.uniform(0, args.jitter_ms/1000))
        start = time.perf_counter_ns()
        result = worker.api("/v1/memories", mutation("executor-benchmark", version))
        ack = time.perf_counter_ns()
        assert result["status"] == "queued"
        end = visible(worker, "executor-benchmark", version, interval)
        if version > args.warmup:
            executor_ready.append(ms(ack,end))
            executor_request.append(ms(start,end))
    output["executor_queue_ack_to_local_sequenced_copy"] = summary(executor_ready)
    output["executor_request_to_local_sequenced_copy"] = summary(executor_request)
    output["reverse_hint_route"] = "blocked" if args.pull_only else "reachable"

    chief.stop()
    start = time.perf_counter_ns()
    end = visible(worker, key, total, interval)
    output["offline_read_ms"] = round(ms(start, end),2)
    pending = worker.api("/v1/memories", mutation(key, total+1))
    assert pending["status"] == "queued"
    start = time.perf_counter_ns()
    chief.start(secret)
    visible(chief, key, total+1, interval)
    end = visible(worker, key, total+1, interval)
    output["offline_proposal_recovered_ms_including_supervisor_startup"] = round(ms(start,end),2)

    clock = "CAST((julianday('now')-2440587.5)*86400000 AS INTEGER)"
    # Both services run on the same kernel clock. These two instrumentation
    # tables only exist in the benchmark's disposable workspaces.
    with sqlite3.connect(worker.dir/"state.db") as db:
        db.executescript(f"""CREATE TABLE benchmark_receipts(run_id TEXT PRIMARY KEY, ms INTEGER);
          CREATE TRIGGER benchmark_outbox AFTER INSERT ON outbox_events WHEN NEW.event_type='run_receipt' BEGIN
          INSERT INTO benchmark_receipts VALUES(NEW.run_id,{clock}); END;""")
    with sqlite3.connect(chief.dir/"state.db") as db:
        db.executescript(f"""CREATE TABLE benchmark_messages(conversation TEXT, role TEXT, ms INTEGER);
          CREATE TRIGGER benchmark_message AFTER INSERT ON messages WHEN NEW.conversation_id LIKE 'benchmark-receipt-%' BEGIN
          INSERT INTO benchmark_messages VALUES(NEW.conversation_id,NEW.sender_role,{clock}); END;""")
    receipt_times, reply_times, successful_jobs = [], [], []
    for i in range(args.receipt_samples):
        conversation = f"benchmark-receipt-{i}"
        mission = {"conversation_id":conversation,"goal":"report completion","acceptance":["report the outcome"]}
        if not args.successful_receipts:
            # Actual executor preflight -> terminal failure receipt before any PTY starts.
            mission["required_fact_keys"] = ["deliberately-absent-fact"]
        run = chief.api("/v1/runs",mission)["run_id"]
        cutoff = time.monotonic()+30
        while time.monotonic()<cutoff:
            messages = chief.api(f"/v1/conversations/{conversation}/messages")
            if any(m["sender_role"]=="assistant" for m in messages):
                observed = time.perf_counter_ns()
                break
            time.sleep(interval)
        else:
            raise AssertionError(f"completion event absent for {run}")
        state = chief.api("/v1/runs/"+run)
        expected = "succeeded" if args.successful_receipts else "failed"
        assert state["observed_state"] == expected, state
        if args.successful_receipts:
            successful_jobs.append(ms(int((receipt_dir/run).read_text()),observed))
        else:
            assert "environment fact" in state["receipt_summary"],state
        with sqlite3.connect(worker.dir/"state.db") as db:
            (start,) = db.execute("SELECT ms FROM benchmark_receipts WHERE run_id=?",(run,)).fetchone()
        with sqlite3.connect(chief.dir/"state.db") as db:
            rows = dict(db.execute("SELECT role,ms FROM benchmark_messages WHERE conversation=?",(conversation,)))
        receipt_times.append(rows["run_receipt"]-start)
        reply_times.append(rows["assistant"]-start)
    output["receipt_scenario"] = "successful_private_pty" if args.successful_receipts else "required_fact_missing"
    output["executor_outbox_to_supervisor_archived_receipt"] = summary(receipt_times)
    output["executor_outbox_to_supervisor_archived_assistant_report"] = summary(reply_times)
    if successful_jobs:
        output["worker_job_marker_to_assistant_report_observed"] = summary(successful_jobs)
    print(json.dumps(output,indent=2),flush=True)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--samples",type=int,default=50)
    parser.add_argument("--warmup",type=int,default=5)
    parser.add_argument("--jitter-ms",type=float,default=350)
    parser.add_argument("--read-interval-ms",type=float,default=3)
    parser.add_argument("--pull-only",action="store_true",help="Block the supervisor-to-executor hint route, leaving normal authenticated pulls available")
    parser.add_argument("--seed",type=int,default=211)
    parser.add_argument("--successful-receipts",action="store_true",help="Use a successful private PTY job instead of a failed preflight; requires TARLA_TEST_HC")
    parser.add_argument("--receipt-samples",type=int,default=5)
    args=parser.parse_args()
    if min(args.samples,args.receipt_samples)<1 or args.warmup<0 or args.read_interval_ms<=0 or args.jitter_ms<0:
        parser.error("sample counts and read interval must be positive; warmup and jitter must be nonnegative")
    with tempfile.TemporaryDirectory(prefix="tarla-latency-") as tmp, contextlib.ExitStack() as cleanup:
        model=ThreadingHTTPServer(("127.0.0.1",0),Model)
        threading.Thread(target=model.serve_forever,daemon=True).start()
        cleanup.callback(model.server_close)
        cleanup.callback(model.shutdown)
        chief=Node(Path(tmp)/"chief","chief","supervisor_only")
        worker=Node(Path(tmp)/"worker","worker","executor_only")
        reverse_peer = worker
        if args.pull_only:
            # Hold an unlistened loopback address so no process can accept hints.
            sink = cleanup.enter_context(socket.socket())
            sink.bind(("127.0.0.1",0))
            reverse_peer = SimpleNamespace(name=worker.name,url=f"http://127.0.0.1:{sink.getsockname()[1]}")
        for node,peer in [(chief,reverse_peer),(worker,chief)]:
            node.configure(peer,model.server_port)
            cleanup.callback(node.stop)
        if args.successful_receipts:
            hc=os.getenv("TARLA_TEST_HC")
            if not hc: parser.error("--successful-receipts requires TARLA_TEST_HC")
            receipt_dir=install_test_slot(worker,hc)
            (receipt_dir/"job.py").write_text(
                "import json,sys,time\nfrom pathlib import Path\n"
                "rid=json.loads(sys.argv[1])['run_id']\nprint('completed',flush=True)\n"
                "Path(rid).write_text(str(time.perf_counter_ns()))\n")
        else:
            receipt_dir=worker.home/"bench-workdir"
            receipt_dir.mkdir()
            with (worker.dir/"workspace.toml").open("a") as cfg:
                cfg.write('\n[[slots]]\nslot_id="fixture"\ndriver="local"\nmodel_family="fixture"\n'
                    +"argv="+json.dumps([sys.executable,"--version"])+"\n"
                    +"workdir="+json.dumps(str(receipt_dir))+"\n")
        measure(chief,worker,args,receipt_dir)


if __name__=="__main__":
    main()
