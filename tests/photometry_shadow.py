#!/usr/bin/env python3
"""BOOM-owned disposable shadow CI; stdlib only, no companion checkout needed.

Requires dedicated empty Redis on a nondefault port, disposable local MongoDB,
built native examples and CH pointing at a verified ClickHouse executable.
The native drivers reject production endpoints and non-isolated databases.
"""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import argparse
import collections
import hashlib
import json
import math
import os
import platform
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parents[1]
TARGET = Path(os.environ.get("CARGO_TARGET_DIR", str(ROOT / "target"))).resolve()
ARTIFACTS = Path(os.environ.get("DEMO_ARTIFACTS", "/tmp/boom-native-shadow-ci")).resolve()
CH = os.environ.get("CH", str(Path.home() / "clickhouse"))

@contextmanager
def fault_gate(upstream_port):
    unavailable = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *_args):
            pass

        def respond(self, status, body):
            self.send_response(status)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_POST(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            if self.path == "/__boom_replay_fault":
                if body == b"unavailable":
                    unavailable.set()
                elif body == b"healthy":
                    unavailable.clear()
                else:
                    self.respond(400, b"invalid replay control")
                    return
                self.respond(200, b"boom-replay-fault-accepted")
                return
            if unavailable.is_set():
                self.respond(503, b"intentional replay sink outage")
                return
            headers = {key: self.headers[key] for key in ["Content-Type", "Authorization"] if key in self.headers}
            request = urllib.request.Request(f"http://127.0.0.1:{upstream_port}{self.path}", data=body, headers=headers)
            try:
                with urllib.request.urlopen(request, timeout=35) as response:
                    self.respond(response.status, response.read())
            except urllib.error.HTTPError as error:
                self.respond(error.code, error.read())
            except (urllib.error.URLError, TimeoutError) as error:
                self.respond(502, str(error).encode())

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server.server_port
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def reference(history, cadence):
    if not 1 <= cadence <= 31536000:
        raise ValueError("cadence must be 1..31536000 seconds")
    bins = collections.defaultdict(lambda: {"n_records": 0, "weights": [], "weighted_flux": []})
    for field, kind in [("prv_candidates", "detection"), ("prv_nondetections", "upper_limit"), ("fp_hists", "forced")]:
        for point in history.get(field, []):
            seconds = int((point["jd"] - 2440587.5) * 86400)
            key = (seconds // cadence * cadence, point["band"], kind, point["programid"])
            item = bins[key]
            item["n_records"] += 1
            flux, error = point.get("psfFlux"), point.get("psfFluxErr")
            if kind != "upper_limit" and flux is not None and error is not None:
                if not math.isfinite(flux) or not math.isfinite(error) or error <= 0:
                    raise ValueError("invalid source measurement")
                weight = 1 / error**2
                item["weights"].append(weight)
                item["weighted_flux"].append(flux * weight)
    result = []
    for (bucket, band, kind, program), item in sorted(bins.items()):
        weight = math.fsum(item["weights"])
        result.append({"bucket_start_unix": bucket, "band": band, "kind": kind, "programid": program,
                       "n_records": item["n_records"], "n_measured": len(item["weights"]),
                       "flux": math.fsum(item["weighted_flux"]) / weight if weight else None,
                       "flux_err": 1 / math.sqrt(weight) if weight else None,
                       "revision": history["version"]})
    return result


def compare(expected, actual):
    def key(row):
        return row["bucket_start_unix"], row["band"], row["kind"], row["programid"]
    expected = {key(row): row for row in expected}
    actual = {key(row): row for row in actual}
    assert expected.keys() == actual.keys(), "bin identity mismatch"
    for identity, row in expected.items():
        for field, value in row.items():
            observed = actual[identity][field]
            if field in ["flux", "flux_err"] and value is not None:
                assert observed is not None and math.isfinite(observed) and math.isclose(value, observed, rel_tol=1e-10, abs_tol=1e-12), (identity, field, value, observed)
            else:
                assert value == observed, (identity, field, value, observed)



def redis_command(port, command):
    assert port != 6379, "exclusive nondefault Redis port required"
    raw = f"*1\r\n${len(command)}\r\n{command}\r\n".encode()
    with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
        sock.sendall(raw)
        return sock.recv(1024).decode().strip()


def configuration(port, database):
    mongo = {"host": "127.0.0.1", "port": int(os.environ.get("REPLAY_MONGO_PORT", "37117")),
             "name": database, "username": "shadow", "password": "shadow-test-only", "max_pool_size": 20,
             "replica_set": None, "srv": False}
    return {"database": mongo, "cutouts_storage": {"type": "mongo", **mongo},
            "api": {"domain": "localhost", "auth": {"secret_key": "shadow-test-only", "token_expiration": 3600,
                "admin_username": "shadow", "admin_password": "shadow-test-only", "admin_email": "shadow@example.invalid"}},
            "kafka": {"consumer": {}, "producer": {"server": "unused.invalid:9092"}},
            "redis": {"host": "127.0.0.1", "port": int(os.environ.get("REPLAY_REDIS_PORT", "37179"))},
            "crossmatch": {}, "workers": {"ztf": {"command_interval": 100, "alert": {"n_workers": 1},
                "enrichment": {"n_workers": 1}, "filter": {"n_workers": 1, "refresh_interval_minutes": 15, "max_match_rate": 5}}},
            "photometry_shadow": {"url": f"http://127.0.0.1:{port}", "database": database, "username": "default",
                "password": "", "poll_seconds": 1, "drain_rows": 64, "concurrent_deliveries": 4}}


def run_driver(example, args, cwd, timeout=900):
    env = {k: v for k, v in os.environ.items() if not k.startswith("BOOM_")}
    env["OTEL_SDK_DISABLED"] = "true"
    with (ARTIFACTS / f"{example}.stdout.log").open("w") as stdout, (ARTIFACTS / f"{example}.stderr.log").open("w") as stderr:
        result = subprocess.run([str(TARGET / "debug/examples" / example), *map(str, args)], cwd=cwd,
                                env=env, stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr, timeout=timeout)
    assert result.returncode == 0, (ARTIFACTS / f"{example}.stderr.log").read_text()[-6000:]


def exercise(directory, port, args):
    base = "boom_shadow_replay_" + directory.name.replace("-", "_")
    def create_database(name):
        with urllib.request.urlopen(urllib.request.Request(f"http://127.0.0.1:{port}", data=f"CREATE DATABASE `{name}`".encode()), timeout=10) as response:
            assert response.status == 200
    raw = configuration(port, base)
    redis_port = raw["redis"]["port"]
    assert redis_command(redis_port, "DBSIZE") == ":0", "dedicated Redis must initially be empty"
    create_database(base)
    config = directory / "config.json"
    config.write_text(json.dumps(raw))
    worker = directory / "worker"
    (worker / "data").mkdir(parents=True)
    (worker / "data/models").symlink_to(ROOT / "data/models", target_is_directory=True)
    provenance = {"source_revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=ROOT, text=True).strip(),
                  "platform": platform.platform(), "cpu_count": os.cpu_count(),
                  "models": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((ROOT / "data/models").glob("*.onnx"))},
                  "clickhouse_version": subprocess.check_output([CH,"local","--version"],stdin=subprocess.DEVNULL,text=True).strip(),
                  "source_dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()),
                  "binaries": {name: hashlib.sha256((TARGET / "debug/examples" / name).read_bytes()).hexdigest()
                               for name in ["photometry_benchmark", "photometry_shadow_replay"]}}
    (ARTIFACTS / "provenance.json").write_text(json.dumps(provenance, indent=2) + "\n")
    recovery = {"cases": []}
    if not args.benchmark_only:
        run_driver("photometry_shadow_replay", [config, ROOT / "tests/data/alerts/ztf", ROOT / "data/filters", ARTIFACTS / "recovery.json"], worker)
        recovery = json.loads((ARTIFACTS / "recovery.json").read_text())
        assert recovery["status"] == "PASS" and len(recovery["cases"]) >= 11
        assert all(case["result"] == "PASS" for case in recovery["cases"])
        assert redis_command(redis_port, "FLUSHDB") == "+OK"
    for phase in ["baseline", "shadow", "recovery"]:
        create_database(base + "_" + phase)
    with fault_gate(port) as proxy:
        raw["benchmark_fault_proxy_url"] = f"http://127.0.0.1:{proxy}"
        config.write_text(json.dumps(raw))
        corpus = {"kind": "fixture_derived", "templates": [str(p) for p in sorted((ROOT / "tests/data/alerts/ztf").glob("*.avro"))],
                  "objects": 32, "epochs": 4, "interval_ms": 30}
        if args.manifest:
            manifest = args.manifest.resolve()
            corpus = json.loads(manifest.read_text())
            assert corpus.get("kind") == "captured", "manifest must identify captured packets"
            for packet in corpus["packets"]:
                packet["path"] = str((manifest.parent / packet["path"]).resolve())
        plan = {"corpus": corpus, "timeout_seconds": args.timeout_seconds,
                "enrichment_samples": args.enrichment_samples, "recovery_overlap": True,
                "filters_are_deployed_export": args.deployed_filters,
                "target_packets_per_second": args.target_rate, "max_lag_seconds": args.max_lag_seconds}
        plan_path = directory / "plan.json"
        plan_path.write_text(json.dumps(plan))
        run_driver("photometry_benchmark", [config, plan_path, args.filters.resolve(), ARTIFACTS / "benchmark.json"], worker, timeout=args.timeout_seconds * 6 + 180)
    benchmark = json.loads((ARTIFACTS / "benchmark.json").read_text())
    assert benchmark["status"] == "PASS"
    comparisons = 0
    models = 0
    for name in ["shadow", "recovery"]:
        phase = benchmark["phases"][name]
        assert len(phase["histories"]) == benchmark["workload"]["objects"]
        assert phase["enrichment"] and all(sample["parity"]["equal"] for sample in phase["enrichment"])
        models += sum(sample["parity"]["source"]["status"] == "evaluated" for sample in phase["enrichment"])
        for sample in phase["enrichment"]:
            measured = sample["binning"]
            for cadence in measured["cadences"]:
                compare(reference(measured["mongo_history"], cadence["cadence_seconds"]), cadence["rows"])
                comparisons += 1
    live = benchmark["phases"]["recovery"]["recovery"]
    assert live["mode"] == "with_arrivals" and live["packets_dispatched_after_restore"] > 0
    assert live["arrival_overlap_seconds"] > 0 and live["completed_reference_lower_bound_during_arrivals"] > 0
    sample_count = min(args.enrichment_samples, benchmark["workload"]["unique_alerts"])
    assert models > 0 and comparisons == sample_count * 6
    summary = (f"## Native photometry shadow: PASS\n\n"
               f"{len(recovery['cases'])} recovery/batching scenarios; {models} sampled CPU model pairs; {comparisons} independent bin comparisons.\n\n"
               f"Recovery overlapped {live['packets_dispatched_after_restore']} new packet dispatches for {live['arrival_overlap_seconds']:.2f} s. "
               f"At least {live['completed_reference_lower_bound_during_arrivals']} references completed during arrivals.\n\n"
               f"{benchmark['workload']['kind']} workload; production capacity and retention are not established.\n")
    (ARTIFACTS / "summary.md").write_text(summary)
    print(summary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, help="Captured packet manifest produced by prepare_photometry_capture.py")
    parser.add_argument("--filters", type=Path, default=ROOT / "data/filters")
    parser.add_argument("--deployed-filters", action="store_true")
    parser.add_argument("--benchmark-only", action="store_true", help="Skip fixture fault cases; still run baseline, shadow and live recovery")
    parser.add_argument("--enrichment-samples", type=int, default=16)
    parser.add_argument("--timeout-seconds", type=int, default=240)
    parser.add_argument("--target-rate", type=float)
    parser.add_argument("--max-lag-seconds", type=float)
    args = parser.parse_args()
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="boom-shadow-ci-") as tmp:
        directory = Path(tmp)
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            port = sock.getsockname()[1]
        config = directory / "server.xml"
        config.write_text(f"""<clickhouse>
          <path>{escape(tmp)}/data/</path>
          <logger><level>warning</level><log>{escape(tmp)}/server.log</log><errorlog>{escape(tmp)}/error.log</errorlog></logger>
          <http_port>{port}</http_port><listen_host>127.0.0.1</listen_host>
          <users_config>{escape(str(config))}</users_config>
          <profiles><default><max_threads>4</max_threads></default></profiles>
          <users><default><password></password><networks><ip>127.0.0.1</ip></networks><profile>default</profile><quota>default</quota></default></users>
          <quotas><default/></quotas><background_schedule_pool_size>8</background_schedule_pool_size>
          <background_buffer_flush_schedule_pool_size>4</background_buffer_flush_schedule_pool_size>
          <background_message_broker_schedule_pool_size>4</background_message_broker_schedule_pool_size>
          <background_distributed_schedule_pool_size>4</background_distributed_schedule_pool_size>
        </clickhouse>""")
        with (ARTIFACTS / "console.log").open("w") as log:
            process = subprocess.Popen([CH, "server", "--config-file", str(config)], cwd=tmp, stdin=subprocess.DEVNULL, stdout=log, stderr=log)
            try:
                deadline = time.monotonic() + 30
                while True:
                    assert process.poll() is None, "ClickHouse exited; inspect console.log"
                    try:
                        with urllib.request.urlopen(f"http://127.0.0.1:{port}/ping", timeout=1) as response:
                            if response.status == 200:
                                break
                    except (urllib.error.URLError, TimeoutError):
                        pass
                    assert time.monotonic() < deadline, "ClickHouse startup timed out"
                    time.sleep(0.1)
                exercise(directory, port, args)
            finally:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10)
                for name in ["server.log", "error.log"]:
                    if (directory / name).is_file():
                        shutil.copy2(directory / name, ARTIFACTS / name)


if __name__ == "__main__":
    main()
