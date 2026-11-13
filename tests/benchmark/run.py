"""One benchmark run: copy the seed, start the stack, replay, collect, analyze.

    uv run --project tests/benchmark tests/benchmark/run.py \\
        --bundle ~/boom-bench/bundles/ztf-20250311-public \\
        --profile paper1-like --mode drain --name legacy-smoke

Results go to $BENCH_RESULTS_ROOT/<timestamp>_<name>/ (default logs/benchmark/):
run_meta.json, config.yaml, manifest.csv, alert_timing.csv, outputs.csv,
objects.csv, queues.csv, docker_stats.jsonl, events.csv, logs/, summary.json.

Modes:
  drain     (default) The whole window is queued in Kafka first, then BOOM
            starts. Measures the throughput ceiling, as the old throughput test did.
  realtime  BOOM is up and idle; packets arrive on their original timing scaled
            by --speed. Measures latency, and whether BOOM keeps up. Takes the
            window's length divided by --speed.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import subprocess
import sys
import threading
import time
from pathlib import Path

import analyze
from benchlib import (API_HOST_PORT, BENCH_DIR, DATA_ROOT, KAFKA_HOST_PORT, MILVUS_HOST_PORT, MONGO_HOST_PORT,
                      REPO_ROOT, RESULTS_ROOT, VALKEY_HOST_PORT)
from benchlib import docker as dk
from benchlib.babamul import ensure_bench_user
from benchlib.bundle import ZTF_PROGRAM_NAMES
from benchlib.collect import dump_alert_timing, dump_objects, dump_outputs
from benchlib.config import build_run_config, crossmatch_catalogs, write_config
from benchlib.kafka_io import create_topics
from benchlib.monitor import DockerStatsSampler, EventLog, QueueSampler
from benchlib.mongo import (catalog_presence, collection_stats, connect, count_premature_objects, insert_filters,
                            load_filters, mark_mpc_orbits_fresh, max_history_jd, reset_survey_state)
from benchlib.schedule import cut_jd_per_survey, plan_replay
from replay import add_plan_args, describe_plan, replay, resolve_plan

CPU_IMAGE = "boom-astro/boom:bench"
GPU_IMAGE = "boom-astro/boom-gpu:bench"
# Logged by the scheduler before its worker pools start (src/bin/scheduler.rs);
# --settle-seconds then covers the pools loading their models.
READY_LINE = "waiting for ctrl-c"
GPU_READY_LINE = "Confirmed GPU runtime preconditions"


def parse_args():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_plan_args(p)
    p.add_argument("--name", default=None)
    p.add_argument("--tag", action="append", default=[], metavar="KEY=VALUE",
                   help="recorded in run_meta.json for report.py to group by (sweep.py sets plan/step/repeat)")
    p.add_argument("--profile", action="append", default=[], help="config overlay in profiles/ (repeatable)")
    p.add_argument("--set", action="append", default=[], metavar="KEY=VALUE",
                   help="config override, e.g. workers.ztf.enrichment.n_workers=6 (repeatable)")
    # p.add_argument("--mode", choices=("realtime", "drain"), default="realtime")
    p.add_argument("--mode", choices=("realtime", "drain"), default="drain")
    p.add_argument("--speed", type=float, default=1.0)
    p.add_argument("--cache", choices=("cold", "warm", "asis"), default="cold",
                   help="page cache when mongod starts: cold drops it (needs a privileged container), "
                        "warm pre-reads the database files, asis leaves it")
    p.add_argument("--mongo-cache-gb", type=float, default=8.0, help="WiredTiger cache size")
    p.add_argument("--filters", type=Path, default=BENCH_DIR / "filters")
    p.add_argument("--n-filters", type=int, default=None)
    p.add_argument("--partitions", type=int, default=15)
    p.add_argument("--consumer-processes", type=int, default=1)
    p.add_argument("--max-in-queue", type=int, default=15000)
    p.add_argument("--settle-seconds", type=float, default=30.0)
    p.add_argument("--startup-timeout", type=float, default=900.0)
    p.add_argument("--drain-timeout", type=float, default=3600.0)
    p.add_argument("--idle-seconds", type=float, default=30.0,
                   help="queues empty and counts unchanged this long means the run is done")
    p.add_argument("--warmup-seconds", type=float, default=60.0,
                   help="realtime: alerts sent this soon after the replay starts are left out of latency")
    p.add_argument("--api", action="store_true", help="start the API (needed by --api-load and query/*.py)")
    p.add_argument("--api-load", type=int, default=0, metavar="CLIENTS",
                   help="concurrent API clients querying objects during the replay")
    p.add_argument("--milvus-outage", default=None, metavar="START:DURATION",
                   help="stop Milvus START s after the replay starts (drain: after the workers start) "
                        "for DURATION s; needs the milvus profile")
    p.add_argument("--strict", action="store_true",
                   help="fail instead of warning when a catalog is empty or the snapshot postdates the cut")
    p.add_argument("--skip-build", action="store_true")
    p.add_argument("--keep-up", action="store_true", help="leave the stack running (e.g. for query/*.py)")
    p.add_argument("--keep-db", action="store_true", help="keep the run's database copy")
    p.add_argument("--data-root", type=lambda s: Path(s).expanduser(), default=DATA_ROOT)
    p.add_argument("--results-root", type=lambda s: Path(s).expanduser(), default=RESULTS_ROOT)
    return p.parse_args()


def git(*args: str) -> str:
    return subprocess.run(["git", "-C", str(REPO_ROOT), *args], capture_output=True, text=True).stdout.strip()


def host_info(data_root: Path) -> dict:
    def sh(cmd):
        try:
            return subprocess.run(cmd, capture_output=True, text=True, timeout=30).stdout.strip()
        except Exception as e:
            return f"unavailable: {e}"

    return {
        "hostname": platform.node(),
        "kernel": platform.release(),
        "nproc": os.cpu_count(),
        "lscpu": sh(["lscpu"]),
        "memory": sh(["free", "-b"]),
        "gpus": sh(["nvidia-smi", "--query-gpu=name,memory.total,driver_version", "--format=csv,noheader"]),
        "data_root_fs": sh(["df", "-hT", str(data_root)]),
        "block_devices": sh(["lsblk", "-d", "-o", "NAME,ROTA,SIZE,MODEL"]),
        "docker": sh(["docker", "version", "--format", "{{.Server.Version}}"]),
    }


def wait_ready(compose: dk.Compose, schedulers: list[str], gpu: bool, timeout: float) -> None:
    deadline = time.time() + timeout
    pending = set(schedulers)
    while pending:
        for svc in sorted(pending):
            code = compose.exited(svc)
            if code is not None:
                print(compose.logs(svc)[-5000:])
                raise RuntimeError(f"{svc} exited with code {code} during startup")
            logs = compose.logs(svc)
            ready = READY_LINE in logs and (not gpu or svc != "scheduler-ztf" or GPU_READY_LINE in logs)
            if ready:
                pending.discard(svc)
        if pending and time.time() > deadline:
            raise TimeoutError(f"not ready after {timeout:.0f} s: {sorted(pending)}")
        if pending:
            time.sleep(3)


def wait_drained(sampler: QueueSampler, db, surveys: list[str], idle_s: float, timeout: float,
                 no_progress_s: float = 900.0) -> bool:
    """True once alerts have arrived, every queue is empty, and no count has moved for `idle_s`."""
    t0 = time.time()
    deadline = t0 + timeout
    last, stable_since = None, None
    while time.time() < deadline:
        counts = {s: db[f"{s}_alerts"].estimated_document_count() for s in surveys}
        if not any(counts.values()):
            # In drain mode the services have only just started; nothing to judge yet.
            if time.time() - t0 > no_progress_s:
                return False
            time.sleep(2)
            continue
        if sampler.total() == 0 and counts == last:
            stable_since = stable_since or time.time()
            if time.time() - stable_since >= idle_s:
                return True
        else:
            stable_since = None
        last = counts
        time.sleep(2)
    return False


def schedule_outage(compose: dk.Compose, spec: str, replay_start: float, events: EventLog) -> threading.Thread:
    start, duration = (float(x) for x in spec.split(":"))

    def run():
        time.sleep(max(0.0, replay_start + start - time.time()))
        compose("stop", "milvus", check=False)
        events("milvus_stopped")
        time.sleep(duration)
        compose("start", "milvus", check=False)
        events("milvus_started")

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t


def main() -> int:
    args = parse_args()
    bundles, plan, clock = resolve_plan(args)
    if plan.empty:
        sys.exit("the replay plan is empty")
    # Headroom compares throughput with the whole night's busiest minute, whatever the window.
    night = describe_plan(plan_replay(bundles, clock=clock, align=args.align), 1.0)
    if args.mode == "drain":
        plan["t_event"] = 0.0
    surveys = sorted(plan["survey"].unique())

    name = args.name or "-".join([args.mode, *[s.lower() for s in surveys], *args.profile])
    stamp = time.strftime("%Y%m%dT%H%M%S")
    run_dir = args.results_root / f"{stamp}_{re.sub(r'[^A-Za-z0-9_.-]+', '_', name)}"
    (run_dir / "logs").mkdir(parents=True, exist_ok=True)
    events = EventLog(run_dir / "events.csv")
    warnings: list[str] = []

    config, directives = build_run_config(args.profile, args.set)
    gpu, milvus = bool(directives.get("gpu")), bool(directives.get("milvus"))
    write_config(config, run_dir / "config.yaml")

    nights = {}
    for b in bundles:
        if nights.setdefault(b.survey, b.night) != b.night:
            sys.exit(f"bundles for {b.survey} come from different nights; the consumer reads one night's topics")
    programids = sorted({int(p) for p in plan.loc[plan["survey"] == "ZTF", "programid"].dropna()} or {1})

    meta = {
        "name": name,
        "run_dir": str(run_dir),
        "tags": dict(t.split("=", 1) for t in args.tag),
        "args": {k: (str(v) if isinstance(v, Path) else v) for k, v in vars(args).items()
                 if k not in ("bundle",)},
        "bundles": [{"path": str(b.path), **b.meta} for b in bundles],
        "clock": clock,
        "surveys": surveys,
        "night": night,
        "plan": describe_plan(plan, args.speed) if args.mode == "realtime" else {"packets": int(len(plan))},
        "cut_jd": cut_jd_per_survey(plan),
        "directives": directives,
        "git_sha": git("rev-parse", "HEAD"),
        "git_dirty": bool(git("status", "--porcelain", "--untracked-files=no")),
        "git_branch": git("rev-parse", "--abbrev-ref", "HEAD"),
        "host": host_info(args.data_root),
    }

    def save_meta():
        meta["warnings"] = warnings
        (run_dir / "run_meta.json").write_text(json.dumps(meta, indent=2, default=str) + "\n")

    save_meta()
    print(f"run directory: {run_dir}")

    if not args.skip_build:
        dk.build_image("Dockerfile", CPU_IMAGE, meta["git_sha"])
        if gpu:
            dk.build_image("Dockerfile.gpu", GPU_IMAGE, meta["git_sha"])
    meta["images"] = {"cpu": dk.image_id(CPU_IMAGE), "gpu": dk.image_id(GPU_IMAGE) if gpu else None}

    seed, run_db = args.data_root / "seed", args.data_root / "run-db"
    if not seed.exists():
        sys.exit(f"no seed database at {seed}; build one with seed_db.py")
    if dk.run(["docker", "ps", "-q", "-f", "name=^boom-bench-seed$"], check=False, capture=True).stdout.strip():
        sys.exit("the seed mongod is running; stop it (seed_db.py stop) so the copy is consistent")

    env = {
        "BENCH_IMAGE": CPU_IMAGE, "BENCH_GPU_IMAGE": GPU_IMAGE,
        "BENCH_CONFIG": str(run_dir / "config.yaml"), "BENCH_DBPATH": str(run_db),
        "BENCH_NV_CACHE": str(args.data_root / "nv-cache"),
        "MONGO_CACHE_GB": str(args.mongo_cache_gb), "MONGO_HOST_PORT": str(MONGO_HOST_PORT),
        "VALKEY_HOST_PORT": str(VALKEY_HOST_PORT), "KAFKA_HOST_PORT": str(KAFKA_HOST_PORT),
        "API_HOST_PORT": str(API_HOST_PORT), "MILVUS_HOST_PORT": str(MILVUS_HOST_PORT),
        "KAFKA_NUM_PARTITIONS": str(args.partitions),
        "CONSUMER_PROCESSES": str(args.consumer_processes), "CONSUMER_MAX_IN_QUEUE": str(args.max_in_queue),
        "ZTF_PROGRAMIDS": ",".join(ZTF_PROGRAM_NAMES[p] for p in programids),
        **{f"{s}_NIGHT": n for s, n in nights.items()},
    }
    compose = dk.Compose(env, gpu=gpu, milvus=milvus)
    bootstrap = f"localhost:{KAFKA_HOST_PORT}"
    schedulers = [f"scheduler-{s.lower()}" for s in surveys]
    consumers = [f"consumer-{s.lower()}" for s in surveys]
    extra = ["api"] if (args.api or args.api_load) else []
    infra = ["mongo", "valkey", "broker"] + (["etcd", "minio", "milvus"] if milvus else [])
    samplers: list = []
    api_load_proc = None
    status = "failed"

    try:
        compose.down()
        events("db_copy", f"{dk.copy_tree(seed, run_db):.1f}s")
        if args.cache == "cold":
            dk.drop_page_cache()
        elif args.cache == "warm":
            events("page_cache_warm", f"{dk.warm_page_cache(run_db):.1f}s")
        compose.up(*infra)
        events("infra_up")

        _, db = connect(MONGO_HOST_PORT)
        reset_survey_state(db, surveys)
        filters, filter_warnings = load_filters(args.filters, surveys, args.n_filters)
        warnings += filter_warnings
        insert_filters(db, filters)
        meta["filters"] = [{"name": f["name"], "survey": f["survey"]} for f in filters]
        meta["mpc_orbits"] = mark_mpc_orbits_fresh(db)
        if extra:
            ensure_bench_user(db)

        presence = catalog_presence(db, list(crossmatch_catalogs(config, surveys)))
        meta["catalogs"] = presence
        empty = sorted(c for c, n in presence.items() if n == 0)
        if empty:
            warnings.append(f"configured catalogs with no rows in the database: {empty}; their cross-matches "
                            "cost almost nothing, so the run understates production")
        for s, cut in meta["cut_jd"].items():
            if f"{s}_alerts_aux" in db.list_collection_names():
                latest = max_history_jd(db, s)
                if latest is not None and latest >= cut:
                    warnings.append(f"{s}_alerts_aux holds photometry at JD {latest:.5f}, at or after the replay "
                                    f"cut {cut:.5f}; those updates will be no-ops (see slice_fixture.py)")
                ids = sorted(set(plan.loc[plan["survey"] == s, "object_id"]))
                premature = count_premature_objects(db, s, ids, cut)
                meta.setdefault("premature_objects", {})[s] = premature
                if premature:
                    warnings.append(f"{premature} of {len(ids)} replayed {s} objects already have a document but "
                                    "no detection before the replay; BOOM skips cross-matches and host "
                                    "association for them, so that cost is missing from the run")
        for w in warnings:
            print(f"WARNING: {w}", file=sys.stderr)
        if args.strict and warnings:
            raise RuntimeError("--strict: refusing to run with the warnings above")
        (run_dir / "collections_before.json").write_text(json.dumps(collection_stats(db), indent=2) + "\n")
        save_meta()

        create_topics(bootstrap, list(plan["topic"].unique()), args.partitions)
        queues = QueueSampler(VALKEY_HOST_PORT, surveys, run_dir / "queues.csv")
        stats = DockerStatsSampler(compose.container_ids, run_dir / "docker_stats.jsonl")
        samplers = [queues, stats]
        for s in samplers:
            s.start()

        if args.mode == "realtime":
            compose.start(*schedulers, *consumers, *extra)
            events("services_started")
            wait_ready(compose, schedulers, gpu, args.startup_timeout)
            events("ready")
            time.sleep(args.settle_seconds)
        replay_start = time.time() + 1.0
        if args.api_load:
            stop_file = run_dir / ".stop_api_load"
            api_load_proc = subprocess.Popen([
                sys.executable, str(BENCH_DIR / "query" / "api_load.py"),
                "--clients", str(args.api_load), "--surveys", ",".join(surveys),
                "--out", str(run_dir / "api_load.csv"), "--stop-file", str(stop_file)])
        if args.milvus_outage and not milvus:
            raise ValueError("--milvus-outage needs a profile with `benchmark: {milvus: true}`")
        if args.milvus_outage and args.mode == "realtime":
            schedule_outage(compose, args.milvus_outage, replay_start, events)
        events("replay_start")
        result = replay(bundles, plan, bootstrap, args.speed, run_dir / "manifest.csv", start_at=replay_start)
        events("replay_end", json.dumps(result))
        meta["replay"] = result
        if args.mode == "drain":
            compose.start(*schedulers, *consumers, *extra)
            events("services_started")
            if args.milvus_outage:
                schedule_outage(compose, args.milvus_outage, time.time(), events)
        drained = wait_drained(queues, db, surveys, args.idle_seconds, args.drain_timeout)
        events("drained" if drained else "drain_timeout")
        if not drained:
            warnings.append(f"queues did not drain within {args.drain_timeout:.0f} s")
        if api_load_proc:
            (run_dir / ".stop_api_load").touch()
            api_load_proc.wait(timeout=120)

        print("collecting results")
        meta["alert_counts"] = dump_alert_timing(db, surveys, run_dir / "alert_timing.csv")
        object_ids = {s: sorted(set(g["object_id"])) for s, g in plan.groupby("survey")}
        dump_objects(db, object_ids, run_dir / "objects.csv")
        meta["output_messages"] = dump_outputs(bootstrap, run_dir / "outputs.csv")
        (run_dir / "collections_after.json").write_text(json.dumps(collection_stats(db), indent=2) + "\n")
        status = "completed" if drained else "drain_timeout"
    except BaseException as e:
        warnings.append(f"run aborted: {e!r}")
        raise
    finally:
        for s in samplers:
            s.stop()
        for svc in infra + schedulers + consumers + extra:
            (run_dir / "logs" / f"{svc}.log").write_text(compose.logs(svc))
        meta["status"] = status
        save_meta()
        if not args.keep_up:
            compose.down()
            if not args.keep_db:
                dk.remove_tree(run_db)

    summary = analyze.analyze_run(run_dir, warmup_s=args.warmup_seconds)
    print(json.dumps(analyze.headline(summary), indent=2))
    return 0 if status == "completed" else 2


if __name__ == "__main__":
    raise SystemExit(main())
