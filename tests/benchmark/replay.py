"""Replay bundles into Kafka on their original timing, scaled by --speed.

Each exposure's packets are sent as one burst when that exposure falls due.
Every packet sent is logged to the manifest with its send time. run.py's
drain mode sends everything at once instead, before BOOM starts.

    uv run --project tests/benchmark tests/benchmark/replay.py \\
        --bundle ~/boom-bench/bundles/ztf-20250311-public \\
        --window-seconds 3600 --plan-only

--plan-only prints what would be sent and the night's arrival rates, then exits.
--clock none sends everything at once.
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
from confluent_kafka import Producer

from benchlib import KAFKA_HOST_PORT
from benchlib.bundle import Bundle
from benchlib.kafka_io import create_topics
from benchlib.schedule import ALIGNMENTS, CLOCKS, peak_rate, plan_replay


def add_plan_args(p: argparse.ArgumentParser) -> None:
    """Arguments that decide which packets are sent and when (shared with run.py and slice_fixture.py)."""
    p.add_argument("--bundle", action="append", required=True, type=lambda s: Path(s).expanduser(),
                   help="bundle directory (repeatable)")
    p.add_argument("--clock", choices=CLOCKS, default=None,
                   help="exposure: exposure times; kafka: captured broker times; none: all at once "
                        "(default: kafka when every bundle has broker times, else exposure)")
    p.add_argument("--align", choices=ALIGNMENTS, default="start",
                   help="start: each bundle begins at t=0; absolute: keep relative timing across bundles")
    p.add_argument("--window-seconds", type=float, default=None, help="replay only this much of the night")
    p.add_argument("--window-start", default="auto",
                   help="seconds into the night where the window starts, or 'auto' for the busiest stretch")


def resolve_plan(args) -> tuple[list[Bundle], pd.DataFrame, str]:
    bundles = [Bundle.open(b) for b in args.bundle]
    clock = args.clock
    if clock is None:
        clock = "kafka" if all(b.index["t_kafka_ms"].notna().all() for b in bundles) else "exposure"
    plan = plan_replay(bundles, clock=clock, align=args.align,
                       window_seconds=args.window_seconds, window_start=args.window_start)
    return bundles, plan, clock


def _arrival(plan, speed: float) -> dict:
    duration = float(plan["t_event"].max() - plan["t_event"].min()) / speed if len(plan) else 0.0
    return {
        "packets": int(len(plan)),
        "duration_s": duration,
        "mean_rate_per_s": float(len(plan) / duration) if duration > 0 else None,
        "peak_minute_rate_per_s": peak_rate(plan["t_event"].to_numpy() / speed) if len(plan) else None,
        "exposure_bursts": int(plan["t_event"].nunique()),
    }


def describe_plan(plan, speed: float) -> dict:
    """Packets and arrival rates of a plan sent at `speed`, overall and per survey."""
    return {**_arrival(plan, speed), "per_survey": {s: _arrival(g, speed) for s, g in plan.groupby("survey")}}


def replay(bundles: list[Bundle], plan, bootstrap: str, speed: float, manifest_path: Path,
           start_at: float | None = None) -> dict:
    producer = Producer({
        "bootstrap.servers": bootstrap,
        "linger.ms": 5,
        "acks": 1,
        "message.max.bytes": 20 * 1024 * 1024,
        "queue.buffering.max.kbytes": 2 * 1024 * 1024,
        "queue.buffering.max.messages": 1_000_000,
    })
    maps = []
    contexts = [b.payloads() for b in bundles]
    for ctx in contexts:
        maps.append(ctx.__enter__())
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    lags = []
    sent = 0
    try:
        with open(manifest_path, "w", newline="") as f:
            out = csv.writer(f)
            out.writerow(["survey", "topic", "alert_id", "object_id", "t_event", "t_due", "t_send"])
            t0 = start_at if start_at is not None else time.time() + 1.0
            bundle_col = plan["bundle"].to_numpy()
            offset_col = plan["offset"].to_numpy()
            length_col = plan["length"].to_numpy()
            topic_col = plan["topic"].to_numpy()
            survey_col = plan["survey"].to_numpy()
            alert_col = plan["alert_id"].to_numpy()
            object_col = plan["object_id"].to_numpy()
            t_event = plan["t_event"].to_numpy()
            # Start index of each burst: packets sharing a t_event go out together.
            starts = np.flatnonzero(np.r_[True, t_event[1:] != t_event[:-1]])
            ends = np.r_[starts[1:], len(t_event)]
            for s, e in zip(starts, ends):
                due = t0 + t_event[s] / speed
                wait = due - time.time()
                if wait > 0:
                    time.sleep(wait)
                lags.append(time.time() - due)
                for i in range(s, e):
                    payload = maps[bundle_col[i]][offset_col[i]: offset_col[i] + length_col[i]]
                    while True:
                        try:
                            producer.produce(topic_col[i], value=payload)
                            break
                        except BufferError:
                            producer.poll(0.1)
                    out.writerow([survey_col[i], topic_col[i], alert_col[i], object_col[i],
                                  f"{t_event[i]:.3f}", f"{due:.6f}", f"{time.time():.6f}"])
                    sent += 1
                producer.poll(0)
            remaining = producer.flush(120)
    finally:
        for ctx in contexts:
            ctx.__exit__(None, None, None)
    lag = np.asarray(lags) if lags else np.zeros(1)
    return {
        "sent": sent,
        "undelivered": int(remaining),
        "t_start": t0,
        "t_end": time.time(),
        # How late bursts left relative to schedule; large values mean the
        # replayer, not BOOM, set the input rate.
        "burst_lag_p50_s": float(np.percentile(lag, 50)),
        "burst_lag_p99_s": float(np.percentile(lag, 99)),
        "burst_lag_max_s": float(lag.max()),
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_plan_args(p)
    p.add_argument("--speed", type=float, default=1.0, help="time compression (2 = twice as fast)")
    p.add_argument("--bootstrap", default=f"localhost:{KAFKA_HOST_PORT}")
    p.add_argument("--manifest", type=Path, default=Path("manifest.csv"))
    p.add_argument("--create-topics", action="store_true", help="create the replay topics first")
    p.add_argument("--partitions", type=int, default=15)
    p.add_argument("--plan-only", action="store_true")
    args = p.parse_args()

    bundles, plan, clock = resolve_plan(args)
    if plan.empty:
        print("nothing to send", file=sys.stderr)
        return 1
    summary = {"clock": clock, **describe_plan(plan, args.speed)}
    print(json.dumps(summary, indent=2))
    if args.plan_only:
        return 0
    if args.create_topics:
        create_topics(args.bootstrap, list(plan["topic"].unique()), args.partitions)
    result = replay(bundles, plan, args.bootstrap, args.speed, args.manifest)
    print(json.dumps(result, indent=2))
    return 0 if result["undelivered"] == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
