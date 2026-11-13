"""Turn a run directory into summary.json.

    uv run --project tests/benchmark tests/benchmark/analyze.py logs/benchmark/<run> [--plots]

Per alert, these instants are joined on (survey, alert id):

    created     the alert worker started on it            (alert `created_at`)
    enriched    its enrichment batch started              (alert `updated_at`)
    babamul /   Babamul or filter-result message produced  (Kafka CreateTime)
    filtered

`created_at` and `updated_at` are whole seconds (flare::Time); Kafka times are ms.

A drain run queues the whole window before BOOM starts, so these give
throughput: enriched alerts per second from the first alert ingested until
every stage is idle, and its ratio to the night's busiest minute of arrivals
(`headroom`).

A realtime run also has each packet's send time (manifest.csv), and so latencies:

    queue_to_alert_worker = created - t_send     Kafka, consumer, packet queue
    alert_stage           = enriched - created   ingest, cross-match, host; enrichment queue
    enrich_to_babamul     = babamul - enriched   ML, fits, Milvus, Babamul publish
    end_to_end_babamul    = babamul - t_send     what a public stream subscriber sees
    end_to_end_filter     = filtered - t_send    what a filter owner sees
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

from benchlib.timeutil import jd_to_unix

PERCENTILES = (50, 90, 95, 99)
LATENCIES = ("queue_to_alert_worker", "alert_stage", "enrich_to_babamul", "end_to_end_babamul",
             "end_to_end_filter")


def _read_csv(path: Path, **kw) -> pd.DataFrame:
    return pd.read_csv(path, **kw) if path.exists() and path.stat().st_size > 0 else pd.DataFrame()


def _stats(x: pd.Series) -> dict:
    x = x.dropna().to_numpy(float)
    if x.size == 0:
        return {"n": 0}
    out = {"n": int(x.size), "mean": float(x.mean()), "max": float(x.max())}
    out.update({f"p{p}": float(np.percentile(x, p)) for p in PERCENTILES})
    return out


def _mem_bytes(s: str) -> float:
    units = {"B": 1, "KiB": 2**10, "MiB": 2**20, "GiB": 2**30, "TiB": 2**40, "kB": 1e3, "MB": 1e6, "GB": 1e9}
    used = str(s).split("/")[0].strip()
    for u in sorted(units, key=len, reverse=True):
        if used.endswith(u):
            return float(used[: -len(u)]) * units[u]
    return float("nan")


def load_alerts(run_dir: Path) -> pd.DataFrame:
    manifest = _read_csv(run_dir / "manifest.csv")
    if manifest.empty:
        return manifest
    timing = _read_csv(run_dir / "alert_timing.csv")
    outputs = _read_csv(run_dir / "outputs.csv")
    df = manifest[["survey", "alert_id", "object_id", "t_event", "t_due", "t_send"]].copy()
    if not timing.empty:
        timing["created"] = jd_to_unix(pd.to_numeric(timing["created_at_jd"], errors="coerce"))
        updated = jd_to_unix(pd.to_numeric(timing["updated_at_jd"], errors="coerce"))
        if "enriched" in timing:
            done = timing["enriched"] == 1
        else:  # collected before the flag existed
            done = (updated > timing["created"]) | (timing["has_classifications"] == 1)
        timing["enriched"] = updated.where(done)
        timing["villar"] = pd.to_numeric(timing["villar_reduced_chi2"], errors="coerce")
        df = df.merge(timing[["survey", "alert_id", "created", "enriched", "has_classifications", "villar"]],
                      on=["survey", "alert_id"], how="left")
    if not outputs.empty:
        outputs["alert_id"] = pd.to_numeric(outputs["alert_id"], errors="coerce")
        outputs = outputs.dropna(subset=["alert_id"])
        outputs["alert_id"] = outputs["alert_id"].astype("int64")
        outputs["t"] = outputs["kafka_ts_ms"] / 1000.0
        is_babamul = outputs["topic"].str.startswith("babamul.")
        for label, mask in (("babamul", is_babamul), ("filtered", ~is_babamul)):
            first = outputs[mask].groupby(["survey", "alert_id"], as_index=False)["t"].min()
            df = df.merge(first.rename(columns={"t": label}), on=["survey", "alert_id"], how="left")
    for col in ("created", "enriched", "babamul", "filtered", "has_classifications", "villar"):
        if col not in df:
            df[col] = np.nan
    df["queue_to_alert_worker"] = df["created"] - df["t_send"]
    df["alert_stage"] = df["enriched"] - df["created"]
    df["enrich_to_babamul"] = df["babamul"] - df["enriched"]
    df["end_to_end_babamul"] = df["babamul"] - df["t_send"]
    df["end_to_end_filter"] = df["filtered"] - df["t_send"]
    return df


def drain_throughput(g: pd.DataFrame, busy: pd.Series, night: dict | None) -> dict:
    """Enriched alerts per second, from the first alert ingested until every stage is idle."""
    created = g["created"].dropna()
    done = pd.concat([g["enriched"], g["babamul"], g["filtered"]]).dropna()
    if created.empty or done.empty:
        return {}
    # An alert that passes no filter leaves no output, so the last stage
    # ends when its queue last held work (1 s sampling).
    end = max(float(done.max()), float(busy.max()) if len(busy) else -np.inf)
    span = end - float(created.min())
    if span <= 0:
        return {}
    rate = int(g["enriched"].notna().sum()) / span
    out = {"per_s": rate, "span_s": span, "t_start": float(created.min()), "t_end": end}
    peak = (night or {}).get("peak_minute_rate_per_s")
    if peak:
        # Above 1, BOOM clears alerts faster than the night's busiest minute delivered them.
        out["headroom"] = rate / peak
    return out


def queue_summary(q: pd.DataFrame, replay_start: float | None, replay_end: float | None, warmup_s: float,
                  realtime: bool) -> dict:
    if q.empty:
        return {}
    out = {}
    for (survey, stage), g in q.groupby(["survey", "stage"]):
        out[f"{survey}.{stage}"] = {"max": int(g["length"].max())}
    total = q.groupby("t")["length"].sum()
    out["total_max"] = int(total.max())
    if realtime and replay_start is not None and replay_end is not None:
        window = total[(total.index >= replay_start + warmup_s) & (total.index <= replay_end)]
        if len(window) > 10:
            # Growth of the backlog while packets are still arriving; a sustained
            # positive slope means BOOM is falling behind the input.
            slope = np.polyfit(window.index.to_numpy(float) - window.index[0], window.to_numpy(float), 1)[0]
            out["backlog_growth_per_s"] = float(slope)
        at_end = total[total.index <= replay_end]
        out["total_at_replay_end"] = int(at_end.iloc[-1]) if len(at_end) else None
    return out


def resource_summary(run_dir: Path) -> dict:
    path = run_dir / "docker_stats.jsonl"
    if not path.exists():
        return {}
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if not rows:
        return {}
    df = pd.DataFrame(rows)
    df["cpu"] = pd.to_numeric(df["CPUPerc"].str.rstrip("%"), errors="coerce")
    df["mem"] = df["MemUsage"].map(_mem_bytes)
    out = {}
    for name, g in df.groupby("Name"):
        out[name] = {"cpu_pct_mean": float(g["cpu"].mean()), "cpu_pct_max": float(g["cpu"].max()),
                     "mem_gb_max": float(g["mem"].max() / 1e9)}
    return out


def outage_summary(ev: dict, t_end: float | None, warnings: list[str]) -> dict | None:
    if "milvus_stopped" not in ev:
        return None
    t0 = ev.get("services_started", ev.get("replay_start"))
    out = {k: ev[k] - t0 for k in ("milvus_stopped", "milvus_started") if k in ev and t0 is not None}
    if t_end is not None and ev["milvus_stopped"] >= t_end:
        warnings.append("Milvus stopped after every stage was idle, so the outage tested nothing; "
                        "start it earlier or replay a longer window")
    return out


def analyze_run(run_dir: Path, warmup_s: float = 60.0, plots: bool = False) -> dict:
    run_dir = Path(run_dir)
    meta = json.loads((run_dir / "run_meta.json").read_text())
    events = _read_csv(run_dir / "events.csv")
    ev = {r.event: float(r.t) for r in events.itertuples()} if not events.empty else {}
    replay_start, replay_end = ev.get("replay_start"), ev.get("replay_end")
    mode = meta.get("args", {}).get("mode", "realtime")
    realtime = mode == "realtime"
    night = meta.get("night") or {}
    df = load_alerts(run_dir)
    summary: dict = {
        "name": meta.get("name"), "tags": meta.get("tags", {}), "status": meta.get("status"),
        "mode": mode, "speed": meta.get("args", {}).get("speed") if realtime else None,
        "surveys": meta.get("surveys"), "git_sha": meta.get("git_sha"), "git_dirty": meta.get("git_dirty"),
        "directives": meta.get("directives"), "warnings": list(meta.get("warnings", [])),
        "replay": meta.get("replay"), "night": {k: v for k, v in night.items() if k != "per_survey"},
        "per_survey": {},
    }
    if df.empty:
        summary["error"] = "no manifest; the replay did not run"
        (run_dir / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        return summary

    q = _read_csv(run_dir / "queues.csv")
    busy = q.loc[q["length"] > 0, ["t", "survey"]] if not q.empty else pd.DataFrame(columns=["t", "survey"])
    measured = df
    if realtime and replay_start is not None:
        measured = df[df["t_send"] >= replay_start + warmup_s]
        summary["latency_excludes_first_s"] = warmup_s

    for survey, g in df.groupby("survey"):
        sent = len(g)
        s: dict = {
            "sent": sent,
            "ingested": int(g["created"].notna().sum()),
            "enriched": int(g["enriched"].notna().sum()),
            "classified": int((g["has_classifications"] == 1).sum()),
            "villar_fit": int(np.isfinite(g["villar"]).sum()),
            "babamul_out": int(g["babamul"].notna().sum()),
            "filter_out": int(g["filtered"].notna().sum()),
        }
        s["missing"] = sent - s["ingested"]
        s["night"] = night.get("per_survey", {}).get(survey)
        created, enriched = g["created"].dropna(), g["enriched"].dropna()
        if len(created) > 1:
            span = float(created.max() - created.min())
            s["ingest_rate_per_s"] = len(created) / span if span > 0 else None
        if len(enriched) > 1 and len(created):
            span = float(enriched.max() - created.min())
            s["enrich_rate_per_s"] = len(enriched) / span if span > 0 else None
        if realtime:
            s["latency_s"] = {k: _stats(measured.loc[measured["survey"] == survey, k]) for k in LATENCIES}
            last = pd.concat([g["enriched"], g["babamul"], g["filtered"]]).max()
            if replay_end is not None and pd.notna(last):
                s["finished_after_replay_end_s"] = float(last - replay_end)
        else:
            s["throughput"] = drain_throughput(g, busy.loc[busy["survey"] == survey, "t"], s["night"])
        summary["per_survey"][survey] = s

    t_end = None
    if not realtime:
        summary["throughput"] = drain_throughput(df, busy["t"], night)
        t_end = summary["throughput"].get("t_end")
    summary["milvus_outage"] = outage_summary(ev, t_end, summary["warnings"])
    summary["queues"] = queue_summary(q, replay_start, replay_end, warmup_s, realtime)
    summary["resources"] = resource_summary(run_dir)
    api = _read_csv(run_dir / "api_load.csv")
    if not api.empty:
        summary["api_load"] = {ep: {**_stats(g["latency_s"]), "errors": int((g["status"] >= 400).sum())}
                               for ep, g in api.groupby("endpoint")}

    if realtime:
        timeline = measured.assign(minute=((measured["t_send"] - df["t_send"].min()) // 60).astype(int))
        rows = []
        for (survey, minute), g in timeline.groupby(["survey", "minute"]):
            row = {"survey": survey, "minute": minute, "sent": len(g)}
            for k in ("queue_to_alert_worker", "alert_stage", "end_to_end_babamul", "end_to_end_filter"):
                v = g[k].dropna()
                row[f"{k}_p50"] = float(v.median()) if len(v) else np.nan
                row[f"{k}_p95"] = float(v.quantile(0.95)) if len(v) else np.nan
            rows.append(row)
        pd.DataFrame(rows).to_csv(run_dir / "latency_timeline.csv", index=False)

    (run_dir / "summary.json").write_text(json.dumps(summary, indent=2, default=float) + "\n")
    if plots:
        make_plots(run_dir, df, q, ev, realtime)
    return summary


def headline(summary: dict) -> dict:
    """The few numbers worth printing at the end of a run."""
    realtime = summary.get("mode") == "realtime"
    out = {"name": summary.get("name"), "status": summary.get("status"), "mode": summary.get("mode"),
           "warnings": summary.get("warnings")}
    if realtime:
        out["speed"] = summary.get("speed")
    else:
        out["throughput"] = {k: summary.get("throughput", {}).get(k)
                             for k in ("per_s", "headroom", "span_s")}
    for survey, s in summary.get("per_survey", {}).items():
        row = {"sent": s.get("sent"), "missing": s.get("missing"), "enriched": s.get("enriched")}
        if realtime:
            lat = s.get("latency_s", {})
            row.update({
                "e2e_babamul_p50_s": lat.get("end_to_end_babamul", {}).get("p50"),
                "e2e_babamul_p95_s": lat.get("end_to_end_babamul", {}).get("p95"),
                "alert_stage_p95_s": lat.get("alert_stage", {}).get("p95"),
                "finished_after_replay_end_s": s.get("finished_after_replay_end_s"),
            })
        else:
            tp = s.get("throughput", {})
            row.update({"throughput_per_s": tp.get("per_s"), "headroom": tp.get("headroom"),
                        "drain_s": tp.get("span_s"), "night_s": (s.get("night") or {}).get("duration_s")})
        out[survey] = row
    return out


def make_plots(run_dir: Path, df: pd.DataFrame, q: pd.DataFrame, ev: dict, realtime: bool) -> None:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, axes = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
    if realtime:
        t0 = df["t_send"].min()
        for survey, g in df.groupby("survey"):
            axes[0].scatter(g["t_send"] - t0, g["end_to_end_babamul"], s=1, alpha=0.3, label=f"{survey} Babamul")
            axes[0].scatter(g["t_send"] - t0, g["queue_to_alert_worker"], s=1, alpha=0.3,
                            label=f"{survey} to alert worker")
        axes[0].set_ylabel("latency (s)")
        axes[0].set_yscale("log")
        axes[0].legend(markerscale=10, fontsize=8)
        xlabel = "seconds since first packet sent"
    else:
        t0 = ev.get("services_started", df["created"].min())
        for survey, g in df.groupby("survey"):
            for col in ("created", "enriched"):
                t = np.sort(g[col].dropna().to_numpy(float))
                axes[0].plot(t - t0, np.arange(1, t.size + 1), label=f"{survey} {col}")
        axes[0].set_ylabel("alerts done")
        axes[0].legend(fontsize=8)
        xlabel = "seconds since the workers started"
    if not q.empty:
        for (survey, stage), g in q.groupby(["survey", "stage"]):
            axes[1].plot(g["t"] - t0, g["length"], label=f"{survey} {stage}")
        axes[1].set_ylabel("queue length")
        axes[1].legend(fontsize=8)
    for name, t in ev.items():
        if name.startswith(("replay_", "milvus_")):
            for ax in axes:
                ax.axvline(t - t0, color="k", lw=0.5, ls="--")
    axes[1].set_xlabel(xlabel)
    fig.tight_layout()
    fig.savefig(run_dir / "timeline.png", dpi=120)
    plt.close(fig)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("run_dir", type=Path, nargs="+")
    p.add_argument("--warmup-seconds", type=float, default=60.0, help="realtime only")
    p.add_argument("--plots", action="store_true")
    args = p.parse_args()
    for run_dir in args.run_dir:
        summary = analyze_run(run_dir, args.warmup_seconds, args.plots)
        print(json.dumps(headline(summary), indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
