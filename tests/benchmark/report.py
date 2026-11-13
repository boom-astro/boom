"""Tabulate many runs: one row per (plan, step), median and range over repeats.

    uv run --project tests/benchmark tests/benchmark/report.py logs/benchmark/* \\
        --group-by plan step --out ablation.md --csv ablation.csv
    uv run --project tests/benchmark tests/benchmark/report.py logs/benchmark/*filters* \\
        --plot step per_survey.ZTF.throughput.per_s --plot-out filters.png

Metric names are dotted paths into summary.json. A plot's x is numeric, or a
tag such as `step`, kept in the order the runs were made.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from benchlib import RESULTS_ROOT

DEFAULT_METRICS = [
    "throughput.per_s",
    "throughput.headroom",
    "per_survey.{s}.throughput.per_s",
    "per_survey.{s}.throughput.headroom",
    "per_survey.{s}.ingest_rate_per_s",
    "per_survey.{s}.enrich_rate_per_s",
    "per_survey.{s}.missing",
]
REALTIME_METRICS = [
    "per_survey.{s}.latency_s.end_to_end_babamul.p50",
    "per_survey.{s}.latency_s.end_to_end_babamul.p95",
    "per_survey.{s}.latency_s.alert_stage.p95",
    "per_survey.{s}.latency_s.queue_to_alert_worker.p95",
    "per_survey.{s}.finished_after_replay_end_s",
    "per_survey.{s}.missing",
    "queues.backlog_growth_per_s",
]


def load(run_dirs: list[Path]) -> pd.DataFrame:
    rows = []
    for d in run_dirs:
        path = d / "summary.json"
        if not path.exists():
            continue
        summary = json.loads(path.read_text())
        flat = pd.json_normalize(summary, sep=".").iloc[0].to_dict()
        flat["run"] = d.name
        for k, v in (summary.get("tags") or {}).items():
            flat[k] = v
        rows.append(flat)
    return pd.DataFrame(rows)


def expand(metrics: list[str], df: pd.DataFrame) -> list[str]:
    surveys = sorted({c.split(".")[1] for c in df.columns if c.startswith("per_survey.")})
    out = []
    for m in metrics:
        out += [m.format(s=s) for s in surveys] if "{s}" in m else [m]
    return [m for m in out if m in df.columns]


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("run_dirs", type=Path, nargs="+")
    p.add_argument("--group-by", nargs="+", default=["plan", "step"])
    p.add_argument("--metric", action="append", default=None, help="dotted summary path (repeatable)")
    p.add_argument("--out", type=Path, default=None, help="write the table as Markdown")
    p.add_argument("--csv", type=Path, default=None, help="write every run's metrics as CSV")
    p.add_argument("--plot", nargs=2, metavar=("X", "Y"), default=None)
    p.add_argument("--plot-out", type=Path, default=RESULTS_ROOT / "report.png")
    args = p.parse_args()

    df = load(args.run_dirs)
    if df.empty:
        print("no summary.json found")
        return 1
    if args.csv:
        df.to_csv(args.csv, index=False)
    group = [g for g in args.group_by if g in df.columns] or ["run"]
    realtime = "mode" in df and (df["mode"] == "realtime").all()
    metrics = expand(args.metric or (REALTIME_METRICS if realtime else DEFAULT_METRICS), df)
    numeric = df[group + metrics].copy()
    for m in metrics:
        numeric[m] = pd.to_numeric(numeric[m], errors="coerce")
    agg = numeric.groupby(group, sort=False)[metrics].agg(["median", "min", "max", "count"])

    lines = ["| " + " | ".join(group + metrics) + " |", "|" + "---|" * (len(group) + len(metrics))]
    for key, row in agg.iterrows():
        key = key if isinstance(key, tuple) else (key,)
        cells = [str(k) for k in key]
        for m in metrics:
            med, lo, hi, n = row[(m, "median")], row[(m, "min")], row[(m, "max")], row[(m, "count")]
            cells.append("" if pd.isna(med) else (f"{med:.3g}" if n < 2 else f"{med:.3g} [{lo:.3g}, {hi:.3g}]"))
        lines.append("| " + " | ".join(cells) + " |")
    table = "\n".join(lines)
    print(table)
    if args.out:
        args.out.write_text(table + "\n")

    if args.plot:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt

        x, y = args.plot
        d = pd.DataFrame({x: df[x], y: pd.to_numeric(df[y], errors="coerce")}).dropna()
        numeric_x = pd.to_numeric(d[x], errors="coerce")
        if numeric_x.notna().all():
            d[x] = numeric_x
            med = d.groupby(x)[y].median()
            pos, ticks = d[x], None
        else:
            # Categories in the order the runs were made, which is the plan's step order.
            order = list(dict.fromkeys(df.sort_values("run")[x].dropna()))
            pos = d[x].map({c: i for i, c in enumerate(order)})
            med = d.groupby(pos)[y].median()
            ticks = order
        fig, ax = plt.subplots(figsize=(6, 4))
        ax.scatter(pos, d[y], s=10, alpha=0.5)
        ax.plot(med.index, med.values, "-o")
        if ticks:
            ax.set_xticks(range(len(ticks)), ticks, rotation=45, ha="right")
        ax.set_xlabel(x)
        ax.set_ylabel(y)
        fig.tight_layout()
        fig.savefig(args.plot_out, dpi=150)
        print(f"wrote {args.plot_out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
