"""Check that two runs of the same replay produced the same results.

The main use is validating a sliced fixture: run the replay once on the slice
and once on a full snapshot, at the same commit. Identical cross-survey links,
hosts and filter passes show the slice holds everything the night touched (a
missing object shows up as a missing link); the throughput difference shows
whether slicing biased the timing.

    uv run --project tests/benchmark tests/benchmark/compare_runs.py RUN_A RUN_B
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def read(run: Path, name: str) -> pd.DataFrame:
    path = run / name
    return pd.read_csv(path, dtype=str, keep_default_na=False) if path.exists() else pd.DataFrame()


def diff(a: pd.DataFrame, b: pd.DataFrame, keys: list[str], cols: list[str], label: str, show: int) -> int:
    if a.empty or b.empty:
        print(f"{label}: missing in one run")
        return 1
    m = a[keys + cols].merge(b[keys + cols], on=keys, how="outer", suffixes=("_a", "_b"), indicator=True)
    only = m[m["_merge"] != "both"]
    both = m[m["_merge"] == "both"]
    changed = both[[any(r[f"{c}_a"] != r[f"{c}_b"] for c in cols) for _, r in both.iterrows()]]
    print(f"{label}: {len(both)} in both, {len(only)} in one only, {len(changed)} differ")
    for _, r in changed.head(show).iterrows():
        print("   ", {k: r[k] for k in keys}, {c: (r[f"{c}_a"], r[f"{c}_b"]) for c in cols if r[f"{c}_a"] != r[f"{c}_b"]})
    return int(len(only) + len(changed))


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("run_a", type=Path)
    p.add_argument("run_b", type=Path)
    p.add_argument("--show", type=int, default=10)
    args = p.parse_args()

    problems = 0
    problems += diff(read(args.run_a, "objects.csv"), read(args.run_b, "objects.csv"), ["survey", "object_id"],
                     ["aliases", "best_host", "best_host_catalog", "n_cross_match_catalogs"], "objects", args.show)
    oa, ob = read(args.run_a, "outputs.csv"), read(args.run_b, "outputs.csv")
    fa = oa[oa["topic"].str.endswith("_alerts_results")] if not oa.empty else oa
    fb = ob[ob["topic"].str.endswith("_alerts_results")] if not ob.empty else ob
    problems += diff(fa, fb, ["survey", "alert_id"], ["filter_ids"], "filter passes", args.show)
    ba = oa[oa["topic"].str.startswith("babamul.")] if not oa.empty else oa
    bb = ob[ob["topic"].str.startswith("babamul.")] if not ob.empty else ob
    problems += diff(ba, bb, ["survey", "alert_id"], ["topic"], "babamul topics", args.show)

    for run in (args.run_a, args.run_b):
        s = json.loads((run / "summary.json").read_text()) if (run / "summary.json").exists() else {}
        per = {k: {"throughput_per_s": (v.get("throughput") or {}).get("per_s"),
                   "e2e_babamul_p50": v.get("latency_s", {}).get("end_to_end_babamul", {}).get("p50"),
                   "enrich_rate_per_s": v.get("enrich_rate_per_s")} for k, v in s.get("per_survey", {}).items()}
        print(f"{run.name}: throughput={(s.get('throughput') or {}).get('per_s')} {per}")
    print("identical results" if problems == 0 else f"{problems} differences")
    return 0 if problems == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
