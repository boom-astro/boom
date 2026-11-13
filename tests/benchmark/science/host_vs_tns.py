"""Check host association against transients with a known redshift.

Objects cross-matched to a TNS entry with a redshift (mostly classified
supernovae, whose redshift comes from the transient or host spectrum) give a
truth set. For them this reports:

  * how often a host is found, overall and per TNS type;
  * how well the best host's redshift agrees, |dz|/(1+z), by host catalog
    (spectroscopic NED-LVS and DESI distances vs Legacy Surveys photo-z);
  * the same for the candidate nearest in angle, the association a fixed
    aperture would make, to show what ranking by DLR changes.

    uv run --project tests/benchmark tests/benchmark/science/host_vs_tns.py \\
        --uri "$PROD_SNAPSHOT_URI" --db boom --survey ZTF
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict

import numpy as np
from pymongo import MongoClient


def summarize(dz: list[float]) -> dict:
    a = np.abs(np.asarray(dz, dtype=float))
    if a.size == 0:
        return {"n": 0}
    return {"n": int(a.size), "median": float(np.median(a)), "p90": float(np.percentile(a, 90)),
            "within_0.02": float((a < 0.02).mean()), "within_0.05": float((a < 0.05).mean()),
            "outliers_gt_0.15": float((a > 0.15).mean())}


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--uri", required=True)
    p.add_argument("--db", default="boom")
    p.add_argument("--survey", default="ZTF")
    p.add_argument("--limit", type=int, default=0, help="0 = every matching object")
    p.add_argument("--out", default=None)
    args = p.parse_args()

    coll = MongoClient(args.uri)[args.db][f"{args.survey}_alerts_aux"]
    query = {"cross_matches.TNS.redshift": {"$gt": 0}, "host_galaxy": {"$exists": True}}
    cursor = coll.find(query, {"cross_matches.TNS": 1, "host_galaxy": 1}, limit=args.limit)

    found_by_type: dict = defaultdict(lambda: [0, 0])
    dz_best: dict = defaultdict(list)
    dz_nearest: list[float] = []
    dz_best_all: list[float] = []
    for d in cursor:
        tns = next((t for t in d["cross_matches"]["TNS"] if (t.get("redshift") or 0) > 0), None)
        if tns is None:
            continue
        z_true = float(tns["redshift"])
        kind = (tns.get("type") or "unclassified").strip() or "unclassified"
        hg = d.get("host_galaxy") or {}
        best = hg.get("best_host")
        found_by_type[kind][1] += 1
        if best:
            found_by_type[kind][0] += 1
            if best.get("z") is not None:
                dz = (best["z"] - z_true) / (1 + z_true)
                dz_best[best.get("catalog") or "unknown"].append(dz)
                dz_best_all.append(dz)
        cands = [c for c in hg.get("candidates") or [] if c.get("z") is not None]
        if cands:
            nearest = min(cands, key=lambda c: c.get("sep_arcsec", np.inf))
            dz_nearest.append((nearest["z"] - z_true) / (1 + z_true))

    total = sum(n for _, n in found_by_type.values())
    found = sum(f for f, _ in found_by_type.values())
    result = {
        "objects_with_tns_redshift": total,
        "host_found_fraction": found / total if total else None,
        "host_found_by_tns_type": {k: {"n": n, "fraction": f / n}
                                   for k, (f, n) in sorted(found_by_type.items(), key=lambda kv: -kv[1][1])
                                   if n >= 20},
        "best_host_dz_over_1pz": summarize(dz_best_all),
        "best_host_dz_by_catalog": {k: summarize(v) for k, v in dz_best.items()},
        "nearest_candidate_dz_over_1pz": summarize(dz_nearest),
    }
    print(json.dumps(result, indent=2))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
