"""How well objects are linked across surveys: rate, offsets, and chance matches.

For each pair of surveys, over a sample of objects of the first:

  * the fraction with a counterpart in the second (`aliases`);
  * the angular offset between linked objects;
  * the chance-coincidence rate: the same lookup BOOM does (nearest object
    within the match radius), at positions shifted by --shift-arcsec, where any
    match is spurious.

Runs against any database with the survey collections, typically a production
snapshot:

    uv run --project tests/benchmark tests/benchmark/science/cross_survey_matching.py \\
        --uri "$PROD_SNAPSHOT_URI" --db boom --sample 50000
"""

from __future__ import annotations

import argparse
import json
import math
import random

import numpy as np
from pymongo import MongoClient

# Match radius per pair: the larger positional uncertainty (src/alert/*.rs).
UNCERTAINTY_ARCSEC = {"ZTF": 2.0, "LSST": 0.1, "DECAM": 1.24, "WINTER": 2.0}


def radec(doc) -> tuple[float, float]:
    lon, lat = doc["coordinates"]["radec_geojson"]["coordinates"]
    return lon + 180.0, lat


def sep_arcsec(ra1, dec1, ra2, dec2) -> float:
    r1, d1, r2, d2 = map(math.radians, (ra1, dec1, ra2, dec2))
    s = math.sin((d2 - d1) / 2) ** 2 + math.cos(d1) * math.cos(d2) * math.sin((r2 - r1) / 2) ** 2
    return math.degrees(2 * math.asin(min(1.0, math.sqrt(s)))) * 3600


def nearest_within(coll, ra, dec, radius_arcsec):
    q = {"coordinates.radec_geojson": {"$nearSphere": [ra - 180.0, dec],
                                       "$maxDistance": math.radians(radius_arcsec / 3600)}}
    return coll.find_one(q, {"_id": 1})


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--uri", required=True)
    p.add_argument("--db", default="boom")
    p.add_argument("--surveys", default="ZTF,LSST,DECAM")
    p.add_argument("--sample", type=int, default=20000)
    p.add_argument("--shift-arcsec", type=float, default=30.0)
    p.add_argument("--out", default=None, help="write the results as JSON")
    args = p.parse_args()

    db = MongoClient(args.uri)[args.db]
    surveys = [s for s in args.surveys.split(",") if f"{s}_alerts_aux" in db.list_collection_names()]
    rng = random.Random(0)
    results = {}
    for a in surveys:
        sample = list(db[f"{a}_alerts_aux"].aggregate([
            {"$sample": {"size": args.sample}},
            {"$project": {"coordinates": 1, "aliases": 1}},
        ]))
        for b in surveys:
            if a == b:
                continue
            coll_b = db[f"{b}_alerts_aux"]
            radius = max(UNCERTAINTY_ARCSEC[a], UNCERTAINTY_ARCSEC[b])
            linked = [d for d in sample if (d.get("aliases") or {}).get(b)]
            seps = []
            for d in linked[:5000]:
                other = coll_b.find_one({"_id": d["aliases"][b][0]}, {"coordinates": 1})
                if other:
                    seps.append(sep_arcsec(*radec(d), *radec(other)))
            spurious = 0
            probes = sample[:5000]
            for d in probes:
                ra, dec = radec(d)
                angle = rng.uniform(0, 2 * math.pi)
                shift = args.shift_arcsec / 3600
                ra2 = ra + shift * math.cos(angle) / max(math.cos(math.radians(dec)), 1e-6)
                dec2 = max(-90.0, min(90.0, dec + shift * math.sin(angle)))
                spurious += nearest_within(coll_b, ra2 % 360, dec2, radius) is not None
            s = np.asarray(seps)
            results[f"{a}->{b}"] = {
                "match_radius_arcsec": radius,
                "sampled": len(sample),
                "linked_fraction": len(linked) / len(sample) if sample else None,
                "offset_arcsec": {f"p{q}": float(np.percentile(s, q)) for q in (50, 90, 99)} if s.size else None,
                "chance_match_rate": spurious / len(probes) if probes else None,
            }
            print(f"{a}->{b}: {json.dumps(results[f'{a}->{b}'])}")
    if args.out:
        with open(args.out, "w") as f:
            json.dump(results, f, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
