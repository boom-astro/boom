"""Do an object's nearest neighbors in embedding space share its class?

Seeds are objects whose TNS cross-match carries a type. For each, the public
similarity route returns its neighbors; those that also have a TNS type are
compared with the seed's. Reported per class as purity (fraction of labeled
neighbors sharing the class) against the base rate of that class among
labeled objects, which is what a random neighbor would score.

Needs the database and the API of the same deployment (Milvus holds the
vectors, Mongo the labels):

    uv run --project tests/benchmark tests/benchmark/science/similarity_purity.py \\
        --uri "$PROD_SNAPSHOT_URI" --db boom --api https://<babamul api> --per-class 100
"""

from __future__ import annotations

import argparse
import json
import random
from collections import Counter, defaultdict

import httpx
from pymongo import MongoClient


def tns_type(doc) -> str | None:
    for t in (doc.get("cross_matches") or {}).get("TNS") or []:
        kind = (t.get("type") or "").strip()
        if kind:
            return kind
    return None


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--uri", required=True)
    p.add_argument("--db", default="boom")
    p.add_argument("--api", required=True)
    p.add_argument("--per-class", type=int, default=100)
    p.add_argument("--min-class-size", type=int, default=30)
    p.add_argument("--top-k", type=int, default=20)
    p.add_argument("--out", default=None)
    args = p.parse_args()

    coll = MongoClient(args.uri)[args.db]["ZTF_alerts_aux"]
    labeled = {}
    for d in coll.find({"cross_matches.TNS.type": {"$nin": [None, ""]}}, {"cross_matches.TNS": 1}):
        kind = tns_type(d)
        if kind:
            labeled[d["_id"]] = kind
    counts = Counter(labeled.values())
    base_rate = {k: n / len(labeled) for k, n in counts.items()}
    by_class = defaultdict(list)
    for oid, kind in labeled.items():
        by_class[kind].append(oid)

    rng = random.Random(0)
    results = {}
    with httpx.Client(base_url=args.api, timeout=60) as http:
        for kind, ids in sorted(by_class.items(), key=lambda kv: -len(kv[1])):
            if len(ids) < args.min_class_size:
                continue
            same = total = seeds_used = 0
            for seed in rng.sample(ids, min(args.per_class, len(ids))):
                resp = http.post("/babamul/similarity/objects", json={"object_id": seed, "top_k": args.top_k})
                if resp.status_code != 200:
                    continue  # no stored embedding for this object
                seeds_used += 1
                for hit in resp.json().get("data") or []:
                    other = labeled.get(hit.get("object_id"))
                    if other:
                        total += 1
                        same += other == kind
            results[kind] = {"labeled_objects": len(ids), "seeds": seeds_used, "labeled_neighbors": total,
                             "purity": same / total if total else None, "base_rate": base_rate[kind]}
            print(f"{kind:25s} {json.dumps(results[kind])}")
    if args.out:
        with open(args.out, "w") as f:
            json.dump(results, f, indent=2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
