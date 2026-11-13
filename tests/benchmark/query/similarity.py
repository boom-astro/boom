"""Similarity search: latency through the API, and recall of the HNSW index.

Latency is measured on the public Babamul route at several concurrencies.
Recall@k compares Milvus's approximate neighbors (the index parameters BOOM
uses: HNSW M=16, efConstruction=200, default search parameters) with an exact
cosine search over every stored public vector, for a sample of seeds.

Against the local Milvus of a run left up with `--profile milvus --api --keep-up`:

    uv run --project tests/benchmark tests/benchmark/query/similarity.py --seeds 200 --exact-seeds 50

Against NRP (read-only), pass --milvus-uri https://milvus.nrp-nautilus.io:50051
with --milvus-token user:password and --milvus-db umn_babamul_vectordb.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import random
import statistics
import sys
import time
from pathlib import Path

import httpx
import numpy as np
from pymilvus import Collection, connections

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchlib import API_HOST_PORT, MILVUS_HOST_PORT, RESULTS_ROOT  # noqa: E402

PUBLIC = "programid == 1"  # what the API searches (src/api/routes/embeddings.rs)


def sample_ids(coll: Collection, n: int, rng: random.Random) -> list[str]:
    ids = [r["object_id"] for r in coll.query(expr=PUBLIC, output_fields=["object_id"], limit=min(16384, n * 20))]
    return rng.sample(ids, min(n, len(ids)))


async def api_latency(base: str, seeds: list[str], top_k: int, concurrency: int) -> list[tuple]:
    sem = asyncio.Semaphore(concurrency)
    out = []
    async with httpx.AsyncClient(base_url=base, timeout=60) as http:
        async def one(seed):
            async with sem:
                t0 = time.perf_counter()
                resp = await http.post("/babamul/similarity/objects", json={"object_id": seed, "top_k": top_k})
                out.append((seed, top_k, concurrency, resp.status_code, time.perf_counter() - t0))
        await asyncio.gather(*(one(s) for s in seeds))
    return out


def all_public_vectors(coll: Collection, batch: int = 10000) -> tuple[list[str], np.ndarray]:
    ids, vecs = [], []
    it = coll.query_iterator(batch_size=batch, expr=PUBLIC, output_fields=["object_id", "embedding"])
    while True:
        res = it.next()
        if not res:
            it.close()
            break
        for r in res:
            ids.append(r["object_id"])
            vecs.append(r["embedding"])
    return ids, np.asarray(vecs, dtype=np.float32)


def recall(coll: Collection, ids: list[str], X: np.ndarray, seeds: list[str], k: int) -> list[float]:
    index = {oid: i for i, oid in enumerate(ids)}
    out = []
    for seed in seeds:
        if seed not in index:
            continue
        q = X[index[seed]]
        scores = X @ q  # vectors are unit length, so this is cosine similarity
        scores[index[seed]] = -np.inf
        exact = {ids[i] for i in np.argpartition(-scores, k)[:k]}
        hits = coll.search(data=[q.tolist()], anns_field="embedding", param={"metric_type": "COSINE", "params": {}},
                           limit=k + 1, expr=PUBLIC, output_fields=["object_id"])[0]
        approx = [h.entity.get("object_id") for h in hits if h.entity.get("object_id") != seed][:k]
        out.append(len(exact & set(approx)) / k)
    return out


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--api", default=f"http://localhost:{API_HOST_PORT}")
    p.add_argument("--milvus-uri", default=f"http://localhost:{MILVUS_HOST_PORT}")
    p.add_argument("--milvus-token", default="")
    p.add_argument("--milvus-db", default="default")
    p.add_argument("--collection", default="boom_ztf_fusion_embeddings")
    p.add_argument("--seeds", type=int, default=200)
    p.add_argument("--top-k", type=int, action="append", default=None, help="default: 10 and 100")
    p.add_argument("--concurrency", type=int, action="append", default=None, help="default: 1, 8, 32")
    p.add_argument("--exact-seeds", type=int, default=50, help="seeds checked against exact search (0 = skip)")
    p.add_argument("--out", type=Path, default=RESULTS_ROOT / "query" / "similarity.csv")
    args = p.parse_args()

    rng = random.Random(0)
    connections.connect(uri=args.milvus_uri, token=args.milvus_token, db_name=args.milvus_db)
    coll = Collection(args.collection)
    coll.load()
    print(f"{coll.num_entities} vectors in {args.collection}")
    seeds = sample_ids(coll, args.seeds, rng)

    rows = []
    for k in args.top_k or [10, 100]:
        for c in args.concurrency or [1, 8, 32]:
            res = asyncio.run(api_latency(args.api, seeds, k, c))
            rows += res
            lat = [r[4] for r in res if r[3] == 200]
            errors = sum(r[3] != 200 for r in res)
            if lat:
                print(f"top_k={k:3d} concurrency={c:3d}: median {statistics.median(lat) * 1e3:7.1f} ms, "
                      f"p95 {np.percentile(lat, 95) * 1e3:7.1f} ms, errors {errors}")
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["seed", "top_k", "concurrency", "status", "latency_s"])
        w.writerows(rows)

    if args.exact_seeds:
        ids, X = all_public_vectors(coll)
        for k in args.top_k or [10, 100]:
            r = recall(coll, ids, X, seeds[: args.exact_seeds], k)
            if r:
                print(f"recall@{k}: mean {np.mean(r):.4f}, min {np.min(r):.4f} over {len(r)} seeds")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
