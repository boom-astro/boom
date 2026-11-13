"""Concurrent API clients, run alongside a replay to see whether user queries slow ingestion.

Each client loops over the requests a Babamul user makes most: fetching an
object page and searching a small cone. Object ids are sampled from objects
already in the database, as a user would look up known sources. Started by
run.py --api-load N; stops when --stop-file appears or --duration runs out.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import random
import sys
import time
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchlib import API_HOST_PORT, API_SECRET_KEY, MONGO_HOST_PORT  # noqa: E402
from benchlib.babamul import bench_token, ensure_bench_user  # noqa: E402
from benchlib.mongo import connect  # noqa: E402


def sample_objects(db, surveys: list[str], n: int) -> list[tuple[str, str, float, float]]:
    out = []
    for s in surveys:
        coll = db[f"{s}_alerts_aux"]
        for d in coll.aggregate([{"$sample": {"size": n}}, {"$project": {"coordinates.radec_geojson": 1}}]):
            lon, lat = d["coordinates"]["radec_geojson"]["coordinates"]
            out.append((s, d["_id"], lon + 180.0, lat))
    return out


async def client(i: int, base: str, token: str, targets, stop: asyncio.Event, rows: list):
    headers = {"Authorization": f"Bearer {token}"}
    rng = random.Random(i)
    async with httpx.AsyncClient(base_url=base, headers=headers, timeout=60) as http:
        while not stop.is_set():
            survey, oid, ra, dec = rng.choice(targets)
            if rng.random() < 0.7:
                endpoint, req = "object", http.get(f"/babamul/surveys/{survey.lower()}/objects/{oid}")
            else:
                endpoint = "alerts_cone_search"
                req = http.post(f"/babamul/surveys/{survey.lower()}/alerts/cone-search",
                                json={"coordinates": {oid: [ra, dec]}, "radius_arcsec": 10.0})
            t0 = time.perf_counter()
            try:
                resp = await req
                status = resp.status_code
            except httpx.HTTPError:
                status = 599
            rows.append((time.time(), i, endpoint, status, time.perf_counter() - t0))


async def run(args) -> None:
    _, db = connect(args.mongo_port)
    ensure_bench_user(db)
    targets = sample_objects(db, args.surveys.split(","), args.sample)
    if not targets:
        print("no objects to query", file=sys.stderr)
        return
    token = bench_token(args.secret)
    stop = asyncio.Event()
    rows: list = []
    tasks = [asyncio.create_task(client(i, args.api, token, targets, stop, rows)) for i in range(args.clients)]
    t_end = time.time() + args.duration if args.duration else None
    while not stop.is_set():
        await asyncio.sleep(1)
        if (args.stop_file and args.stop_file.exists()) or (t_end and time.time() > t_end):
            stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)
    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["t", "client", "endpoint", "status", "latency_s"])
        w.writerows(rows)
    print(f"{len(rows)} requests written to {args.out}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--api", default=f"http://localhost:{API_HOST_PORT}")
    p.add_argument("--mongo-port", type=int, default=MONGO_HOST_PORT)
    p.add_argument("--secret", default=API_SECRET_KEY)
    p.add_argument("--surveys", default="ZTF")
    p.add_argument("--clients", type=int, default=8)
    p.add_argument("--sample", type=int, default=2000, help="objects sampled per survey")
    p.add_argument("--duration", type=float, default=None)
    p.add_argument("--stop-file", type=Path, default=None)
    p.add_argument("--out", type=Path, default=Path("api_load.csv"))
    asyncio.run(run(p.parse_args()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
