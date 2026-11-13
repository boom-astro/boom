"""Time Babamul's skymap search, with and without distance, against a run's alerts.

Each skymap is sent twice: as is, which takes the three-dimensional path when it
carries distance layers, and with those layers removed, which takes the 2D path
over the same credible region. Comparing the two gives the cost of the volume
cut and how many candidates it removes.

Run against a stack left up by `run.py --api --keep-up`:

    uv run --project tests/benchmark tests/benchmark/query/skymap.py \\
        --from-run logs/benchmark/<run> --skymap data/S240618ah_bayestar.fits \\
        --survey ztf --credible-level 0.9 --credible-level 0.95 --repeats 5
"""

from __future__ import annotations

import argparse
import base64
import csv
import io
import json
import math
import statistics
import sys
import time
from pathlib import Path

import httpx
import numpy as np
from astropy.io import fits

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from benchlib import API_HOST_PORT, API_SECRET_KEY, MONGO_HOST_PORT, REPO_ROOT, RESULTS_ROOT  # noqa: E402
from benchlib.babamul import bench_token, ensure_bench_user  # noqa: E402
from benchlib.mongo import connect  # noqa: E402

DIST_COLUMNS = {"DISTMU", "DISTSIGMA", "DISTNORM"}
MAX_WINDOW_DAYS = 7.0  # MOC_SEARCH_MAX_TIME_WINDOW_JD in src/api/routes/babamul/surveys/alerts.rs


def skymap_info(path: Path, levels: list[float]) -> dict:
    with fits.open(path) as hdul:
        hdu = hdul[1]
        names = {c.name.upper() for c in hdu.columns}
        data = hdu.data
        header = hdu.header
        if "UNIQ" in names:
            uniq = np.asarray(data["UNIQ"], dtype=np.int64)
            order = (np.floor(np.log2(uniq / 4) / 2)).astype(int)
            area = 4 * math.pi / (12 * 4.0 ** order)
            dens = np.asarray(data["PROBDENSITY"], dtype=float)
            prob, density = dens * area, dens
        else:
            col = "PROB" if "PROB" in names else hdu.columns[0].name
            prob = np.asarray(data[col], dtype=float).ravel()
            area = np.full(prob.size, 4 * math.pi / prob.size)
            density = prob / area
        idx = np.argsort(density)[::-1]
        cum = np.cumsum(prob[idx]) / prob.sum()
        sr_to_deg2 = (180 / math.pi) ** 2
        areas = {}
        for lvl in levels:
            n = int(np.searchsorted(cum, lvl)) + 1
            areas[str(lvl)] = float(area[idx][:n].sum() * sr_to_deg2)
        return {
            "three_d": DIST_COLUMNS <= names,
            "area_deg2": areas,
            "distmean_mpc": header.get("DISTMEAN"),
            "diststd_mpc": header.get("DISTSTD"),
        }


def without_distance(path: Path) -> bytes:
    with fits.open(path) as hdul:
        hdu = hdul[1]
        cols = [c for c in hdu.columns if c.name.upper() not in DIST_COLUMNS]
        table = fits.BinTableHDU.from_columns(cols, header=hdu.header)
        buf = io.BytesIO()
        fits.HDUList([fits.PrimaryHDU(header=hdul[0].header), table]).writeto(buf)
        return buf.getvalue()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--skymap", action="append", type=Path, default=None,
                   help="FITS skymap (repeatable; default: the skymaps tracked in data/)")
    p.add_argument("--survey", action="append", default=None, help="default: ztf")
    p.add_argument("--credible-level", action="append", type=float, default=None, help="default: 0.9")
    p.add_argument("--from-run", type=Path, default=None, help="search the night that run replayed")
    p.add_argument("--start-jd", type=float, default=None)
    p.add_argument("--end-jd", type=float, default=None)
    p.add_argument("--limit", type=int, default=10000)
    p.add_argument("--repeats", type=int, default=5)
    p.add_argument("--api", default=f"http://localhost:{API_HOST_PORT}")
    p.add_argument("--mongo-port", type=int, default=MONGO_HOST_PORT)
    p.add_argument("--secret", default=API_SECRET_KEY)
    p.add_argument("--out", type=Path, default=RESULTS_ROOT / "query" / "skymap_search.csv")
    args = p.parse_args()

    skymaps = args.skymap or [REPO_ROOT / "data" / "S240618ah_bayestar.fits",
                              REPO_ROOT / "data" / "glg_healpix_all_bn200524211.fits"]
    surveys = args.survey or ["ztf"]
    levels = args.credible_level or [0.9]
    start, end = args.start_jd, args.end_jd
    if args.from_run:
        cuts = json.loads((args.from_run / "run_meta.json").read_text())["cut_jd"]
        start = min(cuts.values()) - 0.5
        end = start + MAX_WINDOW_DAYS - 0.01
    if start is None or end is None:
        p.error("give --from-run or both --start-jd and --end-jd")

    _, db = connect(args.mongo_port)
    ensure_bench_user(db)
    headers = {"Authorization": f"Bearer {bench_token(args.secret)}"}

    rows = []
    with httpx.Client(base_url=args.api, headers=headers, timeout=120) as http:
        for path in skymaps:
            info = skymap_info(path, levels)
            variants = {"2d": base64.b64encode(without_distance(path)).decode()}
            if info["three_d"]:
                variants["3d"] = base64.b64encode(path.read_bytes()).decode()
            for survey in surveys:
                for level in levels:
                    for mode, payload in variants.items():
                        for r in range(args.repeats):
                            body = {"skymap_fits_base64": payload, "credible_level": level,
                                    "start_jd": start, "end_jd": end, "limit": args.limit}
                            t0 = time.perf_counter()
                            resp = http.post(f"/babamul/surveys/{survey}/alerts/skymap-search", json=body)
                            elapsed = time.perf_counter() - t0
                            try:
                                content = resp.json()
                            except ValueError:
                                content = {"message": resp.text[:200]}
                            data = content.get("data") if isinstance(content, dict) else None
                            rows.append({
                                "skymap": path.name, "survey": survey, "credible_level": level, "mode": mode,
                                "repeat": r, "status": resp.status_code, "latency_s": elapsed,
                                "n_results": len(data) if isinstance(data, list) else "",
                                "area_deg2": info["area_deg2"][str(level)],
                                "distmean_mpc": info["distmean_mpc"], "message": content.get("message", ""),
                            })
    args.out.parent.mkdir(parents=True, exist_ok=True)
    with open(args.out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)

    print(f"{'skymap':40s} {'survey':6s} {'CL':>5s} {'mode':4s} {'area deg2':>10s} {'median s':>9s} {'results':>8s}")
    groups: dict = {}
    for r in rows:
        groups.setdefault((r["skymap"], r["survey"], r["credible_level"], r["mode"]), []).append(r)
    for (skymap, survey, level, mode), rs in groups.items():
        # The first request also pays for cold caches; the median of the rest is the steady state.
        warm = [x["latency_s"] for x in rs[1:]] or [rs[0]["latency_s"]]
        print(f"{skymap:40s} {survey:6s} {level:5.2f} {mode:4s} {rs[0]['area_deg2']:10.1f} "
              f"{statistics.median(warm):9.3f} {rs[-1]['n_results']!s:>8s}  {rs[-1]['status']}")
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
