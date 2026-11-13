"""Cut the database state a replay needs out of a production snapshot.

Every per-alert database operation BOOM performs is keyed by object id or by a
small cone around the alert, so a run only ever reads:

  * the objects its alerts belong to, with their full histories;
  * other surveys' objects near those alerts (cross-survey matching reads and
    links them, and filters look them up);
  * catalog rows within each catalog's search radius of the alerts.

This copies exactly that, rolled back to the moment the replay starts: each
survey's photometry from its first replayed exposure onward is removed, objects
first detected after that moment are left out (the replay creates them), and
aliases pointing at such objects are dropped. Missing any of these makes the
replay cheaper than the night really was.

The source should be a snapshot taken before the night (then the rollback is a
no-op), or any later one. Without a window the slice covers the whole night,
which also serves every run whose window starts at the night's start
(--window-start 0).

    uv run --project tests/benchmark tests/benchmark/slice_fixture.py \\
        --bundle ~/boom-bench/bundles/ztf-20261115 --bundle ~/boom-bench/bundles/lsst-20261115 \\
        --source-uri "$PROD_SNAPSHOT_URI" --source-db boom \\
        --config config/prod/umn/config.yaml --dry-run

Then without --dry-run, writing into the running seed (seed_db.py start).
"""

from __future__ import annotations

import argparse
import json
import math
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import numpy as np
import yaml
from pymongo import IndexModel, MongoClient
from pymongo.errors import BulkWriteError

from benchlib import DB_NAME, REPO_ROOT, SEED_MONGO_PORT, SURVEYS, mongo_uri
from benchlib.config import crossmatch_catalogs
from benchlib.schedule import cut_jd_per_survey
from replay import add_plan_args, resolve_plan

PHOTOMETRY_ARRAYS = ("prv_candidates", "prv_nondetections", "fp_hists")
DEFAULT_CATALOG_RADII = {"PS1_DR2": 60.0}  # host photometry is fetched at the host, not the alert
BATCH = 500


def arcsec_to_rad(a: float) -> float:
    return a / 3600.0 * math.pi / 180.0


def cone_query(ra: float, dec: float, radius_arcsec: float) -> dict:
    # BOOM stores longitude as ra - 180 (src/utils/spatial.rs).
    return {"coordinates.radec_geojson": {"$geoWithin": {"$centerSphere": [[ra - 180.0, dec],
                                                                           arcsec_to_rad(radius_arcsec)]}}}


def ids_near(coll, positions: np.ndarray, radius_arcsec: float, workers: int, label: str) -> set:
    found: set = set()
    lock = threading.Lock()
    done = [0]
    t0 = time.time()

    def work(pos):
        ids = [d["_id"] for d in coll.find(cone_query(float(pos[0]), float(pos[1]), radius_arcsec), {"_id": 1})]
        with lock:
            found.update(ids)
            done[0] += 1
            if done[0] % 20000 == 0:
                print(f"  {label}: {done[0]}/{len(positions)} cones, {len(found)} ids, {time.time() - t0:.0f} s")

    with ThreadPoolExecutor(workers) as pool:
        list(pool.map(work, positions))
    return found


def footprint_cones(positions: np.ndarray, radius_deg: float) -> tuple[np.ndarray, float]:
    """One cone per occupied HEALPix cell, so a dense field is not queried once per alert.

    Returns the cell centers and the cone radius in degrees: `radius_deg` plus
    the cell's reach, so an alert at a cell's corner still gets its full margin.
    """
    import astropy.units as u
    from astropy_healpix import HEALPix

    nside = 1
    while HEALPix(nside=nside * 2).pixel_resolution.to_value(u.deg) >= radius_deg / 2:
        nside *= 2
    hp = HEALPix(nside=nside, order="nested")
    pix = np.unique(hp.lonlat_to_healpix(positions[:, 0] * u.deg, positions[:, 1] * u.deg))
    lon, lat = hp.healpix_to_lonlat(pix)
    # pixel_resolution is the square root of the cell area; a cell's corner can sit ~1.2x that from its center.
    reach = 1.2 * hp.pixel_resolution.to_value(u.deg)
    return np.column_stack([lon.to_value(u.deg), lat.to_value(u.deg)]), radius_deg + reach


def existed_before(doc: dict, cut_jd: float) -> bool:
    return any((c.get("jd") or math.inf) < cut_jd for c in doc.get("prv_candidates") or [])


def roll_back(doc: dict, cut_jd: float) -> dict:
    for field in PHOTOMETRY_ARRAYS:
        if isinstance(doc.get(field), list):
            doc[field] = [p for p in doc[field] if not isinstance(p, dict) or (p.get("jd") or -math.inf) < cut_jd]
    return doc


def prune_aliases(doc: dict, kept: dict[str, set]) -> dict:
    aliases = doc.get("aliases")
    if isinstance(aliases, dict):
        doc["aliases"] = {s: [i for i in ids if i in kept.get(s, set())] if isinstance(ids, list) else ids
                          for s, ids in aliases.items()}
    return doc


def copy_indexes(src_coll, dst_coll) -> None:
    models = []
    for spec in src_coll.list_indexes():
        if spec["name"] == "_id_":
            continue
        opts = {k: v for k, v in spec.items() if k not in ("key", "v", "ns")}
        models.append(IndexModel(list(spec["key"].items()), **opts))
    if models:
        dst_coll.create_indexes(models)


def insert(dst_coll, docs: list[dict]) -> None:
    if not docs:
        return
    try:
        dst_coll.insert_many(docs, ordered=False)
    except BulkWriteError as e:
        dupes = [w for w in e.details.get("writeErrors", []) if w.get("code") == 11000]
        if len(dupes) != len(e.details.get("writeErrors", [])):
            raise


def fetch_by_ids(coll, ids: list, projection=None):
    for i in range(0, len(ids), BATCH):
        yield from coll.find({"_id": {"$in": ids[i:i + BATCH]}}, projection)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    add_plan_args(p)
    p.add_argument("--source-uri", required=True)
    p.add_argument("--source-db", default="boom")
    p.add_argument("--target-uri", default=mongo_uri(SEED_MONGO_PORT))
    p.add_argument("--target-db", default=DB_NAME)
    p.add_argument("--config", type=Path, default=REPO_ROOT / "config.yaml",
                   help="config whose crossmatch catalogs and radii to slice for (use the deployment's)")
    p.add_argument("--neighbor-radius-arcsec", type=float, default=3.0,
                   help="other surveys' objects this close to an alert are copied (above BOOM's match radius)")
    p.add_argument("--footprint-radius-deg", type=float, default=0.0,
                   help="also copy every stored object within this radius of the alerts; keeps index "
                        "density realistic at the cost of size (0 = off)")
    p.add_argument("--catalog-whole-below", type=int, default=5_000_000,
                   help="catalogs with fewer rows are copied whole")
    p.add_argument("--catalog-radius", action="append", default=[], metavar="NAME=ARCSEC")
    p.add_argument("--skip-catalogs", action="store_true")
    p.add_argument("--copy-whole", action="append", default=["MPC_orbits"],
                   help="collections copied as they are (default: MPC_orbits)")
    p.add_argument("--workers", type=int, default=16)
    p.add_argument("--dry-run", action="store_true", help="count what would be copied, write nothing")
    p.add_argument("--report", type=Path, default=None)
    args = p.parse_args()

    _, plan, clock = resolve_plan(args)
    plan = plan[(plan["object_id"] != "") & plan["ra"].notna()]
    cuts = cut_jd_per_survey(plan)
    global_cut = min(cuts.values())
    print(f"clock={clock}; replay cut per survey (JD): {json.dumps(cuts)}")

    source = MongoClient(args.source_uri)[args.source_db]
    target = None if args.dry_run else MongoClient(args.target_uri)[args.target_db]
    src_names = set(source.list_collection_names())
    report: dict = {"cuts_jd": cuts, "collections": {}}

    objects = plan.drop_duplicates(["survey", "object_id"])
    positions_by_survey = {s: g[["ra", "dec"]].to_numpy(float) for s, g in objects.groupby("survey")}
    all_positions = np.unique(np.round(objects[["ra", "dec"]].to_numpy(float), 7), axis=0)
    footprint, footprint_radius_deg = (footprint_cones(all_positions, args.footprint_radius_deg)
                                       if args.footprint_radius_deg > 0 else (None, 0.0))

    # Pass 1: which object ids each survey contributes, and which of them existed at the cut.
    candidates: dict[str, set] = {}
    for s in SURVEYS:
        name = f"{s}_alerts_aux"
        if name not in src_names:
            continue
        coll = source[name]
        ids = set(objects.loc[objects["survey"] == s, "object_id"])
        others = [positions_by_survey[o] for o in positions_by_survey if o != s]
        if others:
            ids |= ids_near(coll, np.vstack(others), args.neighbor_radius_arcsec, args.workers, f"{name} neighbors")
        if footprint is not None:
            ids |= ids_near(coll, footprint, footprint_radius_deg * 3600.0, args.workers, f"{name} footprint")
        candidates[s] = ids

    kept: dict[str, set] = {}
    for s, ids in candidates.items():
        cut = cuts.get(s, global_cut)
        coll = source[f"{s}_alerts_aux"]
        kept[s] = {d["_id"] for d in fetch_by_ids(coll, list(ids), {"prv_candidates.jd": 1})
                   if existed_before(d, cut)}
        replayed = set(objects.loc[objects["survey"] == s, "object_id"])
        report["collections"][f"{s}_alerts_aux"] = {
            "candidates": len(ids), "existed_at_cut": len(kept[s]),
            "replayed_objects": len(replayed), "replayed_objects_already_known": len(replayed & kept[s]),
            "cut_jd": cut,
        }
        print(f"{s}_alerts_aux: {len(ids)} candidates, {len(kept[s])} existed at the cut "
              f"({len(replayed & kept[s])}/{len(replayed)} replayed objects already known)")

    # Pass 2: copy them, rolled back and with aliases pruned to what exists.
    if target is not None:
        for s, ids in kept.items():
            name = f"{s}_alerts_aux"
            cut = cuts.get(s, global_cut)
            target.drop_collection(name)
            buf = []
            for doc in fetch_by_ids(source[name], list(ids)):
                buf.append(prune_aliases(roll_back(doc, cut), kept))
                if len(buf) >= BATCH:
                    insert(target[name], buf)
                    buf = []
            insert(target[name], buf)
            copy_indexes(source[name], target[name])

    # Catalogs.
    if not args.skip_catalogs:
        config = yaml.safe_load(args.config.read_text())
        radii = crossmatch_catalogs(config, sorted(cuts))
        host = config.get("host_galaxy") or {}
        if host.get("enabled"):
            ls_radius = radii.get(host.get("ls_dr10_catalog", "LSDR10"), 30.0)
            for key in ("ned_catalog", "ls_dr10_catalog", "ls_dr9_catalog", "ps1_catalog"):
                if host.get(key):
                    radii.setdefault(host[key], ls_radius)
        radii.update(DEFAULT_CATALOG_RADII)
        for item in args.catalog_radius:
            k, v = item.split("=", 1)
            radii[k] = float(v)
        for name, radius in sorted(radii.items()):
            if name not in src_names:
                print(f"catalog {name}: not in the source database")
                report["collections"][name] = {"missing_in_source": True}
                continue
            n = source[name].estimated_document_count()
            whole = n <= args.catalog_whole_below or radius >= 600.0
            if whole:
                ids = None
                print(f"catalog {name}: {n} rows, copied whole")
            else:
                ids = ids_near(source[name], all_positions, radius, args.workers, name)
                print(f"catalog {name}: {len(ids)} of {n} rows within {radius:g}\" of the alerts")
            report["collections"][name] = {"source_rows": n, "copied_rows": n if whole else len(ids),
                                           "radius_arcsec": radius, "whole": whole}
            if target is None:
                continue
            target.drop_collection(name)
            cursor = source[name].find({}) if whole else fetch_by_ids(source[name], list(ids))
            buf = []
            for doc in cursor:
                buf.append(doc)
                if len(buf) >= 5000:
                    insert(target[name], buf)
                    buf = []
            insert(target[name], buf)
            copy_indexes(source[name], target[name])

    for name in args.copy_whole:
        if name not in src_names:
            continue
        n = source[name].estimated_document_count()
        report["collections"][name] = {"source_rows": n, "copied_rows": n, "whole": True}
        print(f"{name}: {n} rows, copied whole")
        if target is None:
            continue
        target.drop_collection(name)
        buf = []
        for doc in source[name].find({}):
            buf.append(doc)
            if len(buf) >= 5000:
                insert(target[name], buf)
                buf = []
        insert(target[name], buf)
        copy_indexes(source[name], target[name])

    report["dry_run"] = args.dry_run
    out = json.dumps(report, indent=2, default=str)
    if args.report:
        args.report.write_text(out + "\n")
    print(out)
    return 0


if __name__ == "__main__":
    sys.exit(main())
