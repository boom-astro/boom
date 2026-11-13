"""Build the seed database every benchmark run starts from.

The seed is a MongoDB data directory under $BENCH_DATA_ROOT/seed. Each run
copies it (a reflink copy where the filesystem supports one) and starts
mongod on the copy, so loading the fixture is paid once rather than per run.

    seed_db.py start                      # mongod on the seed directory, port 27217
    seed_db.py load-legacy                # the old throughput test's NED + ZTF aux fixture
    seed_db.py restore --archive X.gz     # a mongodump archive, e.g. from slice_fixture.py
    seed_db.py import-json --collection NED --file BOOM.NED.json.gz
    seed_db.py status                     # collections, counts, sizes
    seed_db.py stop                       # clean shutdown; required before a run copies it

slice_fixture.py can also write straight into the running seed
(--target-uri mongodb://...:27217).
"""

import argparse
import sys
import time
from pathlib import Path

from benchlib import DATA_ROOT, DB_NAME, MONGO_PASSWORD, MONGO_USER, REPO_ROOT, SEED_MONGO_PORT
from benchlib.docker import run
from benchlib.mongo import collection_stats, connect, not_yet_detected

CONTAINER = "boom-bench-seed"
MONGO_IMAGE = "mongo:8.2"
LEGACY_RELEASE = "https://github.com/boom-astro/boom/releases/download/test-data-v1"
# Earliest exposure in the public ZTF alerts of 2025-03-11.
LEGACY_FIRST_EXPOSURE_JD = 2460745.6315046


def seed_dir(root: Path) -> Path:
    return root / "seed"


def running() -> bool:
    res = run(["docker", "ps", "-q", "-f", f"name=^{CONTAINER}$"], check=False, capture=True)
    return bool(res.stdout.strip())


def cmd_start(args) -> int:
    if running():
        print(f"{CONTAINER} already running on port {SEED_MONGO_PORT}")
        return 0
    seed = seed_dir(args.data_root)
    seed.mkdir(parents=True, exist_ok=True)
    run(["docker", "rm", "-f", CONTAINER], check=False, capture=True)
    run(["docker", "run", "-d", "--name", CONTAINER,
         "-p", f"127.0.0.1:{SEED_MONGO_PORT}:27017",
         "-v", f"{seed}:/data/db",
         "-v", f"{args.data_root}:/bench",
         "-e", f"MONGO_INITDB_ROOT_USERNAME={MONGO_USER}",
         "-e", f"MONGO_INITDB_ROOT_PASSWORD={MONGO_PASSWORD}",
         MONGO_IMAGE, "--wiredTigerCacheSizeGB", str(args.cache_gb)])
    connect(SEED_MONGO_PORT)
    print(f"seed mongod up on 127.0.0.1:{SEED_MONGO_PORT}, data in {seed}")
    return 0


def cmd_stop(args) -> int:
    if running():
        # A clean shutdown leaves a data directory a run can copy without recovery.
        run(["docker", "stop", "-t", "300", CONTAINER])
    # -v: the mongo image declares /data/configdb a volume, which would otherwise outlive the container.
    run(["docker", "rm", "-f", "-v", CONTAINER], check=False, capture=True)
    print("seed stopped")
    return 0


def _in_container(*cmd: str) -> None:
    run(["docker", "exec", CONTAINER, *cmd])


def _bench_path(path: Path, data_root: Path) -> str:
    """Path of a host file as the seed container sees it (it mounts $BENCH_DATA_ROOT at /bench)."""
    path = path.resolve()
    try:
        return "/bench/" + str(path.relative_to(data_root.resolve()))
    except ValueError:
        sys.exit(f"{path} must be under {data_root} so the seed container can read it")


def cmd_restore(args) -> int:
    _require_running()
    container_path = _bench_path(args.archive, args.data_root)
    cmd = ["mongorestore", f"--uri=mongodb://{MONGO_USER}:{MONGO_PASSWORD}@localhost:27017/?authSource=admin",
           "--gzip", f"--archive={container_path}", f"--numInsertionWorkersPerCollection={args.workers}"]
    if args.ns_from:
        cmd += [f"--nsFrom={args.ns_from}.*", f"--nsTo={DB_NAME}.*"]
    if args.drop:
        cmd.append("--drop")
    _in_container(*cmd)
    return 0


def cmd_import_json(args) -> int:
    _require_running()
    container_path = _bench_path(args.file, args.data_root)
    uri = f"mongodb://{MONGO_USER}:{MONGO_PASSWORD}@localhost:27017/{DB_NAME}?authSource=admin"
    reader = f"gunzip -c {container_path}" if str(args.file).endswith(".gz") else f"cat {container_path}"
    _in_container("bash", "-c", f"{reader} | mongoimport '{uri}' --collection {args.collection} "
                                f"--drop {'--jsonArray' if args.json_array else ''} "
                                f"--numInsertionWorkers {args.workers}")
    _, db = connect(SEED_MONGO_PORT)
    db[args.collection].create_index([("coordinates.radec_geojson", "2dsphere")])
    print(f"{args.collection}: {db[args.collection].estimated_document_count()} documents")
    return 0


def cmd_load_legacy(args) -> int:
    """The fixture of tests/throughput: NED-LVS and the ZTF history of 2025-03-11's objects."""
    _require_running()
    staging = args.data_root / "legacy"
    staging.mkdir(parents=True, exist_ok=True)
    files = {
        "BOOM.NED.json.gz": REPO_ROOT / "data" / "alerts" / "BOOM.NED.json.gz",
        "boom_throughput.ZTF_alerts_aux.dump.gz": REPO_ROOT / "data" / "alerts" / "boom_throughput.ZTF_alerts_aux.dump.gz",
    }
    for name, local in files.items():
        target = staging / name
        if target.exists():
            continue
        if local.exists():
            run(["cp", str(local), str(target)])
        else:
            run(["wget", "-q", "-O", str(target), f"{LEGACY_RELEASE}/{name}"])
    cmd_import_json(argparse.Namespace(file=staging / "BOOM.NED.json.gz", collection="NED", json_array=True,
                                       workers=args.workers, data_root=args.data_root))
    cmd_restore(argparse.Namespace(archive=staging / "boom_throughput.ZTF_alerts_aux.dump.gz",
                                   ns_from="boom_throughput", drop=True, workers=args.workers,
                                   data_root=args.data_root))
    _, db = connect(SEED_MONGO_PORT)
    db["ZTF_alerts_aux"].create_index([("coordinates.radec_geojson", "2dsphere")])
    print(f"ZTF_alerts_aux: {db['ZTF_alerts_aux'].estimated_document_count()} documents (expected 27948)")
    if args.remove_new_objects:
        # 4860 of the fixture's objects were first detected on the replayed
        # night. With their documents present the replay updates them instead
        # of creating them, and never cross-matches or associates a host.
        res = db["ZTF_alerts_aux"].delete_many(not_yet_detected(LEGACY_FIRST_EXPOSURE_JD))
        print(f"removed {res.deleted_count} objects first detected on 2025-03-11 (expected 4860)")
    return 0


def cmd_status(args) -> int:
    _require_running()
    _, db = connect(SEED_MONGO_PORT)
    total = 0
    for s in collection_stats(db):
        size = s.get("total_size_bytes") or 0
        total += size
        print(f"{s['collection']:40s} {s.get('count') or 0:>14,} docs {size / 1e9:10.2f} GB")
    print(f"{'total':40s} {'':>19s} {total / 1e9:10.2f} GB")
    return 0


def _require_running() -> None:
    if not running():
        sys.exit("seed mongod is not running; start it with: seed_db.py start")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--data-root", type=Path, default=DATA_ROOT)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("start")
    s.add_argument("--cache-gb", type=float, default=8)
    sub.add_parser("stop")
    sub.add_parser("status")
    s = sub.add_parser("restore")
    s.add_argument("--archive", type=Path, required=True)
    s.add_argument("--ns-from", default=None, help="source database name inside the archive, if not "
                                                    f"{DB_NAME}")
    s.add_argument("--drop", action="store_true")
    s.add_argument("--workers", type=int, default=4)
    s = sub.add_parser("import-json")
    s.add_argument("--file", type=Path, required=True)
    s.add_argument("--collection", required=True)
    s.add_argument("--json-array", action="store_true")
    s.add_argument("--workers", type=int, default=4)
    s = sub.add_parser("load-legacy")
    s.add_argument("--workers", type=int, default=4)
    s.add_argument("--remove-new-objects", action="store_true",
                   help="drop the objects first detected on the replayed night, so the replay creates them "
                        "(and cross-matches them) as production did; omit to reproduce the old test exactly")
    args = p.parse_args()
    args.data_root = args.data_root.resolve()
    return {
        "start": cmd_start, "stop": cmd_stop, "status": cmd_status, "restore": cmd_restore,
        "import-json": cmd_import_json, "load-legacy": cmd_load_legacy,
    }[args.cmd](args)


if __name__ == "__main__":
    t0 = time.time()
    code = main()
    print(f"done in {time.time() - t0:.0f} s")
    raise SystemExit(code)
