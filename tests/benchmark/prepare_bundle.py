"""Turn a directory of archived Avro alerts into a replay bundle.

Works for the archives BOOM's own producer reads (ZTF nights from the UW
archive, and DECam/WINTER nights placed under data/alerts/<survey>/<date>):

    uv run --project tests/benchmark tests/benchmark/prepare_bundle.py \\
        --survey ztf --night 20250311 \\
        --source data/alerts/ztf/public/20250311 \\
        --out ~/boom-bench/bundles/ztf-20250311-public

Archived packets carry no broker timestamp, so these bundles replay on the
exposure clock. Partnership packets go to their own topic, so a night with both
programs is prepared as one bundle per program directory, or both directories
can be passed to --source.

LSST has no archive in this form; capture a live night with capture.py.
"""

import argparse
import sys
from pathlib import Path

from benchlib import normalize_survey
from benchlib.bundle import BundleWriter, describe_packet, topic_for


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--survey", required=True, help="ztf, decam or winter")
    p.add_argument("--night", required=True, help="night label YYYYMMDD; sets the replay topic names")
    p.add_argument("--source", required=True, nargs="+", type=Path, help="director(ies) of .avro files")
    p.add_argument("--out", required=True, type=Path, help="bundle directory to create")
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--limit", type=int, default=None, help="stop after this many packets (for smoke tests)")
    args = p.parse_args()

    survey = normalize_survey(args.survey)
    if survey == "LSST":
        p.error("LSST packets are captured from a live stream; use capture.py")

    files = sorted(f for d in args.source for f in d.rglob("*.avro"))
    if not files:
        p.error(f"no .avro files under {args.source}")

    with BundleWriter(args.out, survey, args.night, source=", ".join(map(str, args.source)),
                      overwrite=args.overwrite) as writer:
        for i, path in enumerate(files):
            if args.limit is not None and writer.count >= args.limit:
                break
            payload = path.read_bytes()
            try:
                info = describe_packet(payload, survey)
            except Exception as e:
                writer.skipped += 1
                print(f"skipping {path}: {e}", file=sys.stderr)
                continue
            writer.add(payload, topic=topic_for(survey, args.night, info["programid"]), **info)
            if (i + 1) % 50000 == 0:
                print(f"{i + 1}/{len(files)} packets read")
        writer.close({"clock": "exposure"})
        print(f"wrote {writer.count} packets ({writer.skipped} skipped) to {args.out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
