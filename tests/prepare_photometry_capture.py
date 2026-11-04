#!/usr/bin/env python3
"""Index existing Avro packets without rewriting their contents or identities.

An archive does not contain broker arrival times. Without an explicit schedule,
use clearly labeled fixed spacing in filename order; never infer arrival from JD.
"""
import argparse
import hashlib
import json
from pathlib import Path


def prepare(directory, spacing_ms=10, schedule=None):
    paths = sorted(directory.resolve().glob("*.avro"))
    if not paths:
        raise ValueError("no .avro files in corpus directory")
    if spacing_ms < 0:
        raise ValueError("spacing must be nonnegative")
    packets = []
    for index, path in enumerate(paths):
        at_ms = schedule[path.name] if schedule is not None else index * spacing_ms
        if not isinstance(at_ms, int) or isinstance(at_ms, bool) or at_ms < 0:
            raise ValueError(f"arrival milliseconds must be a nonnegative integer: {path.name}")
        packets.append({"path": str(path), "at_ms": at_ms,
                        "sha256": hashlib.sha256(path.read_bytes()).hexdigest()})
    if schedule is not None:
        packets.sort(key=lambda row: row["at_ms"])  # stable order for equal arrivals
    return {"kind": "captured", "packets": packets,
            "provenance": {"content": "existing Avro packets, unchanged",
                           "timing_basis": "supplied_arrival_schedule" if schedule is not None else "synthetic_fixed_spacing_in_filename_order",
                           "spacing_ms": None if schedule is not None else spacing_ms,
                           "total_bytes": sum(path.stat().st_size for path in paths)}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--spacing-ms", type=int, default=10)
    parser.add_argument("--source-label", help="Human-supplied dataset/archive identifier, recorded as provenance")
    parser.add_argument("--schedule", type=Path, help="JSON map from packet basename to recorded arrival milliseconds")
    args = parser.parse_args()
    schedule = json.loads(args.schedule.read_text()) if args.schedule else None
    manifest = prepare(args.directory, args.spacing_ms, schedule)
    if args.source_label:
        manifest["provenance"]["source_label"] = args.source_label
    args.output.write_text(json.dumps(manifest, indent=2) + "\n")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
