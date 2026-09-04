"""The command line BOOM's Rust side drives.

One JSON object on stdout per invocation; everything human-readable on stderr,
where the caller forwards it into the log as it arrives. Keeping the two streams
separate is what lets a multi-hour download report progress without corrupting
the result the caller has to parse.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from . import get
from .http import log


def _list_chunks(args: argparse.Namespace) -> dict:
    catalog = get(args.catalog)
    chunks = catalog.list_chunks()
    return {"catalog": catalog.ID, "chunks": [c.as_json() for c in chunks]}


def _fetch_chunk(args: argparse.Namespace) -> dict:
    catalog = get(args.catalog)
    files = catalog.fetch_chunk(args.chunk, Path(args.dest))
    if not files:
        raise RuntimeError(f"{catalog.ID}: chunk {args.chunk} produced no files")
    # Absolute, because the caller resolves these against its own working
    # directory, which is not necessarily ours.
    return {
        "catalog": catalog.ID,
        "chunk": args.chunk,
        "files": [str(Path(f).resolve()) for f in files],
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m boompy.catalogs",
        description="Enumerate and fetch archival catalog source files for BOOM.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    listing = subparsers.add_parser(
        "list-chunks", help="list every chunk of a catalog, in ingest order"
    )
    listing.add_argument("catalog", help="catalog slug, e.g. 2mass")
    listing.set_defaults(handler=_list_chunks)

    fetch = subparsers.add_parser("fetch-chunk", help="download one chunk")
    fetch.add_argument("catalog", help="catalog slug, e.g. 2mass")
    fetch.add_argument("--chunk", required=True, help="chunk id from list-chunks")
    fetch.add_argument("--dest", required=True, help="directory to write into")
    fetch.set_defaults(handler=_fetch_chunk)

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        result = args.handler(args)
    except Exception as e:
        # The caller reports the exit status and the stderr tail, so the message
        # has to be on stderr -- a traceback alone leaves it with nothing useful
        # to put in the log.
        log(f"error: {type(e).__name__}: {e}")
        raise
    json.dump(result, sys.stdout)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
