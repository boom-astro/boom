"""The command line BOOM's Rust side drives.

One JSON object on stdout per invocation; everything human-readable on stderr,
where the caller forwards it into the log as it arrives. Keeping the two streams
separate is what lets a multi-hour download report progress without corrupting
the result the caller has to parse.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from pathlib import Path

from . import get
from .base import Chunk
from .http import log


@dataclasses.dataclass(frozen=True)
class ListChunksOutput:
    """What `list-chunks` puts on stdout.

    Named for the struct that reads it -- `ListChunksOutput` in
    `src/catalogs/download.rs` -- because the field names here *are* the wire
    format, and the two halves are only findable together if they agree.
    """

    catalog: str
    chunks: list[Chunk]

    def as_json(self) -> dict:
        return {
            "catalog": self.catalog,
            "chunks": [chunk.as_json() for chunk in self.chunks],
        }


@dataclasses.dataclass(frozen=True)
class FetchChunkOutput:
    """What `fetch-chunk` puts on stdout; `FetchChunkOutput` on the Rust side."""

    catalog: str
    chunk: str
    files: list[Path]

    def as_json(self) -> dict:
        return {
            "catalog": self.catalog,
            "chunk": self.chunk,
            # Absolute, because the caller resolves these against its own
            # working directory, which is not necessarily ours.
            "files": [str(path.resolve()) for path in self.files],
        }


def _get_chunks(args: argparse.Namespace) -> ListChunksOutput:
    catalog = get(args.catalog)
    return ListChunksOutput(catalog=catalog.ID, chunks=catalog.list_chunks())


def _get_chunk_files(args: argparse.Namespace) -> FetchChunkOutput:
    catalog = get(args.catalog)
    files = catalog.fetch_chunk(args.chunk, Path(args.dest))
    if not files:
        raise RuntimeError(
            f"{catalog.ID}: chunk {args.chunk} produced no files"
        )
    return FetchChunkOutput(
        catalog=catalog.ID, chunk=args.chunk, files=[Path(f) for f in files]
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m boompy.catalog",
        description="Enumerate and fetch archival catalog source files for BOOM.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    listing = subparsers.add_parser(
        "list-chunks", help="list every chunk of a catalog, in ingest order"
    )
    listing.add_argument("catalog", help="catalog slug, e.g. 2mass")
    listing.set_defaults(handler=_get_chunks)

    fetch = subparsers.add_parser("fetch-chunk", help="download one chunk")
    fetch.add_argument("catalog", help="catalog slug, e.g. 2mass")
    fetch.add_argument(
        "--chunk", required=True, help="chunk id from list-chunks"
    )
    fetch.add_argument("--dest", required=True, help="directory to write into")
    fetch.set_defaults(handler=_get_chunk_files)

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
    json.dump(result.as_json(), sys.stdout)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
