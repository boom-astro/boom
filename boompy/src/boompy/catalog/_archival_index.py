"""Stable chunks from atomically published spectroscopy revisions."""

import os
import re
from pathlib import Path

from .base import Chunk

_REVISION = re.compile(r"r[0-9]{19}")
_PART = re.compile(r"part-[0-9]{6}\.jsonl\.gz")


def _root() -> Path:
    return (
        Path(os.environ.get("BOOM_CATALOG_DATA_PATH", "data/catalogs"))
        / "staged"
        / "archival-spectroscopy"
    )


def list_chunks(catalog: str) -> list[Chunk]:
    chunks = []
    for directory in sorted(_root().glob("r*")):
        if not _REVISION.fullmatch(directory.name):
            continue
        for file in sorted((directory / catalog).glob("part-*.jsonl.gz")):
            if file.is_file() and _PART.fullmatch(file.name):
                chunks.append(
                    Chunk(id=f"{directory.name}/{file.name}", label=file.name)
                )
    if not chunks:
        raise FileNotFoundError(f"No {catalog} shards under {_root()}")
    return chunks


def fetch_chunk(catalog: str, chunk_id: str) -> list[Path]:
    revision, sep, name = chunk_id.partition("/")
    if (
        not sep
        or not _REVISION.fullmatch(revision)
        or not _PART.fullmatch(name)
    ):
        raise ValueError(f"Invalid {catalog} chunk {chunk_id!r}")
    path = _root() / revision / catalog / name
    if not path.is_file():
        raise FileNotFoundError(path)
    return [path]
