"""Stage the archival radial velocity pilot as a single gzipped JSONL chunk."""

import os
from pathlib import Path

from ..base import Chunk

ID = "archival-rv-pilot"
CHUNK = "sources-v1.jsonl.gz"


def _file() -> Path:
    return (
        Path(os.getenv("BOOM_CATALOG_DATA_PATH", "data/catalogs"))
        / "staged"
        / ID
        / CHUNK
    )


def list_chunks() -> list[Chunk]:
    if not _file().is_file():
        raise RuntimeError(f"Stage the pilot JSONL at {_file()}")
    return [Chunk(id=CHUNK, label="Archival RV pilot v1")]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    if chunk_id != CHUNK or not _file().is_file():
        raise RuntimeError(f"Unknown or unavailable pilot chunk: {chunk_id}")
    return [_file()]
