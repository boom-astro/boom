"""Stage Pan-STARRS DR2 stack morphology joined to PS1-STRM by objID."""

from __future__ import annotations

import os
from pathlib import Path

from ..base import Chunk

ID = "panstarrs-hosts"


def staged_dir() -> Path:
    return (
        Path(os.getenv("BOOM_CATALOG_DATA_PATH", "data/catalogs"))
        / "staged"
        / ID
    )


def list_chunks() -> list[Chunk]:
    directory = staged_dir()
    files = sorted(
        path.name for path in directory.glob("*.jsonl.gz") if path.is_file()
    )
    if not files:
        raise RuntimeError(
            f"Stage panstarrs-hosts .jsonl.gz chunks in {directory}."
        )
    return [Chunk(id=name, label=name) for name in files]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    if Path(chunk_id).name != chunk_id or not chunk_id.endswith(".jsonl.gz"):
        raise ValueError(f"Invalid panstarrs-hosts chunk name: {chunk_id}")
    path = staged_dir() / chunk_id
    if not path.is_file():
        raise RuntimeError(f"Pan-STARRS host chunk is missing: {path}")
    return [path]
