"""Stage the ranked repeat-RV output produced by boom-catalogs."""

import os
from pathlib import Path

from ..base import Chunk

ID = "archival-repeat-rv"
FILENAME = "candidates-v1.jsonl.gz"


def _path() -> Path:
    directory = Path(os.environ.get("BOOM_CATALOG_DATA_PATH", "data/catalogs"))
    path = directory / "staged" / ID / FILENAME
    if not path.is_file():
        raise FileNotFoundError(f"Stage the completed repeat-RV search output at {path}")
    return path


def list_chunks() -> list[Chunk]:
    return [Chunk(id=FILENAME, label=_path().name)]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    if chunk_id != FILENAME:
        raise ValueError(f"Unknown {ID} chunk {chunk_id}")
    return [_path()]
