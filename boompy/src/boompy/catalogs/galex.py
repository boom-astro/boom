"""GALEX GUVcat_AIS.

Published as gzipped CSV, read directly by BOOM's csv engine.
"""

from __future__ import annotations

from pathlib import Path

from .base import Chunk, already_complete, ensure_dir
from .http import content_length, download, list_index, log

ID = "galex"

BASE_URL = "http://dolomiti.pha.jhu.edu/uvsky/GUVcat/"
FILE_PATTERN = r".*\.csv\.gz"


def list_chunks() -> list[Chunk]:
    names = list_index(BASE_URL, FILE_PATTERN)
    if not names:
        raise RuntimeError(f"no files matching {FILE_PATTERN!r} at {BASE_URL}")
    log(f"GALEX: {len(names)} source files")
    return [Chunk(id=name, label=name) for name in names]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    url = BASE_URL + chunk_id
    path = ensure_dir(dest) / chunk_id
    size = content_length(url)
    if already_complete(path, size):
        log(f"GALEX: {chunk_id} already downloaded")
        return [path]
    log(f"GALEX: downloading {chunk_id}")
    download(url, path, expected_size=size)
    return [path]
