"""Gaia DR3, from the Flatiron mirror of the GaiaSource CSV dumps.

~3400 gzipped CSV files, one per chunk. Left as CSV rather than converted:
BOOM's csv engine reads gzip directly, and running roughly a terabyte through
pandas purely to change container would be a full extra pass over the catalog
for nothing.
"""

from __future__ import annotations

from pathlib import Path

from .base import Chunk, already_complete, ensure_dir
from .http import content_length, download, list_index, log

ID = "gaia-dr3"

BASE_URL = "https://sdsc-users.flatironinstitute.org/~gaia/dr3/csv/GaiaSource/"
FILE_PATTERN = r"GaiaSource_.*\.csv\.gz"


def list_chunks() -> list[Chunk]:
    names = list_index(BASE_URL, FILE_PATTERN)
    if not names:
        raise RuntimeError(f"no files matching {FILE_PATTERN!r} at {BASE_URL}")
    log(f"Gaia DR3: {len(names)} source files")
    return [Chunk(id=name, label=name) for name in names]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    url = BASE_URL + chunk_id
    path = ensure_dir(dest) / chunk_id
    size = content_length(url)
    if already_complete(path, size):
        log(f"Gaia DR3: {chunk_id} already downloaded")
        return [path]
    log(f"Gaia DR3: downloading {chunk_id}")
    download(url, path, expected_size=size)
    return [path]
