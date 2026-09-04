"""2MASS Point Source Catalog.

IRSA publishes the all-sky PSC as ~92 gzipped pipe-delimited files, which is
already the right chunking: one file per chunk, ~2 GB uncompressed each.

Format: https://irsa.ipac.caltech.edu/2MASS/download/allsky/format_psc.html
"""

from __future__ import annotations

from pathlib import Path

from .base import Chunk, already_complete, ensure_dir
from .http import content_length, download, list_index, log

BASE_URL = "https://irsa.ipac.caltech.edu/2MASS/download/allsky/"

#: The index also lists checksums and the extended source catalog; only the
#: point source files are wanted here.
FILE_PATTERN = r"psc_.*\.gz"


ID = "2mass"


def list_chunks() -> list[Chunk]:
    names = list_index(BASE_URL, FILE_PATTERN)
    if not names:
        raise RuntimeError(f"no files matching {FILE_PATTERN!r} at {BASE_URL}")
    log(f"2MASS: {len(names)} source files")
    # The filename is the chunk id and it is stable, which is what lets a
    # resumed run match against what it already ingested.
    return [Chunk(id=name, label=name) for name in names]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    url = BASE_URL + chunk_id
    path = ensure_dir(dest) / chunk_id
    size = content_length(url)
    if already_complete(path, size):
        log(f"2MASS: {chunk_id} already downloaded")
        return [path]
    log(f"2MASS: downloading {chunk_id}")
    download(url, path, expected_size=size)
    return [path]
