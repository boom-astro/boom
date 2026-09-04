"""NED Local Volume Sample.

One ~1.2 GB FITS table, so there is exactly one chunk. `Current` always
redirects to the latest release; since 2026-04-24 that carries the galaxy
angular diameter columns (Diam, Diam_ba, Diam_pa, ...) the Rust reader expects.
"""

from __future__ import annotations

import re
from pathlib import Path

import requests

from .base import Chunk, already_complete, ensure_dir
from .http import CONNECT_TIMEOUT, READ_TIMEOUT, download, log

URL = "https://ned.ipac.caltech.edu/NED::LVS/fits/Current/"
FILENAME = "ned_lvs.fits"


def _head() -> tuple[int | None, str | None]:
    """Advertised size and release name for the current file."""
    response = requests.head(
        URL, allow_redirects=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
    )
    response.raise_for_status()
    size = response.headers.get("content-length")
    # NED serves the release-stamped name (e.g. NEDLVS_20260424.fits) here.
    match = re.search(r"filename=([^\s;]+)", response.headers.get("content-disposition", ""))
    return (int(size) if size else None, match.group(1) if match else None)


ID = "ned-lvs"


def list_chunks() -> list[Chunk]:
    _, release = _head()
    log(f"NED LVS current release: {release or 'unknown'}")
    # A single chunk, with a fixed id. Using the release name as the id would be
    # more informative but would make every new release look like an unfinished
    # chunk of the old one rather than a fresh ingest.
    return [Chunk(id="current", label=release or "current release")]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    size, release = _head()
    path = ensure_dir(dest) / FILENAME
    if already_complete(path, size):
        log(f"NED LVS: {release or FILENAME} already downloaded")
        return [path]
    log(f"NED LVS: downloading {release or FILENAME} ({size or 'unknown'} bytes)")
    download(URL, path, expected_size=size)
    return [path]
