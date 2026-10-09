"""GALEX GUVcat_AIS.

Published as gzipped CSV, read directly by BOOM's csv engine.
"""

from __future__ import annotations

from pathlib import Path

from ..base import Chunk, already_complete, ensure_dir
from ..http import content_length, download, list_index, log

ID = "galex"

#: Plain HTTP because the host does not answer on 443 at all -- not a
#: preference, and not an oversight. GUVcat publishes no checksum alongside the
#: files either, so there is nothing to verify a download against. The rows
#: become crossmatch data, so anyone able to alter the connection could alter
#: what BOOM calls a match; a deployment that cannot accept that should fetch
#: the files out of band, verify them however it likes, and ingest GALEX as a
#: staged catalog instead.
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
