"""AAVSO International Variable Star Index (VSX).

One fixed-width `vsx.dat` from the CDS mirror, so exactly one chunk. Left as
text rather than converted: BOOM's ascii engine reads the fixed columns
directly, and the file's layout is not self-describing enough for a generic
converter to improve on that.
"""

from __future__ import annotations

from pathlib import Path

from .base import Chunk, already_complete, ensure_dir
from .http import content_length, download, log

ID = "vsx"

URL = "https://cdsarc.cds.unistra.fr/ftp/B/vsx/vsx.dat"
FILENAME = "vsx.dat"


def list_chunks() -> list[Chunk]:
    return [Chunk(id="current", label=FILENAME)]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    path = ensure_dir(dest) / FILENAME
    size = content_length(URL)
    if already_complete(path, size):
        log(f"VSX: {FILENAME} already downloaded")
        return [path]
    log(f"VSX: downloading {FILENAME}")
    download(URL, path, expected_size=size)
    return [path]
