"""Local archival spectrum objects staged by boom-catalogs."""

from pathlib import Path

from .. import _archival_index
from ..base import Chunk

ID = "archival-spectrum-objects"


def list_chunks() -> list[Chunk]:
    return _archival_index.list_chunks(ID)


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    return _archival_index.fetch_chunk(ID, chunk_id)
