"""CatWISE2020 catalog.

Published as ~700 gzipped IPAC `.tbl` tables under per-declination directories.
One table per chunk; each is converted to parquet, since a `.tbl` is a
fixed-width IPAC format that only astropy reads.
"""

from __future__ import annotations

from pathlib import Path

from .base import Chunk, already_complete, ensure_dir
from .http import content_length, download, list_index, log

ID = "catwise2020"

BASE_URL = "https://portal.nersc.gov/project/cosmo/data/CatWISE/2020/"
DIR_PATTERN = r"\d+\.\d+/"
FILE_PATTERN = r".*\.tbl\.gz"

COLUMNS = [
    "source_id", "source_name", "ra", "dec", "sigra", "sigdec",
    "w1mpro", "w2mpro", "w1sigmpro", "w2sigmpro", "w1rchi2", "w2rchi2",
    "pmra", "pmdec", "sigpmra", "sigpmdec", "unwise_objid",
]


def list_chunks() -> list[Chunk]:
    # The archive nests tables under one directory per declination band, so the
    # listing is two levels deep. The chunk id carries both, which keeps it
    # stable and lets fetch_chunk rebuild the URL without listing again.
    chunks: list[Chunk] = []
    for directory in list_index(BASE_URL, DIR_PATTERN):
        for name in list_index(BASE_URL + directory, FILE_PATTERN):
            chunks.append(Chunk(id=f"{directory}{name}", label=name))
    if not chunks:
        raise RuntimeError(f"no .tbl.gz files found under {BASE_URL}")
    log(f"CatWISE2020: {len(chunks)} source tables")
    return chunks


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    from astropy.table import Table

    dest = ensure_dir(dest)
    name = chunk_id.rsplit("/", 1)[-1]
    tbl_path = dest / name
    parquet_path = dest / f"{name.removesuffix('.tbl.gz')}.parquet"

    url = BASE_URL + chunk_id
    size = content_length(url)
    if not already_complete(tbl_path, size):
        log(f"CatWISE2020: downloading {name}")
        download(url, tbl_path, expected_size=size)

    table = Table.read(tbl_path, format="ipac")
    missing = [c for c in COLUMNS if c not in table.colnames]
    if missing:
        raise RuntimeError(f"{name} is missing expected columns {missing}")
    table[COLUMNS].to_pandas().to_parquet(parquet_path, index=False)
    log(f"CatWISE2020: converted {len(table)} rows to {parquet_path.name}")
    tbl_path.unlink(missing_ok=True)
    return [parquet_path]
