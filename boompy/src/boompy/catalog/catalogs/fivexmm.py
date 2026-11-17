"""5XMM-DR15 source table: 2 GB decompressed, so read memory-mapped."""

from __future__ import annotations

import gzip
import shutil
from pathlib import Path

from .._fits import fits_to_parquet
from ..base import Chunk, already_complete, ensure_dir
from ..http import BLOCK_SIZE, content_length, download, log

ID = "5xmm-dr15"

URL = (
    "https://heasarc.gsfc.nasa.gov/FTP/xmm/data/catalogues/"
    "5XMM_DR15cat_source_v1.0.fits.gz"
)
ARCHIVE = "5XMM_DR15cat_source_v1.0.fits.gz"
FITS_NAME = "5XMM_DR15cat_source_v1.0.fits"
FILENAME = "5xmm_dr15.parquet"

COLUMNS = [
    "SRCID",
    "IAUNAME",
    "RA",
    "DEC",
    "RADEC_ERR",
    "N_OBS",
    "MJD_FIRST",
    "MJD_LAST",
    "EP_FLUX",
    "EP_FLUX_ERR",
    "EP_DET_ML",
    "EP_HR1",
    "EP_HR2",
    "EP_HR3",
    "EP_HR4",
    "EXTENT",
    "EXTENT_ML",
    "SUM_FLAG",
    "VAR_FLAG",
    "FVAR",
    "VAR_PROB",
    "FRATIO",
    "CLASSX_CLASS",
    "CLASSX_PROB_AGN",
    "CLASSX_PROB_STAR",
    "CLASSX_PROB_GAL_XRB",
    "CLASSX_PROB_GAL_CV",
]


def list_chunks() -> list[Chunk]:
    return [Chunk(id="current", label=ARCHIVE)]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    dest = ensure_dir(dest)
    archive_path = dest / ARCHIVE
    fits_path = dest / FITS_NAME
    parquet_path = dest / FILENAME

    size = content_length(URL)
    if not already_complete(archive_path, size):
        log(f"5XMM-DR15: downloading {ARCHIVE}")
        download(URL, archive_path, expected_size=size)

    with gzip.open(archive_path) as source, open(fits_path, "wb") as out:
        shutil.copyfileobj(source, out, BLOCK_SIZE)
    archive_path.unlink(missing_ok=True)
    fits_to_parquet(
        fits_path, parquet_path, COLUMNS, memmap=True, label="5XMM-DR15"
    )
    fits_path.unlink(missing_ok=True)
    return [parquet_path]
