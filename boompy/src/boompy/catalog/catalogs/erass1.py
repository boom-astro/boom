"""eRASS1 Main (eROSITA-DE DR1), one FITS table in a gzipped tar."""

from __future__ import annotations

import shutil
import tarfile
from pathlib import Path

from .._fits import fits_to_parquet
from ..base import Chunk, already_complete, ensure_dir
from ..http import BLOCK_SIZE, content_length, download, log

ID = "erass1-main"

URL = (
    "https://erosita.mpe.mpg.de/dr1/AllSkySurveyData_dr1/Catalogues_dr1/"
    "MerloniA_DR1/eRASS1_Main.v1.2.fits.tar.gz"
)
ARCHIVE = "eRASS1_Main.v1.2.fits.tar.gz"
FITS_NAME = "eRASS1_Main.v1.2.fits"
FILENAME = "erass1_main.parquet"

COLUMNS = [
    "UID",
    "IAUNAME",
    "RA",
    "DEC",
    "POS_ERR",
    "MJD_MIN",
    "MJD_MAX",
    "EXT",
    "EXT_LIKE",
    "DET_LIKE_0",
    "ML_RATE_1",
    "ML_RATE_ERR_1",
    "ML_FLUX_1",
    "ML_FLUX_ERR_1",
    "ML_RATE_P1",
    "ML_RATE_ERR_P1",
    "ML_RATE_P2",
    "ML_RATE_ERR_P2",
    "ML_RATE_P3",
    "ML_RATE_ERR_P3",
    "FLAG_SP_SNR",
    "FLAG_SP_BPS",
    "FLAG_SP_SCL",
    "FLAG_SP_LGA",
    "FLAG_SP_GC_CONS",
    "FLAG_OPT",
]


def _extract_fits(archive_path: Path, fits_path: Path) -> None:
    with tarfile.open(archive_path, "r|gz") as archive:
        for member in archive:
            if member.isfile() and member.name.endswith(".fits"):
                with (
                    archive.extractfile(member) as source,
                    open(fits_path, "wb") as out,
                ):
                    shutil.copyfileobj(source, out, BLOCK_SIZE)
                return
    raise RuntimeError(f"no FITS file inside {archive_path.name}")


def list_chunks() -> list[Chunk]:
    return [Chunk(id="current", label=FITS_NAME)]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    dest = ensure_dir(dest)
    archive_path = dest / ARCHIVE
    fits_path = dest / FITS_NAME
    parquet_path = dest / FILENAME

    size = content_length(URL)
    if not already_complete(archive_path, size):
        log(f"eRASS1: downloading {ARCHIVE}")
        download(URL, archive_path, expected_size=size)

    _extract_fits(archive_path, fits_path)
    archive_path.unlink(missing_ok=True)
    fits_to_parquet(
        fits_path, parquet_path, COLUMNS, memmap=True, label="eRASS1"
    )
    fits_path.unlink(missing_ok=True)
    return [parquet_path]
