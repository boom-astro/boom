"""DESI DR1 spectroscopic redshifts, from the iron zcatalog.

One large FITS table, filtered down hard before conversion: the published
catalog has ~30 million rows and BOOM stores only the primary spectrum per real
target.
"""

from __future__ import annotations

from pathlib import Path

from ._fits import fits_to_parquet
from .base import Chunk, already_complete, ensure_dir
from .http import content_length, download, log

ID = "desi-dr1"

URL = (
    "https://data.desi.lbl.gov/public/dr1/spectro/redux/iron/"
    "zcatalog/v1/zall-tilecumulative-iron.fits"
)
FILENAME = "desi_dr1.parquet"

COLUMNS = [
    "TARGETID", "TARGET_RA", "TARGET_DEC", "SURVEY", "PROGRAM", "Z", "ZERR",
    "ZWARN", "CHI2", "DELTACHI2", "SPECTYPE", "SUBTYPE", "ZCAT_NSPEC",
]


def _primary_science_rows(table):
    """One best spectrum per target, science fibers only.

    Negative TARGETIDs are sky and calibration fibers: not unique, no real
    source behind them, and often carrying non-finite coordinates that the
    2dsphere index would reject outright.
    """
    return (table["ZCAT_PRIMARY"]) & (table["TARGETID"] > 0)


def list_chunks() -> list[Chunk]:
    return [Chunk(id="current", label="zall-tilecumulative-iron.fits")]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    dest = ensure_dir(dest)
    fits_path = dest / "zall-tilecumulative-iron.fits"
    parquet_path = dest / FILENAME

    size = content_length(URL)
    if not already_complete(fits_path, size):
        log("DESI DR1: downloading zall-tilecumulative-iron.fits")
        download(URL, fits_path, expected_size=size)

    fits_to_parquet(
        fits_path,
        parquet_path,
        COLUMNS,
        row_filter=_primary_science_rows,
        label="DESI DR1",
    )
    fits_path.unlink(missing_ok=True)
    return [parquet_path]
