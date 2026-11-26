"""2SXPS (Swift-XRT), the clean subset of the sources table."""

from __future__ import annotations

from pathlib import Path

from .._fits import fits_to_parquet
from ..base import Chunk, already_complete, ensure_dir
from ..http import content_length, download, log

ID = "2sxps"

URL = "https://www.swift.ac.uk/2SXPS/2SXPS_Sources_clean.fits.gz"
ARCHIVE = "2SXPS_Sources_clean.fits.gz"
FILENAME = "2sxps.parquet"

COLUMNS = [
    "2SXPS_ID",
    "IAUName",
    "RA",
    "Decl",
    "Err90",
    "AstromType",
    "DetFlag",
    "FieldFlag",
    "NumObs",
    "NumDetObs",
    "FirstDetDate",
    "LastDetDate",
    "Rate_band0",
    "Rate_band0_pos",
    "Rate_band0_neg",
    "PeakRate_band0",
    "HR1",
    "HR2",
    "PvarPchiSnapshot_band0",
    "PvarPchiObsID_band0",
    "PowFlux",
    "PowFlux_pos",
    "PowFlux_neg",
]


def list_chunks() -> list[Chunk]:
    return [Chunk(id="current", label=ARCHIVE)]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    dest = ensure_dir(dest)
    archive_path = dest / ARCHIVE
    parquet_path = dest / FILENAME

    size = content_length(URL)
    if not already_complete(archive_path, size):
        log(f"2SXPS: downloading {ARCHIVE}")
        download(URL, archive_path, expected_size=size)

    fits_to_parquet(archive_path, parquet_path, COLUMNS, label="2SXPS")
    archive_path.unlink(missing_ok=True)
    return [parquet_path]
