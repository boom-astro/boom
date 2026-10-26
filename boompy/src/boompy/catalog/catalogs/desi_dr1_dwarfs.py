"""Unpublished DESIRT compilation of DESI DR1 dwarfs, staged by hand at <catalog data path>/staged/desi-dr1-dwarfs/."""

from __future__ import annotations

import os
from pathlib import Path

from .._fits import fits_to_parquet
from ..base import Chunk, ensure_dir

ID = "desi-dr1-dwarfs"

DATA_PATH_ENV = "BOOM_CATALOG_DATA_PATH"
FITS_NAME = "desi_dr1_dwarf_catalog.fits"
HDU = "MAIN"
FILENAME = "desi_dr1_dwarfs.parquet"

COLUMNS = [
    "TARGETID",
    "SURVEY",
    "PROGRAM",
    "RA",
    "DEC",
    "DESINAME",
    "Z",
    "Z_CMB",
    "ZWARN",
    "DELTACHI2",
    "LUMI_DIST_MPC",
    "DIST_SOURCE",
    "LOG_MSTAR_M24",
    "LOG_MSTAR_M24_ERR",
    "MAG_G",
    "MAG_R",
    "MAG_Z",
    "R50_R",
    "DWARF_MASKBIT",
    "MSTAR_MASKBIT",
    "IN_SGA_2020",
]


def staged_path() -> Path:
    root = Path(os.getenv(DATA_PATH_ENV, "data/catalogs"))
    return root / "staged" / ID / FITS_NAME


def _primary_rows(table):
    return table["DWARF_PRIMARY"]


def list_chunks() -> list[Chunk]:
    path = staged_path()
    if not path.is_file():
        raise RuntimeError(
            f"{path} is missing. This catalog is not published: stage the FITS "
            "file there by hand before ingesting."
        )
    return [Chunk(id="current", label=FITS_NAME)]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    dest = ensure_dir(dest)
    fits_to_parquet(
        staged_path(),
        dest / FILENAME,
        COLUMNS,
        hdu=HDU,
        row_filter=_primary_rows,
        label="DESI DR1 dwarfs",
    )
    return [dest / FILENAME]
