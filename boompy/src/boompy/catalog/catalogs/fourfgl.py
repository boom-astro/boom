"""4FGL-DR4 (Fermi-LAT 14-year source catalog), one FITS file."""

from __future__ import annotations

from pathlib import Path

from .._fits import fits_to_parquet
from ..base import Chunk, already_complete, ensure_dir
from ..http import content_length, download, log

ID = "4fgl-dr4"

URL = "https://fermi.gsfc.nasa.gov/ssc/data/access/lat/14yr_catalog/gll_psc_v35.fit"
FITS_NAME = "gll_psc_v35.fit"
HDU = "LAT_Point_Source_Catalog"
FILENAME = "4fgl_dr4.parquet"

COLUMNS = [
    "Source_Name",
    "DataRelease",
    "RAJ2000",
    "DEJ2000",
    "Conf_95_SemiMajor",
    "Conf_95_SemiMinor",
    "Conf_95_PosAng",
    "Signif_Avg",
    "Flux1000",
    "Unc_Flux1000",
    "Energy_Flux100",
    "Unc_Energy_Flux100",
    "SpectrumType",
    "PL_Index",
    "Variability_Index",
    "Frac_Variability",
    "Flags",
    "CLASS1",
    "CLASS2",
    "ASSOC1",
    "ASSOC2",
    "ASSOC_PROB_BAY",
    "ASSOC_PROB_LR",
    "RA_Counterpart",
    "DEC_Counterpart",
]


def list_chunks() -> list[Chunk]:
    return [Chunk(id="current", label=FITS_NAME)]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    dest = ensure_dir(dest)
    fits_path = dest / FITS_NAME
    parquet_path = dest / FILENAME

    size = content_length(URL)
    if not already_complete(fits_path, size):
        log(f"4FGL-DR4: downloading {FITS_NAME}")
        download(URL, fits_path, expected_size=size)

    fits_to_parquet(fits_path, parquet_path, COLUMNS, hdu=HDU, label="4FGL-DR4")
    fits_path.unlink(missing_ok=True)
    return [parquet_path]
