"""Million Quasars (Milliquas) catalog.

One zipped FITS table, so exactly one chunk. Converted to parquet before it is
handed over.
"""

from __future__ import annotations

import zipfile
from pathlib import Path

from ._fits import fits_to_parquet
from .base import Chunk, already_complete, ensure_dir
from .http import content_length, download, log

ID = "milliquas"

URL = "https://quasars.org/milliquas.fits.zip"
FILENAME = "milliquas.parquet"

#: Column names are the published FITS ones. `R` and `B` are the redshift and
#: broad-line classification flags -- short, unhelpful names, but they are what
#: the file uses and renaming them here would only hide the mapping.
COLUMNS = [
    "NAME", "RA", "DEC", "TYPE", "RMAG", "BMAG", "COMMENT",
    "R", "B", "Z", "XNAME", "RNAME", "LOBE1", "LOBE2",
]


def list_chunks() -> list[Chunk]:
    return [Chunk(id="current", label="milliquas.fits")]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    dest = ensure_dir(dest)
    zip_path = dest / "milliquas.fits.zip"
    parquet_path = dest / FILENAME

    size = content_length(URL)
    if not already_complete(zip_path, size):
        log("Milliquas: downloading milliquas.fits.zip")
        download(URL, zip_path, expected_size=size)

    with zipfile.ZipFile(zip_path) as archive:
        names = [n for n in archive.namelist() if n.lower().endswith(".fits")]
        if not names:
            raise RuntimeError(f"no FITS file inside {zip_path.name}")
        archive.extract(names[0], dest)
        fits_path = dest / names[0]

    fits_to_parquet(fits_path, parquet_path, COLUMNS, label="Milliquas")
    # Neither the archive nor the extracted table is read again.
    fits_path.unlink(missing_ok=True)
    zip_path.unlink(missing_ok=True)
    return [parquet_path]
