"""NED Local Volume Sample.

One ~1.2 GB FITS table, so there is exactly one chunk. `Current` always
redirects to the latest release; since 2026-04-24 that carries the galaxy
angular diameter columns (Diam, Diam_ba, Diam_pa, ...) the reader expects.

The download is converted to parquet before it is handed over. BOOM reads one
columnar format, and `astropy` -- already here for the archives -- is what reads
a FITS table; teaching Rust to do it meant linking cfitsio into every BOOM
binary to answer "give me this column as f64".
"""

from __future__ import annotations

import re
from pathlib import Path

import requests

from .base import Chunk, already_complete, ensure_dir
from .http import CONNECT_TIMEOUT, READ_TIMEOUT, download, log, session

URL = "https://ned.ipac.caltech.edu/NED::LVS/fits/Current/"
FITS_FILENAME = "ned_lvs.fits"
FILENAME = "ned_lvs.parquet"

#: Columns BOOM stores, out of the ~30 NED-LVS publishes. Named here because
#: this is where the projection belongs -- the reader asks for these by name and
#: fails loudly, naming the column, if one stops being emitted.
COLUMNS = [
    "objname",
    "ra",
    "dec",
    "objtype",
    "z",
    "z_unc",
    "z_tech",
    "z_qual",
    "z_refcode",
    "DistMpc",
    "DistMpc_unc",
    "DistMpc_method",
    "Diam",
    "Diam_ra",
    "Diam_dec",
    "Diam_ba",
    "Diam_pa",
    "Diam_survey",
    "Diam_filt",
    "Diam_refcode",
    "Diam_qual",
    "ebv",
    "m_Ks",
    "m_Ks_unc",
    "tMASSphot",
    "Mstar",
    "Mstar_unc",
    "MLratio",
]


def _to_parquet(fits_path: Path, out_path: Path) -> Path:
    """Convert the published FITS table to parquet, keeping only COLUMNS.

    Projected before conversion rather than after: NED-LVS is a couple of
    million rows, and carrying every column through pandas costs memory for
    data that is dropped immediately.
    """
    from astropy.table import Table

    table = Table.read(fits_path, hdu=1)
    missing = [c for c in COLUMNS if c not in table.colnames]
    if missing:
        raise RuntimeError(
            f"NED LVS is missing expected columns {missing}; "
            "the published schema may have changed"
        )
    frame = table[COLUMNS].to_pandas()
    # astropy hands back fixed-width bytes for FITS character columns; parquet
    # wants text, and the reader trims the padding.
    for name, dtype in frame.dtypes.items():
        if dtype == object:
            frame[name] = frame[name].apply(
                lambda v: v.decode("utf-8", "replace") if isinstance(v, bytes) else v
            )
    frame.to_parquet(out_path, index=False)
    log(f"NED LVS: converted {len(frame)} rows to {out_path.name}")
    return out_path


def _head() -> tuple[int | None, str | None]:
    """Advertised size and release name for the current file."""
    response = session().head(
        URL, allow_redirects=True, timeout=(CONNECT_TIMEOUT, READ_TIMEOUT)
    )
    response.raise_for_status()
    size = response.headers.get("content-length")
    # NED serves the release-stamped name (e.g. NEDLVS_20260424.fits) here.
    match = re.search(r"filename=([^\s;]+)", response.headers.get("content-disposition", ""))
    return (int(size) if size else None, match.group(1) if match else None)


ID = "ned-lvs"


def list_chunks() -> list[Chunk]:
    _, release = _head()
    log(f"NED LVS current release: {release or 'unknown'}")
    # A single chunk, with a fixed id. Using the release name as the id would be
    # more informative but would make every new release look like an unfinished
    # chunk of the old one rather than a fresh ingest.
    return [Chunk(id="current", label=release or "current release")]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    size, release = _head()
    dest = ensure_dir(dest)
    fits_path = dest / FITS_FILENAME
    parquet_path = dest / FILENAME

    if not already_complete(fits_path, size):
        log(f"NED LVS: downloading {release or FITS_FILENAME} ({size or 'unknown'} bytes)")
        download(URL, fits_path, expected_size=size)
    else:
        log(f"NED LVS: {release or FITS_FILENAME} already downloaded")

    _to_parquet(fits_path, parquet_path)
    # The FITS original is a gigabyte and nothing downstream reads it. Dropping
    # it here keeps peak disk at one chunk, which is the point of chunking.
    fits_path.unlink(missing_ok=True)
    return [parquet_path]
