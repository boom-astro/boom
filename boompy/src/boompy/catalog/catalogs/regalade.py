"""REGALADE v2 galaxies, with distances, redshifts and stellar masses.

Tranin et al. 2026, A&A 706, A284.

Published as one 11.6 GB FITS table on Google Drive, which honors byte ranges,
so each chunk is a slice of rows fetched on its own and wrapped in a FITS header
of its own. That keeps peak disk at one slice rather than the whole file.

Galaxy names repeat (residual duplicates with different distances), so a row is
identified by its position in the file. That is only stable for one file, which
is why the release is pinned by its creation date and a replaced file fails the
ingest rather than shifting every id.
"""

from __future__ import annotations

from pathlib import Path

from astropy.io import fits

from .._fits import fits_to_parquet
from ..base import Chunk, ensure_dir
from ..http import CONNECT_TIMEOUT, READ_TIMEOUT, download, log, session

ID = "regalade"

URL = (
    "https://drive.usercontent.google.com/download"
    "?id=1Qsh9HEYLd3YCVaKVRW4OfOsLpnvuYEgy&export=download&confirm=t"
)

#: DATE-HDU of the table in regalade_v2.fits as published.
RELEASE = "2026-07-14T13:08:16"

#: Both headers, with the ~4.5 kB of VOTable metadata, end by 20 kB.
HEADER_BYTES = 32 * 2880

#: 162 MB per slice, so 72 chunks.
ROWS_PER_CHUNK = 1_000_000

FILENAME = "regalade.parquet"

COLUMNS = [
    "name",
    "gal_ra",
    "gal_dec",
    "D",
    "z",
    "z_err",
    "D_min",
    "D_max",
    "D_input",
    "D_err",
    "D_tmean",
    "ref_D_in",
    "ref_z_in",
    "n_dist",
    "R1",
    "R2",
    "PA",
    "ref_ell",
    "id_cat",
    "gmag",
    "rmag",
    "imag",
    "zmag",
    "W1mag",
    "W2mag",
    "griz_ref",
    "W_ref",
    "EBV",
    "logM",
    "f_reliability",
    "simbad_z",
    "f_simbad_zdiscrepancy",
]


def _header_at(buffer: bytes, offset: int) -> tuple[fits.Header, int]:
    """The header starting at `offset`, and the offset just past it."""
    for card in range(offset, len(buffer), 80):
        if buffer[card : card + 8] == b"END     ":
            end = card + 80
            return (
                fits.Header.fromstring(buffer[offset:end].decode("ascii")),
                -(-end // 2880) * 2880,
            )
    raise RuntimeError(f"REGALADE: no header end in {len(buffer)} bytes")


def table_header(buffer: bytes) -> tuple[fits.Header, int]:
    """The binary table's header, and the byte offset its rows start at."""
    primary, offset = _header_at(buffer, 0)
    data = abs(primary["BITPIX"]) // 8 if primary["NAXIS"] else 0
    for axis in range(1, primary["NAXIS"] + 1):
        data *= primary[f"NAXIS{axis}"]
    header, start = _header_at(buffer, offset + -(-data // 2880) * 2880)

    if header.get("XTENSION") != "BINTABLE":
        raise RuntimeError("REGALADE: the second HDU is not a binary table")
    if header.get("DATE-HDU") != RELEASE:
        raise RuntimeError(
            f"REGALADE: the published file is dated {header.get('DATE-HDU')}, "
            f"not {RELEASE}; row ids are positions in the file, so a new "
            "release needs a new collection"
        )
    published = {header[f"TTYPE{i}"] for i in range(1, header["TFIELDS"] + 1)}
    missing = [c for c in COLUMNS if c not in published]
    if missing:
        raise RuntimeError(f"REGALADE is missing expected columns {missing}")
    return header, start


def _fetch_header() -> tuple[fits.Header, int]:
    response = session().get(
        URL,
        headers={"Range": f"bytes=0-{HEADER_BYTES - 1}"},
        timeout=(CONNECT_TIMEOUT, READ_TIMEOUT),
    )
    response.raise_for_status()
    return table_header(response.content)


def list_chunks() -> list[Chunk]:
    header, _ = _fetch_header()
    rows = header["NAXIS2"]
    log(f"REGALADE: {rows} rows, released {RELEASE}")
    return [
        Chunk(
            id=f"rows-{start:09d}",
            label=f"rows {start} to {min(start + ROWS_PER_CHUNK, rows) - 1}",
        )
        for start in range(0, rows, ROWS_PER_CHUNK)
    ]


def slice_to_fits(header: fits.Header, rows_path: Path, out_path: Path) -> Path:
    """Wrap raw table rows in a FITS file whose header counts only them."""
    width = header["NAXIS1"]
    size = rows_path.stat().st_size
    if size % width:
        raise RuntimeError(f"REGALADE: {size} bytes is not whole rows")
    header = header.copy()
    header["NAXIS2"] = size // width

    with open(out_path, "wb") as out:
        out.write(fits.PrimaryHDU().header.tostring().encode("ascii"))
        out.write(header.tostring().encode("ascii"))
        with open(rows_path, "rb") as rows:
            while block := rows.read(1024 * 1024):
                out.write(block)
        out.write(b"\0" * (-size % 2880))
    return out_path


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    start = int(chunk_id.removeprefix("rows-"))
    header, data_start = _fetch_header()
    width, total = header["NAXIS1"], header["NAXIS2"]
    count = min(ROWS_PER_CHUNK, total - start)
    if start < 0 or count <= 0:
        raise RuntimeError(f"REGALADE has no {chunk_id}; it has {total} rows")

    dest = ensure_dir(dest)
    rows_path = dest / f"{chunk_id}.rows"
    fits_path = dest / f"{chunk_id}.fits"
    parquet_path = dest / FILENAME

    first = data_start + start * width
    log(f"REGALADE: downloading rows {start} to {start + count - 1}")
    download(URL, rows_path, byte_range=(first, first + count * width - 1))
    slice_to_fits(header, rows_path, fits_path)
    rows_path.unlink(missing_ok=True)

    fits_to_parquet(
        fits_path,
        parquet_path,
        COLUMNS,
        row_offset=start,
        label=f"REGALADE {chunk_id}",
    )
    fits_path.unlink(missing_ok=True)
    return [parquet_path]
