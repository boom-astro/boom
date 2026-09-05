"""Shared FITS-to-parquet conversion.

BOOM reads one columnar format. Anything published as a FITS table is converted
here, where astropy already lives, rather than by linking cfitsio into every
BOOM binary to answer "give me this column as f64".
"""

from __future__ import annotations

from pathlib import Path

from .http import log


def fits_to_parquet(
    fits_path: Path,
    out_path: Path,
    columns: list[str],
    hdu: int = 1,
    row_filter=None,
    label: str = "",
) -> Path:
    """Convert `hdu` of a FITS table to parquet, keeping `columns`.

    `row_filter` takes the astropy Table and returns a boolean mask, for
    catalogs where most rows should never reach the database at all -- filtering
    before conversion rather than after keeps the parquet, the transfer and the
    ingest all proportional to what is actually stored.
    """
    from astropy.table import Table

    table = Table.read(fits_path, hdu=hdu)
    missing = [c for c in columns if c not in table.colnames]
    if missing:
        raise RuntimeError(
            f"{label or fits_path.name} is missing expected columns {missing}; "
            "the published schema may have changed"
        )
    if row_filter is not None:
        before = len(table)
        table = table[row_filter(table)]
        log(f"{label}: kept {len(table)} of {before} rows")

    frame = table[columns].to_pandas()
    # astropy hands back fixed-width bytes for FITS character columns; parquet
    # wants text, and the reader trims the padding.
    for name, dtype in frame.dtypes.items():
        if dtype == object:
            frame[name] = frame[name].apply(
                lambda v: v.decode("utf-8", "replace") if isinstance(v, bytes) else v
            )
    frame.to_parquet(out_path, index=False)
    log(f"{label}: converted {len(frame)} rows to {out_path.name}")
    return out_path
