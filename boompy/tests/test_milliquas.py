"""The Milliquas projection.

Its columns were once guessed rather than read from the published table, which
failed only at ingest time against a 42 MB download. A synthetic FITS table with
the real column names catches that in a second instead.
"""

import numpy as np
import pyarrow.parquet as pq
import pytest
from astropy.io import fits
from astropy.table import Table

from boompy.catalogs._fits import fits_to_parquet
from boompy.catalogs.milliquas import COLUMNS

#: Every column the published milliquas.fits actually has, verified against the
#: real file. BOOM stores a subset; the extras are here so the test proves the
#: projection selects rather than merely accepting whatever it is given.
PUBLISHED = [
    "RA", "DEC", "NAME", "TYPE", "RMAG", "BMAG", "COMMENT",
    "R", "B", "Z", "CITE", "ZCITE", "XNAME", "RNAME", "LOBE1", "LOBE2",
]

TEXT_COLUMNS = {"NAME", "TYPE", "COMMENT", "R", "B", "CITE", "ZCITE",
                "XNAME", "RNAME", "LOBE1", "LOBE2"}


def _table(tmp_path, columns=PUBLISHED, rows=2):
    data = {
        name: (np.array(["x" * 4] * rows, dtype="S4") if name in TEXT_COLUMNS
               else np.arange(rows, dtype=np.float64))
        for name in columns
    }
    path = tmp_path / "milliquas.fits"
    fits.BinTableHDU(Table(data)).writeto(path, overwrite=True)
    return path


def test_the_projection_only_names_published_columns():
    """The regression: `RXPCT` and `QPCT` were invented, and nothing caught it
    until a real ingest failed."""
    unknown = set(COLUMNS) - set(PUBLISHED)
    assert not unknown, f"projection names columns milliquas.fits does not have: {unknown}"


def test_the_classification_flags_are_the_one_letter_columns():
    # Short and unhelpful, but they are what the file uses.
    assert "R" in COLUMNS and "B" in COLUMNS


def test_conversion_keeps_exactly_the_projected_columns(tmp_path):
    out = fits_to_parquet(_table(tmp_path), tmp_path / "m.parquet", COLUMNS, label="Milliquas")
    assert set(pq.read_schema(out).names) == set(COLUMNS)
    # CITE/ZCITE are published but not stored.
    assert "CITE" not in pq.read_schema(out).names


def test_a_dropped_published_column_fails_loudly(tmp_path):
    without_r = [c for c in PUBLISHED if c != "R"]
    with pytest.raises(RuntimeError, match="'R'"):
        fits_to_parquet(_table(tmp_path, columns=without_r), tmp_path / "m.parquet",
                        COLUMNS, label="Milliquas")
