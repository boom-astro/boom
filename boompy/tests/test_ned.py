"""The NED FITS-to-parquet conversion.

This is the seam between the two languages: BOOM's reader asks for these columns
by name and expects text, floats and booleans. A synthetic FITS table is written
here rather than downloading the real 1.2 GB one.
"""

import numpy as np
import pyarrow.parquet as pq
import pytest
from astropy.io import fits
from astropy.table import Table

from boompy.catalogs.ned import COLUMNS, _to_parquet


def _fits_table(tmp_path, columns=None, rows=2):
    """A FITS binary table shaped like NED-LVS."""
    columns = COLUMNS if columns is None else columns
    data = {}
    for name in columns:
        if name in ("objname", "objtype", "z_tech", "z_refcode", "DistMpc_method",
                    "Diam_survey", "Diam_filt", "Diam_refcode", "tMASSphot"):
            # FITS pads character columns out to the declared width.
            data[name] = np.array(["NGC 1234    "[: 12]] * rows, dtype="S12")
        elif name in ("z_qual", "Diam_qual"):
            data[name] = np.array([True, False][:rows] * (rows // 2 or 1), dtype=bool)
        else:
            data[name] = np.arange(rows, dtype=np.float64)
    path = tmp_path / "ned_lvs.fits"
    fits.BinTableHDU(Table(data)).writeto(path, overwrite=True)
    return path


def test_conversion_emits_every_column_the_reader_asks_for(tmp_path):
    out = _to_parquet(_fits_table(tmp_path), tmp_path / "ned.parquet")
    schema = pq.read_schema(out)
    assert set(schema.names) == set(COLUMNS)


def test_conversion_decodes_fits_byte_strings(tmp_path):
    """astropy hands back fixed-width bytes for character columns; parquet wants
    text, and a bytes column would arrive as binary the reader cannot read."""
    out = _to_parquet(_fits_table(tmp_path), tmp_path / "ned.parquet")
    table = pq.read_table(out)
    assert table.schema.field("objname").type == "string"
    assert table.column("objname")[0].as_py().strip() == "NGC 1234"


def test_conversion_keeps_types_the_reader_can_coerce(tmp_path):
    out = _to_parquet(_fits_table(tmp_path), tmp_path / "ned.parquet")
    schema = pq.read_schema(out)
    assert schema.field("ra").type in ("double", "float")
    assert schema.field("z_qual").type == "bool"


def test_conversion_drops_columns_boom_does_not_store(tmp_path):
    """NED-LVS publishes ~30 columns and BOOM stores 28; carrying the rest
    through pandas costs memory for data dropped immediately."""
    extra = [*COLUMNS, "some_unused_column"]
    out = _to_parquet(_fits_table(tmp_path, columns=extra), tmp_path / "ned.parquet")
    assert "some_unused_column" not in pq.read_schema(out).names


def test_a_missing_published_column_fails_loudly(tmp_path):
    """If NED changes its schema, that has to stop the ingest rather than
    quietly produce a catalog with a column of nulls."""
    without_diam = [c for c in COLUMNS if c != "Diam_ba"]
    with pytest.raises(RuntimeError, match="Diam_ba"):
        _to_parquet(_fits_table(tmp_path, columns=without_diam), tmp_path / "ned.parquet")
