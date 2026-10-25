import numpy as np
import pyarrow.parquet as pq
import pytest
from astropy.io import fits

from boompy.catalog._fits import fits_to_parquet
from boompy.catalog.catalogs import desi_dr1_dwarfs
from boompy.catalog.catalogs.desi_dr1_dwarfs import COLUMNS, HDU

#: Columns and FITS formats of the real MAIN table.
PUBLISHED = {
    "TARGETID": "K",
    "SURVEY": "7A",
    "PROGRAM": "6A",
    "HEALPIX": "J",
    "Z": "D",
    "DELTACHI2": "D",
    "ZWARN": "J",
    "Z_CMB": "D",
    "RA": "D",
    "DEC": "D",
    "RA_TARGET": "D",
    "DEC_TARGET": "D",
    "DESINAME": "22A",
    "LUMI_DIST_MPC": "D",
    "LOG_MSTAR_M24": "D",
    "LOG_MSTAR_M24_ERR": "D",
    "MAG_G": "D",
    "MAG_R": "D",
    "MAG_Z": "D",
    "MAG_G_TARGET": "D",
    "MAG_R_TARGET": "D",
    "MAG_Z_TARGET": "D",
    "DWARF_MASKBIT": "J",
    "MSTAR_MASKBIT": "J",
    "MAG_TYPE": "13A",
    "PHOTOMETRY_UPDATED": "L",
    "R50_R": "D",
    "SHAPE_PARAMS": "2D",
    "IN_SGA_2020": "L",
    "IS_BGS_BRIGHT": "L",
    "IS_BGS_FAINT": "L",
    "IS_LOWZ": "L",
    "IS_ELG": "L",
    "IS_OTHER": "L",
    "ASSOCIATED_TARGETIDS": "PK()",
    "DWARF_PRIMARY_TARGETID": "K",
    "DWARF_PRIMARY": "L",
    "DIST_SOURCE": "10A",
    "PROPERTY_SOURCE_TARGETID": "K",
}


def _value(fmt, rows):
    if fmt.endswith("A"):
        return np.array(["x"] * rows)
    if fmt == "L":
        return np.ones(rows, dtype=bool)
    if fmt == "2D":
        return np.zeros((rows, 2))
    if fmt == "PK()":
        return np.array([np.arange(2, dtype=np.int64)] * rows, dtype=object)
    return np.arange(rows)


def _table(tmp_path, columns=PUBLISHED, primary=(True, False, True)):
    rows = len(primary)
    cols = []
    for name, fmt in columns.items():
        array = (
            np.array(primary) if name == "DWARF_PRIMARY" else _value(fmt, rows)
        )
        cols.append(fits.Column(name=name, format=fmt, array=array))
    path = tmp_path / desi_dr1_dwarfs.FITS_NAME
    fits.HDUList(
        [fits.PrimaryHDU(), fits.BinTableHDU.from_columns(cols, name=HDU)]
    ).writeto(path, overwrite=True)
    return path


def test_the_projection_only_names_published_columns():
    unknown = set(COLUMNS) - set(PUBLISHED)
    assert not unknown, (
        f"projection names columns the MAIN table does not have: {unknown}"
    )


def test_conversion_keeps_the_projected_columns_and_only_primary_rows(tmp_path):
    out = fits_to_parquet(
        _table(tmp_path),
        tmp_path / "d.parquet",
        COLUMNS,
        hdu=HDU,
        row_filter=desi_dr1_dwarfs._primary_rows,
        label="DESI DR1 dwarfs",
    )
    table = pq.read_table(out)
    assert set(table.schema.names) == set(COLUMNS)
    assert table.num_rows == 2


def test_a_dropped_published_column_fails_loudly(tmp_path):
    without_mass = {k: v for k, v in PUBLISHED.items() if k != "LOG_MSTAR_M24"}
    with pytest.raises(RuntimeError, match="'LOG_MSTAR_M24'"):
        fits_to_parquet(
            _table(tmp_path, columns=without_mass),
            tmp_path / "d.parquet",
            COLUMNS,
            hdu=HDU,
            label="DESI DR1 dwarfs",
        )


def test_the_file_is_read_from_the_staging_directory(tmp_path, monkeypatch):
    monkeypatch.setenv(desi_dr1_dwarfs.DATA_PATH_ENV, str(tmp_path))
    with pytest.raises(RuntimeError, match="stage the FITS file"):
        desi_dr1_dwarfs.list_chunks()

    staged = desi_dr1_dwarfs.staged_path()
    staged.parent.mkdir(parents=True)
    _table(staged.parent)
    assert [c.id for c in desi_dr1_dwarfs.list_chunks()] == ["current"]
    [out] = desi_dr1_dwarfs.fetch_chunk("current", tmp_path / "dl")
    assert pq.read_table(out).num_rows == 2
    assert staged.is_file()
