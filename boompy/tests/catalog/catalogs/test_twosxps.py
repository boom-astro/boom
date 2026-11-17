import numpy as np
import pyarrow.parquet as pq
from astropy.io import fits

from boompy.catalog._fits import fits_to_parquet
from boompy.catalog.catalogs.twosxps import COLUMNS

#: Every stored column and a few that are not, out of the 230 in 2SXPS_Sources_clean.fits.gz.
PUBLISHED = {
    "2SXPS_ID": "J",
    "IAUName": "22A",
    "RA": "D",
    "Decl": "D",
    "Err90": "E",
    "AstromType": "I",
    "DetFlag": "I",
    "FieldFlag": "I",
    "NumObs": "I",
    "NumDetObs": "I",
    "FirstDetDate": "19A",
    "LastDetDate": "19A",
    "Rate_band0": "D",
    "Rate_band0_pos": "D",
    "Rate_band0_neg": "D",
    "PeakRate_band0": "D",
    "HR1": "E",
    "HR2": "E",
    "PvarPchiSnapshot_band0": "D",
    "PvarPchiObsID_band0": "D",
    "PowFlux": "E",
    "PowFlux_pos": "E",
    "PowFlux_neg": "E",
    "OrigErr90": "E",
    "HR1_pos": "E",
    "MatchIn2MASS": "I",
}


def _value(fmt, rows):
    if fmt.endswith("A"):
        return np.array(["x"] * rows)
    if fmt == "L":
        return np.ones(rows, dtype=bool)
    if fmt in ("I", "J", "K"):
        return np.arange(rows)
    if fmt[:-1].isdigit():
        return np.zeros((rows, int(fmt[:-1])))
    return np.arange(rows, dtype=np.float64)


def _table(path, name=None):
    cols = [
        fits.Column(name=n, format=f, array=_value(f, 2))
        for n, f in PUBLISHED.items()
    ]
    fits.HDUList(
        [fits.PrimaryHDU(), fits.BinTableHDU.from_columns(cols, name=name)]
    ).writeto(path, overwrite=True)
    return path


def test_the_projection_only_names_published_columns():
    unknown = set(COLUMNS) - set(PUBLISHED)
    assert not unknown, (
        f"the 2SXPS clean sources table has no columns {unknown}"
    )


def test_conversion_keeps_exactly_the_projected_columns(tmp_path):
    out = fits_to_parquet(
        _table(tmp_path / "2sxps.fits"),
        tmp_path / "s.parquet",
        COLUMNS,
        label="2SXPS",
    )
    assert set(pq.read_schema(out).names) == set(COLUMNS)
