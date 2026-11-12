import numpy as np
import pyarrow.parquet as pq
from astropy.io import fits

from boompy.catalog._fits import fits_to_parquet
from boompy.catalog.catalogs.fourfgl import COLUMNS, HDU

#: Every stored column and a few that are not, out of the 79 in gll_psc_v35.fit.
PUBLISHED = {
    "Source_Name": "18A",
    "DataRelease": "I",
    "RAJ2000": "E",
    "DEJ2000": "E",
    "Conf_95_SemiMajor": "E",
    "Conf_95_SemiMinor": "E",
    "Conf_95_PosAng": "E",
    "Signif_Avg": "E",
    "Flux1000": "E",
    "Unc_Flux1000": "E",
    "Energy_Flux100": "E",
    "Unc_Energy_Flux100": "E",
    "SpectrumType": "17A",
    "PL_Index": "E",
    "Variability_Index": "E",
    "Frac_Variability": "E",
    "Flags": "I",
    "CLASS1": "5A",
    "CLASS2": "10A",
    "ASSOC1": "28A",
    "ASSOC2": "30A",
    "ASSOC_PROB_BAY": "E",
    "ASSOC_PROB_LR": "E",
    "RA_Counterpart": "D",
    "DEC_Counterpart": "D",
    "GLON": "E",
    "Conf_68_SemiMajor": "E",
    "Flux_Band": "8E",
    "ASSOC_TEV": "30A",
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
    assert not unknown, f"4FGL-DR4 has no columns {unknown}"


def test_conversion_reads_the_point_source_table(tmp_path):
    out = fits_to_parquet(
        _table(tmp_path / "gll_psc.fit", name=HDU),
        tmp_path / "f.parquet",
        COLUMNS,
        hdu=HDU,
        label="4FGL-DR4",
    )
    assert set(pq.read_schema(out).names) == set(COLUMNS)
