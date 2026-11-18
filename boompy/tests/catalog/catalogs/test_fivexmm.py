import gzip

import numpy as np
import pyarrow.parquet as pq
import responses
from astropy.io import fits

from boompy.catalog.catalogs import fivexmm
from boompy.catalog.catalogs.fivexmm import COLUMNS

#: Every stored column and a few that are not, out of the 421 in 5XMM_DR15cat_source_v1.0.fits.
PUBLISHED = {
    "SRCID": "K",
    "IAUNAME": "21A",
    "RA": "D",
    "DEC": "D",
    "RADEC_ERR": "E",
    "N_OBS": "J",
    "MJD_FIRST": "D",
    "MJD_LAST": "D",
    "EP_FLUX": "E",
    "EP_FLUX_ERR": "E",
    "EP_DET_ML": "E",
    "EP_HR1": "E",
    "EP_HR2": "E",
    "EP_HR3": "E",
    "EP_HR4": "E",
    "EXTENT": "E",
    "EXTENT_ML": "E",
    "SUM_FLAG": "I",
    "VAR_FLAG": "L",
    "FVAR": "D",
    "VAR_PROB": "D",
    "FRATIO": "D",
    "CLASSX_CLASS": "20A",
    "CLASSX_PROB_AGN": "D",
    "CLASSX_PROB_STAR": "D",
    "CLASSX_PROB_GAL_XRB": "D",
    "CLASSX_PROB_GAL_CV": "D",
    "OBS_ID": "10A",
    "ELL_MAJOR": "E",
    "PN_FLUX": "E",
    "CLASSX_PROB_EXTENDED": "D",
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
    assert not unknown, f"the 5XMM-DR15 source table has no columns {unknown}"


@responses.activate
def test_a_chunk_is_decompressed_converted_and_cleaned_up(tmp_path):
    body = gzip.compress(_table(tmp_path / "source.fits").read_bytes())
    responses.add(
        responses.HEAD, fivexmm.URL, headers={"content-length": str(len(body))}
    )
    responses.add(responses.GET, fivexmm.URL, body=body)

    [out] = fivexmm.fetch_chunk("current", tmp_path / "dl")
    table = pq.read_table(out)
    assert set(table.schema.names) == set(COLUMNS)
    assert table.column("VAR_FLAG").to_pylist() == [True, True]
    assert [p.name for p in (tmp_path / "dl").iterdir()] == [out.name]
