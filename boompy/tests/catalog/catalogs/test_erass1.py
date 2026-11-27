import tarfile

import numpy as np
import pyarrow.parquet as pq
import responses
from astropy.io import fits

from boompy.catalog.catalogs import erass1
from boompy.catalog.catalogs.erass1 import COLUMNS

#: Every stored column and a few that are not, out of the 252 in eRASS1_Main.v1.2.fits.
PUBLISHED = {
    "UID": "K",
    "IAUNAME": "23A",
    "RA": "D",
    "DEC": "D",
    "POS_ERR": "E",
    "MJD_MIN": "E",
    "MJD_MAX": "E",
    "EXT": "E",
    "EXT_LIKE": "E",
    "DET_LIKE_0": "E",
    "ML_RATE_1": "E",
    "ML_RATE_ERR_1": "E",
    "ML_FLUX_1": "E",
    "ML_FLUX_ERR_1": "E",
    "ML_RATE_P1": "E",
    "ML_RATE_ERR_P1": "E",
    "ML_RATE_P2": "E",
    "ML_RATE_ERR_P2": "E",
    "ML_RATE_P3": "E",
    "ML_RATE_ERR_P3": "E",
    "FLAG_SP_SNR": "I",
    "FLAG_SP_BPS": "I",
    "FLAG_SP_SCL": "I",
    "FLAG_SP_LGA": "I",
    "FLAG_SP_GC_CONS": "I",
    "FLAG_OPT": "I",
    "DETUID": "32A",
    "RA_RAW": "D",
    "RADEC_ERR": "E",
    "ML_CTS_1": "E",
    "FLAG_NO_EXT_ERR": "K",
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
    assert not unknown, f"eRASS1 Main has no columns {unknown}"


@responses.activate
def test_a_chunk_is_extracted_converted_and_cleaned_up(tmp_path):
    fits_path = _table(tmp_path / erass1.FITS_NAME)
    archive = tmp_path / erass1.ARCHIVE
    with tarfile.open(archive, "w:gz") as tar:
        tar.add(fits_path, arcname=erass1.FITS_NAME)
    body = archive.read_bytes()
    responses.add(
        responses.HEAD, erass1.URL, headers={"content-length": str(len(body))}
    )
    responses.add(responses.GET, erass1.URL, body=body)

    [out] = erass1.fetch_chunk("current", tmp_path / "dl")
    table = pq.read_table(out)
    assert set(table.schema.names) == set(COLUMNS)
    assert table.column("IAUNAME")[0].as_py() == "x"
    assert [p.name for p in (tmp_path / "dl").iterdir()] == [out.name]
