"""REGALADE: header parsing, row slicing and the conversion, on a synthetic file
laid out like regalade_v2.fits (VOTable metadata in the primary HDU, then the
table)."""

from io import BytesIO

import numpy as np
import pyarrow.parquet as pq
import pytest
import responses
from astropy.io import fits

from boompy.catalog.catalogs import regalade
from boompy.catalog.catalogs.regalade import COLUMNS, RELEASE, table_header

#: Every column of regalade_v2.fits, with its published format.
PUBLISHED = {
    "name": "31A",
    "gal_ra": "E",
    "gal_dec": "E",
    "D": "E",
    "z": "E",
    "D_min": "E",
    "D_max": "E",
    "D_std": "E",
    "D_input": "E",
    "D_err": "E",
    "D_tmean": "E",
    "ref_D_in": "J",
    "n_dist": "J",
    "R1": "E",
    "R2": "E",
    "PA": "E",
    "ref_ell": "J",
    "id_cat": "J",
    "gmag": "E",
    "rmag": "E",
    "imag": "E",
    "zmag": "E",
    "W1mag": "E",
    "W2mag": "E",
    "griz_ref": "J",
    "W_ref": "J",
    "EBV": "E",
    "absgmag": "E",
    "logM": "E",
    "f_reliability": "B",
    "ref_z_in": "J",
    "z_err": "E",
    "match_offset": "E",
    "simbad_z": "E",
    "f_simbad_zdiscrepancy": "I",
}

ROWS = 5


def _file(release=RELEASE) -> bytes:
    columns = []
    for name, fmt in PUBLISHED.items():
        if fmt.endswith("A"):
            array = np.array([f"PGC{i}" for i in range(ROWS)])
        elif name == "gal_ra":
            array = np.linspace(0.0, 360.0, ROWS)
        elif fmt == "E":
            array = np.full(ROWS, 1.5, dtype=np.float32)
        else:
            array = np.ones(ROWS)
        columns.append(fits.Column(name=name, format=fmt, array=array))
    table = fits.BinTableHDU.from_columns(columns, name="2xsimbad")
    table.header["DATE-HDU"] = release
    votable = fits.PrimaryHDU(np.frombuffer(b"<VOTABLE/>" * 451, np.uint8))
    out = BytesIO()
    fits.HDUList([votable, table]).writeto(out)
    return out.getvalue()


def _serve(blob: bytes):
    def callback(request):
        first, last = request.headers["Range"].removeprefix("bytes=").split("-")
        first, last = int(first), min(int(last), len(blob) - 1)
        headers = {"Content-Range": f"bytes {first}-{last}/{len(blob)}"}
        return 206, headers, blob[first : last + 1]

    responses.add_callback(responses.GET, regalade.URL, callback=callback)


def test_the_projection_only_names_published_columns():
    unknown = set(COLUMNS) - set(PUBLISHED)
    assert not unknown, f"REGALADE has no columns {unknown}"


def test_the_rows_start_where_astropy_says(tmp_path):
    blob = _file()
    (tmp_path / "r.fits").write_bytes(blob)
    with fits.open(tmp_path / "r.fits") as hdus:
        expected = hdus.fileinfo(1)["datLoc"]
    header, start = table_header(blob)
    assert start == expected
    assert header["NAXIS2"] == ROWS


def test_a_replaced_file_is_refused():
    """Ids are positions in the file, so a new one would shift every id."""
    with pytest.raises(RuntimeError, match="new collection"):
        table_header(_file(release="2027-01-01T00:00:00"))


@responses.activate
def test_chunks_are_fixed_slices_of_rows(monkeypatch):
    monkeypatch.setattr(regalade, "ROWS_PER_CHUNK", 2)
    _serve(_file())
    chunks = regalade.list_chunks()
    assert [c.id for c in chunks] == [
        "rows-000000000",
        "rows-000000002",
        "rows-000000004",
    ]
    assert chunks[-1].label == "rows 4 to 4"


@responses.activate
def test_a_chunk_is_its_rows_numbered_by_position(tmp_path, monkeypatch):
    monkeypatch.setattr(regalade, "ROWS_PER_CHUNK", 2)
    _serve(_file())
    [path] = regalade.fetch_chunk("rows-000000002", tmp_path)

    table = pq.read_table(path)
    assert set(table.column_names) == {*COLUMNS, "row"}
    assert table.column("row").to_pylist() == [2, 3]
    assert table.column("name").to_pylist() == ["PGC2", "PGC3"]
    assert table.column("gal_ra").to_pylist() == [180.0, 270.0]
    assert [p.name for p in tmp_path.iterdir()] == [path.name]
