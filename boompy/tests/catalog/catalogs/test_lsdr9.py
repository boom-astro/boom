"""Legacy Survey DR9: chunk enumeration, the sweep/photo-z merge and its cuts.

The pair is merged positionally rather than joined, so what a test has to prove
is that the two files really are aligned before the columns are stitched
together: a misalignment silently attaches each source to a different source's
redshift, which nothing downstream could detect.
"""

import numpy as np
import pyarrow.parquet as pq
import pytest
import responses
from astropy.io import fits
from astropy.table import Table

from boompy.catalog import get
from boompy.catalog.catalogs.lsdr9 import (
    PHOTOZ_COLUMNS,
    PHOTOZ_KEY_COLUMNS,
    STORED_COLUMNS,
    SWEEP_COLUMNS,
    _merge_pair,
)

SWEEP_INDEX_URL = (
    "https://portal.nersc.gov/cfs/cosmo/data/legacysurvey/dr9/north/sweep/9.0/"
)

#: The columns a published DR9 sweep carries, as uppercase names, with a few of
#: the ~150 BOOM does not keep. The extras are here so the test proves the
#: projection selects rather than accepting whatever it is handed.
PUBLISHED_SWEEP = [c.upper() for c in SWEEP_COLUMNS] + [
    "BRICKNAME",
    "MASKBITS",
    "DCHISQ",
    "FLUX_IVAR_W1",
    "RA_IVAR",
]

#: Likewise for the photo-z sweep, which also publishes the 68% interval and the
#: training flags.
PUBLISHED_PHOTOZ = [c.upper() for c in PHOTOZ_KEY_COLUMNS + PHOTOZ_COLUMNS] + [
    "Z_PHOT_L68",
    "Z_PHOT_U68",
    "TRAINING",
    "KFOLD",
]

TEXT_COLUMNS = {"TYPE", "SURVEY", "BRICKNAME"}
INT_COLUMNS = {
    "RELEASE",
    "BRICKID",
    "OBJID",
    "NOBS_G",
    "NOBS_R",
    "NOBS_Z",
    "MASKBITS",
    "TRAINING",
    "KFOLD",
}


def _write_fits(path, columns, rows, overrides=None):
    """A FITS table with `columns`, filled with distinguishable values."""
    overrides = overrides or {}
    data = {}
    for name in columns:
        if name in overrides:
            data[name] = np.asarray(overrides[name])
        elif name in TEXT_COLUMNS:
            data[name] = np.array(
                [f"{name[:3]}" for _ in range(rows)], dtype="S10"
            )
        elif name in INT_COLUMNS:
            data[name] = np.arange(rows, dtype=np.int32)
        else:
            data[name] = np.arange(rows, dtype=np.float32) + 1.0
    fits.BinTableHDU(Table(data)).writeto(path, overwrite=True)
    return path


def _pair(tmp_path, rows=4, sweep=None, photoz=None):
    """A row-matched sweep and photo-z pair, north of the resolve boundary."""
    keys = {
        name: np.arange(rows, dtype=np.int32)
        for name in ("RELEASE", "BRICKID", "OBJID")
    }
    sweep_overrides = {
        **keys,
        "DEC": np.full(rows, 40.0, dtype=np.float32),
        "RA": np.linspace(10.0, 11.0, rows).astype(np.float32),
        **(sweep or {}),
    }
    photoz_overrides = {**keys, **(photoz or {})}
    return (
        _write_fits(
            tmp_path / "sweep-010p035-015p040.fits",
            PUBLISHED_SWEEP,
            rows,
            sweep_overrides,
        ),
        _write_fits(
            tmp_path / "sweep-010p035-015p040-pz.fits",
            PUBLISHED_PHOTOZ,
            rows,
            photoz_overrides,
        ),
    )


def test_the_projection_only_names_published_columns():
    """The failure this prevents is a column name that does not exist, which
    otherwise surfaces only when a real sweep has been downloaded."""
    unknown = {c.upper() for c in SWEEP_COLUMNS} - set(PUBLISHED_SWEEP)
    assert not unknown, f"the sweep projection names absent columns: {unknown}"
    unknown = {c.upper() for c in PHOTOZ_COLUMNS} - set(PUBLISHED_PHOTOZ)
    assert not unknown, (
        f"the photo-z projection names absent columns: {unknown}"
    )


def test_dr9_has_no_i_band():
    # DR10 adds it. A DR9 record type that declared flux_i would be asking for a
    # column the sweeps do not have.
    assert "flux_i" not in SWEEP_COLUMNS
    assert "FLUX_I" not in PUBLISHED_SWEEP


def test_the_merge_keeps_exactly_the_stored_columns(tmp_path):
    out = _merge_pair(*_pair(tmp_path), tmp_path / "out.parquet")
    assert list(pq.read_schema(out).names) == STORED_COLUMNS
    # Published in both files but not stored.
    for dropped in ("MASKBITS", "Z_PHOT_L68", "brickname"):
        assert dropped not in pq.read_schema(out).names


def test_a_dropped_published_column_fails_loudly(tmp_path):
    rows = 2
    without_sersic = [c for c in PUBLISHED_SWEEP if c != "SERSIC"]
    sweep = _write_fits(
        tmp_path / "s.fits",
        without_sersic,
        rows,
        {"DEC": np.full(rows, 40.0, dtype=np.float32)},
    )
    photoz = _write_fits(tmp_path / "s-pz.fits", PUBLISHED_PHOTOZ, rows)
    with pytest.raises(RuntimeError, match="sersic"):
        _merge_pair(sweep, photoz, tmp_path / "out.parquet")


def test_a_row_count_mismatch_is_refused(tmp_path):
    """A truncated download is the realistic way the two stop being aligned."""
    sweep = _write_fits(
        tmp_path / "s.fits",
        PUBLISHED_SWEEP,
        4,
        {"DEC": np.full(4, 40.0, dtype=np.float32)},
    )
    photoz = _write_fits(tmp_path / "s-pz.fits", PUBLISHED_PHOTOZ, 3)
    with pytest.raises(RuntimeError, match="row count mismatch"):
        _merge_pair(sweep, photoz, tmp_path / "out.parquet")


def test_files_that_are_not_row_matched_are_refused(tmp_path):
    """Same row count, different sources: the merge would hand every galaxy
    somebody else's redshift, and no later check would notice."""
    sweep, photoz = _pair(
        tmp_path, rows=8, photoz={"OBJID": np.arange(100, 108, dtype=np.int32)}
    )
    with pytest.raises(RuntimeError, match="not row-matched"):
        _merge_pair(sweep, photoz, tmp_path / "out.parquet")


def test_the_missing_value_sentinel_becomes_null(tmp_path):
    """The photo-z sweeps write -99 where they have no value. Stored as-is it
    would read as a real, and wildly wrong, redshift."""
    rows = 3
    sweep, photoz = _pair(
        tmp_path,
        rows=rows,
        photoz={
            "Z_SPEC": np.array([-99.0, 0.25, -99.0], dtype=np.float32),
            "Z_PHOT_MEAN": np.array([0.1, 0.2, 0.3], dtype=np.float32),
            "SURVEY": np.array([b"", b"SDSS", b""], dtype="S10"),
        },
    )
    out = _merge_pair(sweep, photoz, tmp_path / "out.parquet")
    frame = pq.read_table(out).to_pandas()
    assert frame["z_spec"].isna().tolist() == [True, False, True]
    assert frame["z_spec"].dropna().tolist() == [0.25]
    # A photometric redshift is not a sentinel and must survive.
    assert frame["z_phot_mean"].notna().all()
    assert frame["survey"].tolist()[1] == "SDSS"
    assert frame["survey"].isna().tolist() == [True, False, True]


def test_rows_below_the_resolve_boundary_are_dropped(tmp_path):
    """dr9/north reduces equatorial sky as well, which dr9/south covers and
    DR10 covers again. Keeping it would store the same sky twice."""
    rows = 4
    sweep, photoz = _pair(
        tmp_path,
        rows=rows,
        sweep={"DEC": np.array([10.0, 32.0, 32.4, 45.0], dtype=np.float32)},
    )
    out = _merge_pair(sweep, photoz, tmp_path / "out.parquet")
    frame = pq.read_table(out).to_pandas()
    assert frame["dec"].tolist() == pytest.approx([32.4, 45.0], abs=1e-4)


def test_the_stored_type_column_keeps_its_published_name(tmp_path):
    """`type` is what the sweeps call it, and the Rust reader asks for that
    name; renaming it here would fail the ingest with a missing column."""
    out = _merge_pair(*_pair(tmp_path), tmp_path / "out.parquet")
    assert "type" in pq.read_schema(out).names


@responses.activate
def test_chunks_are_one_per_sweep_file_above_the_boundary():
    responses.add(
        responses.GET,
        SWEEP_INDEX_URL,
        body="""
        <a href="sweep-000m005-010p000.fits">one</a>
        <a href="sweep-010p030-015p035.fits">two</a>
        <a href="sweep-010p035-015p040.fits">three</a>
        <a href="sweep-010p030-015p035.sha256sum">checksum</a>
        """,
    )
    chunks = get("lsdr9").list_chunks()
    # The first file's whole Dec box is below the boundary, so every row in it
    # would be cut: that is decidable from the name, before a gigabyte moves.
    assert [c.id for c in chunks] == [
        "north/sweep-010p030-015p035.fits",
        "north/sweep-010p035-015p040.fits",
    ]
    assert chunks[0].label == "sweep-010p030-015p035.fits"


@responses.activate
def test_an_empty_listing_is_an_error_not_an_empty_catalog():
    """An archive reorganization must not read as a catalog with nothing in it;
    the ingest would record it complete and move on."""
    responses.add(responses.GET, SWEEP_INDEX_URL, body="<html></html>")
    with pytest.raises(RuntimeError, match="no files matching"):
        get("lsdr9").list_chunks()


@pytest.mark.parametrize(
    "chunk_id",
    [
        "../etc/passwd",
        "south/sweep-010p030-015p035.fits",
        "north/../sweep.fits",
        "sweep.fits",
    ],
)
def test_a_chunk_id_that_is_not_a_north_sweep_is_refused(tmp_path, chunk_id):
    """The id becomes a path to a file this module writes and then deletes, and
    it arrives from whatever a previous run recorded."""
    with pytest.raises(RuntimeError, match="sweep chunk"):
        get("lsdr9").fetch_chunk(chunk_id, tmp_path)


def test_a_sweep_with_no_photo_z_still_writes_a_typed_survey_column(tmp_path):
    """The far north has whole sweeps where every photo-z field is -99. Left to
    inference the all-absent column comes out as parquet's untyped null, and the
    ingest would fail the chunk rather than store rows that simply have no
    redshift."""
    rows = 3
    sweep, photoz = _pair(
        tmp_path,
        rows=rows,
        photoz={
            "Z_SPEC": np.full(rows, -99.0, dtype=np.float32),
            "Z_PHOT_MEAN": np.full(rows, -99.0, dtype=np.float32),
            "SURVEY": np.array([b"", b"", b""], dtype="S10"),
        },
    )
    out = _merge_pair(sweep, photoz, tmp_path / "out.parquet")
    assert str(pq.read_schema(out).field("survey").type) == "string"
    frame = pq.read_table(out).to_pandas()
    assert frame["survey"].isna().all()
    assert frame["z_spec"].isna().all()
