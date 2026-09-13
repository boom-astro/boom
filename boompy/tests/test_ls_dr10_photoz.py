"""The staged LS DR10 photo-z source.

Unlike every other catalog, this one is not downloaded -- it reads an artifact
built offline. These tests cover the "it isn't there" paths, which are the ones
an operator will actually hit.
"""

import pytest

from boompy.catalogs import ls_dr10_photoz as lsdr10


def _stage(tmp_path, monkeypatch, files):
    root = tmp_path / "staged"
    for relative in files:
        target = root / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(b"")
    monkeypatch.setenv(lsdr10.DIR_ENV, str(root))
    return root


def test_each_staged_partition_is_a_chunk(tmp_path, monkeypatch):
    _stage(tmp_path, monkeypatch, ["ra_deg=0/part-0.parquet", "ra_deg=1/part-0.parquet"])
    chunks = lsdr10.list_chunks()
    assert [c.id for c in chunks] == ["ra_deg=0/part-0.parquet", "ra_deg=1/part-0.parquet"]


def test_chunk_ids_are_relative_so_the_dataset_can_move(tmp_path, monkeypatch):
    """Ids are matched against the already-done list on a resumed run, so an
    absolute path would invalidate all progress if the mount point changed."""
    root = _stage(tmp_path, monkeypatch, ["ra_deg=7/part-0.parquet"])
    chunk = lsdr10.list_chunks()[0]
    assert not chunk.id.startswith("/")
    assert str(root) not in chunk.id


def test_an_absent_dataset_says_it_is_staged_not_downloaded(tmp_path, monkeypatch):
    """The failure an operator hits first. A bare 'not found' would send them
    looking for a broken download that never existed."""
    monkeypatch.setenv(lsdr10.DIR_ENV, str(tmp_path / "nope"))
    with pytest.raises(RuntimeError, match="built offline and staged"):
        lsdr10.list_chunks()


def test_an_empty_directory_is_also_an_error(tmp_path, monkeypatch):
    """An existing but empty directory must not read as a catalog with no rows,
    which the ingest would record as complete."""
    _stage(tmp_path, monkeypatch, [])
    (tmp_path / "staged").mkdir(parents=True, exist_ok=True)
    with pytest.raises(RuntimeError, match="no parquet files"):
        lsdr10.list_chunks()


def test_fetch_returns_the_staged_file_in_place(tmp_path, monkeypatch):
    """Nothing is copied: the file is already where it belongs, and copying
    would double the disk this catalog needs."""
    root = _stage(tmp_path, monkeypatch, ["ra_deg=3/part-0.parquet"])
    files = lsdr10.fetch_chunk("ra_deg=3/part-0.parquet", tmp_path / "elsewhere")
    assert files == [root / "ra_deg=3/part-0.parquet"]


def test_a_missing_partition_names_itself(tmp_path, monkeypatch):
    _stage(tmp_path, monkeypatch, ["ra_deg=3/part-0.parquet"])
    with pytest.raises(RuntimeError, match="ra_deg=9/part-0.parquet"):
        lsdr10.fetch_chunk("ra_deg=9/part-0.parquet", tmp_path)


def test_it_falls_back_to_the_shared_catalog_data_path(tmp_path, monkeypatch):
    monkeypatch.delenv(lsdr10.DIR_ENV, raising=False)
    monkeypatch.setenv(lsdr10.DATA_PATH_ENV, str(tmp_path))
    assert lsdr10.staged_dir() == tmp_path / "ls-dr10-photoz"
