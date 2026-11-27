import pytest

from boompy.catalog.catalogs import archival_rv_pilot


def test_archival_rv_pilot_lists_only_the_versioned_chunk(tmp_path, monkeypatch):
    monkeypatch.setenv("BOOM_CATALOG_DATA_PATH", str(tmp_path))
    with pytest.raises(RuntimeError, match="Stage the pilot"):
        archival_rv_pilot.list_chunks()

    folder = tmp_path / "staged" / "archival-rv-pilot"
    folder.mkdir(parents=True)
    path = folder / "sources-v1.jsonl.gz"
    path.touch()

    (chunk,) = archival_rv_pilot.list_chunks()
    assert chunk.id == path.name
    assert archival_rv_pilot.fetch_chunk(chunk.id, tmp_path) == [path]
    with pytest.raises(RuntimeError, match="Unknown"):
        archival_rv_pilot.fetch_chunk("../sources-v1.jsonl.gz", tmp_path)
