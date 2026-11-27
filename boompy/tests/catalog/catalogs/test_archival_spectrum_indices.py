import pytest

from boompy.catalog.catalogs import archival_spectrum_objects, archival_spectrum_visits


def test_catalogs_discover_only_their_published_revision_shards(tmp_path, monkeypatch):
    monkeypatch.setenv("BOOM_CATALOG_DATA_PATH", str(tmp_path))
    root = tmp_path / "staged" / "archival-spectroscopy"
    first = root / "r0000000000000000001" / archival_spectrum_objects.ID
    second = root / "r0000000000000000002" / archival_spectrum_visits.ID
    temporary = root / ".incoming-tmp" / archival_spectrum_objects.ID
    for directory in (first, second, temporary):
        directory.mkdir(parents=True)
        (directory / "part-000000.jsonl.gz").touch()
    [obj] = archival_spectrum_objects.list_chunks()
    [visit] = archival_spectrum_visits.list_chunks()
    assert obj.id.startswith("r0000000000000000001/")
    assert visit.id.startswith("r0000000000000000002/")
    assert archival_spectrum_objects.fetch_chunk(obj.id, tmp_path) == [first / "part-000000.jsonl.gz"]
    with pytest.raises(ValueError):
        archival_spectrum_visits.fetch_chunk("../part-000000.jsonl.gz", tmp_path)
