import gzip

import pytest

from boompy.catalog.catalogs import archival_repeat_rv as catalog


def test_archival_repeat_rv_uses_only_the_named_staged_chunk(monkeypatch, tmp_path):
    monkeypatch.setenv("BOOM_CATALOG_DATA_PATH", str(tmp_path))
    with pytest.raises(FileNotFoundError):
        catalog.list_chunks()
    archive = tmp_path / "staged" / catalog.ID / catalog.FILENAME
    archive.parent.mkdir(parents=True)
    with gzip.open(archive, "wt") as stream:
        stream.write('{"_id":"gaia-dr3:42:lamost:dr12-v2.0:mrs:LAMOST_MRS_BR1"}\n')
    assert [chunk.id for chunk in catalog.list_chunks()] == [catalog.FILENAME]
    assert catalog.fetch_chunk(catalog.FILENAME, tmp_path / "other") == [archive]
    with pytest.raises(ValueError):
        catalog.fetch_chunk("../other.jsonl.gz", tmp_path)
