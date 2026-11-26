import gzip

import pytest

from boompy.catalog.catalogs import panstarrs_hosts


def test_staged_chunks_are_read_in_place(tmp_path, monkeypatch):
    monkeypatch.setenv("BOOM_CATALOG_DATA_PATH", str(tmp_path))
    with pytest.raises(RuntimeError, match="Stage panstarrs-hosts"):
        panstarrs_hosts.list_chunks()

    directory = tmp_path / "staged" / "panstarrs-hosts"
    directory.mkdir(parents=True)
    part = directory / "part-0001.jsonl.gz"
    with gzip.open(part, "wt") as stream:
        stream.write(
            '{"_id":191000742646808255,"ra":74.2654807,"dec":69.17252295,'
            '"rKronRad":9.7508,"rExtNSigma":41.55,"strm_class":"GALAXY",'
            '"strm_prob_galaxy":0.9994,"strm_z_phot":0.0381,"strm_z_phot_err":0.0086}\n'
        )
    assert [chunk.id for chunk in panstarrs_hosts.list_chunks()] == [part.name]
    assert panstarrs_hosts.fetch_chunk(part.name, tmp_path / "download") == [
        part
    ]
    with pytest.raises(ValueError, match="Invalid"):
        panstarrs_hosts.fetch_chunk("../part-0001.jsonl.gz", tmp_path)
