"""The pieces every catalog module is built from."""

from boompy.catalog.base import Chunk, already_complete


def test_already_complete_needs_a_known_size(tmp_path):
    """Without a size, a leftover file is suspect: it may be a truncated
    download from a run that died, and skipping it would ingest a short file."""
    path = tmp_path / "f.gz"
    path.write_bytes(b"payload")
    assert already_complete(path, 7)
    assert not already_complete(path, 8)
    assert not already_complete(path, None)
    assert not already_complete(tmp_path / "missing.gz", 7)


def test_chunk_json_shape_matches_the_rust_struct():
    assert Chunk(id="a", label="b").as_json() == {"id": "a", "label": "b"}
    assert Chunk(id="a").as_json() == {"id": "a", "label": None}
