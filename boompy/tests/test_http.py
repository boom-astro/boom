"""The fetching helpers, exercised against a stubbed HTTP layer.

Nothing here touches the network: these run in CI, and the archives they would
otherwise hit are slow, rate-limited, and occasionally down.
"""

import pytest
import requests
import responses

from boompy.catalogs.http import content_length, download, list_index

INDEX_HTML = """
<html><body>
<a href="?C=N;O=D">Name</a>
<a href="/2MASS/">Parent Directory</a>
<a href="psc_aaa.gz">psc_aaa.gz</a>
<a href="psc_aab.gz">psc_aab.gz</a>
<a href="psc_aaa.gz.md5">psc_aaa.gz.md5</a>
<a href="xsc_aaa.gz">xsc_aaa.gz</a>
</body></html>
"""


@responses.activate
def test_list_index_matches_pattern_and_sorts():
    responses.add(responses.GET, "https://example.test/dir/", body=INDEX_HTML)
    assert list_index("https://example.test/dir/", r"psc_.*\.gz") == [
        "psc_aaa.gz",
        "psc_aab.gz",
    ]


@responses.activate
def test_list_index_excludes_checksums_and_other_catalogs():
    """A `.md5` sidecar and the extended source catalog both live in the same
    index; ingesting either as a PSC file would fail deep in the Rust parser."""
    responses.add(responses.GET, "https://example.test/dir/", body=INDEX_HTML)
    names = list_index("https://example.test/dir/", r"psc_.*\.gz")
    assert not any(n.endswith(".md5") for n in names)
    assert not any(n.startswith("xsc_") for n in names)


@responses.activate
def test_content_length_returns_none_when_head_fails():
    """A missing size means "download without checking", not a hard failure."""
    responses.add(responses.HEAD, "https://example.test/f.gz", status=500)
    assert content_length("https://example.test/f.gz") is None


@responses.activate
def test_download_writes_file_and_removes_partial(tmp_path):
    responses.add(responses.GET, "https://example.test/f.gz", body=b"payload")
    dest = tmp_path / "f.gz"
    download("https://example.test/f.gz", dest, expected_size=7)
    assert dest.read_bytes() == b"payload"
    assert not list(tmp_path.glob("*.part"))


@responses.activate
def test_download_rejects_a_short_transfer(tmp_path):
    """A truncated file that was accepted would ingest as a silently short
    catalog, so a size mismatch has to fail the whole download."""
    responses.add(responses.GET, "https://example.test/f.gz", body=b"short")
    dest = tmp_path / "f.gz"
    with pytest.raises(RuntimeError, match="after 2 attempts"):
        download("https://example.test/f.gz", dest, expected_size=999, attempts=2)
    assert not dest.exists()
    assert not list(tmp_path.glob("*.part"))


@responses.activate
def test_download_retries_then_succeeds(tmp_path, monkeypatch):
    monkeypatch.setattr("boompy.catalogs.http.time.sleep", lambda _: None)
    responses.add(
        responses.GET,
        "https://example.test/f.gz",
        body=requests.exceptions.ConnectionError("dropped"),
    )
    responses.add(responses.GET, "https://example.test/f.gz", body=b"payload")
    dest = tmp_path / "f.gz"
    download("https://example.test/f.gz", dest, expected_size=7, attempts=3)
    assert dest.read_bytes() == b"payload"


@responses.activate
def test_download_restarts_when_server_ignores_range(tmp_path, monkeypatch):
    """Appending a 200 response onto an existing `.part` would concatenate the
    whole file onto a prefix of itself, which no checksum downstream would
    catch."""
    monkeypatch.setattr("boompy.catalogs.http.time.sleep", lambda _: None)
    dest = tmp_path / "f.gz"
    partial = dest.with_suffix(dest.suffix + ".part")
    partial.write_bytes(b"pay")

    responses.add(responses.GET, "https://example.test/f.gz", body=b"payload", status=200)
    download("https://example.test/f.gz", dest, expected_size=7)
    assert dest.read_bytes() == b"payload"
