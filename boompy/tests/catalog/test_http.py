"""The fetching helpers, exercised against a stubbed HTTP layer.

Nothing here touches the network: these run in CI, and the archives they would
otherwise hit are slow, rate-limited, and occasionally down.
"""

import pytest
import requests
import responses

from boompy.catalog.http import (
    content_length,
    download,
    list_index,
    stays_inside,
)

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
        download(
            "https://example.test/f.gz", dest, expected_size=999, attempts=2
        )
    assert not dest.exists()
    assert not list(tmp_path.glob("*.part"))


@responses.activate
def test_download_retries_then_succeeds(tmp_path, monkeypatch):
    monkeypatch.setattr("boompy.catalog.http.time.sleep", lambda _: None)
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
    monkeypatch.setattr("boompy.catalog.http.time.sleep", lambda _: None)
    dest = tmp_path / "f.gz"
    partial = dest.with_suffix(dest.suffix + ".part")
    partial.write_bytes(b"pay")

    responses.add(
        responses.GET, "https://example.test/f.gz", body=b"payload", status=200
    )
    download("https://example.test/f.gz", dest, expected_size=7)
    assert dest.read_bytes() == b"payload"


def test_every_request_identifies_boom():
    """At least one archive (quasars.org) answers 406 to the default
    `python-requests/x.y` agent, so a valid download fails for no visible
    reason. The agent is load-bearing, not cosmetic."""
    from boompy.catalog.http import USER_AGENT, session

    assert session().headers["User-Agent"] == USER_AGENT
    assert "python-requests" not in USER_AGENT
    # An operator seeing this traffic should be able to tell what it is.
    assert "boom" in USER_AGENT and "github.com/boom-astro" in USER_AGENT


@responses.activate
def test_the_agent_is_sent_on_download_and_head(tmp_path):
    from boompy.catalog.http import USER_AGENT, content_length, download

    responses.add(
        responses.HEAD,
        "https://example.test/f.gz",
        headers={"content-length": "7"},
    )
    responses.add(responses.GET, "https://example.test/f.gz", body=b"payload")
    content_length("https://example.test/f.gz")
    download("https://example.test/f.gz", tmp_path / "f.gz", expected_size=7)

    assert len(responses.calls) == 2
    for call in responses.calls:
        assert call.request.headers["User-Agent"] == USER_AGENT


def test_a_listing_entry_cannot_escape_its_directory():
    """Chunk ids come from the scraped index and are joined onto a destination
    directory, so a traversal here is a file write outside the staging area.
    `.` matches a slash, so any pattern using `.*` would admit these."""
    assert stays_inside("psc_aaa.gz")
    assert stays_inside("9.0/sweep-000.fits")
    assert not stays_inside("../../../etc/cron.d/x.csv.gz")
    assert not stays_inside("/etc/passwd")
    assert not stays_inside("~/.ssh/authorized_keys")
    assert not stays_inside("https://elsewhere.example/x.csv.gz")


@responses.activate
def test_list_index_drops_a_traversing_entry():
    responses.add(
        responses.GET,
        "https://archive.example/",
        body=(
            '<a href="psc_aaa.gz">a</a><a href="../../etc/psc_evil.gz">b</a>'
        ),
    )
    assert list_index("https://archive.example/", r"psc_[^/]*\.gz") == [
        "psc_aaa.gz"
    ]
    # And it would have been dropped even by a pattern careless about slashes.
    assert list_index("https://archive.example/", r".*psc_.*\.gz") == [
        "psc_aaa.gz"
    ]
