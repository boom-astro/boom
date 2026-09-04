"""Chunk enumeration and the CLI protocol the Rust side parses."""

import json

import pytest
import responses

from boompy.catalogs import CATALOGS, get
from boompy.catalogs.base import CatalogModule, Chunk, already_complete
from boompy.catalogs.cli import main

TWOMASS_INDEX = """
<a href="psc_aaa.gz">psc_aaa.gz</a>
<a href="psc_aab.gz">psc_aab.gz</a>
"""


def test_every_catalog_id_matches_its_registry_key():
    """The Rust side looks catalogs up by slug in both languages; a mismatch
    here would make a catalog unreachable from one side only."""
    for key, catalog in CATALOGS.items():
        assert key == catalog.ID


def test_every_catalog_module_implements_the_interface():
    """The interface is a Protocol rather than a base class, so nothing forces a
    module to define all three names -- this is the check that does."""
    for catalog in CATALOGS.values():
        assert isinstance(catalog, CatalogModule), catalog.__name__


def test_get_reports_the_known_catalogs_on_a_bad_slug():
    with pytest.raises(KeyError, match="known catalogs are"):
        get("nope")


@responses.activate
def test_twomass_lists_one_chunk_per_file():
    responses.add(
        responses.GET,
        "https://irsa.ipac.caltech.edu/2MASS/download/allsky/",
        body=TWOMASS_INDEX,
    )
    chunks = get("2mass").list_chunks()
    assert [c.id for c in chunks] == ["psc_aaa.gz", "psc_aab.gz"]


@responses.activate
def test_twomass_raises_when_the_index_is_empty():
    """An archive reorganization that empties the listing must not read as a
    catalog with nothing in it -- the ingest would record it complete."""
    responses.add(
        responses.GET,
        "https://irsa.ipac.caltech.edu/2MASS/download/allsky/",
        body="<html></html>",
    )
    with pytest.raises(RuntimeError, match="no files matching"):
        get("2mass").list_chunks()


@responses.activate
def test_ned_is_a_single_chunk_with_a_stable_id():
    """The id stays `current` across releases: keying it on the release name
    would make each new release look like an unfinished chunk of the last."""
    responses.add(
        responses.HEAD,
        "https://ned.ipac.caltech.edu/NED::LVS/fits/Current/",
        headers={
            "content-length": "1234",
            "content-disposition": "attachment; filename=NEDLVS_20260424.fits",
        },
    )
    chunks = get("ned-lvs").list_chunks()
    assert [c.id for c in chunks] == ["current"]
    assert "NEDLVS_20260424.fits" in chunks[0].label


def test_already_complete_needs_a_known_size(tmp_path):
    """Without a size, a leftover file is suspect: it may be a truncated
    download from a run that died, and skipping it would ingest a short file."""
    path = tmp_path / "f.gz"
    path.write_bytes(b"payload")
    assert already_complete(path, 7)
    assert not already_complete(path, 8)
    assert not already_complete(path, None)
    assert not already_complete(tmp_path / "missing.gz", 7)


@responses.activate
def test_cli_list_chunks_emits_parseable_json(capsys):
    responses.add(
        responses.GET,
        "https://irsa.ipac.caltech.edu/2MASS/download/allsky/",
        body=TWOMASS_INDEX,
    )
    main(["list-chunks", "2mass"])
    payload = json.loads(capsys.readouterr().out)
    assert payload["catalog"] == "2mass"
    assert payload["chunks"][0] == {"id": "psc_aaa.gz", "label": "psc_aaa.gz"}


@responses.activate
def test_cli_fetch_chunk_returns_absolute_paths(tmp_path, capsys):
    url = "https://irsa.ipac.caltech.edu/2MASS/download/allsky/psc_aaa.gz"
    responses.add(responses.HEAD, url, headers={"content-length": "7"})
    responses.add(responses.GET, url, body=b"payload")

    main(["fetch-chunk", "2mass", "--chunk", "psc_aaa.gz", "--dest", str(tmp_path)])
    payload = json.loads(capsys.readouterr().out)
    # The caller resolves these against its own cwd, which is not ours.
    assert payload["files"] == [str((tmp_path / "psc_aaa.gz").resolve())]


def test_cli_keeps_logs_off_stdout(capsys):
    """stdout carries the JSON result and nothing else; a stray print here
    would make the Rust side fail to parse a run that actually succeeded."""
    with pytest.raises(KeyError):
        main(["list-chunks", "nope"])
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "unknown catalog" in captured.err


def test_chunk_json_shape_matches_the_rust_struct():
    assert Chunk(id="a", label="b").as_json() == {"id": "a", "label": "b"}
    assert Chunk(id="a").as_json() == {"id": "a", "label": None}
