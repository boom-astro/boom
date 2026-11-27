"""The CLI protocol the Rust side parses."""

import json
from pathlib import Path

import pytest
import responses

from boompy.catalog.base import Chunk
from boompy.catalog.cli import FetchChunkOutput, ListChunksOutput, main

# The CLI is driven through 2MASS here, so its index is the one stubbed.
TWOMASS_INDEX = """
<a href="psc_aaa.gz">psc_aaa.gz</a>
<a href="psc_aab.gz">psc_aab.gz</a>
"""


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

    main(
        [
            "fetch-chunk",
            "2mass",
            "--chunk",
            "psc_aaa.gz",
            "--dest",
            str(tmp_path),
        ]
    )
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


def test_output_json_shapes_match_the_rust_structs():
    """`ListChunksOutput` and `FetchChunkOutput` exist on both sides of the
    subprocess boundary, and only the key names hold them together."""
    listing = ListChunksOutput(catalog="2mass", chunks=[Chunk(id="psc_aaa.gz")])
    assert listing.as_json() == {
        "catalog": "2mass",
        "chunks": [{"id": "psc_aaa.gz", "label": None}],
    }
    fetched = FetchChunkOutput(
        catalog="2mass", chunk="psc_aaa.gz", files=[Path("psc_aaa.gz")]
    )
    assert fetched.as_json() == {
        "catalog": "2mass",
        "chunk": "psc_aaa.gz",
        # Resolved against the test's cwd, which is the point: the caller's is
        # not ours.
        "files": [str(Path("psc_aaa.gz").resolve())],
    }
