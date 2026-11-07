"""2MASS: one chunk per file in the published index."""

import pytest
import responses

from boompy.catalog import get

TWOMASS_INDEX = """
<a href="psc_aaa.gz">psc_aaa.gz</a>
<a href="psc_aab.gz">psc_aab.gz</a>
"""


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
