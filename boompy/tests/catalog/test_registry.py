"""Catalog discovery: the registry is the directory listing."""

import pkgutil

import pytest

from boompy.catalog import CATALOGS, catalogs, get, registry
from boompy.catalog.base import CatalogModule


def test_every_catalog_id_matches_its_registry_key():
    """The Rust side looks catalogs up by slug in both languages; a mismatch
    here would make a catalog unreachable from one side only."""
    for key, catalog in CATALOGS.items():
        assert key == catalog.ID


def test_every_module_in_the_catalogs_package_is_a_catalog():
    """The registry is the directory listing, so a helper module parked in
    there would be imported and asked for an `ID` it does not have. Machinery
    belongs one level up, in `boompy.catalog`."""
    names = {info.name for info in pkgutil.iter_modules(catalogs.__path__)}
    assert names, "found no modules -- has the catalogs package moved?"
    assert {c.__name__.rsplit(".", 1)[1] for c in CATALOGS.values()} == names


def test_a_module_without_an_id_is_rejected_by_name(monkeypatch, tmp_path):
    """The failure a mis-parked module produces has to say where it belongs,
    not surface as an `AttributeError` on `ID` from inside the loop."""
    (tmp_path / "helper.py").write_text("WIDTH = 3\n")
    monkeypatch.setattr(catalogs, "__path__", [str(tmp_path)])
    monkeypatch.setattr(catalogs, "__name__", "boompy.catalog.catalogs")
    monkeypatch.syspath_prepend(tmp_path)
    with pytest.raises(RuntimeError, match="defines no ID"):
        registry._discover()


def test_every_catalog_module_implements_the_interface():
    """The interface is a Protocol rather than a base class, so nothing forces a
    module to define all three names -- this is the check that does."""
    for catalog in CATALOGS.values():
        assert isinstance(catalog, CatalogModule), catalog.__name__


def test_get_reports_the_known_catalogs_on_a_bad_slug():
    with pytest.raises(KeyError, match="known catalogs are"):
        get("nope")


def test_registry_matches_the_rust_side():
    """Every catalog BOOM declares must be sourceable from here.

    The two halves are separate files in separate languages; a slug added to one
    and not the other produces an Ingest button that fails at fetch time, which
    is exactly the drift this catches.
    """
    import re
    from pathlib import Path

    root = Path(__file__).resolve().parents[3]
    mod = root / "src" / "catalogs" / "mod.rs"
    rust_slugs = set(
        re.findall(r'^\s*id: "([^"]+)",', mod.read_text(), re.MULTILINE)
    )
    assert rust_slugs, "found no CatalogDef ids -- has mod.rs moved?"
    missing = rust_slugs - set(CATALOGS)
    assert not missing, (
        f"declared in Rust but not sourceable from boompy: {sorted(missing)}"
    )
