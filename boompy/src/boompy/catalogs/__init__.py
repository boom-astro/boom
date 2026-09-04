"""Catalog sources: how BOOM gets archival catalog files onto disk.

Every catalog here has a matching `CatalogDef` in `src/catalogs/mod.rs` under
the same slug. This side knows where the data lives and how to fetch it; the
Rust side knows what the columns mean and how to store them.

A catalog is one module defining `ID`, `list_chunks` and `fetch_chunk` -- see
`base.CatalogModule` for the shape.
"""

from __future__ import annotations

from . import allwise, ned, twomass
from .base import CatalogModule, Chunk

#: Every catalog boompy can source, by slug.
CATALOGS: dict[str, CatalogModule] = {
    module.ID: module for module in (twomass, ned, allwise)
}

__all__ = ["CATALOGS", "CatalogModule", "Chunk", "get"]


def get(catalog_id: str) -> CatalogModule:
    """Look up a catalog module by slug."""
    try:
        return CATALOGS[catalog_id]
    except KeyError:
        known = ", ".join(sorted(CATALOGS))
        raise KeyError(f"unknown catalog {catalog_id!r}; known catalogs are {known}") from None
