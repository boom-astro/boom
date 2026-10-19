"""Catalog sources: how BOOM gets archival catalog files onto disk.

Every catalog here has a matching `CatalogDef` in `src/catalogs/mod.rs` under
the same slug. This side knows where the data lives and how to fetch it; the
Rust side knows what the columns mean and how to store them.

A catalog is one module in `catalogs/` defining `ID`, `list_chunks` and
`fetch_chunk` -- see `base.CatalogModule` for the shape. Everything else in
this package is machinery shared between them.
"""

from __future__ import annotations

from .base import CatalogModule, Chunk
from .registry import CATALOGS, get

__all__ = ["CATALOGS", "CatalogModule", "Chunk", "get"]
