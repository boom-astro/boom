"""The index of catalog modules, built by listing `boompy.catalog.catalogs`.

Discovery rather than a hand-written list: a catalog used to be added in three
places -- the module, an import, and a tuple -- and the only thing stopping a
new catalog from being invisible on this side was remembering the third. The
directory is the registry now.
"""

from __future__ import annotations

import importlib
import pkgutil

from . import catalogs
from .base import CatalogModule


def _discover() -> dict[str, CatalogModule]:
    found: dict[str, CatalogModule] = {}
    for info in pkgutil.iter_modules(catalogs.__path__):
        if info.name.startswith("_"):
            continue
        module = importlib.import_module(f"{catalogs.__name__}.{info.name}")
        catalog_id = getattr(module, "ID", None)
        if catalog_id is None:
            raise RuntimeError(
                f"{module.__name__} is in the catalogs package but defines no "
                "ID; shared machinery belongs in boompy.catalog, one level up"
            )
        found[catalog_id] = module
    return dict(sorted(found.items()))


#: Every catalog boompy can source, by slug.
CATALOGS: dict[str, CatalogModule] = _discover()


def get(catalog_id: str) -> CatalogModule:
    """Look up a catalog module by slug."""
    try:
        return CATALOGS[catalog_id]
    except KeyError:
        known = ", ".join(sorted(CATALOGS))
        raise KeyError(
            f"unknown catalog {catalog_id!r}; known catalogs are {known}"
        ) from None
