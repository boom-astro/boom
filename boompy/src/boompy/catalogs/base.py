"""The chunk interface every catalog module implements.

BOOM's Rust side drives the ingest loop and calls in here twice per catalog:
once to enumerate the chunks, then once per chunk to fetch it. Keeping the loop
on the Rust side is what makes the ingest resumable and keeps peak disk at one
chunk -- so a catalog module only has to answer "what are the pieces?" and "put
this piece on disk".

A catalog is a **module**, not a class: there is only ever one of each, they
hold no state, and a base class here would document the interface without
enforcing it. `CatalogModule` below is a `typing.Protocol`, so a type checker
still verifies the shape structurally and there is nothing to inherit from.
"""

from __future__ import annotations

import dataclasses
import os
from pathlib import Path
from typing import Protocol, runtime_checkable


@dataclasses.dataclass(frozen=True)
class Chunk:
    """One independently fetchable, independently ingestable piece of a catalog.

    `id` must be **stable across runs**: it is what a resumed ingest matches
    against the chunks it has already done, so it cannot embed a timestamp, an
    ordinal, or anything else that shifts when the catalog is re-enumerated.
    """

    id: str
    label: str | None = None

    def as_json(self) -> dict:
        return {"id": self.id, "label": self.label}


@runtime_checkable
class CatalogModule(Protocol):
    """What a module in this package has to define to be a catalog."""

    #: Kebab-case slug, matching the `CatalogDef.id` on the Rust side.
    ID: str

    def list_chunks(self) -> list[Chunk]:
        """Every chunk of this catalog, in the order they should be ingested."""
        ...

    def fetch_chunk(self, chunk_id: str, dest: Path) -> list[Path]:
        """Fetch one chunk into `dest` and return the files written.

        Must be safe to call again for the same chunk: a fetch interrupted
        partway leaves a partial file behind, and the retry has to replace it
        rather than resume into it or skip it.
        """
        ...


def already_complete(path: Path, expected_size: int | None) -> bool:
    """Whether `path` is a complete previous download of `expected_size`.

    Size is the only completeness signal these archives offer; without a known
    size a leftover file is treated as suspect and re-fetched, because a
    truncated catalog file ingests as a silently short catalog.
    """
    if not path.exists() or expected_size is None:
        return False
    return path.stat().st_size == expected_size


def ensure_dir(path: Path) -> Path:
    os.makedirs(path, exist_ok=True)
    return path
