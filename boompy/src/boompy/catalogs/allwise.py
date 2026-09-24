"""AllWISE Source Catalog, from the LSDB HATS mirror.

This is the catalog that justifies boompy existing. AllWISE is served as a HATS
(HEALPix Adaptive Tiling Scheme) dataset, and `lsdb` is what reads one: there is
no plain file listing to walk, and reimplementing HATS partition resolution in
Rust to avoid one subprocess would be a poor trade.

The partitioning is also exactly the chunking we want -- one HEALPix pixel per
chunk, a few hundred MB of parquet each, out of a 748-million-source catalog
that would otherwise need hundreds of gigabytes of disk staged at once.
"""

from __future__ import annotations

import functools
from pathlib import Path

from .base import Chunk, ensure_dir
from .http import log

HATS_URL = "https://data.lsdb.io/hats/wise/allwise"

#: Projected at read time rather than after download: AllWISE has ~300 columns
#: and BOOM stores 19 of them. Must stay in step with `ALLWISE_COLUMNS` in
#: `src/catalogs/types.rs`, which is where the reader's expectations live.
COLUMNS = [
    "source_id",
    "ra",
    "dec",
    "sigra",
    "sigdec",
    "w1mpro",
    "w2mpro",
    "w3mpro",
    "w4mpro",
    "w1sigmpro",
    "w2sigmpro",
    "w3sigmpro",
    "w4sigmpro",
    "w1rchi2",
    "w2rchi2",
    "pmra",
    "pmdec",
    "sigpmra",
    "sigpmdec",
]


@functools.cache
def _catalog():
    """Open the HATS catalog once per process.

    Opening it reads the partition metadata over the network, and both
    list-chunks and fetch-chunk need it.
    """
    import lsdb

    return lsdb.open_catalog(HATS_URL, columns=COLUMNS)


def _chunk_id(order: int, pixel: int) -> str:
    return f"order{order}_pix{pixel}"


def _parse_chunk_id(chunk_id: str) -> tuple[int, int]:
    try:
        order, pixel = chunk_id.split("_")
        return int(order.removeprefix("order")), int(pixel.removeprefix("pix"))
    except (ValueError, AttributeError) as e:
        raise ValueError(f"malformed AllWISE chunk id {chunk_id!r}") from e


ID = "allwise"


def list_chunks() -> list[Chunk]:
    pixels = _catalog().get_healpix_pixels()
    log(f"AllWISE: {len(pixels)} HEALPix partitions")
    # Sorted so the ingest order is stable across runs; the HATS pixel list is
    # not guaranteed to come back in the same order twice.
    return [
        Chunk(
            id=_chunk_id(p.order, p.pixel),
            label=f"HEALPix order {p.order} pixel {p.pixel}",
        )
        for p in sorted(pixels, key=lambda p: (p.order, p.pixel))
    ]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    order, pixel = _parse_chunk_id(chunk_id)
    path = ensure_dir(dest) / f"{chunk_id}.parquet"
    # Unlike the HTTP catalogs there is no size to check against, so a leftover
    # file from an interrupted write cannot be trusted; always recompute.
    # `.compute()` materializes the partition, which is why one partition rather
    # than the whole catalog is the unit here.
    log(f"AllWISE: reading partition order={order} pixel={pixel}")
    frame = _catalog().get_partition(order, pixel).compute()
    frame.to_parquet(path)
    log(f"AllWISE: wrote {len(frame)} rows to {path.name}")
    return [path]
