"""Pan-STARRS object-mean (otmo) table, from the HATS mirror on S3.

Same shape as AllWISE: a HATS dataset read with `lsdb`, one HEALPix partition
per chunk, which is also the chunking BOOM wants.

The bucket is `stpubdata`, which is STScI public data served **requester-pays**,
so the host needs AWS credentials with permission to pay for the transfer even
though the data itself is public. Without them the fetch fails with an access
error rather than a missing-object error.
"""

from __future__ import annotations

import functools
from pathlib import Path

from .base import Chunk, ensure_dir
from .http import log

ID = "panstarrs"

HATS_URL = "s3://stpubdata/panstarrs/ps1/public/hats/otmo"

#: Projected at read time: the otmo table is wide and BOOM stores 13 columns.
COLUMNS = [
    "objID",
    "raMean",
    "decMean",
    "gMeanPSFMag",
    "rMeanPSFMag",
    "iMeanPSFMag",
    "zMeanPSFMag",
    "yMeanPSFMag",
    "gMeanPSFMagErr",
    "rMeanPSFMagErr",
    "iMeanPSFMagErr",
    "zMeanPSFMagErr",
    "yMeanPSFMagErr",
]


@functools.cache
def _catalog():
    """Open the HATS catalog once per process; opening reads partition metadata."""
    import lsdb

    return lsdb.open_catalog(
        HATS_URL,
        columns=COLUMNS,
        storage_options={"requester_pays": True},
    )


def _chunk_id(order: int, pixel: int) -> str:
    return f"order{order}_pix{pixel}"


def _parse_chunk_id(chunk_id: str) -> tuple[int, int]:
    try:
        order, pixel = chunk_id.split("_")
        return int(order.removeprefix("order")), int(pixel.removeprefix("pix"))
    except (ValueError, AttributeError) as e:
        raise ValueError(f"malformed Pan-STARRS chunk id {chunk_id!r}") from e


def list_chunks() -> list[Chunk]:
    pixels = _catalog().get_healpix_pixels()
    log(f"Pan-STARRS: {len(pixels)} HEALPix partitions")
    # Sorted so ingest order is stable across runs; the HATS pixel list is not
    # guaranteed to come back in the same order twice.
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
    log(f"Pan-STARRS: reading partition order={order} pixel={pixel}")
    frame = _catalog().get_partition(order, pixel).compute()
    frame.to_parquet(path)
    log(f"Pan-STARRS: wrote {len(frame)} rows to {path.name}")
    return [path]
