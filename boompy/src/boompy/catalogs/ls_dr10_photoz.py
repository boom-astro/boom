"""Legacy Survey DR10 astrometry joined to photo-z.

Unlike every other catalog here, boompy does not fetch this one -- it reads a
dataset that was built offline and staged on disk.

Building it is a three-stage pipeline in boom-catalogs: minify the tractor
sweeps (~101 GB) and the photo-z catalog (~34 GB), then join them on `lsid` with
an out-of-core DuckDB hash join into a hive-partitioned parquet dataset. That is
~135 GB of input and hours of work, and it cannot be done chunk-by-chunk,
because the two inputs are partitioned differently and the join key is unique
per source. So BOOM ingests the artifact and does not pretend to build it.

Stage the output directory (the one containing `ra_deg=NN/` subdirectories) at
`$BOOM_LS_DR10_PHOTOZ_DIR`, or at `<catalog data path>/ls-dr10-photoz`. Each
parquet file is one chunk, so the ingest is still resumable and still bounded --
and because the files are the artifact rather than a download, BOOM never
deletes them.
"""

from __future__ import annotations

import os
from pathlib import Path

from .base import Chunk
from .http import log

ID = "ls-dr10-photoz"

#: Where the built dataset is staged.
DIR_ENV = "BOOM_LS_DR10_PHOTOZ_DIR"
#: Falls back to a subdirectory of the shared catalog data path, which is what
#: the task worker already mounts.
DATA_PATH_ENV = "BOOM_CATALOG_DATA_PATH"


def staged_dir() -> Path:
    explicit = os.getenv(DIR_ENV)
    if explicit:
        return Path(explicit)
    return Path(os.getenv(DATA_PATH_ENV, "data/catalogs")) / "ls-dr10-photoz"


def _missing(directory: Path) -> RuntimeError:
    return RuntimeError(
        f"no parquet files under {directory}. This catalog is built offline and staged, "
        f"not downloaded -- see boompy/src/boompy/catalogs/ls_dr10_photoz.py. Set "
        f"{DIR_ENV} if it lives elsewhere."
    )


def list_chunks() -> list[Chunk]:
    directory = staged_dir()
    if not directory.is_dir():
        raise _missing(directory)
    # Sorted so ingest order and chunk ids are stable across runs; the ids are
    # relative paths, which stay valid if the dataset is moved.
    files = sorted(p.relative_to(directory) for p in directory.rglob("*.parquet"))
    if not files:
        raise _missing(directory)
    log(f"LS DR10 photo-z: {len(files)} staged partitions under {directory}")
    return [Chunk(id=str(f), label=f.name) for f in files]


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    # `dest` is ignored: the file is already where it belongs, and copying it
    # would double the disk this catalog needs for no benefit.
    path = staged_dir() / chunk_id
    if not path.is_file():
        raise RuntimeError(f"staged partition {chunk_id} is missing from {staged_dir()}")
    return [path]
