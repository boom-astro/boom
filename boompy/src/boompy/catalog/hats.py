"""Reading HATS partitions by path with pyarrow.

For records joined from HATS tables partitioned differently (Euclid SPE): a chunk
reads only the columns and rows it needs from each table, where
`lsdb.get_partition` loads whole partitions.
"""

from __future__ import annotations

import functools
from collections.abc import Iterator
from dataclasses import dataclass

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as pafs


@dataclass(frozen=True, order=True)
class Partition:
    order: int
    pixel: int

    @property
    def dir(self) -> int:
        return self.pixel // 10_000 * 10_000

    def overlaps(self, other: Partition) -> bool:
        """Whether one of the two pixels contains the other."""
        low, high = sorted((self, other), key=lambda p: p.order)
        return high.pixel >> 2 * (high.order - low.order) == low.pixel


@functools.cache
def s3(region: str = "us-east-1") -> pafs.FileSystem:
    """An anonymous client for a public bucket."""
    return pafs.S3FileSystem(anonymous=True, region=region)


def partitions(fs: pafs.FileSystem, root: str) -> list[Partition]:
    """Every partition of the HATS catalog at `root`, in HEALPix order."""
    with fs.open_input_stream(f"{root}/partition_info.csv") as f:
        lines = f.read().decode().split()
    header = lines[0].split(",")
    order, pixel = header.index("Norder"), header.index("Npix")
    return sorted(
        Partition(int(row[order]), int(row[pixel]))
        for row in (line.split(",") for line in lines[1:])
    )


def _dataset(fs: pafs.FileSystem, root: str, partition: Partition):
    path = (
        f"{root}/dataset/Norder={partition.order}/Dir={partition.dir}"
        f"/Npix={partition.pixel}"
    )
    return ds.dataset(path, filesystem=fs, format="parquet")


def scan(
    fs: pafs.FileSystem,
    root: str,
    partition: Partition,
    columns: list[str],
    where: pc.Expression | None = None,
) -> Iterator[pa.RecordBatch]:
    """One partition's `columns`, keeping the rows matching `where`, in
    batches, for a partition too large to hold whole."""
    dataset = _dataset(fs, root, partition)
    # pyarrow's default read-ahead decodes many row groups at once, which
    # costs gigabytes on a partition this size.
    return dataset.to_batches(
        columns=columns,
        filter=where,
        batch_size=1_000_000,
        batch_readahead=2,
        fragment_readahead=1,
    )


def read(
    fs: pafs.FileSystem,
    root: str,
    partition: Partition,
    columns: list[str],
    where: pc.Expression | None = None,
) -> pa.Table:
    """One partition's `columns`, keeping the rows matching `where`."""
    dataset = _dataset(fs, root, partition)
    return dataset.to_table(columns=columns, filter=where)
