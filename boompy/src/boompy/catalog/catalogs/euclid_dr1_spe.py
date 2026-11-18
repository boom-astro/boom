"""Euclid DR1 spectroscopic redshifts, from IRSA's HATS mirror on S3.

SPE fits every MER object, so the published tables are mostly noise. BOOM keeps
the objects with a spectrum (a row in the classification table) classified as a
galaxy or QSO whose rank-0 redshift for that class has `spe_z_prob` at least
`MIN_Z_PROB`. Classified objects carry `SPE_ERROR_FLAG` 0 (checked on a Deep
partition and a Wide tile), so the quality table, seven times larger, is not
read.

A chunk is one classification partition. The candidate tables are partitioned
more finely, so a chunk reads the candidate partitions overlapping it, and
matching on `object_id` keeps the rows inside it.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .. import hats
from ..base import Chunk, ensure_dir
from ..http import log

ID = "euclid-dr1-spe"

ROOT = "nasa-irsa-euclid-dr1/contributed/dr1/hats/reformatted_catalogs"

#: Chunk-id prefix to IRSA's table suffix. Deep first, so an object in both
#: keeps its Deep redshift: the ingest keeps the first document per `_id`.
SURVEYS = {"deep": "deep", "wide": "wide_survey"}

MIN_Z_PROB = 0.99

CLASS_COLUMNS = [
    "object_id",
    "ra_from_object_id",
    "dec_from_object_id",
    "spe_class",
    "spe_star_prob",
    "spe_gal_prob",
    "spe_qso_prob",
]
CANDIDATE_COLUMNS = [
    "object_id",
    "spe_z",
    "spe_z_err",
    "spe_z_prob",
    "spe_z_rel",
    "spe_subclass",
]
#: SPE class to the candidate table its redshift comes from.
CANDIDATES = {"galaxy": "galaxy_candidates", "qso": "qso_candidates"}


def root(table: str, survey: str) -> str:
    name = f"spectro_z_spe_{table}_{SURVEYS[survey]}"
    return f"{ROOT}/{name}/euclid_dr1_reformatted_catalogs_{name}-hats"


def _filesystem():
    return hats.s3()


def list_chunks() -> list[Chunk]:
    chunks = []
    for survey in SURVEYS:
        parts = hats.partitions(_filesystem(), root("classification", survey))
        log(f"Euclid DR1 SPE {survey}: {len(parts)} partitions")
        chunks += [
            Chunk(
                id=f"{survey}_order{p.order}_pix{p.pixel}",
                label=f"{survey}, HEALPix order {p.order} pixel {p.pixel}",
            )
            for p in parts
        ]
    return chunks


def _parse_chunk_id(chunk_id: str) -> tuple[str, hats.Partition]:
    try:
        survey, order, pixel = chunk_id.split("_")
        if survey not in SURVEYS:
            raise ValueError(survey)
        return survey, hats.Partition(
            int(order.removeprefix("order")), int(pixel.removeprefix("pix"))
        )
    except ValueError as e:
        raise ValueError(f"malformed Euclid SPE chunk id {chunk_id!r}") from e


def _reliable_candidates(
    fs, table: str, survey: str, partition: hats.Partition
) -> pa.Table | None:
    """Rank-0 candidates passing the cut, from the partitions of `table`
    overlapping `partition`. A small fraction of the rows, so held whole."""
    candidates = root(table, survey)
    where = (pc.field("spe_rank") == 0) & (pc.field("spe_z_prob") >= MIN_Z_PROB)
    found = [
        hats.read(fs, candidates, part, CANDIDATE_COLUMNS, where)
        for part in hats.partitions(fs, candidates)
        if part.overlaps(partition)
    ]
    return pa.concat_tables(found) if found else None


def _attach(
    objects: pa.Table, candidates: pa.Table, lookup: pc.SetLookupOptions
) -> pa.Table:
    """The `objects` with a candidate, with its columns appended.

    A lookup on `object_id` rather than `Table.join`, which rebuilds its hash
    table for every batch and peaked at 7 GB on the largest Wide partition.
    """
    index = pc.index_in(objects.column("object_id"), options=lookup)
    found = pc.is_valid(index)
    rows = candidates.take(pc.filter(index, found)).drop_columns("object_id")
    objects = objects.filter(found)
    return pa.Table.from_arrays(
        objects.columns + rows.columns,
        names=objects.column_names + rows.column_names,
    )


def reliable(survey: str, partition: hats.Partition) -> pa.Table | None:
    """The objects of `partition` with a reliable redshift for their class."""
    fs = _filesystem()
    candidates = {
        spe_class: _reliable_candidates(fs, table, survey, partition)
        for spe_class, table in CANDIDATES.items()
    }
    candidates = {k: v for k, v in candidates.items() if v is not None}
    if not candidates:
        return None
    lookups = {
        spe_class: pc.SetLookupOptions(found.column("object_id"))
        for spe_class, found in candidates.items()
    }

    # A Wide classification partition holds up to 21 million objects, so it is
    # matched a batch at a time.
    matched = []
    for batch in hats.scan(
        fs,
        root("classification", survey),
        partition,
        CLASS_COLUMNS,
        pc.field("spe_class").isin(list(CANDIDATES)),
    ):
        objects = pa.Table.from_batches([batch])
        for spe_class, found in candidates.items():
            of_class = objects.filter(pc.field("spe_class") == spe_class)
            matched.append(_attach(of_class, found, lookups[spe_class]))

    out = pa.concat_tables(matched).rename_columns(
        {"ra_from_object_id": "ra", "dec_from_object_id": "dec"}
    )
    out = out.append_column("survey", pa.array([survey] * len(out)))
    # Batches arrive in no fixed order; this keeps a rerun's output identical.
    return out.sort_by("object_id")


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    survey, partition = _parse_chunk_id(chunk_id)
    path = ensure_dir(dest) / f"{chunk_id}.parquet"
    table = reliable(survey, partition)
    if table is None:
        log(f"Euclid DR1 SPE {chunk_id}: no candidate partitions overlap it")
        return []
    pq.write_table(table, path)
    log(f"Euclid DR1 SPE {chunk_id}: {len(table)} reliable redshifts")
    return [path]
