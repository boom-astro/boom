"""Euclid DR1 photometric redshifts, from IRSA's HATS mirror on S3.

Deep fields only: IRSA has not yet published the Wide photo-z as HATS. Adding
it is an entry in `SURVEYS` once it has.

Kept: rows with `phz_flags` 0 that PHZ accepts as a galaxy and not as a star,
which is what a host redshift needs. The 601-point PDF is not read.
"""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .. import hats
from ..base import Chunk, ensure_dir
from ..http import log

ID = "euclid-dr1-phz"

ROOT = "nasa-irsa-euclid-dr1/contributed/dr1/hats/reformatted_catalogs"

#: Chunk-id prefix to IRSA's table name.
SURVEYS = {"deep": "phz_photo_z_deep_survey"}

#: `phz_classification` bits.
STAR, GALAXY = 1, 2

COLUMNS = [
    "object_id",
    "ra_from_object_id",
    "dec_from_object_id",
    "phz_median",
    "phz_70_int",
    "phz_90_int",
    "phz_mode_1",
    "phz_mode_1_area",
    "phz_mode_2",
    "phz_mode_2_area",
    "phz_classification",
    "phz_flags",
    "phz_weight",
    "best_chi2",
    # The matching `fluxerr_*_unif` columns are published as all zeros.
    "flux_vis_unif",
    "flux_y_unif",
    "flux_j_unif",
    "flux_h_unif",
]

#: Interval columns published as two-element lists, stored as IRSA's TAP
#: service names them.
INTERVALS = ["phz_70_int", "phz_90_int"]


def root(survey: str) -> str:
    name = SURVEYS[survey]
    return f"{ROOT}/{name}/euclid_dr1_reformatted_catalogs_{name}-hats"


def _filesystem():
    return hats.s3()


def list_chunks() -> list[Chunk]:
    chunks = []
    for survey in SURVEYS:
        parts = hats.partitions(_filesystem(), root(survey))
        log(f"Euclid DR1 PHZ {survey}: {len(parts)} partitions")
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
        raise ValueError(f"malformed Euclid PHZ chunk id {chunk_id!r}") from e


def galaxies(survey: str, partition: hats.Partition) -> pa.Table:
    """The rows of `partition` PHZ is confident are galaxies."""
    classification = pc.field("phz_classification")
    where = (
        (pc.field("phz_flags") == 0)
        & (pc.bit_wise_and(classification, GALAXY) != 0)
        & (pc.bit_wise_and(classification, STAR) == 0)
    )
    table = hats.read(_filesystem(), root(survey), partition, COLUMNS, where)
    for name in INTERVALS:
        bounds = table.column(name)
        index = table.schema.get_field_index(name)
        table = table.remove_column(index)
        for i in (1, 2):
            bound = pc.list_element(bounds, i - 1)
            table = table.add_column(index + i - 1, f"{name}_{i}", bound)
    table = table.rename_columns(
        {"ra_from_object_id": "ra", "dec_from_object_id": "dec"}
    )
    return table.append_column("survey", pa.array([survey] * len(table)))


def fetch_chunk(chunk_id: str, dest: Path) -> list[Path]:
    survey, partition = _parse_chunk_id(chunk_id)
    path = ensure_dir(dest) / f"{chunk_id}.parquet"
    table = galaxies(survey, partition)
    pq.write_table(table, path)
    log(f"Euclid DR1 PHZ {chunk_id}: {len(table)} galaxies")
    return [path]
