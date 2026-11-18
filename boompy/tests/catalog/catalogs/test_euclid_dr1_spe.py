"""Euclid DR1 SPE: the join and the reliability cut, on a synthetic HATS tree
laid out like IRSA's (classification coarser than the candidate tables)."""

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

from boompy.catalog import hats
from boompy.catalog.catalogs import euclid_dr1_spe as spe

#: object_id -> (class, galaxy rank-0 (z, prob), qso rank-0 (z, prob))
OBJECTS = {
    1: ("galaxy", (1.2, 0.999), (2.0, 0.2)),
    2: ("galaxy", (0.9, 0.5), (2.1, 0.999)),  # unreliable as a galaxy
    3: ("qso", (1.0, 0.1), (2.5, 0.995)),
    4: ("star", (1.1, 0.999), (2.2, 0.999)),
    5: ("galaxy", (1.4, 0.998), (2.0, 0.1)),  # in a candidate partition
}


def _write(root, table, order, pixel, rows):
    path = (
        root
        / f"spectro_z_spe_{table}_deep"
        / f"euclid_dr1_reformatted_catalogs_spectro_z_spe_{table}_deep-hats"
    )
    part = path / "dataset" / f"Norder={order}/Dir=0/Npix={pixel}"
    part.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), part / "part0.parquet")
    info = path / "partition_info.csv"
    if not info.exists():
        info.write_text("Norder,Dir,Npix,num_rows\n")
    with info.open("a") as f:
        f.write(f"{order},0,{pixel},{len(rows)}\n")


def _candidates(object_id, z, prob):
    base = {
        "object_id": object_id,
        "spe_z_err": 0.001,
        "spe_z_rel": 0.2,
        "spe_subclass": "Sbc",
    }
    return [
        {**base, "spe_rank": 0, "spe_z": z, "spe_z_prob": prob},
        # A worse candidate that would pass the cut if rank were ignored.
        {**base, "spe_rank": 1, "spe_z": z + 1, "spe_z_prob": 0.9999},
    ]


@pytest.fixture
def tree(tmp_path, monkeypatch):
    monkeypatch.setattr(spe, "ROOT", str(tmp_path))
    monkeypatch.setattr(spe, "SURVEYS", {"deep": "deep"})
    monkeypatch.setattr(spe, "_filesystem", pafs.LocalFileSystem)

    _write(
        tmp_path,
        "classification",
        0,
        2,
        [
            {
                "object_id": i,
                "ra_from_object_id": float(i),
                "dec_from_object_id": -float(i),
                "spe_class": c,
                "spe_star_prob": 0.0,
                "spe_gal_prob": 1.0,
                "spe_qso_prob": 0.0,
            }
            for i, (c, _, _) in OBJECTS.items()
        ],
    )
    for table, which in (("galaxy_candidates", 1), ("qso_candidates", 2)):
        rows = {8: [], 9: []}
        for i, values in OBJECTS.items():
            rows[8 if i < 5 else 9] += _candidates(i, *values[which])
        for pixel, part in rows.items():
            _write(tmp_path, table, 1, pixel, part)
        # Order 1 pixel 12 is inside order 0 pixel 3, not this chunk.
        _write(tmp_path, table, 1, 12, _candidates(99, 1.0, 0.999))
    return tmp_path


def test_each_object_takes_the_reliable_redshift_of_its_class(tree):
    table = spe.reliable("deep", hats.Partition(0, 2))
    rows = {r["object_id"]: r for r in table.to_pylist()}
    assert set(rows) == {1, 3, 5}
    assert rows[1]["spe_z"] == pytest.approx(1.2)
    assert rows[3]["spe_class"] == "qso"
    assert rows[3]["spe_z"] == pytest.approx(2.5)
    assert rows[5]["ra"] == 5.0 and rows[5]["dec"] == -5.0
    assert rows[1]["survey"] == "deep"


def test_a_chunk_is_a_classification_partition(tree, tmp_path):
    [chunk] = spe.list_chunks()
    assert chunk.id == "deep_order0_pix2"
    [path] = spe.fetch_chunk(chunk.id, tmp_path / "out")
    assert set(pq.read_schema(path).names) == {
        "object_id",
        "ra",
        "dec",
        "survey",
        "spe_class",
        "spe_star_prob",
        "spe_gal_prob",
        "spe_qso_prob",
        "spe_z",
        "spe_z_err",
        "spe_z_prob",
        "spe_z_rel",
        "spe_subclass",
    }


def test_a_malformed_chunk_id_is_refused():
    with pytest.raises(ValueError, match="malformed"):
        spe.fetch_chunk("shallow_order0_pix2", None)


def test_deep_is_ingested_before_wide():
    """The ingest keeps the first document per `_id`."""
    assert list(spe.SURVEYS) == ["deep", "wide"]
