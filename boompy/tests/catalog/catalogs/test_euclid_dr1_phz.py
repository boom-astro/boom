"""Euclid DR1 PHZ: the galaxy cut and the interval columns, on a synthetic
HATS partition shaped like IRSA's."""

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

from boompy.catalog import hats
from boompy.catalog.catalogs import euclid_dr1_phz as phz

#: object_id -> (phz_classification, phz_flags)
ROWS = {1: (2, 0), 2: (3, 0), 3: (2, 11), 4: (1, 0), 5: (6, 0)}


@pytest.fixture
def tree(tmp_path, monkeypatch):
    monkeypatch.setattr(phz, "ROOT", str(tmp_path))
    monkeypatch.setattr(phz, "_filesystem", pafs.LocalFileSystem)
    root = tmp_path / phz.SURVEYS["deep"]
    root = root / f"euclid_dr1_reformatted_catalogs_{phz.SURVEYS['deep']}-hats"
    part = root / "dataset" / "Norder=6/Dir=30000/Npix=35923"
    part.mkdir(parents=True)
    (root / "partition_info.csv").write_text(
        "Norder,Dir,Npix,num_rows\n6,30000,35923,5\n"
    )
    columns = {name: [1.0] * len(ROWS) for name in phz.COLUMNS}
    columns["object_id"] = list(ROWS)
    columns["phz_classification"] = [c for c, _ in ROWS.values()]
    columns["phz_flags"] = [f for _, f in ROWS.values()]
    for name in phz.INTERVALS:
        columns[name] = [[0.5, 0.7]] * len(ROWS)
    # The PDF is published but never read.
    columns["phz_pdf"] = [[0.0] * 601] * len(ROWS)
    pq.write_table(pa.table(columns), part / "part0.parquet")


def test_only_unflagged_non_stellar_galaxies_are_kept(tree):
    table = phz.galaxies("deep", hats.Partition(6, 35923))
    assert table.column("object_id").to_pylist() == [1, 5]


def test_intervals_are_split_into_bounds(tree, tmp_path):
    [chunk] = phz.list_chunks()
    assert chunk.id == "deep_order6_pix35923"
    [path] = phz.fetch_chunk(chunk.id, tmp_path / "out")
    row = pq.read_table(path).to_pylist()[0]
    assert (row["phz_70_int_1"], row["phz_70_int_2"]) == (0.5, 0.7)
    assert "phz_70_int" not in row and "phz_pdf" not in row
    assert (row["ra"], row["survey"]) == (1.0, "deep")
