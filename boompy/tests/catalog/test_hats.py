import pyarrow.fs as pafs

from boompy.catalog.hats import Partition, partitions


def test_a_pixel_overlaps_its_ancestors_and_descendants():
    assert Partition(0, 2).overlaps(Partition(1, 8))
    assert Partition(1, 11).overlaps(Partition(0, 2))
    assert Partition(2, 45).overlaps(Partition(2, 45))
    assert not Partition(0, 2).overlaps(Partition(1, 12))
    assert not Partition(1, 8).overlaps(Partition(1, 9))


def test_dir_groups_pixels_by_ten_thousand():
    assert Partition(6, 35923).dir == 30000
    assert Partition(0, 2).dir == 0


def test_partitions_are_read_from_partition_info(tmp_path):
    (tmp_path / "partition_info.csv").write_text(
        "Norder,Dir,Npix,num_rows\n1,0,9,5\n0,0,2,10\n"
    )
    assert partitions(pafs.LocalFileSystem(), str(tmp_path)) == [
        Partition(0, 2),
        Partition(1, 9),
    ]
