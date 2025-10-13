use cdshealpix::nested::get;
use moc::deser::fits::{from_fits_ivoa, MocIdxType, MocQtyType, MocType};
use moc::moc::range::CellSelection;
use moc::moc::range::RangeMOC;
use moc::moc::CellMOCIntoIterator;
use moc::moc::CellMOCIterator;
use moc::moc::HasMaxDepth;
use moc::qty::Hpx;

fn main() {
    let path = "./data/ls_dr10_footprint_moc.fits";
    let file = std::fs::File::open(path).expect("Failed to open file");
    let reader = std::io::BufReader::new(file);

    let moc: RangeMOC<u32, Hpx<u32>> = match from_fits_ivoa(reader) {
        Ok(MocIdxType::U32(MocQtyType::Hpx(MocType::Ranges(moc)))) => {
            let moc = RangeMOC::new(moc.depth_max(), moc.collect());
            moc
        }
        Ok(MocIdxType::U32(MocQtyType::Hpx(MocType::Cells(moc)))) => {
            let moc = RangeMOC::new(moc.depth_max(), moc.into_cell_moc_iter().ranges().collect());
            moc
        }
        _ => unreachable!("Type not supposed to be != from U32"),
    };

    // create a RangeMOC<u64, Hpx<u64>> from the moc
    // println!("MOC max depth: {}", moc.depth_max());
    // println!("MOC number of cells: {}", moc.len());

    let moc64: RangeMOC<u64, Hpx<u64>> = RangeMOC::new(
        moc.depth_max(),
        moc.moc_ranges()
            .iter()
            .map(|r| (r.start as u64)..(r.end as u64))
            .collect(),
    );
    // println!("MOC64 number of cells: {}", moc64.len());

    // now convert back to RangeMOC<u32, Hpx<u32>>
    let moc32: RangeMOC<u32, Hpx<u32>> = RangeMOC::new(
        moc64.depth_max(),
        moc64
            .moc_ranges()
            .iter()
            .map(|r| (r.start as u32)..(r.end as u32))
            .collect(),
    );
    // println!("MOC32 number of cells: {}", moc32.len());
    // verify that its equal to the original moc
    assert_eq!(moc, moc32);

    let ra: f64 = 190.910481;
    let dec: f64 = -64.9348182;
    let ra = ra.to_radians();
    let dec = dec.to_radians();

    let depth = 11_u8;
    let layer = get(depth);
    let start = std::time::Instant::now();
    let cell = layer.hash(ra, dec);
    let duration = start.elapsed();
    assert!(cell <= u32::MAX as u64, "Cell index overflows u32");
    println!(
        "\nCell index (u32, at depth {}): {} (took {:?})",
        depth, cell, duration
    );
    let cell = cell as u32;
    let start = std::time::Instant::now();
    let is_in_footprint = moc.contains_cell(depth, cell);
    let duration = start.elapsed();
    println!(
        "Is in footprint (u32): {} (took {:?})",
        is_in_footprint, duration
    );

    // same but with the moc64 and depth = 29
    let depth = 14_u8;
    let layer = get(depth);
    let start = std::time::Instant::now();
    let cell = layer.hash(ra, dec);
    let duration = start.elapsed();
    println!(
        "\nCell index (u64, at depth {}): {} (took {:?})",
        depth, cell, duration
    );
    let start = std::time::Instant::now();
    let is_in_footprint = moc64.contains_cell(depth, cell);
    let duration = start.elapsed();
    println!(
        "Is in footprint (u64): {} (took {:?})",
        is_in_footprint, duration
    );

    // let's try a different approach, we create a moc from the ra/dec with a small radius
    let depth = 11_u8;
    let radius = (30.0_f64 / 3600.0_f64).to_radians(); // in radians
    let ra2 = ra;
    let dec2 = dec;
    let start = std::time::Instant::now();
    let moc_point: RangeMOC<u32, Hpx<u32>> =
        RangeMOC::from_cone(ra2, dec2, radius, depth, 2, CellSelection::All).into();
    // print the max depth of the moc_point, the time to create it, and the number of cells
    let duration = start.elapsed();
    println!(
        "\nMOC point: max depth: {}, took {:?}, number of cells: {}",
        moc_point.depth_max(),
        duration,
        moc_point.len()
    );

    let start = std::time::Instant::now();
    let intersection = moc.intersection(&moc_point);
    let duration = start.elapsed();
    println!(
        "Has intersection (u32): {} (took {:?})",
        !intersection.is_empty(),
        duration
    );

    // now with the moc64
    let depth = 14_u8; // angular resolution ~ 1.61 arcsec
    let start = std::time::Instant::now();
    let moc_point: RangeMOC<u64, Hpx<u64>> =
        RangeMOC::from_cone(ra2, dec2, radius, depth, 2, CellSelection::All).into();
    // print the max depth of the moc_point, the time to create it, and the number of cells
    let duration = start.elapsed();
    println!(
        "\nMOC point: max depth: {}, took {:?}, number of cells: {}",
        moc_point.depth_max(),
        duration,
        moc_point.len()
    );
    let start = std::time::Instant::now();
    let intersection = moc64.intersection(&moc_point);
    let duration = start.elapsed();
    println!(
        "Has intersection (u64): {} (took {:?})",
        !intersection.is_empty(),
        duration
    );
}
