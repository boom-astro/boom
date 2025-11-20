use fitsio::{hdu::HduInfo, FitsFile};
use indicatif::ProgressBar;
use moc::moc::range::CellSelection;
use moc::moc::range::RangeMOC;
use moc::qty::Hpx;

#[derive(Debug)]
struct Brick {
    brickname: String, // Name of the brick.
    ra: f64,           // RA of the center of the brick.
    dec: f64,          // Dec of the center of the brick.
    nexp_g: u16, // Median number of exposures in the unique area (i.e. BRICK_PRIMARY area) of the brick in g-band.
    nexp_r: u16, // Median number of exposures in the unique area of the brick in r-band.
    nexp_i: u16, // Median number of exposures in the unique area of the brick in i-band.
    nexp_z: u16, // Median number of exposures in the unique area of the brick in z-band.
    // nexphist_g: [i32; 11], // Histogram of number of exposures in g band (int32[11]).
    // nexphist_r: [i32; 11], // Histogram of number of exposures in r band (int32[11]).
    // nexphist_i: [i32; 11], // Histogram of number of exposures in i band (int32[11]).
    // nexphist_z: [i32; 11], // Histogram of number of exposures in z band (int32[11]).
    ra1: f64,  // Lower RA boundary.
    ra2: f64,  // Upper RA boundary.
    dec1: f64, // Lower Dec boundary.
    dec2: f64, // Upper Dec boundary.
}

impl Brick {
    fn read_batch(
        hdu: &fitsio::hdu::FitsHdu,
        fptr: &mut FitsFile,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<Brick>, fitsio::errors::Error> {
        let brickname_col: Vec<String> = hdu.read_col_range(fptr, "brickname", &range)?;
        let ra_col: Vec<f64> = hdu.read_col_range(fptr, "ra", &range)?;
        let dec_col: Vec<f64> = hdu.read_col_range(fptr, "dec", &range)?;
        let ra1_col: Vec<f64> = hdu.read_col_range(fptr, "ra1", &range)?;
        let ra2_col: Vec<f64> = hdu.read_col_range(fptr, "ra2", &range)?;
        let dec1_col: Vec<f64> = hdu.read_col_range(fptr, "dec1", &range)?;
        let dec2_col: Vec<f64> = hdu.read_col_range(fptr, "dec2", &range)?;
        let nexp_g_col: Vec<u16> = hdu.read_col_range(fptr, "nexp_g", &range)?;
        let nexp_r_col: Vec<u16> = hdu.read_col_range(fptr, "nexp_r", &range)?;
        let nexp_i_col: Vec<u16> = match hdu.read_col_range(fptr, "nexp_i", &range) {
            Ok(col) => col,
            Err(_) => vec![0; range.end - range.start], // if column not found, fill with zeros
        };
        let nexp_z_col: Vec<u16> = hdu.read_col_range(fptr, "nexp_z", &range)?;

        // Combine the columns into a Vec<Brick>
        let mut rows = Vec::with_capacity(brickname_col.len());
        for i in 0..brickname_col.len() {
            rows.push(Brick {
                brickname: brickname_col[i].clone(),
                ra: ra_col[i],
                dec: dec_col[i],
                ra1: ra1_col[i],
                ra2: ra2_col[i],
                dec1: dec1_col[i],
                dec2: dec2_col[i],
                nexp_g: nexp_g_col[i],
                nexp_r: nexp_r_col[i],
                nexp_i: nexp_i_col[i],
                nexp_z: nexp_z_col[i],
            });
        }
        Ok(rows)
    }
}

fn create_moc_from_bricks(
    fits_path: &str,
    depth: u8,
    batch_size: usize,
) -> RangeMOC<u64, Hpx<u64>> {
    let mut fptr = FitsFile::open(fits_path).expect("Failed to open FITS file");
    let tlb_hdu = fptr.hdu(1).expect("Failed to get HDU");

    let num_rows = if let HduInfo::TableInfo { num_rows, .. } = tlb_hdu.info {
        num_rows
    } else {
        0
    };

    let progress_bar = ProgressBar::new(num_rows as u64)
        .with_message(format!("Processing bricks from {}", fits_path))
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} {pos}/{len} ({eta})")
                .unwrap(),
        );

    println!("Number of rows in the table: {}", num_rows);

    let mut footprint_moc: Option<RangeMOC<u64, Hpx<u64>>> = None;
    for chunk_start in (0..num_rows).step_by(batch_size) {
        let chunk_end = (chunk_start + batch_size).min(num_rows);
        let range = chunk_start..chunk_end;
        let rows = Brick::read_batch(&tlb_hdu, &mut fptr, range).expect("Failed to read batch");
        for row in rows {
            // only process bricks that have imaging data in at least one band
            // if row.nexp_g == 0 && row.nexp_r == 0 && row.nexp_i == 0 && row.nexp_z == 0 {
            //     progress_bar.inc(1);
            //     continue;
            // }
            let ra_lower_rad = row.ra1.to_radians();
            let ra_upper_rad = row.ra2.to_radians();
            let dec_lower_rad = row.dec1.to_radians();
            let dec_upper_rad = row.dec2.to_radians();
            let moc = RangeMOC::from_zone(
                ra_lower_rad,
                dec_lower_rad,
                ra_upper_rad,
                dec_upper_rad,
                depth,
                CellSelection::All,
            );
            match &mut footprint_moc {
                Some(fp_moc) => {
                    *fp_moc = fp_moc.or(&moc);
                }
                None => {
                    footprint_moc = Some(moc);
                }
            }
            progress_bar.inc(1);
        }
    }

    let footprint_moc = footprint_moc.expect("No footprint MOC generated");
    let footprint_moc = footprint_moc.fill_holes(None);
    footprint_moc
}

fn main() {
    let batch_size = 10_000;
    let depth = 11_u8;

    // let dr10_south_footprint_moc = create_moc_from_bricks("./data/summary-dr10.1-all.fits", depth, batch_size);
    // let dr9_south_footprint_moc = create_moc_from_bricks("./data/survey-bricks-dr9-south.fits", depth, batch_size);
    // let dr9_north_footprint_moc = create_moc_from_bricks("./data/survey-bricks-dr9-north.fits", depth, batch_size);
    // let mut footprint_moc = dr10_south_footprint_moc.or(&dr9_north_footprint_moc).or(&dr9_south_footprint_moc);
    // footprint_moc = footprint_moc.fill_holes(None);
    // let's just have a vec of paths to process
    let fits_paths = vec![
        "./data/summary-dr10.1-all.fits",
        "./data/survey-bricks-dr9-south.fits",
        "./data/survey-bricks-dr9-north.fits",
    ];
    let mut footprint_moc: RangeMOC<u64, Hpx<u64>> =
        create_moc_from_bricks(fits_paths[0], depth, batch_size);
    for fits_path in &fits_paths[1..] {
        let moc = create_moc_from_bricks(fits_path, depth, batch_size);
        footprint_moc = footprint_moc.or(&moc);
    }
    footprint_moc = footprint_moc.fill_holes(None);

    println!(
        "Final footprint MOC after filling holes: depth {}, number of cells {}",
        footprint_moc.depth_max(),
        footprint_moc.len()
    );
    // print all the cells
    let coverage_percentage = footprint_moc.coverage_percentage();
    println!(
        "Footprint coverage percentage: {:.6}%",
        coverage_percentage * 100.0
    );

    let max_depth = footprint_moc.depth_max();
    println!("Footprint MOC max depth: {}", max_depth);

    // println!("Moc: {:?}", footprint_moc);

    // we have a RangeMOC<u64, Hpx<u64>>, convert it to RangeMOC<u32, Hpx<u32>>
    // let footprint_moc: RangeMOC<u32, Hpx<u32>> = footprint_moc.into();

    // save the footprint MOC to a file
    let output_path = "./data/ls_dr10_footprint_moc_new.fits";
    let path = std::path::Path::new(output_path);
    footprint_moc
        .to_fits_file_ivoa(None, None, path)
        .expect("Failed to write MOC to FITS file");
    println!("Footprint MOC saved to {}", output_path);
}
