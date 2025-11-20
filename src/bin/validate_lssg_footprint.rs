use boom::conf::build_db;
use boom::conf::load_config;
use cdshealpix::nested::get;
use futures::StreamExt;
use indicatif::ProgressBar;
use moc::deser::fits::{from_fits_ivoa, MocIdxType, MocQtyType, MocType};
use moc::moc::range::RangeMOC;
use moc::moc::CellMOCIntoIterator;
use moc::moc::CellMOCIterator;
use moc::moc::HasMaxDepth;
use moc::qty::Hpx;
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct LSSG {
    #[serde(rename = "_id")]
    id: i64,
    ra: f64,
    dec: f64,
}

#[tokio::main]
async fn main() {
    let path = "./data/ls_dr10_footprint_moc_new_v2.fits";
    let file = std::fs::File::open(path).expect("Failed to open file");
    let reader = std::io::BufReader::new(file);

    let moc: RangeMOC<u64, Hpx<u64>> = match from_fits_ivoa(reader) {
        Ok(MocIdxType::U64(MocQtyType::Hpx(MocType::Ranges(moc)))) => {
            let moc = RangeMOC::new(moc.depth_max(), moc.collect());
            moc
        }
        Ok(MocIdxType::U64(MocQtyType::Hpx(MocType::Cells(moc)))) => {
            let moc = RangeMOC::new(moc.depth_max(), moc.into_cell_moc_iter().ranges().collect());
            moc
        }
        _ => unreachable!("Type not supposed to be != from U64"),
    };

    // create a RangeMOC<u64, Hpx<u64>> from the moc
    println!("MOC max depth: {}", moc.depth_max());
    println!("MOC number of cells: {}", moc.len());
    println!(
        "MOC coverage percentage: {:.6}%",
        moc.coverage_percentage() * 100.0
    );

    let depth = 11_u8;
    let layer = get(depth);

    let config = load_config("./config.yaml").expect("Failed to load config");
    let db = build_db(&config)
        .await
        .expect("Failed to build database connection");
    let collection = db.collection::<LSSG>("lssg");

    // let's do all the sources. For that, do an estimate of the total count first
    let total_count = collection
        .estimated_document_count()
        .await
        .expect("Failed to estimate document count");
    println!("Total number of LSSG sources: {}", total_count);

    // then do a find with a batch size (sort by _id to have a deterministic order)
    let batch_size = 100_000;

    let progress_bar = ProgressBar::new(total_count)
        .with_message(format!("Validating LSSG sources against footprint MOC"))
        .with_style(
            indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} {pos}/{len} ({eta})")
                .unwrap(),
        );

    let mut nb_in_footprint = 0u64;
    let mut nb_out_footprint = 0u64;

    let mut last_id = None;
    for _ in (0..total_count).step_by(batch_size as usize) {
        let mut cursor = match last_id {
            Some(id) => collection
                .find(doc! { "_id": { "$gt": id } })
                .sort(doc! { "_id": 1 })
                .limit(batch_size as i64)
                .await
                .expect("Failed to execute find"),
            None => collection
                .find(doc! {})
                .sort(doc! { "_id": 1 })
                .limit(batch_size as i64)
                .await
                .expect("Failed to execute find"),
        };

        while let Some(result) = cursor.next().await {
            let source = result.expect("Failed to get source from cursor");
            let ra = source.ra.to_radians();
            let dec = source.dec.to_radians();
            let cell = layer.hash(ra, dec);
            if !moc.contains_cell(depth, cell) {
                // println!(
                //     "Source ID {} at RA: {}, Dec: {} is outside the footprint",
                //     source.id, source.ra, source.dec
                // );
                // panic!(
                //     "Validation failed: source ID {} is outside the footprint",
                //     source.id
                // );
                nb_out_footprint += 1;
                println!(
                    "Source ID {} at RA: {}, Dec: {} is outside the footprint",
                    source.id, source.ra, source.dec
                );
            } else {
                nb_in_footprint += 1;
            }
            last_id = Some(source.id);
            progress_bar.inc(1);
        }

        // every 10M sources, print a summary
        if (nb_in_footprint + nb_out_footprint) % 10_000_000 == 0 {
            println!(
                "Processed {} sources: {} in footprint, {} out of footprint",
                nb_in_footprint + nb_out_footprint,
                nb_in_footprint,
                nb_out_footprint
            );
        }
    }

    println!("Validation completed.");
    println!("Total sources in footprint: {}", nb_in_footprint);
    println!("Total sources out of footprint: {}", nb_out_footprint);
}
