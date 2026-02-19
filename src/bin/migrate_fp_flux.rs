use boom::conf::{load_dotenv, AppConfig};
use clap::Parser;
use futures::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use mongodb::bson::{doc, Bson, Document};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

/// Fixed zeropoint for ZTF forced photometry.
const ZTF_ZP: f64 = 23.9;

/// Migrate ZTF forced photometry flux values to a fixed zeropoint.
///
/// Rescales `psfFlux` and `psfFluxErr` in `fp_hists` from per-datapoint
/// `magzpsci` zeropoints to the fixed ZTF_ZP = 23.9 zeropoint.
///
/// Formula: new_value = old_value * 10^((23.9 - magzpsci) / 2.5)
///
/// Safe to re-run: old values are saved as `_old_psfFlux` / `_old_psfFluxErr`
/// in each fp_hists entry. Documents already containing these fields are skipped.
/// Use `--cleanup` to remove the `_old_*` fields once migration is verified.
#[derive(Parser)]
struct Cli {
    /// Number of document IDs to collect per update_many batch
    #[arg(long, default_value = "5000")]
    batch_size: usize,

    /// Remove _old_psfFlux and _old_psfFluxErr fields instead of migrating
    #[arg(long)]
    cleanup: bool,
}

/// Run batched updates by streaming IDs from a cursor and calling update_many
/// with `{ _id: { $in: [...] } }` per batch.
async fn run_batched_update(
    collection: &mongodb::Collection<Document>,
    filter: Document,
    pipeline: Vec<Document>,
    batch_size: usize,
    estimated_total: u64,
    label: &str,
) -> i64 {
    let pb = ProgressBar::new(estimated_total);
    pb.set_style(
        ProgressStyle::with_template(
            "{msg} {bar:40} {pos}/{len} [{elapsed_precise} < {eta_precise}]",
        )
        .unwrap(),
    );
    pb.set_message(label.to_string());

    let mut cursor = match collection
        .find(filter)
        .projection(doc! { "_id": 1 })
        .no_cursor_timeout(true)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            error!("error querying documents: {}", e);
            std::process::exit(1);
        }
    };

    let mut ids: Vec<Bson> = Vec::with_capacity(batch_size);
    let mut total_modified: i64 = 0;

    while let Some(d) = cursor.try_next().await.unwrap() {
        ids.push(d.get("_id").unwrap().clone());

        if ids.len() >= batch_size {
            let n = ids.len() as u64;
            let batch_filter = doc! { "_id": { "$in": &ids } };
            match collection.update_many(batch_filter, pipeline.clone()).await {
                Ok(result) => {
                    total_modified += result.modified_count as i64;
                }
                Err(e) => {
                    error!("error writing batch: {}", e);
                    std::process::exit(1);
                }
            }
            pb.inc(n);
            ids.clear();
        }
    }

    if !ids.is_empty() {
        let n = ids.len() as u64;
        let batch_filter = doc! { "_id": { "$in": &ids } };
        match collection.update_many(batch_filter, pipeline).await {
            Ok(result) => {
                total_modified += result.modified_count as i64;
            }
            Err(e) => {
                error!("error writing final batch: {}", e);
                std::process::exit(1);
            }
        }
        pb.inc(n);
    }

    pb.finish();
    total_modified
}

#[tokio::main]
async fn main() {
    load_dotenv();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Cli::parse();

    let config = AppConfig::from_default_path().unwrap();
    let db = match config.build_db().await {
        Ok(db) => db,
        Err(e) => {
            error!("error building db: {}", e);
            std::process::exit(1);
        }
    };

    let collection = db.collection::<Document>("ZTF_alerts_aux");

    if args.cleanup {
        cleanup(&collection, args.batch_size).await;
    } else {
        migrate(&collection, args.batch_size).await;
    }
}

async fn migrate(collection: &mongodb::Collection<Document>, batch_size: usize) {
    let estimated_count = match collection.estimated_document_count().await {
        Ok(c) => c,
        Err(e) => {
            error!("error estimating document count: {}", e);
            std::process::exit(1);
        }
    };
    info!("Estimated ~{} documents in collection", estimated_count);

    // Skip documents already migrated (they have _old_psfFlux in their fp_hists)
    let filter = doc! {
        "fp_hists.0": { "$exists": true },
        "fp_hists.0._old_psfFlux": { "$exists": false },
    };

    // Scale factor: 10^((ZTF_ZP - magzpsci) / 2.5)
    let scale_factor = doc! {
        "$pow": [
            10.0,
            { "$divide": [
                { "$subtract": [ZTF_ZP, "$$fp.magzpsci"] },
                2.5
            ]}
        ]
    };

    let new_flux = doc! {
        "$cond": {
            "if": { "$and": [
                { "$ne": ["$$fp.magzpsci", Bson::Null] },
                { "$ne": ["$$fp.psfFlux", Bson::Null] },
            ]},
            "then": { "$multiply": ["$$fp.psfFlux", scale_factor.clone()] },
            "else": "$$fp.psfFlux"
        }
    };

    let new_flux_err = doc! {
        "$cond": {
            "if": { "$ne": ["$$fp.magzpsci", Bson::Null] },
            "then": { "$multiply": ["$$fp.psfFluxErr", scale_factor] },
            "else": "$$fp.psfFluxErr"
        }
    };

    let pipeline = vec![doc! {
        "$set": {
            "fp_hists": {
                "$map": {
                    "input": "$fp_hists",
                    "as": "fp",
                    "in": {
                        "$mergeObjects": [
                            "$$fp",
                            {
                                "_old_psfFlux": "$$fp.psfFlux",
                                "_old_psfFluxErr": "$$fp.psfFluxErr",
                                "psfFlux": new_flux.clone(),
                                "psfFluxErr": new_flux_err.clone(),
                            }
                        ]
                    }
                }
            },
        }
    }];

    let total = run_batched_update(
        collection,
        filter,
        pipeline,
        batch_size,
        estimated_count,
        "migrate",
    )
    .await;

    info!(
        "Migration complete. Modified {} documents. Run with --cleanup to remove _old_ fields.",
        total
    );
}

async fn cleanup(collection: &mongodb::Collection<Document>, batch_size: usize) {
    info!("Removing _old_psfFlux and _old_psfFluxErr fields...");
    let filter = doc! { "fp_hists.0._old_psfFlux": { "$exists": true } };

    let count = match collection.count_documents(filter.clone()).await {
        Ok(c) => c,
        Err(e) => {
            error!("error counting documents: {}", e);
            std::process::exit(1);
        }
    };

    if count == 0 {
        info!("No _old_ fields to remove. Exiting.");
        return;
    }

    info!("Found {} documents to clean up", count);

    let pipeline = vec![doc! {
        "$set": {
            "fp_hists": {
                "$map": {
                    "input": "$fp_hists",
                    "as": "fp",
                    "in": {
                        "$unsetField": {
                            "field": "_old_psfFluxErr",
                            "input": {
                                "$unsetField": {
                                    "field": "_old_psfFlux",
                                    "input": "$$fp",
                                }
                            }
                        }
                    }
                }
            }
        }
    }];

    let total =
        run_batched_update(collection, filter, pipeline, batch_size, count, "cleanup").await;
    info!("Cleanup complete. Cleaned {} documents.", total);
}
