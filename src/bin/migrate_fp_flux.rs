use std::collections::HashMap;

use boom::conf::{load_dotenv, AppConfig};
use boom::utils::lightcurves::ZTF_ZP;
use clap::Parser;
use futures::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use mongodb::bson::{doc, Bson, Document};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

const FLUXERR2MAGERR_FACTOR: f64 = 2.5_f64 / 2.30258509299_f64;

/// Fixed zeropoint for ZTF forced photometry.

/// Migrate ZTF forced photometry flux values to a fixed zeropoint.
///
/// Recomputes `psfFlux` and `psfFluxErr` in `fp_hists` from the raw IPAC
/// `forcediffimflux` and `forcediffimfluxunc` fields, converting to nJy at
/// the fixed ZTF_ZP = 23.9 zeropoint.
///
/// Formula: value = raw_value * 1e9 * 10^((23.9 - magzpsci) / 2.5)
///
/// Idempotent: since it always recomputes from raw fields, running this
/// multiple times produces the same result.
#[derive(Parser)]
struct Cli {
    /// Number of document IDs to collect per update_many batch
    #[arg(long, default_value = "5000")]
    batch_size: usize,
    /// Whether or not validation should run after migration. Defaults to False (caution, it's very slow!)
    #[arg(long, default_value = "false")]
    validate: bool,
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

    migrate(&collection, args.batch_size).await;

    // then validate
    if args.validate {
        info!("Starting validation...");
        validate(&collection).await;
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

    // Only process documents that have fp_hists
    let filter = doc! {
        "fp_hists.0": { "$exists": true },
    };

    // Converts raw flux to nJy at the fixed zeropoint:
    //   psf_flux * 1e9 * 10^((ZTF_ZP - magzpsci) / 2.5)
    let scale_factor = doc! {
        "$multiply": [
            1e9_f64,
            { "$pow": [
                10.0_f64,
                { "$divide": [
                    { "$subtract": [ZTF_ZP as f64, "$$fp.magzpsci"] },
                    2.5_f64
                ]}
            ]}
        ]
    };

    // Filter out invalid raw flux values (-99999.0)
    let valid_raw_flux = doc! {
        "$and": [
            { "$ne": ["$$fp.forcediffimflux", Bson::Null] },
            { "$ne": ["$$fp.forcediffimflux", -99999.0] },
        ]
    };

    let valid_raw_flux_err = doc! {
        "$and": [
            { "$ne": ["$$fp.forcediffimfluxunc", Bson::Null] },
            { "$ne": ["$$fp.forcediffimfluxunc", -99999.0] },
        ]
    };

    // psfFlux: computed from raw forcediffimflux when both it and magzpsci are valid
    let new_flux = doc! {
        "$cond": {
            "if": { "$and": [
                &valid_raw_flux,
                { "$ne": ["$$fp.magzpsci", Bson::Null] },
            ]},
            "then": { "$multiply": ["$$fp.forcediffimflux", &scale_factor] },
            "else": Bson::Null,
        }
    };

    // psfFluxErr: computed from raw forcediffimfluxunc when both it and magzpsci are valid
    let new_flux_err = doc! {
        "$cond": {
            "if": { "$and": [
                &valid_raw_flux_err,
                { "$ne": ["$$fp.magzpsci", Bson::Null] },
            ]},
            "then": { "$multiply": ["$$fp.forcediffimfluxunc", &scale_factor] },
            "else": Bson::Null,
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
                                "psfFlux": &new_flux,
                                "psfFluxErr": &new_flux_err,
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

    info!("Migration complete. Modified {} documents.", total);
}

async fn validate(collection: &mongodb::Collection<Document>) {
    // here we want to validate that where the raw values are valid,
    // the psfFlux and psfFluxErr were correctly updated. We can do this by
    // taking the newly added psfFlux and psfFluxErr and checking that we
    // can compute the magpsf and sigmapsf that match the existing ones, within some tolerance.
    // we can skip computing this where psfFlux.abs() / psfFluxErr < 3, and where procstatus != "0"

    let pipeline = vec![doc! {
        "$set": {
            "validation": {
                "$map": {
                    "input": "$fp_hists",
                    "as": "fp",
                    "in": {
                        "computed_magpsf": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$ne": ["$$fp.psfFlux", Bson::Null] },
                                    { "$ne": ["$$fp.psfFluxErr", Bson::Null] },
                                    { "$gt": [
                                        { "$abs": { "$divide": ["$$fp.psfFlux", "$$fp.psfFluxErr"] } },
                                        3
                                    ]},
                                    { "$eq": ["$$fp.procstatus", "0"] },
                                ]},
                                "then": {
                                    // magpsf = -2.5 * log10(abs(psfFlux / 1e9)) + ZTF_ZP
                                    "$add": [
                                        { "$multiply": [
                                            -2.5,
                                            { "$log10": { "$abs": { "$divide": ["$$fp.psfFlux", 1e9_f64] } }}
                                        ]},
                                        ZTF_ZP as f64
                                    ]
                                },
                                "else": Bson::Null,
                            }
                        },
                        "computed_sigmapsf": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$ne": ["$$fp.psfFluxErr", Bson::Null] },
                                    { "$gt": [
                                        { "$abs": { "$divide": ["$$fp.psfFlux", "$$fp.psfFluxErr"] } },
                                        3
                                    ]},
                                    { "$eq": ["$$fp.procstatus", "0"] },
                                ]},
                                "then": {
                                    // (2.5 / ln(10)) * (psfFluxErr * 1e-9 / abs(psfFlux * 1e-9))
                                    "$multiply": [
                                        FLUXERR2MAGERR_FACTOR,
                                        { "$divide": [
                                            { "$multiply": ["$$fp.psfFluxErr", 1e-9_f64] },
                                            { "$abs": { "$multiply": ["$$fp.psfFlux", 1e-9_f64] } }
                                        ]}
                                    ]
                                },
                                "else": Bson::Null,
                            }
                        },
                        "procstatus": "$$fp.procstatus",
                        "magpsf": "$$fp.magpsf",
                        "sigmapsf": "$$fp.sigmapsf",
                        "snr": "$$fp.snr",
                    }
                }
            }
        }
    }];

    let estimated_count = match collection.estimated_document_count().await {
        Ok(c) => c,
        Err(e) => {
            error!("error estimating document count: {}", e);
            std::process::exit(1);
        }
    };

    let pb = ProgressBar::new(estimated_count);
    pb.set_style(
        ProgressStyle::with_template(
            "validate {bar:40} {pos}/{len} [{elapsed_precise} < {eta_precise}]",
        )
        .unwrap(),
    );
    pb.set_message("validate".to_string());

    let mut cursor = match collection.aggregate(pipeline).await {
        Ok(c) => c,
        Err(e) => {
            error!("error running validation aggregation: {}", e);
            std::process::exit(1);
        }
    };

    let mut num_validated = 0;
    let mut num_failed = 0;
    let mut num_skipped = 0;
    let mut skipped_by_reason = HashMap::new();
    let mut failed_by_reason = HashMap::new();

    while let Some(d) = cursor.try_next().await.unwrap() {
        let validation = d.get("validation").unwrap().as_array().unwrap();
        for fp in validation {
            let fp = fp.as_document().unwrap();
            let procstatus = fp.get_str("procstatus").unwrap();
            if procstatus != "0" {
                num_skipped += 1;
                *skipped_by_reason.entry("invalid_procstatus").or_insert(0) += 1;
                continue;
            }
            // check if an SNR is there, if not then skip
            if fp.get("snr").is_none() || fp.get_f64("snr").unwrap().abs() <= 3.0 {
                num_skipped += 1;
                *skipped_by_reason.entry("low_snr").or_insert(0) += 1;
                continue;
            }

            if fp.get("computed_magpsf").is_none() || fp.get("computed_sigmapsf").is_none() {
                num_skipped += 1;
                *skipped_by_reason
                    .entry("missing_computed_values")
                    .or_insert(0) += 1;
                continue;
            }
            // if computed_magpsf is defined but null/None
            let computed_magpsf = fp.get("computed_magpsf").unwrap();
            if computed_magpsf.as_null().is_some() {
                num_skipped += 1;
                *skipped_by_reason
                    .entry("invalid_computed_magpsf")
                    .or_insert(0) += 1;
                continue;
            }
            // same for computed_sigmapsf
            let computed_sigmapsf = fp.get("computed_sigmapsf").unwrap();
            if computed_sigmapsf.as_null().is_some() {
                num_skipped += 1;
                *skipped_by_reason
                    .entry("invalid_computed_sigmapsf")
                    .or_insert(0) += 1;
                continue;
            }
            let computed_magpsf = fp.get_f64("computed_magpsf").unwrap();
            let computed_sigmapsf = fp.get_f64("computed_sigmapsf").unwrap();
            let magpsf = fp.get_f64("magpsf").unwrap();
            let sigmapsf = fp.get_f64("sigmapsf").unwrap();

            if (computed_magpsf - magpsf).abs() >= 1e-5 {
                num_failed += 1;
                *failed_by_reason.entry("magpsf_mismatch").or_insert(0) += 1;
                error!("Validation failed for document {}: computed magpsf {}±{} vs existing magpsf {}±{}",
                    d.get("_id").unwrap(), computed_magpsf, computed_sigmapsf, magpsf, sigmapsf);
            } else if (computed_sigmapsf - sigmapsf).abs() >= 1e-5 {
                num_failed += 1;
                *failed_by_reason.entry("sigmapsf_mismatch").or_insert(0) += 1;
                error!("Validation failed for document {}: computed sigmapsf {} vs existing sigmapsf {}",
                    d.get("_id").unwrap(), computed_sigmapsf, sigmapsf);
            } else {
                num_validated += 1;
            }
        }
        pb.inc(1);
    }

    info!(
        "Validation complete. {} validated, {} failed, {} skipped.",
        num_validated, num_failed, num_skipped
    );
    info!("Skipped by reason: {:?}", skipped_by_reason);
    info!("Failed by reason: {:?}", failed_by_reason);
}
