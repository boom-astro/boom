use std::collections::HashMap;

use boom::conf::{load_dotenv, AppConfig};
use clap::Parser;
use futures::TryStreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use mongodb::bson::{doc, Bson, Document};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

/// Recompute `snr_psf`, `snr_ap`, `apFlux`, and `apFluxErr` for all ZTF and
/// LSST alerts and their lightcurves (prv_candidates and fp_hists in the aux
/// collections).
///
/// For ZTF, `apFlux` and `apFluxErr` are computed from `magap` and `sigmagap`:
///   flux_raw = 10^(-0.4 * (magap - ZTF_ZP))
///   apFlux   = ±flux_raw * 1e9     (nJy, sign from isdiffpos)
///   apFluxErr= (sigmagap / FACTOR) * flux_raw * 1e9   (nJy)
/// and then SNR is computed as:
///   snr_psf  = abs(psfFlux) / psfFluxErr
///   snr_ap   = abs(apFlux)  / apFluxErr
///
/// For LSST, `apFlux` and `apFluxErr` already exist (from DiaSource), so we
/// only recompute:
///   snr_psf = abs(psfFlux) / psfFluxErr
///   snr_ap  = abs(apFlux)  / apFluxErr
///
/// Idempotent: always recomputes from the stored flux / mag fields so running
/// multiple times produces the same result.
#[derive(Parser)]
struct Cli {
    /// Which survey(s) to migrate: ztf, lsst, or all
    #[arg(long, default_value = "all")]
    survey: String,

    /// Number of document IDs to collect per update_many batch
    #[arg(long, default_value = "5000")]
    batch_size: usize,

    /// Whether or not validation should run after migration. Defaults to false (caution, it's slow!)
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

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const ZTF_ZP: f64 = 23.9;
const FACTOR: f64 = 1.0857362047581294; // 2.5 / ln(10)

// ---------------------------------------------------------------------------
// Shared BSON helpers
// ---------------------------------------------------------------------------

/// `abs($$var.field) / $$var.field_err` guarded by null/zero checks.
/// Returns the new SNR value when valid, otherwise keeps the existing value.
fn snr_expr(var: &str, flux_field: &str, flux_err_field: &str, snr_field: &str) -> Document {
    doc! {
        "$cond": {
            "if": { "$and": [
                { "$ne": [format!("{}.{}", var, flux_field), Bson::Null] },
                { "$ne": [format!("{}.{}", var, flux_err_field), Bson::Null] },
                { "$gt": [format!("{}.{}", var, flux_err_field), 0.0_f64] },
            ]},
            "then": {
                "$divide": [
                    { "$abs": format!("{}.{}", var, flux_field) },
                    format!("{}.{}", var, flux_err_field),
                ]
            },
            "else": format!("{}.{}", var, snr_field),
        }
    }
}

/// Same as `snr_expr` but for top-level document fields (e.g. `$candidate.psfFlux`).
fn snr_expr_top(prefix: &str, flux_field: &str, flux_err_field: &str, snr_field: &str) -> Document {
    doc! {
        "$cond": {
            "if": { "$and": [
                { "$ne": [format!("${}.{}", prefix, flux_field), Bson::Null] },
                { "$ne": [format!("${}.{}", prefix, flux_err_field), Bson::Null] },
                { "$gt": [format!("${}.{}", prefix, flux_err_field), 0.0_f64] },
            ]},
            "then": {
                "$divide": [
                    { "$abs": format!("${}.{}", prefix, flux_field) },
                    format!("${}.{}", prefix, flux_err_field),
                ]
            },
            "else": format!("${}.{}", prefix, snr_field),
        }
    }
}

// ---------------------------------------------------------------------------
// ZTF mag2flux helpers (compute apFlux / apFluxErr from magap / sigmagap)
// ---------------------------------------------------------------------------

/// MongoDB aggregation expression for the raw flux:
///   flux_raw = 10^(-0.4 * (magap - ZTF_ZP))
fn ztf_flux_raw_expr(magap_ref: &str) -> Document {
    doc! {
        "$pow": [
            10.0_f64,
            { "$multiply": [-0.4_f64, { "$subtract": [magap_ref, ZTF_ZP] }] }
        ]
    }
}

/// Compute `apFlux = flux_raw * 1e9` from magap, with sign determined by isdiffpos.
/// Positive when `isdiffpos` is true, negative otherwise (matching psfFlux convention).
fn ztf_ap_flux_expr(magap_ref: &str, isdiffpos_ref: &str, existing_ref: &str) -> Document {
    doc! {
        "$cond": {
            "if": { "$ne": [magap_ref, Bson::Null] },
            "then": {
                "$cond": {
                    "if": { "$eq": [isdiffpos_ref, true] },
                    "then": { "$multiply": [ztf_flux_raw_expr(magap_ref), 1e9_f64] },
                    "else": { "$multiply": [ztf_flux_raw_expr(magap_ref), -1e9_f64] },
                }
            },
            "else": existing_ref,
        }
    }
}

/// Compute `apFluxErr = (sigmagap / FACTOR) * flux_raw * 1e9` from magap + sigmagap.
fn ztf_ap_flux_err_expr(magap_ref: &str, sigmagap_ref: &str, existing_ref: &str) -> Document {
    doc! {
        "$cond": {
            "if": { "$and": [
                { "$ne": [magap_ref, Bson::Null] },
                { "$ne": [sigmagap_ref, Bson::Null] },
            ]},
            "then": {
                "$multiply": [
                    { "$divide": [sigmagap_ref, FACTOR] },
                    ztf_flux_raw_expr(magap_ref),
                    1e9_f64,
                ]
            },
            "else": existing_ref,
        }
    }
}

// ---------------------------------------------------------------------------
// ZTF migration
// ---------------------------------------------------------------------------

async fn migrate_ztf_alerts(db: &mongodb::Database, batch_size: usize) {
    let collection = db.collection::<Document>("ZTF_alerts");
    let estimated = collection.estimated_document_count().await.unwrap_or(0);
    info!("ZTF_alerts: estimated ~{} documents", estimated);

    // First pass: compute apFlux and apFluxErr from magap/sigmagap
    let flux_pipeline = vec![doc! {
        "$set": {
            "candidate.apFlux": ztf_ap_flux_expr(
                "$candidate.magap",
                "$candidate.isdiffpos",
                "$candidate.apFlux",
            ),
            "candidate.apFluxErr": ztf_ap_flux_err_expr(
                "$candidate.magap",
                "$candidate.sigmagap",
                "$candidate.apFluxErr",
            ),
        }
    }];

    let total = run_batched_update(
        &collection,
        doc! {},
        flux_pipeline,
        batch_size,
        estimated,
        "ZTF_alerts apFlux",
    )
    .await;
    info!("ZTF_alerts apFlux: modified {} documents", total);

    // Second pass: recompute SNR from the (now present) flux fields
    let snr_pipeline = vec![doc! {
        "$set": {
            "candidate.snr_psf": snr_expr_top("candidate", "psfFlux", "psfFluxErr", "snr_psf"),
            "candidate.snr_ap": snr_expr_top("candidate", "apFlux", "apFluxErr", "snr_ap"),
        }
    }];

    let total = run_batched_update(
        &collection,
        doc! {},
        snr_pipeline,
        batch_size,
        estimated,
        "ZTF_alerts snr",
    )
    .await;
    info!("ZTF_alerts snr: modified {} documents", total);
}

async fn migrate_ztf_alerts_aux(db: &mongodb::Database, batch_size: usize) {
    let collection = db.collection::<Document>("ZTF_alerts_aux");
    let estimated = collection.estimated_document_count().await.unwrap_or(0);
    info!("ZTF_alerts_aux: estimated ~{} documents", estimated);

    // First pass: compute apFlux / apFluxErr from magap / sigmagap in prv_candidates
    let flux_pipeline = vec![doc! {
        "$set": {
            "prv_candidates": {
                "$map": {
                    "input": "$prv_candidates",
                    "as": "pc",
                    "in": {
                        "$mergeObjects": [
                            "$$pc",
                            {
                                "apFlux": ztf_ap_flux_expr(
                                    "$$pc.magap",
                                    "$$pc.isdiffpos",
                                    "$$pc.apFlux",
                                ),
                                "apFluxErr": ztf_ap_flux_err_expr(
                                    "$$pc.magap",
                                    "$$pc.sigmagap",
                                    "$$pc.apFluxErr",
                                ),
                            }
                        ]
                    }
                }
            },
        }
    }];

    let filter = doc! {
        "prv_candidates.0": { "$exists": true },
    };

    let total = run_batched_update(
        &collection,
        filter,
        flux_pipeline,
        batch_size,
        estimated,
        "ZTF_alerts_aux apFlux",
    )
    .await;
    info!("ZTF_alerts_aux apFlux: modified {} documents", total);

    // Second pass: recompute all SNR fields (prv_candidates + fp_hists)
    let snr_pipeline = vec![doc! {
        "$set": {
            "prv_candidates": {
                "$map": {
                    "input": "$prv_candidates",
                    "as": "pc",
                    "in": {
                        "$mergeObjects": [
                            "$$pc",
                            {
                                "snr_psf": snr_expr("$$pc", "psfFlux", "psfFluxErr", "snr_psf"),
                                "snr_ap":  snr_expr("$$pc", "apFlux",  "apFluxErr",  "snr_ap"),
                            }
                        ]
                    }
                }
            },
            "fp_hists": {
                "$map": {
                    "input": "$fp_hists",
                    "as": "fp",
                    "in": {
                        "$mergeObjects": [
                            "$$fp",
                            {
                                "snr_psf": snr_expr("$$fp", "psfFlux", "psfFluxErr", "snr_psf"),
                            }
                        ]
                    }
                }
            },
        }
    }];

    let filter = doc! {
        "$or": [
            { "prv_candidates.0": { "$exists": true } },
            { "fp_hists.0": { "$exists": true } },
        ]
    };

    let total = run_batched_update(
        &collection,
        filter,
        snr_pipeline,
        batch_size,
        estimated,
        "ZTF_alerts_aux snr",
    )
    .await;
    info!("ZTF_alerts_aux snr: modified {} documents", total);
}

// ---------------------------------------------------------------------------
// LSST migration
// ---------------------------------------------------------------------------

async fn migrate_lsst_alerts(db: &mongodb::Database, batch_size: usize) {
    let collection = db.collection::<Document>("LSST_alerts");
    let estimated = collection.estimated_document_count().await.unwrap_or(0);
    info!("LSST_alerts: estimated ~{} documents", estimated);

    let pipeline = vec![doc! {
        "$set": {
            "candidate.snr_psf": snr_expr_top("candidate", "psfFlux", "psfFluxErr", "snr_psf"),
            "candidate.snr_ap": snr_expr_top("candidate", "apFlux", "apFluxErr", "snr_ap"),
        }
    }];

    let total = run_batched_update(
        &collection,
        doc! {},
        pipeline,
        batch_size,
        estimated,
        "LSST_alerts snr",
    )
    .await;
    info!("LSST_alerts: modified {} documents", total);
}

async fn migrate_lsst_alerts_aux(db: &mongodb::Database, batch_size: usize) {
    let collection = db.collection::<Document>("LSST_alerts_aux");
    let estimated = collection.estimated_document_count().await.unwrap_or(0);
    info!("LSST_alerts_aux: estimated ~{} documents", estimated);

    let pipeline = vec![doc! {
        "$set": {
            "prv_candidates": {
                "$map": {
                    "input": "$prv_candidates",
                    "as": "pc",
                    "in": {
                        "$mergeObjects": [
                            "$$pc",
                            {
                                "snr_psf": snr_expr("$$pc", "psfFlux", "psfFluxErr", "snr_psf"),
                                "snr_ap":  snr_expr("$$pc", "apFlux",  "apFluxErr",  "snr_ap"),
                            }
                        ]
                    }
                }
            },
            "fp_hists": {
                "$map": {
                    "input": "$fp_hists",
                    "as": "fp",
                    "in": {
                        "$mergeObjects": [
                            "$$fp",
                            {
                                "snr_psf": snr_expr("$$fp", "psfFlux", "psfFluxErr", "snr_psf"),
                            }
                        ]
                    }
                }
            },
        }
    }];

    let filter = doc! {
        "$or": [
            { "prv_candidates.0": { "$exists": true } },
            { "fp_hists.0": { "$exists": true } },
        ]
    };

    let total = run_batched_update(
        &collection,
        filter,
        pipeline,
        batch_size,
        estimated,
        "LSST_alerts_aux snr",
    )
    .await;
    info!("LSST_alerts_aux: modified {} documents", total);
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

/// LSST zero-point in nJy: 8.90 + 22.5 = 31.4
const LSST_ZP_AB_NJY: f64 = 31.4;

/// Aggregation sub-expression: compute magap from apFlux.
/// `magap = -2.5 * log10(abs(flux_ref / scale)) + zp`
/// where `scale = 1e9` for ZTF (flux stored as nJy, ZP in Jy-mag) and `1.0` for LSST (flux in nJy, ZP already accounts for that).
fn computed_mag_expr(flux_ref: &str, scale: f64, zp: f64) -> Document {
    doc! {
        "$add": [
            { "$multiply": [
                -2.5_f64,
                { "$log10": { "$abs": { "$divide": [flux_ref, scale] } } }
            ]},
            zp
        ]
    }
}

/// Aggregation sub-expression: compute sigmagap from apFluxErr / apFlux.
/// `sigmagap = FACTOR * fluxErr / abs(flux)`
fn computed_magerr_expr(flux_ref: &str, flux_err_ref: &str) -> Document {
    doc! {
        "$multiply": [
            FACTOR,
            { "$divide": [flux_err_ref, { "$abs": flux_ref }] }
        ]
    }
}

/// Aggregation sub-expression: compute expected SNR = abs(flux) / fluxErr.
fn computed_snr_expr(flux_ref: &str, flux_err_ref: &str) -> Document {
    doc! {
        "$divide": [
            { "$abs": flux_ref },
            flux_err_ref
        ]
    }
}

struct ValidationCounters {
    validated: u64,
    failed: u64,
    skipped: u64,
    skipped_by_reason: HashMap<&'static str, u64>,
    failed_by_reason: HashMap<&'static str, u64>,
}

impl ValidationCounters {
    fn new() -> Self {
        Self {
            validated: 0,
            failed: 0,
            skipped: 0,
            skipped_by_reason: HashMap::new(),
            failed_by_reason: HashMap::new(),
        }
    }

    fn skip(&mut self, reason: &'static str) {
        self.skipped += 1;
        *self.skipped_by_reason.entry(reason).or_insert(0) += 1;
    }

    fn fail(&mut self, reason: &'static str) {
        self.failed += 1;
        *self.failed_by_reason.entry(reason).or_insert(0) += 1;
    }

    fn report(&self, label: &str) {
        info!(
            "{}: {} validated, {} failed, {} skipped.",
            label, self.validated, self.failed, self.skipped
        );
        info!("  Skipped by reason: {:?}", self.skipped_by_reason);
        if self.failed > 0 {
            info!("  Failed by reason: {:?}", self.failed_by_reason);
        }
    }
}

/// Validate a single document's aperture-flux and SNR fields.
/// `entry` is a BSON document with: computed_magap, computed_sigmagap, magap, sigmagap,
/// computed_snr_psf, snr_psf, computed_snr_ap, snr_ap, apFlux, apFluxErr, psfFlux, psfFluxErr.
fn validate_entry(
    entry: &Document,
    doc_id: &Bson,
    counters: &mut ValidationCounters,
    has_ap: bool,
    tolerance: f64,
) {
    // --- apFlux / apFluxErr validation (magap round-trip) ---
    if has_ap {
        let ap_flux = entry.get("apFlux").and_then(|v| get_f64(v));
        let magap = entry.get("magap").and_then(|v| get_f64(v));
        let sigmagap = entry.get("sigmagap").and_then(|v| get_f64(v));
        let computed_magap = entry.get("computed_magap").and_then(|v| get_f64(v));
        let computed_sigmagap = entry.get("computed_sigmagap").and_then(|v| get_f64(v));

        match (ap_flux, magap, sigmagap, computed_magap, computed_sigmagap) {
            (Some(_), Some(orig_mag), Some(orig_sig), Some(comp_mag), Some(comp_sig)) => {
                if (comp_mag - orig_mag).abs() >= tolerance {
                    counters.fail("magap_mismatch");
                    error!(
                        "doc {}: computed magap {:.6} vs stored {:.6} (delta {:.2e})",
                        doc_id,
                        comp_mag,
                        orig_mag,
                        (comp_mag - orig_mag).abs()
                    );
                } else if (comp_sig - orig_sig).abs() >= tolerance {
                    counters.fail("sigmagap_mismatch");
                    error!(
                        "doc {}: computed sigmagap {:.6} vs stored {:.6} (delta {:.2e})",
                        doc_id,
                        comp_sig,
                        orig_sig,
                        (comp_sig - orig_sig).abs()
                    );
                } else {
                    counters.validated += 1;
                }
            }
            (None, _, _, _, _) => counters.skip("null_apFlux"),
            (_, None, _, _, _) | (_, _, None, _, _) => counters.skip("null_magap_sigmagap"),
            _ => counters.skip("null_computed_magap"),
        }
    }

    // --- SNR PSF validation ---
    let snr_psf = entry.get("snr_psf").and_then(|v| get_f64(v));
    let computed_snr_psf = entry.get("computed_snr_psf").and_then(|v| get_f64(v));
    match (snr_psf, computed_snr_psf) {
        (Some(stored), Some(expected)) => {
            if (stored - expected).abs() / expected.abs().max(1e-12) >= tolerance {
                counters.fail("snr_psf_mismatch");
                error!(
                    "doc {}: snr_psf {:.6} vs expected {:.6}",
                    doc_id, stored, expected
                );
            } else {
                counters.validated += 1;
            }
        }
        (None, _) => counters.skip("null_snr_psf"),
        (_, None) => counters.skip("null_psfFlux_for_snr"),
    }

    // --- SNR AP validation ---
    if has_ap {
        let snr_ap = entry.get("snr_ap").and_then(|v| get_f64(v));
        let computed_snr_ap = entry.get("computed_snr_ap").and_then(|v| get_f64(v));
        match (snr_ap, computed_snr_ap) {
            (Some(stored), Some(expected)) => {
                if (stored - expected).abs() / expected.abs().max(1e-12) >= tolerance {
                    counters.fail("snr_ap_mismatch");
                    error!(
                        "doc {}: snr_ap {:.6} vs expected {:.6}",
                        doc_id, stored, expected
                    );
                } else {
                    counters.validated += 1;
                }
            }
            (None, _) => counters.skip("null_snr_ap"),
            (_, None) => counters.skip("null_apFlux_for_snr"),
        }
    }
}

/// Helper to extract f64 from Bson (handles both Double and Int32/Int64 and Null).
fn get_f64(v: &Bson) -> Option<f64> {
    match v {
        Bson::Double(d) => Some(*d),
        Bson::Int32(i) => Some(*i as f64),
        Bson::Int64(i) => Some(*i as f64),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// ZTF validation
// ---------------------------------------------------------------------------

async fn validate_ztf_alerts(db: &mongodb::Database) {
    let collection = db.collection::<Document>("ZTF_alerts");
    let estimated = collection.estimated_document_count().await.unwrap_or(0);

    // Aggregation: project candidate-level fields + computed round-trip values
    let pipeline = vec![doc! {
        "$project": {
            "validation": {
                "apFlux": "$candidate.apFlux",
                "apFluxErr": "$candidate.apFluxErr",
                "magap": "$candidate.magap",
                "sigmagap": "$candidate.sigmagap",
                "psfFlux": "$candidate.psfFlux",
                "psfFluxErr": "$candidate.psfFluxErr",
                "snr_psf": "$candidate.snr_psf",
                "snr_ap": "$candidate.snr_ap",
                "computed_magap": {
                    "$cond": {
                        "if": { "$and": [
                            { "$ne": ["$candidate.apFlux", Bson::Null] },
                            { "$gt": [{ "$abs": "$candidate.apFlux" }, 0.0_f64] },
                        ]},
                        "then": computed_mag_expr("$candidate.apFlux", 1e9, ZTF_ZP as f64),
                        "else": Bson::Null,
                    }
                },
                "computed_sigmagap": {
                    "$cond": {
                        "if": { "$and": [
                            { "$ne": ["$candidate.apFlux", Bson::Null] },
                            { "$ne": ["$candidate.apFluxErr", Bson::Null] },
                            { "$gt": [{ "$abs": "$candidate.apFlux" }, 0.0_f64] },
                        ]},
                        "then": computed_magerr_expr("$candidate.apFlux", "$candidate.apFluxErr"),
                        "else": Bson::Null,
                    }
                },
                "computed_snr_psf": {
                    "$cond": {
                        "if": { "$and": [
                            { "$ne": ["$candidate.psfFlux", Bson::Null] },
                            { "$ne": ["$candidate.psfFluxErr", Bson::Null] },
                            { "$gt": ["$candidate.psfFluxErr", 0.0_f64] },
                        ]},
                        "then": computed_snr_expr("$candidate.psfFlux", "$candidate.psfFluxErr"),
                        "else": Bson::Null,
                    }
                },
                "computed_snr_ap": {
                    "$cond": {
                        "if": { "$and": [
                            { "$ne": ["$candidate.apFlux", Bson::Null] },
                            { "$ne": ["$candidate.apFluxErr", Bson::Null] },
                            { "$gt": ["$candidate.apFluxErr", 0.0_f64] },
                        ]},
                        "then": computed_snr_expr("$candidate.apFlux", "$candidate.apFluxErr"),
                        "else": Bson::Null,
                    }
                },
            }
        }
    }];

    let pb = ProgressBar::new(estimated);
    pb.set_style(
        ProgressStyle::with_template(
            "validate ZTF_alerts {bar:40} {pos}/{len} [{elapsed_precise} < {eta_precise}]",
        )
        .unwrap(),
    );

    let mut cursor = match collection.aggregate(pipeline).await {
        Ok(c) => c,
        Err(e) => {
            error!("error running validation aggregation: {}", e);
            return;
        }
    };

    let mut counters = ValidationCounters::new();
    while let Some(d) = cursor.try_next().await.unwrap() {
        let doc_id = d.get("_id").unwrap().clone();
        let entry = d.get_document("validation").unwrap();
        validate_entry(entry, &doc_id, &mut counters, true, 1e-4);
        pb.inc(1);
    }
    pb.finish();
    counters.report("ZTF_alerts");
}

async fn validate_ztf_alerts_aux(db: &mongodb::Database) {
    let collection = db.collection::<Document>("ZTF_alerts_aux");
    let estimated = collection.estimated_document_count().await.unwrap_or(0);

    let pipeline = vec![doc! {
        "$project": {
            "prv_validation": {
                "$map": {
                    "input": { "$ifNull": ["$prv_candidates", []] },
                    "as": "pc",
                    "in": {
                        "apFlux": "$$pc.apFlux",
                        "apFluxErr": "$$pc.apFluxErr",
                        "magap": "$$pc.magap",
                        "sigmagap": "$$pc.sigmagap",
                        "psfFlux": "$$pc.psfFlux",
                        "psfFluxErr": "$$pc.psfFluxErr",
                        "snr_psf": "$$pc.snr_psf",
                        "snr_ap": "$$pc.snr_ap",
                        "computed_magap": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$ne": ["$$pc.apFlux", Bson::Null] },
                                    { "$gt": [{ "$abs": "$$pc.apFlux" }, 0.0_f64] },
                                ]},
                                "then": computed_mag_expr("$$pc.apFlux", 1e9, ZTF_ZP as f64),
                                "else": Bson::Null,
                            }
                        },
                        "computed_sigmagap": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$ne": ["$$pc.apFlux", Bson::Null] },
                                    { "$ne": ["$$pc.apFluxErr", Bson::Null] },
                                    { "$gt": [{ "$abs": "$$pc.apFlux" }, 0.0_f64] },
                                ]},
                                "then": computed_magerr_expr("$$pc.apFlux", "$$pc.apFluxErr"),
                                "else": Bson::Null,
                            }
                        },
                        "computed_snr_psf": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$ne": ["$$pc.psfFlux", Bson::Null] },
                                    { "$ne": ["$$pc.psfFluxErr", Bson::Null] },
                                    { "$gt": ["$$pc.psfFluxErr", 0.0_f64] },
                                ]},
                                "then": computed_snr_expr("$$pc.psfFlux", "$$pc.psfFluxErr"),
                                "else": Bson::Null,
                            }
                        },
                        "computed_snr_ap": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$ne": ["$$pc.apFlux", Bson::Null] },
                                    { "$ne": ["$$pc.apFluxErr", Bson::Null] },
                                    { "$gt": ["$$pc.apFluxErr", 0.0_f64] },
                                ]},
                                "then": computed_snr_expr("$$pc.apFlux", "$$pc.apFluxErr"),
                                "else": Bson::Null,
                            }
                        },
                    }
                }
            },
            "fp_validation": {
                "$map": {
                    "input": { "$ifNull": ["$fp_hists", []] },
                    "as": "fp",
                    "in": {
                        "psfFlux": "$$fp.psfFlux",
                        "psfFluxErr": "$$fp.psfFluxErr",
                        "snr_psf": "$$fp.snr_psf",
                        "computed_snr_psf": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$ne": ["$$fp.psfFlux", Bson::Null] },
                                    { "$ne": ["$$fp.psfFluxErr", Bson::Null] },
                                    { "$gt": ["$$fp.psfFluxErr", 0.0_f64] },
                                ]},
                                "then": computed_snr_expr("$$fp.psfFlux", "$$fp.psfFluxErr"),
                                "else": Bson::Null,
                            }
                        },
                    }
                }
            },
        }
    }];

    let pb = ProgressBar::new(estimated);
    pb.set_style(
        ProgressStyle::with_template(
            "validate ZTF_alerts_aux {bar:40} {pos}/{len} [{elapsed_precise} < {eta_precise}]",
        )
        .unwrap(),
    );

    let mut cursor = match collection.aggregate(pipeline).await {
        Ok(c) => c,
        Err(e) => {
            error!("error running validation aggregation: {}", e);
            return;
        }
    };

    let mut counters = ValidationCounters::new();
    while let Some(d) = cursor.try_next().await.unwrap() {
        let doc_id = d.get("_id").unwrap().clone();
        // prv_candidates
        if let Ok(arr) = d.get_array("prv_validation") {
            for item in arr {
                if let Some(entry) = item.as_document() {
                    validate_entry(entry, &doc_id, &mut counters, true, 1e-4);
                }
            }
        }
        // fp_hists (SNR psf only, no aperture flux)
        if let Ok(arr) = d.get_array("fp_validation") {
            for item in arr {
                if let Some(entry) = item.as_document() {
                    validate_entry(entry, &doc_id, &mut counters, false, 1e-4);
                }
            }
        }
        pb.inc(1);
    }
    pb.finish();
    counters.report("ZTF_alerts_aux");
}

// ---------------------------------------------------------------------------
// LSST validation
// ---------------------------------------------------------------------------

async fn validate_lsst_alerts(db: &mongodb::Database) {
    let collection = db.collection::<Document>("LSST_alerts");
    let estimated = collection.estimated_document_count().await.unwrap_or(0);

    // For LSST, apFlux/apFluxErr are raw (in nJy), and the ZP is LSST_ZP_AB_NJY.
    // magap = -2.5 * log10(abs(apFlux)) + LSST_ZP_AB_NJY
    let pipeline = vec![doc! {
        "$project": {
            "validation": {
                "apFlux": "$candidate.apFlux",
                "apFluxErr": "$candidate.apFluxErr",
                "magap": "$candidate.magap",
                "sigmagap": "$candidate.sigmagap",
                "psfFlux": "$candidate.psfFlux",
                "psfFluxErr": "$candidate.psfFluxErr",
                "snr_psf": "$candidate.snr_psf",
                "snr_ap": "$candidate.snr_ap",
                "computed_magap": {
                    "$cond": {
                        "if": { "$and": [
                            { "$ne": ["$candidate.apFlux", Bson::Null] },
                            { "$gt": [{ "$abs": "$candidate.apFlux" }, 0.0_f64] },
                        ]},
                        "then": computed_mag_expr("$candidate.apFlux", 1.0, LSST_ZP_AB_NJY),
                        "else": Bson::Null,
                    }
                },
                "computed_sigmagap": {
                    "$cond": {
                        "if": { "$and": [
                            { "$ne": ["$candidate.apFlux", Bson::Null] },
                            { "$ne": ["$candidate.apFluxErr", Bson::Null] },
                            { "$gt": [{ "$abs": "$candidate.apFlux" }, 0.0_f64] },
                        ]},
                        "then": computed_magerr_expr("$candidate.apFlux", "$candidate.apFluxErr"),
                        "else": Bson::Null,
                    }
                },
                "computed_snr_psf": {
                    "$cond": {
                        "if": { "$and": [
                            { "$ne": ["$candidate.psfFlux", Bson::Null] },
                            { "$ne": ["$candidate.psfFluxErr", Bson::Null] },
                            { "$gt": ["$candidate.psfFluxErr", 0.0_f64] },
                        ]},
                        "then": computed_snr_expr("$candidate.psfFlux", "$candidate.psfFluxErr"),
                        "else": Bson::Null,
                    }
                },
                "computed_snr_ap": {
                    "$cond": {
                        "if": { "$and": [
                            { "$ne": ["$candidate.apFlux", Bson::Null] },
                            { "$ne": ["$candidate.apFluxErr", Bson::Null] },
                            { "$gt": ["$candidate.apFluxErr", 0.0_f64] },
                        ]},
                        "then": computed_snr_expr("$candidate.apFlux", "$candidate.apFluxErr"),
                        "else": Bson::Null,
                    }
                },
            }
        }
    }];

    let pb = ProgressBar::new(estimated);
    pb.set_style(
        ProgressStyle::with_template(
            "validate LSST_alerts {bar:40} {pos}/{len} [{elapsed_precise} < {eta_precise}]",
        )
        .unwrap(),
    );

    let mut cursor = match collection.aggregate(pipeline).await {
        Ok(c) => c,
        Err(e) => {
            error!("error running validation aggregation: {}", e);
            return;
        }
    };

    let mut counters = ValidationCounters::new();
    while let Some(d) = cursor.try_next().await.unwrap() {
        let doc_id = d.get("_id").unwrap().clone();
        let entry = d.get_document("validation").unwrap();
        validate_entry(entry, &doc_id, &mut counters, true, 1e-4);
        pb.inc(1);
    }
    pb.finish();
    counters.report("LSST_alerts");
}

async fn validate_lsst_alerts_aux(db: &mongodb::Database) {
    let collection = db.collection::<Document>("LSST_alerts_aux");
    let estimated = collection.estimated_document_count().await.unwrap_or(0);

    let pipeline = vec![doc! {
        "$project": {
            "prv_validation": {
                "$map": {
                    "input": { "$ifNull": ["$prv_candidates", []] },
                    "as": "pc",
                    "in": {
                        "apFlux": "$$pc.apFlux",
                        "apFluxErr": "$$pc.apFluxErr",
                        "magap": "$$pc.magap",
                        "sigmagap": "$$pc.sigmagap",
                        "psfFlux": "$$pc.psfFlux",
                        "psfFluxErr": "$$pc.psfFluxErr",
                        "snr_psf": "$$pc.snr_psf",
                        "snr_ap": "$$pc.snr_ap",
                        "computed_magap": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$ne": ["$$pc.apFlux", Bson::Null] },
                                    { "$gt": [{ "$abs": "$$pc.apFlux" }, 0.0_f64] },
                                ]},
                                "then": computed_mag_expr("$$pc.apFlux", 1.0, LSST_ZP_AB_NJY),
                                "else": Bson::Null,
                            }
                        },
                        "computed_sigmagap": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$ne": ["$$pc.apFlux", Bson::Null] },
                                    { "$ne": ["$$pc.apFluxErr", Bson::Null] },
                                    { "$gt": [{ "$abs": "$$pc.apFlux" }, 0.0_f64] },
                                ]},
                                "then": computed_magerr_expr("$$pc.apFlux", "$$pc.apFluxErr"),
                                "else": Bson::Null,
                            }
                        },
                        "computed_snr_psf": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$ne": ["$$pc.psfFlux", Bson::Null] },
                                    { "$ne": ["$$pc.psfFluxErr", Bson::Null] },
                                    { "$gt": ["$$pc.psfFluxErr", 0.0_f64] },
                                ]},
                                "then": computed_snr_expr("$$pc.psfFlux", "$$pc.psfFluxErr"),
                                "else": Bson::Null,
                            }
                        },
                        "computed_snr_ap": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$ne": ["$$pc.apFlux", Bson::Null] },
                                    { "$ne": ["$$pc.apFluxErr", Bson::Null] },
                                    { "$gt": ["$$pc.apFluxErr", 0.0_f64] },
                                ]},
                                "then": computed_snr_expr("$$pc.apFlux", "$$pc.apFluxErr"),
                                "else": Bson::Null,
                            }
                        },
                    }
                }
            },
            "fp_validation": {
                "$map": {
                    "input": { "$ifNull": ["$fp_hists", []] },
                    "as": "fp",
                    "in": {
                        "psfFlux": "$$fp.psfFlux",
                        "psfFluxErr": "$$fp.psfFluxErr",
                        "snr_psf": "$$fp.snr_psf",
                        "computed_snr_psf": {
                            "$cond": {
                                "if": { "$and": [
                                    { "$ne": ["$$fp.psfFlux", Bson::Null] },
                                    { "$ne": ["$$fp.psfFluxErr", Bson::Null] },
                                    { "$gt": ["$$fp.psfFluxErr", 0.0_f64] },
                                ]},
                                "then": computed_snr_expr("$$fp.psfFlux", "$$fp.psfFluxErr"),
                                "else": Bson::Null,
                            }
                        },
                    }
                }
            },
        }
    }];

    let pb = ProgressBar::new(estimated);
    pb.set_style(
        ProgressStyle::with_template(
            "validate LSST_alerts_aux {bar:40} {pos}/{len} [{elapsed_precise} < {eta_precise}]",
        )
        .unwrap(),
    );

    let mut cursor = match collection.aggregate(pipeline).await {
        Ok(c) => c,
        Err(e) => {
            error!("error running validation aggregation: {}", e);
            return;
        }
    };

    let mut counters = ValidationCounters::new();
    while let Some(d) = cursor.try_next().await.unwrap() {
        let doc_id = d.get("_id").unwrap().clone();
        if let Ok(arr) = d.get_array("prv_validation") {
            for item in arr {
                if let Some(entry) = item.as_document() {
                    validate_entry(entry, &doc_id, &mut counters, true, 1e-4);
                }
            }
        }
        if let Ok(arr) = d.get_array("fp_validation") {
            for item in arr {
                if let Some(entry) = item.as_document() {
                    validate_entry(entry, &doc_id, &mut counters, false, 1e-4);
                }
            }
        }
        pb.inc(1);
    }
    pb.finish();
    counters.report("LSST_alerts_aux");
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

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

    let survey = args.survey.to_lowercase();

    if survey == "ztf" || survey == "all" {
        info!("=== Migrating ZTF SNR fields ===");
        migrate_ztf_alerts(&db, args.batch_size).await;
        migrate_ztf_alerts_aux(&db, args.batch_size).await;
    }

    if survey == "lsst" || survey == "all" {
        info!("=== Migrating LSST SNR fields ===");
        migrate_lsst_alerts(&db, args.batch_size).await;
        migrate_lsst_alerts_aux(&db, args.batch_size).await;
    }

    if survey != "ztf" && survey != "lsst" && survey != "all" {
        error!("Unknown survey '{}'. Use ztf, lsst, or all.", survey);
        std::process::exit(1);
    }

    info!("SNR migration complete.");

    if args.validate {
        info!("Starting validation...");
        if survey == "ztf" || survey == "all" {
            info!("=== Validating ZTF ===");
            validate_ztf_alerts(&db).await;
            validate_ztf_alerts_aux(&db).await;
        }
        if survey == "lsst" || survey == "all" {
            info!("=== Validating LSST ===");
            validate_lsst_alerts(&db).await;
            validate_lsst_alerts_aux(&db).await;
        }
        info!("Validation complete.");
    }
}
