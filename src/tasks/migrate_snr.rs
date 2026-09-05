//! The `migrate_snr` task: recompute signal-to-noise across ZTF and LSST.
//!
//! Recomputes `snr_psf`, `snr_ap`, and (for ZTF) `apFlux` / `apFluxErr` on
//! alerts and on the lightcurves in the aux collections.
//!
//! For ZTF, `apFlux` and `apFluxErr` come from `magap` / `sigmagap`; for LSST
//! they already exist on the DiaSource, so only the SNRs are recomputed. Either
//! way the values derive from stored photometry, never from a previous run's
//! output, which is what makes this **idempotent** -- and therefore safe for the
//! queue to requeue after a lost lease.
//!
//! Ported from `src/bin/migrate_snr.rs`, which is now a thin wrapper. As with
//! `migrate_fp_flux`, the move traded `process::exit` for errors, added
//! cancellation checks at batch boundaries, and pointed progress at the run.

use super::batch::{run_batched_update, BatchError, PROGRESS_EVERY};
use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use futures::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
// Per-document validation findings go to the process log rather than the run
// log: there can be a great many of them, the run log is capped, and the run
// gets the summary. They still reach Loki through the normal container path.
use tracing::{error, info};
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "migrate_snr";

/// Which surveys to migrate.
///
/// A closed set rather than a free string: the collections are named per
/// variant, so an unrecognized value could only ever be a typo, and catching it
/// at submit time beats a run that does nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SnrSurvey {
    Ztf,
    Lsst,
    All,
}

impl SnrSurvey {
    fn includes_ztf(&self) -> bool {
        matches!(self, SnrSurvey::Ztf | SnrSurvey::All)
    }
    fn includes_lsst(&self) -> bool {
        matches!(self, SnrSurvey::Lsst | SnrSurvey::All)
    }
}

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MigrateSnrParams {
    #[serde(default = "default_survey")]
    pub survey: SnrSurvey,
    /// Document ids collected per `update_many` batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Run the validation pass afterwards. Off by default: it re-derives every
    /// magnitude and SNR to compare against what was written, and is slow.
    #[serde(default)]
    pub validate: bool,
}

fn default_survey() -> SnrSurvey {
    SnrSurvey::All
}

fn default_batch_size() -> usize {
    5_000
}

const MAX_BATCH_SIZE: usize = 100_000;

impl MigrateSnrParams {
    pub fn validate_params(&self) -> Result<(), String> {
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        Ok(())
    }
}

/// Run the migration.
pub async fn run(
    ctx: &TaskContext,
    params: MigrateSnrParams,
) -> Result<serde_json::Value, super::TaskError> {
    let db = ctx.db().clone();
    let to_task_error = |e: BatchError| match e {
        BatchError::Canceled { .. } => super::TaskError::Canceled,
        other => super::TaskError::Failed(other.to_string()),
    };

    let mut modified: i64 = 0;
    let mut collections: Vec<&str> = Vec::new();

    if params.survey.includes_ztf() {
        ctx.info("migrating ZTF SNR fields");
        modified += migrate_ztf_alerts(ctx, &db, params.batch_size)
            .await
            .map_err(to_task_error)?;
        modified += migrate_ztf_alerts_aux(ctx, &db, params.batch_size)
            .await
            .map_err(to_task_error)?;
        collections.extend(["ZTF_alerts", "ZTF_alerts_aux"]);
    }
    if params.survey.includes_lsst() {
        ctx.info("migrating LSST SNR fields");
        modified += migrate_lsst_alerts(ctx, &db, params.batch_size)
            .await
            .map_err(to_task_error)?;
        modified += migrate_lsst_alerts_aux(ctx, &db, params.batch_size)
            .await
            .map_err(to_task_error)?;
        collections.extend(["LSST_alerts", "LSST_alerts_aux"]);
    }

    if params.validate {
        ctx.info("starting validation (this is slow)");
        if params.survey.includes_ztf() {
            validate_ztf_alerts(ctx, &db).await.map_err(to_task_error)?;
            validate_ztf_alerts_aux(ctx, &db)
                .await
                .map_err(to_task_error)?;
        }
        if params.survey.includes_lsst() {
            validate_lsst_alerts(ctx, &db)
                .await
                .map_err(to_task_error)?;
            validate_lsst_alerts_aux(ctx, &db)
                .await
                .map_err(to_task_error)?;
        }
    }

    // One entry per collection touched, so "what has been done to
    // ZTF_alerts_aux" is answerable without parsing a combined record.
    for collection in &collections {
        ctx.record_mutation(
            MutationTarget {
                database: db.name().to_string(),
                collection: (*collection).to_string(),
                catalog: None,
                survey: Some(
                    if collection.starts_with("ZTF") {
                        "ztf"
                    } else {
                        "lsst"
                    }
                    .to_string(),
                ),
            },
            Operation::Recompute,
            doc! {
                "documents_modified_total": modified,
                "batch_size": params.batch_size as i64,
                "validated": params.validate,
                "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                    .unwrap_or(Bson::Null),
            },
        )
        .await;
    }

    Ok(serde_json::json!({
        "collections": collections,
        "documents_modified": modified,
        "validated": params.validate,
    }))
}

const ZTF_ZP: f64 = 23.9;
const FACTOR: f64 = 1.0857362047581294; // 2.5 / ln(10)

// ---------------------------------------------------------------------------
// Shared BSON helpers
// ---------------------------------------------------------------------------

/// `chi2 / ndata` guarded by null checks and ndata > 0.
/// For array element references use `var` like `"$$pc"`, for top-level use `"$candidate"`.
fn chipsf_expr(chi2_ref: &str, ndata_ref: &str, existing_ref: &str) -> Document {
    doc! {
        "$cond": {
            "if": { "$and": [
                { "$ne": [chi2_ref, Bson::Null] },
                { "$ne": [ndata_ref, Bson::Null] },
                { "$gt": [ndata_ref, 0] },
            ]},
            "then": {
                "$divide": [chi2_ref, ndata_ref]
            },
            "else": existing_ref,
        }
    }
}

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

async fn migrate_ztf_alerts(
    ctx: &TaskContext,
    db: &mongodb::Database,
    batch_size: usize,
) -> Result<i64, BatchError> {
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
        ctx,
        &collection,
        doc! {},
        flux_pipeline,
        batch_size,
        estimated,
        "ZTF_alerts apFlux",
    )
    .await?;
    ctx.info(format!("ZTF_alerts apFlux: modified {} documents", total));
    let first_pass = total;

    // Second pass: recompute SNR from the (now present) flux fields
    let snr_pipeline = vec![doc! {
        "$set": {
            "candidate.snr_psf": snr_expr_top("candidate", "psfFlux", "psfFluxErr", "snr_psf"),
            "candidate.snr_ap": snr_expr_top("candidate", "apFlux", "apFluxErr", "snr_ap"),
        }
    }];

    let total = run_batched_update(
        ctx,
        &collection,
        doc! {},
        snr_pipeline,
        batch_size,
        estimated,
        "ZTF_alerts snr",
    )
    .await?;
    ctx.info(format!("ZTF_alerts snr: modified {} documents", total));
    // Both passes touched the collection; report the work, not just the last
    // pass, or the ledger under-counts what a run changed.
    Ok(first_pass + total)
}

async fn migrate_ztf_alerts_aux(
    ctx: &TaskContext,
    db: &mongodb::Database,
    batch_size: usize,
) -> Result<i64, BatchError> {
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
        ctx,
        &collection,
        filter,
        flux_pipeline,
        batch_size,
        estimated,
        "ZTF_alerts_aux apFlux",
    )
    .await?;
    ctx.info(format!(
        "ZTF_alerts_aux apFlux: modified {} documents",
        total
    ));
    let first_pass = total;

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
        ctx,
        &collection,
        filter,
        snr_pipeline,
        batch_size,
        estimated,
        "ZTF_alerts_aux snr",
    )
    .await?;
    ctx.info(format!("ZTF_alerts_aux snr: modified {} documents", total));
    Ok(first_pass + total)
}

// ---------------------------------------------------------------------------
// LSST migration
// ---------------------------------------------------------------------------

async fn migrate_lsst_alerts(
    ctx: &TaskContext,
    db: &mongodb::Database,
    batch_size: usize,
) -> Result<i64, BatchError> {
    let collection = db.collection::<Document>("LSST_alerts");
    let estimated = collection.estimated_document_count().await.unwrap_or(0);
    info!("LSST_alerts: estimated ~{} documents", estimated);

    let pipeline = vec![doc! {
        "$set": {
            "candidate.snr_psf": snr_expr_top("candidate", "psfFlux", "psfFluxErr", "snr_psf"),
            "candidate.snr_ap": snr_expr_top("candidate", "apFlux", "apFluxErr", "snr_ap"),
            "candidate.chipsf": chipsf_expr(
                "$candidate.psfChi2",
                "$candidate.psfNdata",
                "$candidate.chipsf",
            ),
        }
    }];

    let total = run_batched_update(
        ctx,
        &collection,
        doc! {},
        pipeline,
        batch_size,
        estimated,
        "LSST_alerts snr+chipsf",
    )
    .await?;
    ctx.info(format!("LSST_alerts: modified {} documents", total));
    Ok(total)
}

async fn migrate_lsst_alerts_aux(
    ctx: &TaskContext,
    db: &mongodb::Database,
    batch_size: usize,
) -> Result<i64, BatchError> {
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
                                "chipsf": chipsf_expr(
                                    "$$pc.psfChi2",
                                    "$$pc.psfNdata",
                                    "$$pc.chipsf",
                                ),
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
        ctx,
        &collection,
        filter,
        pipeline,
        batch_size,
        estimated,
        "LSST_alerts_aux snr",
    )
    .await?;
    ctx.info(format!("LSST_alerts_aux: modified {} documents", total));
    Ok(total)
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

/// Aggregation sub-expression: compute expected chipsf = psfChi2 / psfNdata.
fn computed_chipsf_expr(chi2_ref: &str, ndata_ref: &str) -> Document {
    doc! {
        "$cond": {
            "if": { "$and": [
                { "$ne": [chi2_ref, Bson::Null] },
                { "$ne": [ndata_ref, Bson::Null] },
                { "$gt": [ndata_ref, 0] },
            ]},
            "then": { "$divide": [chi2_ref, ndata_ref] },
            "else": Bson::Null,
        }
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

/// Validate a single document's aperture-flux, SNR, and chipsf fields.
fn validate_entry(
    entry: &Document,
    doc_id: &Bson,
    counters: &mut ValidationCounters,
    has_ap: bool,
    has_chipsf: bool,
    tolerance: f64,
) {
    // --- apFlux / apFluxErr validation (magap round-trip) ---
    if has_ap {
        let ap_flux = entry.get("apFlux").and_then(get_f64);
        let magap = entry.get("magap").and_then(get_f64);
        let sigmagap = entry.get("sigmagap").and_then(get_f64);
        let computed_magap = entry.get("computed_magap").and_then(get_f64);
        let computed_sigmagap = entry.get("computed_sigmagap").and_then(get_f64);

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
    let snr_psf = entry.get("snr_psf").and_then(get_f64);
    let computed_snr_psf = entry.get("computed_snr_psf").and_then(get_f64);
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
        let snr_ap = entry.get("snr_ap").and_then(get_f64);
        let computed_snr_ap = entry.get("computed_snr_ap").and_then(get_f64);
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

    // --- chipsf validation ---
    if has_chipsf {
        let chipsf = entry.get("chipsf").and_then(get_f64);
        let computed_chipsf = entry.get("computed_chipsf").and_then(get_f64);
        match (chipsf, computed_chipsf) {
            (Some(stored), Some(expected)) => {
                if (stored - expected).abs() / expected.abs().max(1e-12) >= tolerance {
                    counters.fail("chipsf_mismatch");
                    error!(
                        "doc {}: chipsf {:.6} vs expected {:.6}",
                        doc_id, stored, expected
                    );
                } else {
                    counters.validated += 1;
                }
            }
            (None, None) => counters.skip("null_chipsf_both"),
            (Some(_), None) => counters.skip("null_psfChi2_psfNdata"),
            (None, Some(_)) => {
                counters.fail("chipsf_missing_but_expected");
                error!("doc {}: chipsf is null but expected a value", doc_id);
            }
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

async fn validate_ztf_alerts(ctx: &TaskContext, db: &mongodb::Database) -> Result<(), BatchError> {
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

    let mut cursor = collection.aggregate(pipeline).await?;

    let mut counters = ValidationCounters::new();
    let mut seen: u64 = 0;
    let mut last_reported: u64 = 0;
    while let Some(d) = cursor.try_next().await? {
        // Validation only reads, so stopping anywhere is safe.
        if ctx.is_canceled() {
            ctx.warn("validation canceled");
            return Err(BatchError::Canceled { modified: 0 });
        }
        seen += 1;
        if seen - last_reported >= PROGRESS_EVERY {
            last_reported = seen;
            ctx.progress(
                seen,
                estimated.max(seen),
                format!("validating {seen} documents"),
            )
            .await;
        }
        let doc_id = d.get("_id").unwrap().clone();
        let entry = d.get_document("validation").unwrap();
        validate_entry(entry, &doc_id, &mut counters, true, false, 1e-4);
    }
    counters.report("ZTF_alerts");
    Ok(())
}

async fn validate_ztf_alerts_aux(
    ctx: &TaskContext,
    db: &mongodb::Database,
) -> Result<(), BatchError> {
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

    let mut cursor = collection.aggregate(pipeline).await?;

    let mut counters = ValidationCounters::new();
    let mut seen: u64 = 0;
    let mut last_reported: u64 = 0;
    while let Some(d) = cursor.try_next().await? {
        // Validation only reads, so stopping anywhere is safe.
        if ctx.is_canceled() {
            ctx.warn("validation canceled");
            return Err(BatchError::Canceled { modified: 0 });
        }
        seen += 1;
        if seen - last_reported >= PROGRESS_EVERY {
            last_reported = seen;
            ctx.progress(
                seen,
                estimated.max(seen),
                format!("validating {seen} documents"),
            )
            .await;
        }
        let doc_id = d.get("_id").unwrap().clone();
        // prv_candidates
        if let Ok(arr) = d.get_array("prv_validation") {
            for item in arr {
                if let Some(entry) = item.as_document() {
                    validate_entry(entry, &doc_id, &mut counters, true, false, 1e-4);
                }
            }
        }
        // fp_hists (SNR psf only, no aperture flux)
        if let Ok(arr) = d.get_array("fp_validation") {
            for item in arr {
                if let Some(entry) = item.as_document() {
                    validate_entry(entry, &doc_id, &mut counters, false, false, 1e-4);
                }
            }
        }
    }
    counters.report("ZTF_alerts_aux");
    Ok(())
}

// ---------------------------------------------------------------------------
// LSST validation
// ---------------------------------------------------------------------------

async fn validate_lsst_alerts(ctx: &TaskContext, db: &mongodb::Database) -> Result<(), BatchError> {
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
                "chipsf": "$candidate.chipsf",
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
                "computed_chipsf": computed_chipsf_expr("$candidate.psfChi2", "$candidate.psfNdata"),
            }
        }
    }];

    let mut cursor = collection.aggregate(pipeline).await?;

    let mut counters = ValidationCounters::new();
    let mut seen: u64 = 0;
    let mut last_reported: u64 = 0;
    while let Some(d) = cursor.try_next().await? {
        // Validation only reads, so stopping anywhere is safe.
        if ctx.is_canceled() {
            ctx.warn("validation canceled");
            return Err(BatchError::Canceled { modified: 0 });
        }
        seen += 1;
        if seen - last_reported >= PROGRESS_EVERY {
            last_reported = seen;
            ctx.progress(
                seen,
                estimated.max(seen),
                format!("validating {seen} documents"),
            )
            .await;
        }
        let doc_id = d.get("_id").unwrap().clone();
        let entry = d.get_document("validation").unwrap();
        validate_entry(entry, &doc_id, &mut counters, true, true, 1e-4);
    }
    counters.report("LSST_alerts");
    Ok(())
}

async fn validate_lsst_alerts_aux(
    ctx: &TaskContext,
    db: &mongodb::Database,
) -> Result<(), BatchError> {
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
                        "chipsf": "$$pc.chipsf",
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
                        "computed_chipsf": computed_chipsf_expr("$$pc.psfChi2", "$$pc.psfNdata"),
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

    let mut cursor = collection.aggregate(pipeline).await?;

    let mut counters = ValidationCounters::new();
    let mut seen: u64 = 0;
    let mut last_reported: u64 = 0;
    while let Some(d) = cursor.try_next().await? {
        // Validation only reads, so stopping anywhere is safe.
        if ctx.is_canceled() {
            ctx.warn("validation canceled");
            return Err(BatchError::Canceled { modified: 0 });
        }
        seen += 1;
        if seen - last_reported >= PROGRESS_EVERY {
            last_reported = seen;
            ctx.progress(
                seen,
                estimated.max(seen),
                format!("validating {seen} documents"),
            )
            .await;
        }
        let doc_id = d.get("_id").unwrap().clone();
        if let Ok(arr) = d.get_array("prv_validation") {
            for item in arr {
                if let Some(entry) = item.as_document() {
                    validate_entry(entry, &doc_id, &mut counters, true, true, 1e-4);
                }
            }
        }
        if let Ok(arr) = d.get_array("fp_validation") {
            for item in arr {
                if let Some(entry) = item.as_document() {
                    validate_entry(entry, &doc_id, &mut counters, false, false, 1e-4);
                }
            }
        }
    }
    counters.report("LSST_alerts_aux");
    Ok(())
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn survey_selection_covers_the_right_collections() {
        assert!(SnrSurvey::All.includes_ztf() && SnrSurvey::All.includes_lsst());
        assert!(SnrSurvey::Ztf.includes_ztf() && !SnrSurvey::Ztf.includes_lsst());
        assert!(SnrSurvey::Lsst.includes_lsst() && !SnrSurvey::Lsst.includes_ztf());
    }

    #[test]
    fn an_unknown_survey_is_rejected_at_submit() {
        // A closed set rather than a free string: the collections are named per
        // variant, so a typo could only ever produce a run that does nothing.
        assert!(
            crate::tasks::validate_params(TASK_TYPE, &serde_json::json!({ "survey": "ztff" }))
                .is_err()
        );
        assert!(
            crate::tasks::validate_params(TASK_TYPE, &serde_json::json!({ "survey": "ztf" }))
                .is_ok()
        );
    }

    #[test]
    fn params_default_to_migrating_everything() {
        let parsed: MigrateSnrParams =
            serde_json::from_value(serde_json::json!({})).expect("defaults");
        assert_eq!(parsed.survey, SnrSurvey::All);
        assert_eq!(parsed.batch_size, default_batch_size());
        assert!(!parsed.validate, "validation is slow, so it is opt-in");
    }

    #[test]
    fn batch_size_is_bounded() {
        let bad = MigrateSnrParams {
            survey: SnrSurvey::All,
            batch_size: 0,
            validate: false,
        };
        assert!(bad.validate_params().is_err());
    }

    #[test]
    fn the_task_is_registered_and_retryable() {
        // It derives from stored photometry rather than its own output, so the
        // queue may resume it after a lost lease.
        assert!(crate::tasks::is_retryable(TASK_TYPE));
        assert!(!crate::tasks::find(TASK_TYPE).unwrap().destructive);
    }

    #[test]
    fn single_flight_is_keyed_by_survey() {
        // Migrating ZTF and LSST at once is fine; two runs over the same survey
        // would rewrite the same documents.
        assert_eq!(
            crate::tasks::single_flight_key(TASK_TYPE, &serde_json::json!({ "survey": "ztf" })),
            Some(mongodb::bson::doc! { "survey": "ztf" })
        );
    }
}
