//! The `migrate_fp_flux` task: put ZTF forced photometry on a fixed zeropoint.
//!
//! Recomputes `psfFlux` and `psfFluxErr` in `fp_hists` from the raw IPAC
//! `forcediffimflux` / `forcediffimfluxunc` fields, converting to nJy at the
//! fixed `ZTF_ZP` = 23.9 zeropoint:
//!
//! ```text
//! value = raw_value * 1e9 * 10^((23.9 - magzpsci) / 2.5)
//! ```
//!
//! Idempotent, because it always recomputes from the raw fields rather than
//! from what it wrote last time -- which is what makes it safe for the task
//! system to requeue after a lost lease.
//!
//! Ported from `src/bin/migrate_fp_flux.rs`, which is now a thin wrapper over
//! this. Three things changed in the move: the batch loop checks for
//! cancellation, failures return errors instead of calling `process::exit`
//! (which would kill the whole worker and every other run on it), and progress
//! is reported to the run rather than only to a terminal progress bar.

use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use crate::utils::lightcurves::ZTF_ZP;
use futures::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "migrate_fp_flux";

/// The collection this task rewrites. Not a parameter: the migration is
/// specific to ZTF's forced photometry schema, and pointing it at another
/// collection would silently do nothing or corrupt it.
const COLLECTION: &str = "ZTF_alerts_aux";

const FLUXERR2MAGERR_FACTOR: f64 = 2.5_f64 / std::f64::consts::LN_10;

/// How often to publish progress while a migration runs.
const PROGRESS_EVERY: u64 = 50_000;

#[derive(thiserror::Error, Debug)]
pub enum MigrateError {
    #[error(transparent)]
    Mongo(#[from] mongodb::error::Error),
    #[error("canceled after {modified} documents")]
    Canceled { modified: i64 },
}

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MigrateFpFluxParams {
    /// Document ids collected per `update_many` batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Run the validation pass afterwards. Off by default: it recomputes
    /// magnitudes for every forced-photometry point and is very slow.
    #[serde(default)]
    pub validate: bool,
}

fn default_batch_size() -> usize {
    5_000
}

/// Upper bound on a batch, so a client cannot ask for one large enough to
/// build a filter document Mongo will reject.
const MAX_BATCH_SIZE: usize = 100_000;

impl MigrateFpFluxParams {
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
    params: MigrateFpFluxParams,
) -> Result<serde_json::Value, super::TaskError> {
    let collection = ctx.db().collection::<Document>(COLLECTION);

    let modified = migrate(ctx, &collection, params.batch_size)
        .await
        .map_err(|e| match e {
            MigrateError::Canceled { .. } => super::TaskError::Canceled,
            other => super::TaskError::Failed(other.to_string()),
        })?;

    if params.validate {
        ctx.info("starting validation (this is slow)");
        validate(ctx, &collection)
            .await
            .map_err(|e| super::TaskError::Failed(e.to_string()))?;
    }

    ctx.record_mutation(
        MutationTarget {
            database: ctx.db().name().to_string(),
            collection: COLLECTION.to_string(),
            catalog: None,
            survey: Some("ztf".to_string()),
        },
        // Recompute rather than Backfill: every value is derived from fields
        // already on the document, with no external source involved.
        Operation::Recompute,
        doc! {
            "documents_modified": modified,
            "batch_size": params.batch_size as i64,
            "validated": params.validate,
            "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                .unwrap_or(Bson::Null),
        },
    )
    .await;

    Ok(serde_json::json!({
        "collection": COLLECTION,
        "documents_modified": modified,
        "validated": params.validate,
    }))
}

async fn run_batched_update(
    ctx: &TaskContext,
    collection: &mongodb::Collection<Document>,
    filter: Document,
    pipeline: Vec<Document>,
    batch_size: usize,
    estimated_total: u64,
    label: &str,
) -> Result<i64, MigrateError> {
    let mut cursor = collection
        .find(filter)
        .projection(doc! { "_id": 1 })
        .no_cursor_timeout(true)
        .await?;

    let mut ids: Vec<Bson> = Vec::with_capacity(batch_size);
    let mut total_modified: i64 = 0;
    let mut seen: u64 = 0;
    let mut last_reported: u64 = 0;

    while let Some(d) = cursor.try_next().await? {
        let Some(id) = d.get("_id") else {
            // A document without an _id cannot exist in Mongo; skipping rather
            // than unwrapping keeps a malformed cursor row from killing a
            // migration that is otherwise fine.
            continue;
        };
        ids.push(id.clone());

        if ids.len() >= batch_size {
            // Checked between batches, not mid-batch: an update_many is atomic
            // per document, so a batch boundary is the only point where
            // stopping leaves a state that is easy to describe.
            if ctx.is_canceled() {
                ctx.warn(format!(
                    "{label} canceled after {total_modified} documents; \
                     the work already done stands, and re-running resumes it \
                     because the migration recomputes from the raw fields"
                ));
                return Err(MigrateError::Canceled {
                    modified: total_modified,
                });
            }

            let n = ids.len() as u64;
            let batch_filter = doc! { "_id": { "$in": &ids } };
            let result = collection
                .update_many(batch_filter, pipeline.clone())
                .await?;
            total_modified += result.modified_count as i64;
            seen += n;
            ids.clear();

            if seen - last_reported >= PROGRESS_EVERY {
                last_reported = seen;
                ctx.progress(
                    seen,
                    estimated_total.max(seen),
                    format!("{label}: {total_modified} documents updated"),
                )
                .await;
            }
        }
    }

    if !ids.is_empty() {
        let batch_filter = doc! { "_id": { "$in": &ids } };
        let result = collection.update_many(batch_filter, pipeline).await?;
        total_modified += result.modified_count as i64;
    }

    Ok(total_modified)
}

async fn migrate(
    ctx: &TaskContext,
    collection: &mongodb::Collection<Document>,
    batch_size: usize,
) -> Result<i64, MigrateError> {
    let estimated_count = collection.estimated_document_count().await?;
    ctx.info(format!(
        "migrating fp_hists in {COLLECTION}: ~{estimated_count} documents"
    ));

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
        ctx,
        collection,
        filter,
        pipeline,
        batch_size,
        estimated_count,
        "migrate",
    )
    .await?;

    ctx.info(format!("migration complete: {total} documents modified"));
    Ok(total)
}

async fn validate(
    ctx: &TaskContext,
    collection: &mongodb::Collection<Document>,
) -> Result<(), MigrateError> {
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

    let estimated_count = collection.estimated_document_count().await?;
    let mut cursor = collection.aggregate(pipeline).await?;

    let mut num_validated = 0;
    let mut num_failed = 0;
    let mut num_skipped = 0;
    let mut skipped_by_reason = HashMap::new();
    let mut failed_by_reason = HashMap::new();

    let mut seen: u64 = 0;
    let mut last_reported: u64 = 0;

    while let Some(d) = cursor.try_next().await? {
        // Validation only reads, so stopping anywhere is safe.
        if ctx.is_canceled() {
            ctx.warn("validation canceled");
            return Err(MigrateError::Canceled { modified: 0 });
        }
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
                ctx.warn(format!(
                    "validation mismatch on {:?}: computed magpsf {computed_magpsf}±{computed_sigmapsf} \
                     vs stored {magpsf}±{sigmapsf}",
                    d.get("_id")
                ));
            } else if (computed_sigmapsf - sigmapsf).abs() >= 1e-5 {
                num_failed += 1;
                *failed_by_reason.entry("sigmapsf_mismatch").or_insert(0) += 1;
                ctx.warn(format!(
                    "validation mismatch on {:?}: computed sigmapsf {computed_sigmapsf} \
                     vs stored {sigmapsf}",
                    d.get("_id")
                ));
            } else {
                num_validated += 1;
            }
        }
        seen += 1;
        if seen - last_reported >= PROGRESS_EVERY {
            last_reported = seen;
            ctx.progress(
                seen,
                estimated_count.max(seen),
                format!("validate: {num_failed} mismatches so far"),
            )
            .await;
        }
    }

    let summary = format!(
        "validation complete: {num_validated} validated, {num_failed} failed, \
         {num_skipped} skipped; skipped by reason {skipped_by_reason:?}, \
         failed by reason {failed_by_reason:?}"
    );
    // A mismatch means the migration produced values that do not round-trip
    // back to the stored magnitudes, which is worth more than an info line.
    if num_failed > 0 {
        ctx.error(summary);
    } else {
        ctx.info(summary);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(batch_size: usize) -> MigrateFpFluxParams {
        MigrateFpFluxParams {
            batch_size,
            validate: false,
        }
    }

    #[test]
    fn a_zero_batch_size_is_rejected() {
        // Zero would collect no ids and update nothing, forever.
        assert!(params(0).validate_params().is_err());
    }

    #[test]
    fn an_absurd_batch_size_is_rejected() {
        // A filter document with a million ids in `$in` is one Mongo refuses,
        // and the run would fail deep in the loop rather than at submit.
        assert!(params(MAX_BATCH_SIZE + 1).validate_params().is_err());
        assert!(params(MAX_BATCH_SIZE).validate_params().is_ok());
    }

    #[test]
    fn params_default_to_something_runnable() {
        // The admin page can submit `{}` for this task.
        let parsed: MigrateFpFluxParams =
            serde_json::from_value(serde_json::json!({})).expect("defaults");
        assert_eq!(parsed.batch_size, default_batch_size());
        assert!(!parsed.validate, "validation is slow, so it is opt-in");
        assert!(parsed.validate_params().is_ok());
    }

    #[test]
    fn the_task_is_registered_and_declared_idempotent() {
        // Idempotence is what lets the queue requeue a run whose lease lapsed;
        // a task that is not idempotent must not claim to be.
        let spec = crate::tasks::find(TASK_TYPE).expect("registered");
        assert!(spec.idempotent);
        // It recomputes from raw fields, never from its own previous output.
        assert!(!spec.destructive);
    }

    #[test]
    fn submitting_it_is_single_flight_across_the_whole_collection() {
        // Unlike a catalog ingest, which is keyed per catalog, two of these
        // would rewrite the same documents with the same pipeline.
        let key = crate::tasks::single_flight_key(TASK_TYPE, &serde_json::json!({}));
        assert_eq!(key, Some(mongodb::bson::doc! {}));
    }

    #[test]
    fn bad_params_are_rejected_before_a_worker_ever_sees_them() {
        assert!(
            crate::tasks::validate_params(TASK_TYPE, &serde_json::json!({ "batch_size": 0 }))
                .is_err()
        );
        assert!(crate::tasks::validate_params(TASK_TYPE, &serde_json::json!({})).is_ok());
    }
}
