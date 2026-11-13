//! Recompute every stored ZTF flux in nJy from the magnitudes and raw forced fluxes.

use super::batch::{run_batched_update, BatchError};
use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use super::migrate_fp_flux::fp_hists_flux_expr;
use crate::utils::lightcurves::ZTF_ZP;
use mongodb::bson::{doc, Bson, Document};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

pub const TASK_TYPE: &str = "migrate_ztf_flux";

const ALERTS: &str = "ZTF_alerts";
const AUX: &str = "ZTF_alerts_aux";

const FACTOR: f64 = 2.5_f64 / std::f64::consts::LN_10;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MigrateZtfFluxParams {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    5_000
}

const MAX_BATCH_SIZE: usize = 100_000;

impl MigrateZtfFluxParams {
    pub fn validate_params(&self) -> Result<(), String> {
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        Ok(())
    }
}

pub async fn run(
    ctx: &TaskContext,
    params: MigrateZtfFluxParams,
) -> Result<serde_json::Value, super::TaskError> {
    let mut modified = serde_json::Map::new();
    for (collection, filter, pipeline) in [
        (ALERTS, doc! {}, alerts_pipeline()),
        (
            AUX,
            doc! { "$or": [
                { "prv_candidates.0": { "$exists": true } },
                { "prv_nondetections.0": { "$exists": true } },
                { "fp_hists.0": { "$exists": true } },
            ]},
            aux_pipeline(),
        ),
    ] {
        let coll = ctx.db().collection::<Document>(collection);
        let estimated = coll.estimated_document_count().await.unwrap_or(0);
        ctx.info(format!("migrating {collection}: ~{estimated} documents"));
        let count = run_batched_update(
            ctx,
            &coll,
            filter,
            pipeline,
            params.batch_size,
            estimated,
            collection,
        )
        .await
        .map_err(|e| match e {
            BatchError::Canceled { .. } => super::TaskError::Canceled,
            other => super::TaskError::Failed(other.to_string()),
        })?;
        ctx.info(format!("{collection}: modified {count} documents"));

        ctx.record_mutation(
            MutationTarget {
                database: ctx.db().name().to_string(),
                collection: collection.to_string(),
                catalog: None,
                survey: Some("ztf".to_string()),
            },
            Operation::Recompute,
            doc! {
                "documents_modified": count,
                "batch_size": params.batch_size as i64,
                "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                    .unwrap_or(Bson::Null),
            },
        )
        .await;
        modified.insert(collection.to_string(), count.into());
    }

    Ok(serde_json::json!({ "documents_modified": modified }))
}

fn alerts_pipeline() -> Vec<Document> {
    vec![doc! {
        "$set": {
            "candidate.psfFlux": signed_flux("$candidate.magpsf", "$candidate.isdiffpos", "$candidate.psfFlux"),
            "candidate.psfFluxErr": flux_err("$candidate.magpsf", "$candidate.sigmapsf", "$candidate.psfFluxErr"),
            "candidate.apFlux": signed_flux("$candidate.magap", "$candidate.isdiffpos", "$candidate.apFlux"),
            "candidate.apFluxErr": flux_err("$candidate.magap", "$candidate.sigmagap", "$candidate.apFluxErr"),
        }
    }]
}

fn aux_pipeline() -> Vec<Document> {
    let prv_candidates = doc! {
        "$map": {
            "input": "$prv_candidates",
            "as": "pc",
            "in": { "$mergeObjects": ["$$pc", {
                "psfFlux": signed_flux("$$pc.magpsf", "$$pc.isdiffpos", "$$pc.psfFlux"),
                "psfFluxErr": flux_err("$$pc.magpsf", "$$pc.sigmapsf", "$$pc.psfFluxErr"),
                "apFlux": signed_flux("$$pc.magap", "$$pc.isdiffpos", "$$pc.apFlux"),
                "apFluxErr": flux_err("$$pc.magap", "$$pc.sigmagap", "$$pc.apFluxErr"),
            }]}
        }
    };
    let prv_nondetections = doc! {
        "$map": {
            "input": "$prv_nondetections",
            "as": "nd",
            "in": { "$mergeObjects": ["$$nd", {
                "psfFluxErr": {
                    "$cond": {
                        "if": { "$isNumber": "$$nd.diffmaglim" },
                        "then": { "$divide": [flux("$$nd.diffmaglim"), 5.0_f64] },
                        "else": "$$nd.psfFluxErr",
                    }
                },
            }]}
        }
    };
    vec![doc! {
        "$set": {
            "prv_candidates": if_array("$prv_candidates", prv_candidates),
            "prv_nondetections": if_array("$prv_nondetections", prv_nondetections),
            "fp_hists": if_array("$fp_hists", fp_hists_flux_expr()),
        }
    }]
}

fn if_array(field: &str, expr: Document) -> Document {
    doc! { "$cond": { "if": { "$isArray": field }, "then": expr, "else": field } }
}

fn flux(mag: &str) -> Document {
    doc! { "$pow": [10.0_f64, { "$multiply": [-0.4_f64, { "$subtract": [mag, ZTF_ZP as f64] }] }] }
}

fn signed_flux(mag: &str, isdiffpos: &str, existing: &str) -> Document {
    doc! {
        "$cond": {
            "if": { "$isNumber": mag },
            "then": {
                "$cond": {
                    "if": { "$eq": [isdiffpos, true] },
                    "then": flux(mag),
                    "else": { "$multiply": [flux(mag), -1.0_f64] },
                }
            },
            "else": existing,
        }
    }
}

fn flux_err(mag: &str, sigma: &str, existing: &str) -> Document {
    doc! {
        "$cond": {
            "if": { "$and": [{ "$isNumber": mag }, { "$isNumber": sigma }] },
            "then": { "$multiply": [{ "$divide": [sigma, FACTOR] }, flux(mag)] },
            "else": existing,
        }
    }
}
