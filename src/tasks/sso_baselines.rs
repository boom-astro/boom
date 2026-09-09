//! The `sso_baselines` task: fit phase-curve baselines for solar system objects.
//!
//! Every ZTF detection of a known solar system object carries the geometry
//! needed to place it on a phase curve. Fitting one per object per band gives a
//! baseline brightness, which is what later detections are judged against when
//! deciding whether an object is in outburst.
//!
//! **Idempotent.** Each baseline is an upsert keyed on the designation, refit
//! from the same detections, so re-running converges on the same values.
//!
//! Ported from `src/bin/sso_baselines.rs`. Beyond the mechanical move, a read
//! or write failure now fails the run instead of being logged and stepped over
//! -- a partial fit that reports success would leave baselines silently stale.

use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use crate::utils::outburst::{Point, MAX_SEPARATION_ARCSEC};
use crate::utils::phase_curve::{baseline_document, fit, PhaseCurve, BASELINES_COLLECTION};
use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use mongodb::options::{ReplaceOneModel, WriteModel};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "sso_baselines";

const ALERT_COLLECTION: &str = "ZTF_alerts";

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct SsoBaselinesParams {
    /// Baselines per bulk write.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Fit and report without writing.
    #[serde(default)]
    pub dry_run: bool,
    /// Stop after this many objects. For sampling the fit before committing to
    /// a full pass.
    #[serde(default)]
    pub limit: Option<usize>,
}

fn default_batch_size() -> usize {
    1_000
}

const MAX_BATCH_SIZE: usize = 100_000;

impl SsoBaselinesParams {
    pub fn validate_params(&self) -> Result<(), String> {
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        if self.limit == Some(0) {
            return Err("limit must be greater than zero".to_string());
        }
        Ok(())
    }
}

/// Fit and store the baselines.
pub async fn run(
    ctx: &TaskContext,
    params: SsoBaselinesParams,
) -> Result<serde_json::Value, super::TaskError> {
    let failed = super::TaskError::Failed;
    let db = ctx.db().clone();

    let alerts: mongodb::Collection<Document> = db.collection(ALERT_COLLECTION);
    let baselines: mongodb::Collection<Document> = db.collection(BASELINES_COLLECTION);
    let now = chrono::Utc::now().timestamp() as f64;

    // Sorted by designation so one object's detections arrive together and only
    // that object is ever held in memory. The same index serves the sort.
    let cursor = alerts
        .find(doc! {
            "candidate.ssnamenr": { "$exists": true },
            "properties.sso.helio_dist": { "$ne": null },
            // A static source near the predicted track carries the object's
            // designation but not its brightness, and would set the baseline
            // that later detections are judged against.
            "candidate.ssdistnr": { "$gte": 0.0, "$lt": MAX_SEPARATION_ARCSEC },
        })
        .projection(doc! {
            "_id": 0,
            "candidate.ssnamenr": 1, "candidate.fid": 1,
            "candidate.magpsf": 1, "candidate.sigmapsf": 1,
            "properties.sso.helio_dist": 1, "properties.sso.topo_dist": 1,
            "properties.sso.phase_angle": 1,
        })
        .sort(doc! { "candidate.ssnamenr": 1 })
        .no_cursor_timeout(true)
        .await;

    let mut cursor =
        cursor.map_err(|e| failed(format!("failed to query {ALERT_COLLECTION}: {e}")))?;

    let mut pending: Vec<WriteModel> = Vec::with_capacity(params.batch_size);
    let (mut objects, mut fitted, mut written) = (0usize, 0usize, 0usize);
    let mut current: Option<(String, Vec<Point>)> = None;
    let mut done = false;

    loop {
        // A read failure part-way leaves earlier baselines written, which is
        // fine -- they are recomputed from the same detections next run -- but
        // it must not look like a clean finish.
        let next = cursor
            .try_next()
            .await
            .map_err(|e| failed(format!("failed to read {ALERT_COLLECTION}: {e}")))?;
        let exhausted = next.is_none();

        let incoming = next.as_ref().and_then(point_from);
        let boundary = match (&current, &incoming) {
            (Some((name, _)), Some((next_name, _))) => name != next_name,
            (Some(_), None) => exhausted,
            _ => false,
        };

        if boundary || (exhausted && current.is_some()) {
            if let Some((name, points)) = current.take() {
                objects += 1;
                let curves = fit_bands(&points);
                if !curves.is_empty() {
                    fitted += 1;
                    let document = baseline_document(&name, &curves, now);
                    pending.push(WriteModel::ReplaceOne(
                        ReplaceOneModel::builder()
                            .namespace(baselines.namespace())
                            .filter(doc! { "_id": &name })
                            .replacement(document)
                            .upsert(true)
                            .build(),
                    ));
                }
                if params.limit.is_some_and(|limit| objects >= limit) {
                    done = true;
                }
            }
        }

        if pending.len() >= params.batch_size || (done || exhausted) && !pending.is_empty() {
            if params.dry_run {
                written += pending.len();
                pending.clear();
            } else {
                match db.client().bulk_write(std::mem::take(&mut pending)).await {
                    Ok(result) => {
                        written += (result.modified_count + result.upserted_count) as usize
                    }
                    Err(e) => {
                        return Err(failed(format!("bulk write failed: {e}")));
                    }
                }
            }
            ctx.progress(
                objects as u64,
                params.limit.unwrap_or(objects) as u64,
                format!("{objects} objects, {fitted} fitted, {written} written"),
            )
            .await;
        }

        if ctx.is_canceled() {
            // An object boundary: the pending batch has been flushed above and
            // no object is half-fitted.
            ctx.warn(format!("canceled after {objects} objects"));
            return Err(super::TaskError::Canceled);
        }

        if done || exhausted {
            break;
        }

        if let Some((name, point)) = incoming {
            match &mut current {
                Some((current_name, points)) if *current_name == name => points.push(point),
                _ => current = Some((name, vec![point])),
            }
        }
    }

    ctx.info(format!(
        "finished: {objects} objects, {fitted} fitted, {written} written (dry_run={})",
        params.dry_run
    ));

    if !params.dry_run && written > 0 {
        ctx.record_mutation(
            MutationTarget {
                database: db.name().to_string(),
                collection: BASELINES_COLLECTION.to_string(),
                catalog: None,
                survey: Some("ztf".to_string()),
            },
            // Fitted from detections already stored on the alerts.
            Operation::Recompute,
            doc! {
                "objects_scanned": objects as i64,
                "objects_fitted": fitted as i64,
                "baselines_written": written as i64,
                "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                    .unwrap_or(mongodb::bson::Bson::Null),
            },
        )
        .await;
    }

    Ok(serde_json::json!({
        "collection": BASELINES_COLLECTION,
        "objects_scanned": objects,
        "objects_fitted": fitted,
        "baselines_written": written,
        "dry_run": params.dry_run,
    }))
}

fn fit_bands(points: &[Point]) -> HashMap<u8, PhaseCurve> {
    let mut by_band: HashMap<u8, Vec<Point>> = HashMap::new();
    for p in points {
        by_band.entry(p.band).or_default().push(*p);
    }
    by_band
        .into_iter()
        .filter_map(|(band, band_points)| Some((band, fit(&band_points)?)))
        .collect()
}

/// One archived detection, or `None` when it is missing photometry or geometry.
fn point_from(doc: &Document) -> Option<(String, Point)> {
    let candidate = doc.get_document("candidate").ok()?;
    let sso = doc
        .get_document("properties")
        .ok()?
        .get_document("sso")
        .ok()?;
    let number = |d: &Document, key: &str| d.get(key).and_then(crate::utils::bson_number);
    Some((
        candidate.get_str("ssnamenr").ok()?.to_string(),
        Point {
            rh: number(sso, "helio_dist")?,
            delta: number(sso, "topo_dist")?,
            phase: number(sso, "phase_angle")?,
            mag: number(candidate, "magpsf")?,
            mag_err: number(candidate, "sigmapsf")?,
            band: number(candidate, "fid")? as u8,
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn params_default_to_a_full_pass() {
        let parsed: SsoBaselinesParams =
            serde_json::from_value(serde_json::json!({})).expect("defaults");
        assert_eq!(parsed.batch_size, default_batch_size());
        assert!(parsed.limit.is_none(), "no limit means every object");
        assert!(!parsed.dry_run);
        assert!(parsed.validate_params().is_ok());
    }

    #[test]
    fn a_zero_limit_is_rejected() {
        // Would fit nothing and report success, which reads as "no objects
        // needed baselines" rather than "you asked for none".
        let p = SsoBaselinesParams {
            batch_size: 10,
            dry_run: false,
            limit: Some(0),
        };
        assert!(p.validate_params().is_err());
    }

    #[test]
    fn batch_size_is_bounded() {
        let p = SsoBaselinesParams {
            batch_size: 0,
            dry_run: false,
            limit: None,
        };
        assert!(p.validate_params().is_err());
    }

    #[test]
    fn the_task_is_registered_and_retryable() {
        // Each baseline is an upsert keyed on designation, refit from the same
        // detections, so a resumed run converges.
        assert!(crate::tasks::is_retryable(TASK_TYPE));
    }

    #[test]
    fn only_one_fit_runs_at_a_time() {
        // Two would upsert the same baselines from the same detections.
        assert_eq!(
            crate::tasks::single_flight_key(TASK_TYPE, &serde_json::json!({})),
            Some(mongodb::bson::doc! {})
        );
    }
}
