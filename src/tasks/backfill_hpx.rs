//! Write `coordinates.hpx` onto documents that predate the field.
//!
//! The index is a pure function of the position already stored, but HEALPix is
//! not something mongo can evaluate, so each document has to be read, hashed
//! here, and written back. Until this has covered a collection, a MOC range
//! query silently misses everything in it -- an absent index is
//! indistinguishable from a position outside the region.
//!
//! Ported from the `backfill_hpx` binary (#653) so it runs through the task
//! system rather than from a shell: recorded, cancellable, with its logs kept.

use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use crate::utils::{enums::Survey, spatial::HPX_DEPTH};
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, Bson, Document},
    options::{UpdateModifications, UpdateOneModel, WriteModel},
    Collection,
};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "backfill_hpx";

const MAX_BATCH_SIZE: usize = 100_000;

/// How many documents to scan between progress publishes.
const PROGRESS_EVERY: u64 = 50_000;

fn default_batch_size() -> usize {
    2_000
}

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BackfillHpxParams {
    /// Survey to process. Omit to process every survey present in this
    /// deployment.
    #[serde(default)]
    pub survey: Option<Survey>,
    /// Documents per bulk write.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Count what would be written without writing it.
    #[serde(default)]
    pub dry_run: bool,
}

impl BackfillHpxParams {
    pub fn validate_params(&self) -> Result<(), String> {
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        Ok(())
    }
}

/// Every collection holding positions, alerts and their aux documents alike.
fn collections_for(survey: &Survey) -> Vec<String> {
    let name = survey.as_str();
    vec![format!("{name}_alerts"), format!("{name}_alerts_aux")]
}

/// The HEALPix index for a stored GeoJSON position, which is `[ra - 180, dec]`.
fn hpx_for(doc: &Document) -> Option<i64> {
    let coords = doc
        .get_document("coordinates")
        .ok()?
        .get_document("radec_geojson")
        .ok()?
        .get_array("coordinates")
        .ok()?;
    let lon = coords.first().and_then(Bson::as_f64)?;
    let dec = coords.get(1).and_then(Bson::as_f64)?;
    let ra = lon + 180.0;
    Some(cdshealpix::nested::get(HPX_DEPTH).hash(ra.to_radians(), dec.to_radians()) as i64)
}

struct CollectionReport {
    written: u64,
    skipped: u64,
}

fn failed(e: mongodb::error::Error) -> super::TaskError {
    super::TaskError::Failed(e.to_string())
}

async fn backfill(
    ctx: &TaskContext,
    collection: &Collection<Document>,
    batch_size: usize,
    dry_run: bool,
    done_before: u64,
    estimated_total: u64,
) -> Result<CollectionReport, super::TaskError> {
    let client = collection.client().clone();
    // Only documents still missing the field, so a resumed run skips its own
    // work -- which is also what makes the task idempotent.
    let mut cursor = collection
        .find(doc! { "coordinates.hpx": { "$exists": false } })
        .projection(doc! { "_id": 1, "coordinates.radec_geojson": 1 })
        .no_cursor_timeout(true)
        .await
        .map_err(failed)?;

    let mut batch: Vec<WriteModel> = Vec::with_capacity(batch_size);
    let mut written: u64 = 0;
    let mut skipped: u64 = 0;
    let mut last_reported: u64 = 0;

    while let Some(d) = cursor.try_next().await.map_err(failed)? {
        let Some(id) = d.get("_id") else {
            continue;
        };
        let Some(hpx) = hpx_for(&d) else {
            skipped += 1;
            continue;
        };
        batch.push(WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(collection.namespace())
                .filter(doc! { "_id": id.clone() })
                .update(UpdateModifications::Document(
                    doc! { "$set": { "coordinates.hpx": hpx } },
                ))
                .build(),
        ));

        if batch.len() >= batch_size {
            // Checked at the batch boundary: every written index is correct on
            // its own, so stopping here leaves a state that is easy to describe.
            if ctx.is_canceled() {
                ctx.warn(format!(
                    "{}: canceled after {written} document(s); what is written stays, and \
                     re-running resumes from the documents still missing the field",
                    collection.name()
                ));
                return Err(super::TaskError::Canceled);
            }
            let n = batch.len() as u64;
            if dry_run {
                batch.clear();
            } else {
                client
                    .bulk_write(std::mem::take(&mut batch))
                    .ordered(false)
                    .await
                    .map_err(failed)?;
            }
            written += n;
            let seen = done_before + written;
            if seen - last_reported >= PROGRESS_EVERY {
                last_reported = seen;
                ctx.progress(
                    seen,
                    estimated_total.max(seen),
                    format!("{}: {written} indexed", collection.name()),
                )
                .await;
            }
        }
    }

    if !batch.is_empty() {
        let n = batch.len() as u64;
        if !dry_run {
            client
                .bulk_write(batch)
                .ordered(false)
                .await
                .map_err(failed)?;
        }
        written += n;
    }
    Ok(CollectionReport { written, skipped })
}

pub async fn run(
    ctx: &TaskContext,
    params: BackfillHpxParams,
) -> Result<serde_json::Value, super::TaskError> {
    params
        .validate_params()
        .map_err(super::TaskError::InvalidParams)?;
    let db = ctx.db().clone();

    let surveys: Vec<Survey> = match &params.survey {
        Some(survey) => vec![survey.clone()],
        None => vec![Survey::Ztf, Survey::Lsst, Survey::Decam, Survey::Winter],
    };

    // Sized from the metadata estimate: an exact count filters on an unindexed
    // field, which on the larger collections costs more than the pass itself.
    let mut targets: Vec<(Survey, Collection<Document>)> = Vec::new();
    let mut estimated_total = 0u64;
    for survey in &surveys {
        for name in collections_for(survey) {
            let collection = db.collection::<Document>(&name);
            let estimate = collection.estimated_document_count().await.unwrap_or(0);
            // A survey absent from this deployment is not an error.
            if estimate == 0 {
                continue;
            }
            estimated_total += estimate;
            targets.push((survey.clone(), collection));
        }
    }

    ctx.info(format!(
        "indexing {} collection(s) at HEALPix depth {HPX_DEPTH}{}",
        targets.len(),
        if params.dry_run {
            " (dry run, nothing will be written)"
        } else {
            ""
        }
    ));

    let mut total = 0u64;
    let mut per_collection = serde_json::Map::new();
    for (survey, collection) in &targets {
        let report = backfill(
            ctx,
            collection,
            params.batch_size,
            params.dry_run,
            total,
            estimated_total,
        )
        .await?;
        total += report.written;

        if report.written == 0 {
            ctx.info(format!("{}: already complete", collection.name()));
        } else {
            ctx.info(format!(
                "{}: {} document(s) {}",
                collection.name(),
                report.written,
                if params.dry_run {
                    "would be indexed"
                } else {
                    "indexed"
                }
            ));
        }
        if report.skipped > 0 {
            ctx.warn(format!(
                "{}: {} document(s) had no usable position and were left alone",
                collection.name(),
                report.skipped
            ));
        }
        per_collection.insert(
            collection.name().to_string(),
            serde_json::json!({ "written": report.written, "skipped": report.skipped }),
        );

        if !params.dry_run && report.written > 0 {
            ctx.record_mutation(
                MutationTarget {
                    database: db.name().to_string(),
                    collection: collection.name().to_string(),
                    catalog: None,
                    survey: Some(survey.as_str().to_string()),
                },
                // Recompute: the index is derived from the position already on
                // the document, not fetched from anywhere.
                Operation::Recompute,
                doc! {
                    "field": "coordinates.hpx",
                    "depth": HPX_DEPTH as i64,
                    "written": report.written as i64,
                },
            )
            .await;
        }
    }

    ctx.progress(
        total,
        estimated_total.max(total),
        format!("{total} indexed"),
    )
    .await;

    Ok(serde_json::json!({
        "depth": HPX_DEPTH,
        "indexed": total,
        "dry_run": params.dry_run,
        "collections": per_collection,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_index_is_computed_from_the_stored_geojson_position() {
        // Stored as [ra - 180, dec]; the index must be of the real ra, or every
        // backfilled document lands half a sky away from where a MOC query
        // looks for it.
        let (ra, dec): (f64, f64) = (187.2779, 2.0524); // 3C 273
        let d = doc! { "coordinates": { "radec_geojson": {
            "type": "Point", "coordinates": [ra - 180.0, dec]
        } } };
        let expected =
            cdshealpix::nested::get(HPX_DEPTH).hash(ra.to_radians(), dec.to_radians()) as i64;
        assert_eq!(hpx_for(&d), Some(expected));
    }

    #[test]
    fn a_document_without_a_usable_position_is_skipped() {
        assert_eq!(hpx_for(&doc! {}), None);
        assert_eq!(
            hpx_for(&doc! { "coordinates": { "radec_geojson": { "coordinates": ["x", 1.0] } } }),
            None
        );
    }

    #[test]
    fn both_the_alerts_and_their_aux_documents_are_covered() {
        assert_eq!(
            collections_for(&Survey::Ztf),
            vec!["ZTF_alerts".to_string(), "ZTF_alerts_aux".to_string()]
        );
    }

    #[test]
    fn omitting_the_survey_means_every_survey_and_writing_is_the_default() {
        let p: BackfillHpxParams = serde_json::from_value(serde_json::json!({})).unwrap();
        assert!(p.survey.is_none());
        assert!(!p.dry_run);
        assert_eq!(p.batch_size, default_batch_size());
        assert!(p.validate_params().is_ok());
    }

    #[test]
    fn batch_size_is_bounded() {
        let mut p: BackfillHpxParams = serde_json::from_value(serde_json::json!({})).unwrap();
        p.batch_size = 0;
        assert!(p.validate_params().is_err());
        p.batch_size = MAX_BATCH_SIZE + 1;
        assert!(p.validate_params().is_err());
    }
}
