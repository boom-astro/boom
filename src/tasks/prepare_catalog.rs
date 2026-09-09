//! The `prepare_catalog` task: make a hand-imported collection crossmatchable.
//!
//! A collection imported straight from a file (Compass' `Add Data`, `mongoimport`)
//! has flat columns. Crossmatch and the cone-search endpoint need the same
//! spatial fields the alert pipeline writes: `ra`/`dec` as degrees, a
//! `coordinates.radec_geojson` point with longitude shifted to `ra - 180`,
//! galactic `coordinates.l`/`b`, and a 2dsphere index.
//!
//! **Idempotent.** Documents that already carry `coordinates` are skipped unless
//! `force` is set, and the index creation is a no-op when it already exists.
//!
//! Documents whose ra/dec are missing or out of range are left untouched and
//! reported rather than guessed at -- the 2dsphere index would reject them, and
//! a fabricated position is worse than an absent one.
//!
//! Ported from `src/bin/prepare_catalog.rs`. This one needed the least changing:
//! it was already sharded, already had a dry run, and already returned reports
//! rather than exiting from inside the work.

use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use crate::{
    api::catalogs::WATCHLIST_PREFIX,
    utils::{
        db::{
            check_shard_coverage, collection_exists, create_index, exact_count, join_tasks,
            merge_filters, range_shards, shard_field, CURSOR_BATCH_SIZE,
        },
        spatial::Coordinates,
    },
};
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, to_bson, Bson, Document},
    options::{UpdateOneModel, WriteModel},
    Collection,
};
use serde::{Deserialize, Serialize};

use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "prepare_catalog";

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct PrepareCatalogParams {
    /// The collection to prepare. Must already exist -- this adds spatial
    /// fields to an import, it does not create one.
    pub catalog: String,
    #[serde(default = "default_ra_field")]
    pub ra_field: String,
    #[serde(default = "default_dec_field")]
    pub dec_field: String,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Shards the scan is split into, processed in parallel.
    #[serde(default = "default_processes")]
    pub processes: usize,
    /// Recompute coordinates for every document, not just those without them.
    #[serde(default)]
    pub force: bool,
    /// Report what would change without writing, and skip index creation.
    #[serde(default)]
    pub dry_run: bool,
}

fn default_ra_field() -> String {
    "ra".to_string()
}
fn default_dec_field() -> String {
    "dec".to_string()
}
fn default_batch_size() -> usize {
    1_000
}
fn default_processes() -> usize {
    1
}

const MAX_BATCH_SIZE: usize = 100_000;
const MAX_PROCESSES: usize = 64;

impl PrepareCatalogParams {
    pub fn validate_params(&self) -> Result<(), String> {
        if self.catalog.trim().is_empty() {
            return Err("catalog is required".to_string());
        }
        // A protected operational collection is not a catalog, and giving one
        // spatial fields and a 2dsphere index would be a strange thing to do by
        // accident.
        if crate::api::db::PROTECTED_COLLECTION_NAMES.contains(&self.catalog.as_str()) {
            return Err(format!(
                "{} is an operational collection, not a catalog",
                self.catalog
            ));
        }
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        if self.processes == 0 || self.processes > MAX_PROCESSES {
            return Err(format!("processes must be between 1 and {MAX_PROCESSES}"));
        }
        Ok(())
    }
}

/// Prepare the collection.
pub async fn run(
    ctx: &TaskContext,
    params: PrepareCatalogParams,
) -> Result<serde_json::Value, super::TaskError> {
    let db = ctx.db().clone();
    let failed = |e: String| super::TaskError::Failed(e);

    match collection_exists(&db, &params.catalog).await {
        Ok(true) => {}
        // Not an error the task can fix: the import has to happen first, and
        // saying so is more useful than a generic failure.
        Ok(false) => {
            return Err(super::TaskError::InvalidParams(format!(
                "collection {} does not exist in {}; import it first",
                params.catalog,
                db.name()
            )))
        }
        Err(e) => return Err(failed(e.to_string())),
    }

    let collection = db.collection::<Document>(&params.catalog);
    let base_filter = if params.force {
        doc! {}
    } else {
        doc! { "coordinates": { "$exists": false } }
    };
    // Counting the non-indexed coordinates filter would collection-scan twice.
    let total = if params.force {
        exact_count(&collection).await
    } else {
        collection.estimated_document_count().await
    }
    .map_err(|e| failed(e.to_string()))?;

    ctx.info(if params.force {
        format!("{}: {total} document(s) to process", params.catalog)
    } else {
        format!(
            "{}: scanning about {total} document(s) for missing coordinates",
            params.catalog
        )
    });

    let mut report = Report::default();
    let mut shard_count = 1;

    if total > 0 {
        if ctx.is_canceled() {
            return Err(super::TaskError::Canceled);
        }
        let shard_field = shard_field(&collection).await;
        let shards = range_shards(&collection, params.processes, shard_field, &base_filter).await;
        shard_count = shards.len();
        ctx.info(format!(
            "scanning {} in {} shard(s) cut on {shard_field:?}",
            params.catalog,
            shards.len()
        ));

        let mut handles = Vec::with_capacity(shards.len());
        for shard in &shards {
            let collection = collection.clone();
            let filter = merge_filters(&base_filter, shard);
            let ra_field = params.ra_field.clone();
            let dec_field = params.dec_field.clone();
            let batch_size = params.batch_size;
            let dry_run = params.dry_run;
            handles.push(tokio::spawn(async move {
                process_shard(collection, filter, ra_field, dec_field, batch_size, dry_run).await
            }));
        }

        match join_tasks(handles, "shard").await {
            Ok(reports) => {
                for shard_report in reports {
                    report.merge(shard_report);
                }
            }
            Err(e) => return Err(failed(e.to_string())),
        }
    }

    ctx.info(if params.dry_run {
        format!("dry run: {} document(s) would be updated", report.updated)
    } else {
        format!("updated {} document(s)", report.updated)
    });
    if report.missing > 0 {
        ctx.warn(format!(
            "{} document(s) skipped: no numeric {} / {}",
            report.missing, params.ra_field, params.dec_field
        ));
    }
    if report.out_of_range > 0 {
        ctx.warn(format!(
            "{} document(s) skipped: ra outside [0, 360] or dec outside [-90, 90]",
            report.out_of_range
        ));
    }
    for sample in &report.samples {
        ctx.warn(format!("  skipped _id {sample}"));
    }

    let scanned = report.updated + report.missing + report.out_of_range;
    let covered = !params.force || check_shard_coverage(scanned, total, shard_count);

    if params.dry_run {
        ctx.info("dry run: skipping index creation");
        if !covered {
            return Err(failed(
                "sharding did not cover every document; re-run without dry_run once the \
                 coverage warning above is understood"
                    .to_string(),
            ));
        }
    } else {
        create_index(
            &collection,
            doc! { "coordinates.radec_geojson": "2dsphere" },
            false,
        )
        .await
        .map_err(|e| failed(e.to_string()))?;
        ctx.info("2dsphere index on coordinates.radec_geojson ready");

        if !covered {
            return Err(failed(
                "sharding did not cover every document; some may still lack coordinates"
                    .to_string(),
            ));
        }

        ctx.record_mutation(
            MutationTarget {
                database: db.name().to_string(),
                collection: params.catalog.clone(),
                catalog: Some(params.catalog.clone()),
                survey: None,
            },
            // The spatial fields are derived from columns already on each
            // document, with no external source involved.
            Operation::Recompute,
            doc! {
                "documents_updated": report.updated as i64,
                "skipped_missing_coordinates": report.missing as i64,
                "skipped_out_of_range": report.out_of_range as i64,
                "ra_field": &params.ra_field,
                "dec_field": &params.dec_field,
                "force": params.force,
                "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                    .unwrap_or(Bson::Null),
            },
        )
        .await;

        ctx.info(format!(
            "{} is ready; declare it under crossmatch.<survey> in the config, then run \
             reprocess_crossmatch to backfill existing alerts",
            params.catalog
        ));
        if params.catalog.starts_with(WATCHLIST_PREFIX) {
            ctx.info(
                "watchlist catalog: grant access with PATCH /users/{user_id}/watchlist_access \
                 before it can be queried or bound to a filter",
            );
        }
    }

    Ok(serde_json::json!({
        "collection": params.catalog,
        "documents_updated": report.updated,
        "skipped_missing_coordinates": report.missing,
        "skipped_out_of_range": report.out_of_range,
        "dry_run": params.dry_run,
    }))
}

fn as_f64(value: Option<&Bson>) -> Option<f64> {
    match value? {
        Bson::Double(v) => Some(*v),
        Bson::Int32(v) => Some(*v as f64),
        Bson::Int64(v) => Some(*v as f64),
        Bson::String(v) => v.trim().parse().ok(),
        _ => None,
    }
}

const MAX_SAMPLES: usize = 10;

#[derive(Default)]
struct Report {
    updated: u64,
    missing: u64,
    out_of_range: u64,
    samples: Vec<String>,
}

impl Report {
    fn reject(&mut self, id: &Bson, reason: &str) {
        if self.samples.len() < MAX_SAMPLES {
            self.samples.push(format!("{} ({})", id, reason));
        }
    }

    fn merge(&mut self, other: Report) {
        self.updated += other.updated;
        self.missing += other.missing;
        self.out_of_range += other.out_of_range;
        for sample in other.samples {
            if self.samples.len() < MAX_SAMPLES {
                self.samples.push(sample);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn process_shard(
    collection: Collection<Document>,
    filter: Document,
    ra_field: String,
    dec_field: String,
    batch_size: usize,
    dry_run: bool,
) -> Result<Report, mongodb::error::Error> {
    let client = collection.client().clone();
    let namespace = collection.namespace();
    let mut cursor = collection
        .find(filter)
        .projection(doc! { &ra_field: 1, &dec_field: 1 })
        .no_cursor_timeout(true)
        .batch_size(CURSOR_BATCH_SIZE)
        .await?;

    let mut report = Report::default();
    let mut writes: Vec<WriteModel> = Vec::with_capacity(batch_size);

    while let Some(doc) = cursor.try_next().await? {
        let id = match doc.get("_id") {
            Some(id) => id.clone(),
            None => continue,
        };

        let (ra, dec) = match (as_f64(doc.get(&ra_field)), as_f64(doc.get(&dec_field))) {
            (Some(ra), Some(dec)) => (ra, dec),
            _ => {
                report.missing += 1;
                report.reject(&id, "missing or non-numeric ra/dec");
                continue;
            }
        };
        if !(0.0..=360.0).contains(&ra) || !(-90.0..=90.0).contains(&dec) {
            report.out_of_range += 1;
            report.reject(&id, &format!("ra={} dec={} out of range", ra, dec));
            continue;
        }

        report.updated += 1;
        if dry_run {
            continue;
        }

        let coordinates = to_bson(&Coordinates::new(ra, dec)).expect("coordinates serialize");
        writes.push(WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(namespace.clone())
                .filter(doc! { "_id": id })
                .update(doc! { "$set": { "coordinates": coordinates, "ra": ra, "dec": dec } })
                .build(),
        ));

        if writes.len() >= batch_size {
            client
                .bulk_write(std::mem::take(&mut writes))
                .ordered(false)
                .await?;
            writes = Vec::with_capacity(batch_size);
        }
    }

    if !writes.is_empty() {
        client.bulk_write(writes).ordered(false).await?;
    }
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params(catalog: &str) -> PrepareCatalogParams {
        PrepareCatalogParams {
            catalog: catalog.to_string(),
            ra_field: default_ra_field(),
            dec_field: default_dec_field(),
            batch_size: default_batch_size(),
            processes: default_processes(),
            force: false,
            dry_run: false,
        }
    }

    #[test]
    fn a_catalog_name_is_required() {
        assert!(params("  ").validate_params().is_err());
        assert!(params("NED").validate_params().is_ok());
    }

    #[test]
    fn operational_collections_are_refused() {
        // Giving `users` or `filters` a 2dsphere index would be a strange thing
        // to do by accident, and the task cannot tell it was a mistake.
        for name in crate::api::db::PROTECTED_COLLECTION_NAMES {
            assert!(
                params(name).validate_params().is_err(),
                "{name} should not be preparable as a catalog"
            );
        }
    }

    #[test]
    fn coordinate_fields_default_to_ra_and_dec() {
        let parsed: PrepareCatalogParams =
            serde_json::from_value(serde_json::json!({ "catalog": "NED" })).expect("defaults");
        assert_eq!(parsed.ra_field, "ra");
        assert_eq!(parsed.dec_field, "dec");
        assert!(!parsed.force, "existing coordinates are kept unless forced");
        assert!(!parsed.dry_run);
    }

    #[test]
    fn the_shard_count_is_bounded() {
        let mut p = params("NED");
        p.processes = MAX_PROCESSES + 1;
        assert!(p.validate_params().is_err());
    }

    #[test]
    fn the_task_is_registered_and_retryable() {
        assert!(crate::tasks::is_retryable(TASK_TYPE));
    }

    #[test]
    fn single_flight_is_keyed_by_collection() {
        // Two runs over one collection would rewrite the same documents and
        // race on creating the index; different collections are independent.
        assert_eq!(
            crate::tasks::single_flight_key(TASK_TYPE, &serde_json::json!({ "catalog": "NED" })),
            Some(mongodb::bson::doc! { "catalog": "NED" })
        );
    }
}
