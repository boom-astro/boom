//! Fill in `host_galaxy` on a survey's `alerts_aux` records.
//!
//! Association is a pure function of the galaxy cross-matches already stored on
//! each record, so this reads `cross_matches` rather than re-querying the
//! catalogs. Records written before `host_galaxy.enabled` was turned on, or
//! before the galaxy catalogs were added to `crossmatch.<survey>`, have no field
//! at all; a change to the scoring parameters instead leaves a stale one.
//!
//! A record whose cross-matches predate the galaxy catalogs needs
//! `reprocess_crossmatch` over NED and LSDR10 first: without those entries there
//! is nothing to associate against and this writes an empty association.
//!
//! Ported from the `backfill_host_galaxy` binary (#566), so it runs through the
//! task system: recorded in the ledger, cancellable, with its logs kept.
//!
//! Records are read as raw documents rather than deserialized into a typed
//! struct, because a collection this old contains records whose `coordinates`
//! or `cross_matches` do not have the shape the current code expects. Typed,
//! one of those ends the whole run on a deserialization error; here it is
//! counted, named in the log, and left without a `host_galaxy`. That tolerance
//! comes from #696, which fixed it in the binary this replaced.

use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use crate::utils::{
    db::CURSOR_BATCH_SIZE,
    enums::Survey,
    host::{self, HostGalaxyConfig},
};
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, to_bson, Bson, Document},
    options::{UpdateOneModel, WriteModel},
    Namespace,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "backfill_host_galaxy";

const MAX_BATCH_SIZE: usize = 100_000;
const MAX_PROCESSES: usize = 64;
const QUEUE_MULTIPLIER: usize = 2;
/// Sample size for the preflight check that the galaxy catalogs are present.
const CATALOG_PROBE_SAMPLE: i64 = 1_000;
/// How many records to scan between progress publishes.
const PROGRESS_EVERY: u64 = 50_000;

fn default_batch_size() -> usize {
    5_000
}

fn default_processes() -> usize {
    4
}

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct BackfillHostGalaxyParams {
    pub survey: Survey,
    /// Records accumulated per worker before a bulk write.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Parallel association workers.
    #[serde(default = "default_processes")]
    pub processes: usize,
    /// Skip records that already carry a `host_galaxy`, which makes an
    /// interrupted run resumable. Leave it off to rescore everything after a
    /// change to the association parameters.
    #[serde(default)]
    pub skip_existing: bool,
    /// Report what would be written without writing it.
    #[serde(default)]
    pub dry_run: bool,
}

impl BackfillHostGalaxyParams {
    pub fn validate_params(&self) -> Result<(), String> {
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        if self.processes == 0 || self.processes > MAX_PROCESSES {
            return Err(format!("processes must be between 1 and {MAX_PROCESSES}"));
        }
        Ok(())
    }
}

/// `coordinates.radec_geojson.coordinates` is `[ra - 180, dec]`.
///
/// `None` for a record whose position is missing, the wrong shape, or not
/// finite, which is a record this task leaves alone rather than one that ends
/// the run.
fn position(record: &Document) -> Option<(f64, f64)> {
    let point = record
        .get_document("coordinates")
        .ok()?
        .get_document("radec_geojson")
        .ok()?
        .get_array("coordinates")
        .ok()?;
    let [ra, dec] = point.as_slice() else {
        return None;
    };
    let (ra, dec) = (ra.as_f64()? + 180.0, dec.as_f64()?);
    (ra.is_finite() && dec.is_finite()).then_some((ra, dec))
}

/// The galaxy catalogs' rows off a raw `cross_matches`, or `None` if the field
/// is not the shape association expects.
///
/// An absent `cross_matches` is an empty map rather than a rejection: a record
/// that has never been crossmatched has no host to find, and that is a fact
/// about the record, not a defect in it.
fn galaxy_matches(
    cross_matches: Option<Bson>,
    catalogs: &[&str],
) -> Option<HashMap<String, Vec<Document>>> {
    let mut cross_matches = match cross_matches {
        None => return Some(HashMap::new()),
        Some(Bson::Document(cross_matches)) => cross_matches,
        Some(_) => return None,
    };
    let mut matches = HashMap::new();
    for catalog in catalogs {
        let Some(rows) = cross_matches.remove(*catalog) else {
            continue;
        };
        let Bson::Array(rows) = rows else {
            return None;
        };
        let rows = rows
            .into_iter()
            .map(|row| match row {
                Bson::Document(row) => Some(row),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        matches.insert(catalog.to_string(), rows);
    }
    Some(matches)
}

fn failed(e: impl std::fmt::Display) -> super::TaskError {
    super::TaskError::Failed(e.to_string())
}

/// Warn when no sampled record carries a shape column, which produces a run
/// that writes empty associations over the whole collection.
///
/// A bare `cross_matches.NED` is not enough: that key predates host association
/// and its rows were projected without `Diam`, so they carry no extent to score.
async fn probe_catalogs(
    ctx: &TaskContext,
    collection: &mongodb::Collection<Document>,
    config: &HostGalaxyConfig,
) {
    let present = vec![
        doc! { format!("cross_matches.{}.Diam", config.ned_catalog): { "$exists": true } },
        doc! { format!("cross_matches.{}", config.ls_dr10_catalog): { "$exists": true } },
    ];
    let found = collection
        .aggregate(vec![
            doc! { "$sample": { "size": CATALOG_PROBE_SAMPLE } },
            doc! { "$match": { "$or": present } },
            doc! { "$limit": 1 },
        ])
        .await;
    match found {
        Ok(mut cursor) => match cursor.try_next().await {
            Ok(None) => ctx.warn(format!(
                "none of {} sampled records carry {}.Diam or {}; run reprocess_crossmatch over \
                 the galaxy catalogs first, or every association will be empty",
                CATALOG_PROBE_SAMPLE, config.ned_catalog, config.ls_dr10_catalog
            )),
            Ok(Some(_)) => {}
            Err(e) => ctx.warn(format!("catalog probe failed, continuing: {e}")),
        },
        Err(e) => ctx.warn(format!("catalog probe failed, continuing: {e}")),
    }
}

/// Associate and write one worker's share of the stream.
async fn worker(
    ctx: &TaskContext,
    rx: async_channel::Receiver<Document>,
    client: mongodb::Client,
    aux_ns: Namespace,
    config: HostGalaxyConfig,
    batch_size: usize,
    dry_run: bool,
) -> Result<(u64, u64), super::TaskError> {
    let catalogs = [config.ned_catalog.as_str(), config.ls_dr10_catalog.as_str()];
    let mut batch: Vec<WriteModel> = Vec::with_capacity(batch_size);
    let mut written = 0u64;
    let mut unreadable = 0u64;
    // Named in the log, but only the first few: a systematic problem would
    // otherwise write one line per record into the run's logs.
    let mut say = |ctx: &TaskContext, unreadable: u64, msg: String| {
        if unreadable <= 5 {
            ctx.warn(msg);
        }
    };

    while let Ok(mut record) = rx.recv().await {
        let Some(object_id) = record.remove("_id") else {
            continue;
        };
        let Some((ra, dec)) = position(&record) else {
            unreadable += 1;
            say(
                ctx,
                unreadable,
                format!("{object_id}: unreadable coordinates, left without host_galaxy"),
            );
            continue;
        };
        let Some(matches) = galaxy_matches(record.remove("cross_matches"), &catalogs) else {
            unreadable += 1;
            say(
                ctx,
                unreadable,
                format!("{object_id}: unreadable cross_matches, left without host_galaxy"),
            );
            continue;
        };
        // `enabled` is checked once up front, so this is always `Some`.
        let Some(association) = host::associate_from_xmatches(ra, dec, &matches, &config) else {
            continue;
        };
        let value = match to_bson(&association) {
            Ok(v) => v,
            Err(e) => {
                unreadable += 1;
                say(
                    ctx,
                    unreadable,
                    format!("{object_id}: failed to encode the association, skipping: {e}"),
                );
                continue;
            }
        };
        batch.push(WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(aux_ns.clone())
                .filter(doc! { "_id": object_id })
                .update(doc! { "$set": { "host_galaxy": value } })
                .build(),
        ));

        if batch.len() >= batch_size {
            // Checked at the batch boundary: each association is independently
            // correct, so stopping here leaves a state that is easy to describe
            // -- some records scored, the rest untouched.
            if ctx.is_canceled() {
                ctx.warn(format!(
                    "canceled after writing {written} record(s) in this worker; what is \
                     written stays, and re-running with skip_existing resumes"
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
    Ok((written, unreadable))
}

pub async fn run(
    ctx: &TaskContext,
    params: BackfillHostGalaxyParams,
) -> Result<serde_json::Value, super::TaskError> {
    params
        .validate_params()
        .map_err(super::TaskError::InvalidParams)?;

    let config = ctx.config().host_galaxy.clone();
    if !config.enabled {
        // Running with it off would write an association from parameters the
        // pipeline is not using, which is worse than doing nothing.
        return Err(super::TaskError::InvalidParams(
            "host_galaxy.enabled is false in this deployment's config".to_string(),
        ));
    }

    let db = ctx.db().clone();
    let aux_name = format!("{}_alerts_aux", params.survey);
    let probe: mongodb::Collection<Document> = db.collection(&aux_name);
    probe_catalogs(ctx, &probe, &config).await;

    let aux_collection: mongodb::Collection<Document> = db.collection(&aux_name);
    let estimated = aux_collection.estimated_document_count().await.unwrap_or(0);
    ctx.info(format!(
        "associating hosts on {aux_name}: ~{estimated} record(s), {} worker(s){}",
        params.processes,
        if params.dry_run {
            " (dry run, nothing will be written)"
        } else {
            ""
        }
    ));

    let aux_ns = Namespace {
        db: db.name().to_string(),
        coll: aux_name.clone(),
    };
    let queue_capacity = params.processes * params.batch_size * QUEUE_MULTIPLIER;
    let (tx, rx) = async_channel::bounded::<Document>(queue_capacity);

    // buffer_unordered over borrowed futures rather than tokio::spawn: the
    // workers borrow `ctx` to log and to check cancellation.
    let workers = futures::future::try_join_all((0..params.processes).map(|_| {
        worker(
            ctx,
            rx.clone(),
            db.client().clone(),
            aux_ns.clone(),
            config.clone(),
            params.batch_size,
            params.dry_run,
        )
    }));
    drop(rx);

    let mut projection = doc! { "_id": 1, "coordinates.radec_geojson.coordinates": 1 };
    for catalog in [&config.ned_catalog, &config.ls_dr10_catalog] {
        projection.insert(format!("cross_matches.{catalog}"), 1);
    }

    let find_filter = if params.skip_existing {
        doc! { "host_galaxy": { "$exists": false } }
    } else {
        doc! {}
    };

    let feed = async {
        let mut cursor = aux_collection
            .find(find_filter)
            // Only the galaxy catalogs' rows: `cross_matches` as a whole is
            // every catalog the pipeline queries, which is far more BSON than
            // association reads.
            .projection(projection)
            .batch_size(CURSOR_BATCH_SIZE)
            .no_cursor_timeout(true)
            .await
            .map_err(failed)?;
        let mut fed = 0u64;
        let mut last_reported = 0u64;
        while let Some(record) = cursor.try_next().await.map_err(failed)? {
            if ctx.is_canceled() {
                ctx.warn(format!("canceled while streaming after {fed} record(s)"));
                break;
            }
            if tx.send(record).await.is_err() {
                break;
            }
            fed += 1;
            if fed - last_reported >= PROGRESS_EVERY {
                last_reported = fed;
                ctx.progress(fed, estimated.max(fed), format!("{fed} record(s) scanned"))
                    .await;
            }
        }
        drop(tx);
        Ok::<u64, super::TaskError>(fed)
    };

    let (fed, per_worker) = futures::try_join!(feed, workers)?;
    let total: u64 = per_worker.iter().map(|(written, _)| written).sum();
    let unreadable: u64 = per_worker.iter().map(|(_, unreadable)| unreadable).sum();
    if unreadable > 0 {
        ctx.warn(format!(
            "{unreadable} record(s) were left without a host_galaxy because their coordinates \
             or cross_matches could not be read"
        ));
    }

    if !params.dry_run && total > 0 {
        ctx.record_mutation(
            MutationTarget {
                database: db.name().to_string(),
                collection: aux_name.clone(),
                catalog: None,
                survey: Some(params.survey.to_string().to_lowercase()),
            },
            // Recompute: the association is derived from the cross-matches
            // already on the record, not fetched from anywhere.
            Operation::Recompute,
            doc! {
                "field": "host_galaxy",
                "scanned": fed as i64,
                "written": total as i64,
                "unreadable": unreadable as i64,
                "skip_existing": params.skip_existing,
            },
        )
        .await;
    }

    ctx.progress(
        fed,
        estimated.max(fed),
        format!("{total} record(s) associated"),
    )
    .await;

    Ok(serde_json::json!({
        "survey": params.survey.to_string(),
        "scanned": fed,
        "written": total,
        "unreadable": unreadable,
        "skip_existing": params.skip_existing,
        "dry_run": params.dry_run,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> BackfillHostGalaxyParams {
        serde_json::from_value(serde_json::json!({ "survey": "ztf" })).unwrap()
    }

    #[test]
    fn writing_is_the_default_and_every_record_is_rescored() {
        // skip_existing off by default: a change to the scoring parameters
        // leaves a stale association rather than an absent one, so the useful
        // default is to rescore rather than to fill gaps.
        let p = params();
        assert!(!p.dry_run);
        assert!(!p.skip_existing);
        assert_eq!(p.batch_size, default_batch_size());
        assert_eq!(p.processes, default_processes());
        assert!(p.validate_params().is_ok());
    }

    #[test]
    fn the_concurrency_knobs_are_bounded() {
        let mut p = params();
        p.batch_size = 0;
        assert!(p.validate_params().is_err());
        p.batch_size = MAX_BATCH_SIZE + 1;
        assert!(p.validate_params().is_err());
        p.batch_size = default_batch_size();
        p.processes = 0;
        assert!(p.validate_params().is_err());
        p.processes = MAX_PROCESSES + 1;
        assert!(p.validate_params().is_err());
    }

    #[test]
    fn single_flight_is_keyed_by_survey() {
        let ztf =
            crate::tasks::single_flight_key(TASK_TYPE, &serde_json::json!({ "survey": "ztf" }));
        let lsst =
            crate::tasks::single_flight_key(TASK_TYPE, &serde_json::json!({ "survey": "lsst" }));
        assert!(ztf.is_some());
        assert_ne!(ztf, lsst, "two surveys must not block each other");
    }
}
