use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use super::TaskError;
use crate::catalogs::STATE_COLLECTION;
use crate::utils::enums::Survey;
use crate::utils::spatial::{xmatch, Coordinates};
use crate::utils::tns::{
    object_document, read_objects, ObjectRow, ObjectsFile, ObjectsTable, TnsClient, TnsError,
    TNS_COLLECTION,
};
use futures::TryStreamExt;
use mongodb::bson::{doc, from_document, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use mongodb::{Database, IndexModel};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

pub const TASK_TYPE: &str = "tns_sync";

const HOUR: i64 = 3600;
const HOURLY_FILES_SPAN: i64 = 24 * HOUR;
const BATCH_SIZE: usize = 1_000;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TnsSyncParams {
    /// Apply the full objects file even when the hourly ones would do.
    #[serde(default)]
    pub full: bool,
}

impl TnsSyncParams {
    pub fn validate_params(&self) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct SyncState {
    synced_through: i64,
    full_at: i64,
}

#[derive(Debug, Default)]
struct Report {
    files: u64,
    upserted: u64,
    modified: u64,
    rejected: u64,
    unreadable: u64,
    rematched: HashMap<Survey, u64>,
}

fn failed(error: impl std::fmt::Display) -> TaskError {
    TaskError::Failed(error.to_string())
}

fn from_tns(error: TnsError) -> TaskError {
    match error {
        TnsError::Canceled => TaskError::Canceled,
        other => failed(other),
    }
}

fn iso(timestamp: i64) -> String {
    chrono::DateTime::from_timestamp(timestamp, 0)
        .map(|t| t.to_rfc3339())
        .unwrap_or_default()
}

fn needs_full(forced: bool, state: SyncState, now: i64, full_every_hours: u64) -> bool {
    forced
        || now - state.full_at >= full_every_hours as i64 * HOUR
        || now - state.synced_through >= HOURLY_FILES_SPAN
}

pub async fn run(ctx: &TaskContext, params: TnsSyncParams) -> Result<serde_json::Value, TaskError> {
    let mut report = Report::default();
    let outcome = sync(ctx, &params, &mut report).await;
    record(ctx, &report).await;
    let state = outcome?;
    Ok(serde_json::json!({
        "files": report.files,
        "upserted": report.upserted,
        "modified": report.modified,
        "rejected": report.rejected,
        "unreadable": report.unreadable,
        "rematched": report.rematched.values().sum::<u64>(),
        "synced_through": iso(state.synced_through),
    }))
}

async fn sync(
    ctx: &TaskContext,
    params: &TnsSyncParams,
    report: &mut Report,
) -> Result<SyncState, TaskError> {
    let config = &ctx.config().tns;
    let mut client = TnsClient::new(config).map_err(from_tns)?;
    let canceled = ctx.cancel_flag();
    let now = chrono::Utc::now().timestamp();
    initialize_indexes(ctx).await;

    let previous = read_state(ctx.db()).await.map_err(failed)?;
    let mut state = match previous {
        Some(state) if !needs_full(params.full, state, now, config.full_sync_every_hours) => state,
        _ => sync_full(ctx, &mut client, previous, params.full, report).await?,
    };

    let mut start = state.synced_through.div_euclid(HOUR) * HOUR;
    while start + HOUR <= now {
        if ctx.is_canceled() {
            return Err(TaskError::Canceled);
        }
        let file = ObjectsFile::Hourly { start };
        let Some(archive) = client
            .objects_file(&file, &canceled)
            .await
            .map_err(from_tns)?
        else {
            ctx.info(format!("{} is not published yet", file.name()));
            break;
        };
        let table = read_objects(&archive).map_err(failed)?;
        if table.start != start {
            ctx.info(format!(
                "{} still covers {}, waiting for {}",
                file.name(),
                iso(table.start),
                iso(start)
            ));
            break;
        }
        apply(ctx, &file, &table, |_| true, report).await?;
        start += HOUR;
        state.synced_through = start;
        write_state(ctx.db(), state).await?;
    }
    ctx.info(format!(
        "TNS is synced through {}",
        iso(state.synced_through)
    ));
    Ok(state)
}

async fn sync_full(
    ctx: &TaskContext,
    client: &mut TnsClient,
    previous: Option<SyncState>,
    forced: bool,
    report: &mut Report,
) -> Result<SyncState, TaskError> {
    let file = ObjectsFile::Full;
    let archive = client
        .objects_file(&file, &ctx.cancel_flag())
        .await
        .map_err(from_tns)?
        .ok_or_else(|| failed("TNS has no full objects file"))?;
    let table = read_objects(&archive).map_err(failed)?;
    if let Some(previous) = previous.filter(|p| !forced && p.full_at == table.start) {
        ctx.info(format!(
            "the full objects file is still the one cut at {}",
            iso(table.start)
        ));
        return Ok(previous);
    }
    let after = previous.map(|p| p.synced_through);
    let modified_since = |row: &ObjectRow| {
        after
            .zip(row.modified_at())
            .is_some_and(|(after, at)| at >= after)
    };
    apply(ctx, &file, &table, modified_since, report).await?;
    if previous.is_none() {
        ctx.info(
            "first sync: run reprocess_crossmatch over TNS for each survey to match the \
             objects BOOM saw before they were reported",
        );
    }
    let state = SyncState {
        synced_through: after.map_or(table.start, |after| after.max(table.start)),
        full_at: table.start,
    };
    write_state(ctx.db(), state).await?;
    Ok(state)
}

async fn apply(
    ctx: &TaskContext,
    file: &ObjectsFile,
    table: &ObjectsTable,
    rematch: impl Fn(&ObjectRow) -> bool,
    report: &mut Report,
) -> Result<(), TaskError> {
    let collection = ctx.db().collection::<Document>(TNS_COLLECTION);
    let namespace = collection.namespace();
    let total = table.rows.len() as u64;
    let mut writes = Vec::with_capacity(BATCH_SIZE);
    let mut positions = Vec::new();
    let mut parsed = 0u64;
    for (i, row) in table.rows.iter().enumerate() {
        let Some(object) = object_document(row) else {
            report.rejected += 1;
            continue;
        };
        parsed += 1;
        if rematch(row) {
            positions.push((object.ra, object.dec));
        }
        writes.push(WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(namespace.clone())
                .filter(doc! { "_id": object.objid })
                .update(doc! { "$set": object.document })
                .upsert(true)
                .build(),
        ));
        if writes.len() >= BATCH_SIZE {
            write(ctx, std::mem::take(&mut writes), report).await?;
            ctx.progress(i as u64 + 1, total, file.name()).await;
            if ctx.is_canceled() {
                return Err(TaskError::Canceled);
            }
        }
    }
    write(ctx, writes, report).await?;
    if parsed == 0 && total > 0 {
        return Err(failed(format!(
            "none of the {total} rows of {} could be read",
            file.name()
        )));
    }
    report.files += 1;
    report.unreadable += table.unreadable as u64;
    rematch_aux(ctx, &positions, report).await
}

async fn write(
    ctx: &TaskContext,
    writes: Vec<WriteModel>,
    report: &mut Report,
) -> Result<(), TaskError> {
    if writes.is_empty() {
        return Ok(());
    }
    let result = ctx
        .db()
        .client()
        .bulk_write(writes)
        .ordered(false)
        .await
        .map_err(failed)?;
    report.upserted += result.upserted_count as u64;
    report.modified += result.modified_count as u64;
    Ok(())
}

fn position(record: &Document) -> Option<(f64, f64)> {
    let coordinates: Coordinates =
        from_document(record.get_document("coordinates").ok()?.clone()).ok()?;
    Some(coordinates.get_radec())
}

async fn rematch_aux(
    ctx: &TaskContext,
    positions: &[(f64, f64)],
    report: &mut Report,
) -> Result<(), TaskError> {
    if positions.is_empty() {
        return Ok(());
    }
    let db = ctx.db();
    for (survey, catalogs) in &ctx.config().crossmatch {
        let Some(tns) = catalogs
            .iter()
            .find(|c| c.collection_name() == TNS_COLLECTION)
        else {
            continue;
        };
        let aux = db.collection::<Document>(&format!("{survey}_alerts_aux"));
        let field = format!("cross_matches.{}", tns.catalog);
        for &(ra, dec) in positions {
            let mut records = aux
                .find(doc! {
                    "coordinates.radec_geojson": {
                        "$geoWithin": { "$centerSphere": [[ra - 180.0, dec], tns.radius] }
                    },
                    "cross_matches": { "$type": "object" },
                })
                .projection(doc! { "coordinates": 1 })
                .await
                .map_err(failed)?;
            while let Some(record) = records.try_next().await.map_err(failed)? {
                let (Ok(object_id), Some((ra, dec))) = (record.get_str("_id"), position(&record))
                else {
                    continue;
                };
                let matches = xmatch(ra, dec, object_id, survey, std::slice::from_ref(tns), db)
                    .await
                    .map_err(failed)?
                    .remove(&tns.catalog)
                    .unwrap_or_default();
                aux.update_one(
                    doc! { "_id": object_id },
                    doc! { "$set": { &field: matches } },
                )
                .await
                .map_err(failed)?;
                *report.rematched.entry(survey.clone()).or_default() += 1;
            }
        }
    }
    Ok(())
}

async fn record(ctx: &TaskContext, report: &Report) {
    let database = ctx.db().name().to_string();
    if report.upserted + report.modified > 0 {
        ctx.record_mutation(
            MutationTarget {
                database: database.clone(),
                collection: TNS_COLLECTION.to_string(),
                catalog: None,
                survey: None,
            },
            Operation::Ingest,
            doc! {
                "files": report.files as i64,
                "upserted": report.upserted as i64,
                "modified": report.modified as i64,
            },
        )
        .await;
    }
    for (survey, updated) in &report.rematched {
        ctx.record_mutation(
            MutationTarget {
                database: database.clone(),
                collection: format!("{survey}_alerts_aux"),
                catalog: None,
                survey: Some(survey.to_string().to_lowercase()),
            },
            Operation::Recompute,
            doc! { "field": "cross_matches.TNS", "updated": *updated as i64 },
        )
        .await;
    }
}

async fn initialize_indexes(ctx: &TaskContext) {
    let tns = ctx.db().collection::<Document>(TNS_COLLECTION);
    for keys in [
        doc! { "coordinates.radec_geojson": "2dsphere" },
        doc! { "discovery_jd": -1 },
    ] {
        if let Err(error) = tns
            .create_index(IndexModel::builder().keys(keys).build())
            .await
        {
            ctx.warn(format!("could not index {TNS_COLLECTION}: {error}"));
        }
    }
}

async fn read_state(db: &Database) -> Result<Option<SyncState>, mongodb::error::Error> {
    let state = db
        .collection::<Document>(STATE_COLLECTION)
        .find_one(doc! { "_id": TNS_COLLECTION })
        .await?;
    Ok(state.and_then(|s| {
        Some(SyncState {
            synced_through: s.get_i64("synced_through").ok()?,
            full_at: s.get_i64("full_at").ok()?,
        })
    }))
}

async fn write_state(db: &Database, state: SyncState) -> Result<(), TaskError> {
    db.collection::<Document>(STATE_COLLECTION)
        .update_one(
            doc! { "_id": TNS_COLLECTION },
            doc! { "$set": {
                "synced_through": state.synced_through,
                "full_at": state.full_at,
            } },
        )
        .upsert(true)
        .await
        .map_err(failed)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_full_file_is_needed_once_the_hourly_ones_cannot_cover_the_gap() {
        let now = 1_769_382_000;
        let fresh = SyncState {
            synced_through: now - 2 * HOUR,
            full_at: now - 10 * HOUR,
        };
        assert!(!needs_full(false, fresh, now, 24));
        assert!(needs_full(true, fresh, now, 24));
        assert!(needs_full(false, fresh, now, 6));
        let stale = SyncState {
            synced_through: now - HOURLY_FILES_SPAN,
            full_at: now - 10 * HOUR,
        };
        assert!(needs_full(false, stale, now, 24));
    }
}
