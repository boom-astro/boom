//! Move ZTF objects that are clearly bogus, with their alerts, out of the main collections.

use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use crate::utils::{
    db::{create_index, merge_filters, range_shards, shard_field, CURSOR_BATCH_SIZE},
    enums::Survey,
};
use futures::{StreamExt, TryStreamExt};
use mongodb::{
    bson::{doc, Bson, Document},
    Collection,
};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use utoipa::ToSchema;

pub const TASK_TYPE: &str = "move_ztf_bogus";

const ALERTS: &str = "ZTF_alerts";
const AUX: &str = "ZTF_alerts_aux";
const ALERTS_BOGUS: &str = "ZTF_alerts_bogus";
const AUX_BOGUS: &str = "ZTF_alerts_aux_bogus";

const KNOWN_SSO_RADIUS_ARCSEC: f64 = 5.0;
const MAX_ALLOWED_DRB: f64 = 0.1;
const MIN_ALLOWED_AGE_DAYS: f64 = 30.0;
const MAX_BATCH_SIZE: usize = 10_000;
const MAX_PROCESSES: usize = 64;
const PROGRESS_EVERY: u64 = 50_000;

fn default_max_drb() -> f64 {
    0.01
}

fn default_min_age_days() -> f64 {
    MIN_ALLOWED_AGE_DAYS
}

fn default_batch_size() -> usize {
    1_000
}

fn default_processes() -> usize {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MoveZtfBogusParams {
    /// An object moves only if every one of its detections has a drb below this (at most 0.1).
    #[serde(default = "default_max_drb")]
    pub max_drb: f64,
    /// An object moves only if its last detection is at least this many days old (at least 30).
    #[serde(default = "default_min_age_days")]
    pub min_age_days: f64,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_processes")]
    pub processes: usize,
    /// Count what would move without writing anything.
    #[serde(default)]
    pub dry_run: bool,
}

impl MoveZtfBogusParams {
    pub fn validate_params(&self) -> Result<(), String> {
        if self.max_drb.is_nan() || self.max_drb <= 0.0 || self.max_drb > MAX_ALLOWED_DRB {
            return Err(format!(
                "max_drb must be above 0 and at most {MAX_ALLOWED_DRB}"
            ));
        }
        if self.min_age_days.is_nan() || self.min_age_days < MIN_ALLOWED_AGE_DAYS {
            return Err(format!(
                "min_age_days must be at least {MIN_ALLOWED_AGE_DAYS}"
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

fn aux_filter(max_drb: f64, cutoff_jd: f64) -> Document {
    let mut filter = doc! {
        "prv_candidates.0": { "$exists": true },
        "$nor": [{ "prv_candidates": { "$elemMatch": { "$or": [
            { "drb": { "$not": { "$gte": 0.0, "$lt": max_drb } } },
            { "jd": { "$not": { "$lt": cutoff_jd } } },
            { "ssdistnr": { "$gte": 0.0, "$lte": KNOWN_SSO_RADIUS_ARCSEC } },
        ] } } }],
        "cross_matches.TNS.0": { "$exists": false },
    };
    for survey in <Survey as clap::ValueEnum>::value_variants() {
        if *survey != Survey::Ztf {
            filter.insert(format!("aliases.{survey}.0"), doc! { "$exists": false });
        }
    }
    filter
}

fn alert_blocks_move(max_drb: f64, cutoff_jd: f64) -> Document {
    doc! { "$or": [
        { "candidate.drb": { "$not": { "$gte": 0.0, "$lt": max_drb } } },
        { "candidate.jd": { "$not": { "$lt": cutoff_jd } } },
        { "candidate.ssdistnr": { "$gte": 0.0, "$lte": KNOWN_SSO_RADIUS_ARCSEC } },
    ] }
}

fn merge_into(collection: &str) -> Document {
    doc! { "$merge": {
        "into": collection,
        "on": "_id",
        "whenMatched": "replace",
        "whenNotMatched": "insert",
    } }
}

#[derive(Clone)]
struct Collections {
    alerts: Collection<Document>,
    aux: Collection<Document>,
    aux_bogus: Collection<Document>,
}

struct Selection {
    aux: Document,
    alert_blocks: Document,
}

#[derive(Default)]
struct Counters {
    selected: AtomicU64,
    blocked: AtomicU64,
    objects: AtomicU64,
    alerts: AtomicU64,
    last_reported: AtomicU64,
}

async fn move_batch(
    cols: &Collections,
    selection: &Selection,
    ids: Vec<Bson>,
    dry_run: bool,
    counters: &Counters,
) -> Result<(), mongodb::error::Error> {
    let selected = ids.len() as u64;
    let blocked: HashSet<String> = cols
        .alerts
        .distinct(
            "objectId",
            merge_filters(
                &doc! { "objectId": { "$in": &ids } },
                &selection.alert_blocks,
            ),
        )
        .await?
        .into_iter()
        .filter_map(|id| id.as_str().map(str::to_owned))
        .collect();
    let keep: Vec<Bson> = ids
        .into_iter()
        .filter(|id| id.as_str().is_none_or(|id| !blocked.contains(id)))
        .collect();
    counters.selected.fetch_add(selected, Ordering::Relaxed);
    counters
        .blocked
        .fetch_add(selected - keep.len() as u64, Ordering::Relaxed);
    if keep.is_empty() {
        return Ok(());
    }

    let alerts_selection = doc! {
        "objectId": { "$in": &keep },
        "$nor": [selection.alert_blocks.clone()],
    };
    if dry_run {
        let alerts = cols.alerts.count_documents(alerts_selection).await?;
        counters.alerts.fetch_add(alerts, Ordering::Relaxed);
        counters
            .objects
            .fetch_add(keep.len() as u64, Ordering::Relaxed);
        return Ok(());
    }

    cols.alerts
        .aggregate(vec![
            doc! { "$match": alerts_selection.clone() },
            merge_into(ALERTS_BOGUS),
        ])
        .await?
        .try_collect::<Vec<Document>>()
        .await?;
    let alerts = cols
        .alerts
        .delete_many(alerts_selection)
        .await?
        .deleted_count;

    let aux_selection = merge_filters(&doc! { "_id": { "$in": &keep } }, &selection.aux);
    cols.aux
        .aggregate(vec![
            doc! { "$match": aux_selection.clone() },
            merge_into(AUX_BOGUS),
        ])
        .await?
        .try_collect::<Vec<Document>>()
        .await?;
    let objects = cols.aux.delete_many(aux_selection).await?.deleted_count;

    if (objects as usize) < keep.len() {
        let stayed: Vec<Bson> = cols
            .aux
            .find(doc! { "_id": { "$in": &keep } })
            .projection(doc! { "_id": 1 })
            .await?
            .try_collect::<Vec<Document>>()
            .await?
            .into_iter()
            .filter_map(|d| d.get("_id").cloned())
            .collect();
        if !stayed.is_empty() {
            cols.aux_bogus
                .delete_many(doc! { "_id": { "$in": stayed } })
                .await?;
        }
    }

    counters.alerts.fetch_add(alerts, Ordering::Relaxed);
    counters.objects.fetch_add(objects, Ordering::Relaxed);
    Ok(())
}

async fn report_progress(ctx: &TaskContext, counters: &Counters, total: u64) {
    let selected = counters.selected.load(Ordering::Relaxed);
    if selected.saturating_sub(counters.last_reported.load(Ordering::Relaxed)) < PROGRESS_EVERY {
        return;
    }
    counters.last_reported.store(selected, Ordering::Relaxed);
    ctx.progress(
        selected,
        total.max(selected),
        format!(
            "{} object(s) and {} alert(s) moved",
            counters.objects.load(Ordering::Relaxed),
            counters.alerts.load(Ordering::Relaxed)
        ),
    )
    .await;
}

async fn move_shard(
    ctx: &TaskContext,
    cols: Collections,
    selection: &Selection,
    shard: Document,
    params: &MoveZtfBogusParams,
    counters: &Counters,
    total: u64,
) -> Result<(), super::TaskError> {
    let failed = |e: mongodb::error::Error| super::TaskError::Failed(e.to_string());
    let mut cursor = cols
        .aux
        .find(merge_filters(&selection.aux, &shard))
        .projection(doc! { "_id": 1 })
        .no_cursor_timeout(true)
        .batch_size(CURSOR_BATCH_SIZE)
        .await
        .map_err(failed)?;
    let mut batch: Vec<Bson> = Vec::with_capacity(params.batch_size);
    loop {
        let next = cursor.try_next().await.map_err(failed)?;
        if let Some(id) = next.as_ref().and_then(|d| d.get("_id")) {
            batch.push(id.clone());
        }
        if batch.len() < params.batch_size && next.is_some() {
            continue;
        }
        if ctx.is_canceled() {
            ctx.warn(
                "canceled; what was moved stays moved, and re-running picks up the objects \
                 still in the main collections",
            );
            return Err(super::TaskError::Canceled);
        }
        if !batch.is_empty() {
            move_batch(
                &cols,
                selection,
                std::mem::take(&mut batch),
                params.dry_run,
                counters,
            )
            .await
            .map_err(failed)?;
            report_progress(ctx, counters, total).await;
        }
        if next.is_none() {
            return Ok(());
        }
    }
}

pub async fn run(
    ctx: &TaskContext,
    params: MoveZtfBogusParams,
) -> Result<serde_json::Value, super::TaskError> {
    params
        .validate_params()
        .map_err(super::TaskError::InvalidParams)?;

    let db = ctx.db();
    let cols = Collections {
        alerts: db.collection(ALERTS),
        aux: db.collection(AUX),
        aux_bogus: db.collection(AUX_BOGUS),
    };
    if !params.dry_run {
        create_index(
            &db.collection::<Document>(ALERTS_BOGUS),
            doc! { "objectId": 1 },
            false,
        )
        .await
        .map_err(|e| super::TaskError::Failed(e.to_string()))?;
    }

    let cutoff_jd = flare::Time::now().to_jd() - params.min_age_days;
    let selection = Selection {
        aux: aux_filter(params.max_drb, cutoff_jd),
        alert_blocks: alert_blocks_move(params.max_drb, cutoff_jd),
    };
    let total = cols.aux.estimated_document_count().await.unwrap_or(0);
    let shard_key = shard_field(&cols.aux).await;
    let shards = range_shards(&cols.aux, params.processes, shard_key, &Document::new()).await;
    ctx.info(format!(
        "scanning ~{total} object(s) in {AUX} across {} shard(s) for drb < {} and no detection \
         after jd {cutoff_jd:.1}{}",
        shards.len(),
        params.max_drb,
        if params.dry_run {
            " (dry run, nothing will be written)"
        } else {
            ""
        }
    ));

    let counters = Counters::default();
    futures::stream::iter(shards)
        .map(|shard| {
            move_shard(
                ctx,
                cols.clone(),
                &selection,
                shard,
                &params,
                &counters,
                total,
            )
        })
        .buffer_unordered(params.processes)
        .try_collect::<Vec<()>>()
        .await?;

    let selected = counters.selected.load(Ordering::Relaxed);
    let blocked = counters.blocked.load(Ordering::Relaxed);
    let objects = counters.objects.load(Ordering::Relaxed);
    let alerts = counters.alerts.load(Ordering::Relaxed);
    ctx.info(format!(
        "{selected} object(s) selected, {blocked} kept because one of their alerts does not \
         qualify, {objects} object(s) and {alerts} alert(s) {}",
        if params.dry_run {
            "would move"
        } else {
            "moved"
        }
    ));

    if !params.dry_run {
        for (collection, moved_to, count) in
            [(ALERTS, ALERTS_BOGUS, alerts), (AUX, AUX_BOGUS, objects)]
        {
            if count == 0 {
                continue;
            }
            ctx.record_mutation(
                MutationTarget {
                    database: db.name().to_string(),
                    collection: collection.to_string(),
                    catalog: None,
                    survey: Some("ztf".to_string()),
                },
                Operation::Delete,
                doc! {
                    "moved_to": moved_to,
                    "documents": count as i64,
                    "max_drb": params.max_drb,
                    "cutoff_jd": cutoff_jd,
                },
            )
            .await;
        }
    }

    Ok(serde_json::json!({
        "selected": selected,
        "kept_by_alerts": blocked,
        "objects_moved": objects,
        "alerts_moved": alerts,
        "cutoff_jd": cutoff_jd,
        "dry_run": params.dry_run,
    }))
}
