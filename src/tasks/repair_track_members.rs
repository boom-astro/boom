use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use super::TaskError;
use crate::utils::discovery::refind;
use crate::utils::enums::Survey;
use crate::utils::tracks::{
    acquire_lock, alert_update, alerts_by_survey, locate_members, release_lock, renew_lock,
    AlertTrack, Member, StoredTrack, TracksLock, LOCK_RENEW_EVERY, SHARED_FOR_IDENTITY,
    TRACKS_COLLECTION,
};
use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateManyModel, UpdateOneModel, WriteModel};
use mongodb::Database;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Instant;
use utoipa::ToSchema;

pub const TASK_TYPE: &str = "repair_track_members";

const MAX_BATCH_SIZE: usize = 10_000;

fn default_batch_size() -> usize {
    1_000
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RepairTrackMembersParams {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default)]
    pub dry_run: bool,
}

impl RepairTrackMembersParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        Ok(())
    }
}

fn failed(e: impl std::fmt::Display) -> TaskError {
    TaskError::Failed(e.to_string())
}

struct Held<'a> {
    db: &'a Database,
    lock: &'a TracksLock,
    renewed: Instant,
}

impl Held<'_> {
    async fn keep(&mut self) -> Result<(), TaskError> {
        if self.renewed.elapsed() < LOCK_RENEW_EVERY {
            return Ok(());
        }
        if !renew_lock(self.db, self.lock).await.map_err(failed)? {
            return Err(TaskError::Failed("lost the tracks lock".to_string()));
        }
        self.renewed = Instant::now();
        Ok(())
    }
}

struct Located {
    id: String,
    members: Vec<Member>,
    updated_at: f64,
}

#[derive(Default)]
struct Report {
    tracks: u64,
    unreadable: u64,
    rewritten: u64,
    relocated: u64,
    stamped: u64,
    pairs: u64,
    refound: u64,
    absorbed: u64,
    skipped: u64,
    failed: u64,
}

fn canceled(ctx: &TaskContext) -> TaskError {
    ctx.warn("canceled; re-running resumes from what is left");
    TaskError::Canceled
}

async fn relocate(
    ctx: &TaskContext,
    params: &RepairTrackMembersParams,
    held: &mut Option<Held<'_>>,
    report: &mut Report,
) -> Result<Vec<Located>, TaskError> {
    let db = ctx.db();
    let tracks = db.collection::<Document>(TRACKS_COLLECTION);
    let namespace = tracks.namespace();
    let total = tracks.estimated_document_count().await.map_err(failed)?;
    let mut located = Vec::new();
    let mut last = String::new();
    loop {
        let docs = tracks
            .find(doc! { "_id": { "$gt": &last } })
            .sort(doc! { "_id": 1 })
            .limit(params.batch_size as i64)
            .await
            .map_err(failed)?
            .try_collect::<Vec<Document>>()
            .await
            .map_err(failed)?;
        let Some(end) = docs.last().and_then(|d| d.get_str("_id").ok()) else {
            break;
        };
        last = end.to_string();
        let read = docs.len();
        let batch: Vec<StoredTrack> = docs
            .into_iter()
            .filter_map(|d| mongodb::bson::from_document(d).ok())
            .collect();
        report.tracks += read as u64;
        report.unreadable += (read - batch.len()) as u64;
        let found = locate_members(db, &batch).await.map_err(failed)?;
        let (mut rewrites, mut stamps) = (Vec::new(), Vec::new());
        for (track, members) in batch.iter().zip(found) {
            let moved: Vec<Member> = members
                .iter()
                .zip(track.member_keys())
                .filter(|(new, old)| **new != *old)
                .map(|(new, _)| new.clone())
                .collect();
            if !track.member_surveys.is_empty() && moved.is_empty() {
                located.push(Located {
                    id: track.id.clone(),
                    members,
                    updated_at: track.updated_at,
                });
                continue;
            }
            report.rewritten += 1;
            report.relocated += moved.len() as u64;
            let (surveys, ids): (Vec<Survey>, Vec<i64>) = members.iter().cloned().unzip();
            let surveys = mongodb::bson::to_bson(&surveys).map_err(failed)?;
            rewrites.push(WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(namespace.clone())
                    .filter(doc! { "_id": &track.id, "members": track.members.clone() })
                    .update(doc! { "$set": { "members": ids, "member_surveys": surveys } })
                    .build(),
            ));
            for (survey, ids) in alerts_by_survey(&moved) {
                stamps.push(WriteModel::UpdateMany(
                    UpdateManyModel::builder()
                        .namespace(
                            db.collection::<Document>(&format!("{survey}_alerts"))
                                .namespace(),
                        )
                        .filter(doc! { "_id": { "$in": ids }, "track": { "$exists": false } })
                        .update(alert_update(&AlertTrack::from(track)))
                        .build(),
                ));
            }
            located.push(Located {
                id: track.id.clone(),
                members,
                updated_at: track.updated_at,
            });
        }
        if let Some(held) = held {
            held.keep().await?;
            if !rewrites.is_empty() {
                db.client()
                    .bulk_write(rewrites)
                    .ordered(false)
                    .await
                    .map_err(failed)?;
            }
            if !stamps.is_empty() {
                let result = db
                    .client()
                    .bulk_write(stamps)
                    .ordered(false)
                    .await
                    .map_err(failed)?;
                report.stamped += result.modified_count as u64;
            }
        }
        ctx.progress(
            report.tracks,
            total.max(report.tracks),
            format!("{} rewritten", report.rewritten),
        )
        .await;
        if ctx.is_canceled() {
            return Err(canceled(ctx));
        }
    }
    Ok(located)
}

fn duplicates(located: &[Located], report: &mut Report) -> BTreeSet<String> {
    let mut owners: HashMap<&Member, Vec<usize>> = HashMap::new();
    for (i, track) in located.iter().enumerate() {
        for member in track.members.iter().collect::<HashSet<_>>() {
            owners.entry(member).or_default().push(i);
        }
    }
    let mut newer = BTreeSet::new();
    for (i, track) in located.iter().enumerate() {
        let mut shared: HashMap<usize, usize> = HashMap::new();
        for member in track.members.iter().collect::<HashSet<_>>() {
            for &j in &owners[member] {
                if j > i {
                    *shared.entry(j).or_default() += 1;
                }
            }
        }
        for (j, n) in shared {
            if n < SHARED_FOR_IDENTITY {
                continue;
            }
            report.pairs += 1;
            let other = &located[j];
            let pick = if (other.updated_at, &other.id) > (track.updated_at, &track.id) {
                other
            } else {
                track
            };
            newer.insert(pick.id.clone());
        }
    }
    newer
}

async fn merge(
    ctx: &TaskContext,
    newer: BTreeSet<String>,
    held: &mut Held<'_>,
    report: &mut Report,
) -> Result<(), TaskError> {
    let db = ctx.db();
    let tracks = db.collection::<StoredTrack>(TRACKS_COLLECTION);
    let total = newer.len() as u64;
    for (done, id) in newer.into_iter().enumerate() {
        held.keep().await?;
        if ctx.is_canceled() {
            return Err(canceled(ctx));
        }
        let Some(track) = tracks.find_one(doc! { "_id": &id }).await.map_err(failed)? else {
            continue;
        };
        if track.epochs.len() != track.members.len() {
            report.skipped += 1;
            continue;
        }
        match refind(db, &track).await {
            Ok(up) => {
                report.refound += 1;
                report.absorbed += up.superseded.len() as u64;
            }
            Err(e) => {
                ctx.warn(format!("could not merge {id}: {e}"));
                report.failed += 1;
            }
        }
        ctx.progress(
            done as u64 + 1,
            total,
            format!("{} absorbed", report.absorbed),
        )
        .await;
    }
    Ok(())
}

async fn repair(
    ctx: &TaskContext,
    params: &RepairTrackMembersParams,
    lock: Option<&TracksLock>,
    report: &mut Report,
) -> Result<(), TaskError> {
    let mut held = lock.map(|lock| Held {
        db: ctx.db(),
        lock,
        renewed: Instant::now(),
    });
    let located = relocate(ctx, params, &mut held, report).await?;
    ctx.info(format!(
        "{} track(s) rewritten, {} member(s) moved to another survey",
        report.rewritten, report.relocated
    ));
    let newer = duplicates(&located, report);
    ctx.info(format!(
        "{} pair(s) of tracks share {SHARED_FOR_IDENTITY} or more detections, {} track(s) to merge",
        report.pairs,
        newer.len()
    ));
    if let Some(held) = &mut held {
        merge(ctx, newer, held, report).await?;
    }
    Ok(())
}

pub async fn run(
    ctx: &TaskContext,
    params: RepairTrackMembersParams,
) -> Result<serde_json::Value, TaskError> {
    let db = ctx.db();
    let lock = if params.dry_run {
        None
    } else {
        let lock = acquire_lock(db).await.map_err(failed)?;
        Some(lock.ok_or_else(|| {
            TaskError::Failed("another run holds the tracks lock, retry once it is done".into())
        })?)
    };
    let mut report = Report::default();
    let result = repair(ctx, &params, lock.as_ref(), &mut report).await;
    if let Some(lock) = &lock {
        if let Err(e) = release_lock(db, lock).await {
            ctx.warn(format!("could not release the tracks lock: {e}"));
        }
    }
    result?;

    if report.rewritten + report.refound > 0 && !params.dry_run {
        ctx.record_mutation(
            MutationTarget {
                database: db.name().to_string(),
                collection: TRACKS_COLLECTION.to_string(),
                catalog: None,
                survey: None,
            },
            Operation::Recompute,
            doc! {
                "fields": ["members", "member_surveys"],
                "rewritten": report.rewritten as i64,
                "merged": report.refound as i64,
                "absorbed": report.absorbed as i64,
            },
        )
        .await;
    }

    Ok(serde_json::json!({
        "dry_run": params.dry_run,
        "tracks": report.tracks,
        "unreadable": report.unreadable,
        "rewritten": report.rewritten,
        "relocated_members": report.relocated,
        "stamped_alerts": report.stamped,
        "duplicate_pairs": report.pairs,
        "merged": report.refound,
        "absorbed": report.absorbed,
        "skipped_without_epochs": report.skipped,
        "failed": report.failed,
    }))
}
