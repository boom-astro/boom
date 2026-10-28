//! The `stream_kowalski_alerts` task: back-fill BOOM from a Kowalski deployment.
//!
//! Opens one cursor over Kowalski's `ZTF_alerts` and streams every document
//! through a pool of workers. Each worker takes a batch, drops alerts whose
//! objectId BOOM does not already know, bulk-inserts the rest, then fetches
//! cutouts from Kowalski only for the candids that were actually new.
//!
//! Cutouts are excluded from the streaming projection on purpose -- they are
//! large, and only a subset of alerts turn out to be new.
//!
//! **Idempotent.** Inserts are unordered and duplicate-key errors are skipped,
//! so re-running from the beginning is safe and is how an interrupted run
//! resumes.
//!
//! The endpoints are connection URIs, which carry passwords; parameters are
//! redacted everywhere they are read back. See `tasks::redact`.

use super::batch::PROGRESS_EVERY;
use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use crate::utils::cutouts::AlertCutout;
use crate::{
    alert::{deserialize_candidate, deserialize_cutout_as_bytes, ZtfAlert, ZtfCandidate},
    utils::spatial::Coordinates,
};
use anyhow::{Context as _, Result};
use futures::StreamExt;
use mongodb::{
    bson::{doc, Bson},
    options::InsertManyOptions,
    Client, Collection,
};
use redis::AsyncCommands;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tracing::{debug, info, warn};
use utoipa::ToSchema;

/// Stable identifier for this task type.
pub const TASK_TYPE: &str = "stream_kowalski_alerts";

/// What a client may ask for.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct StreamKowalskiParams {
    /// Kowalski MongoDB URI.
    pub kowalski_uri: String,
    /// Enrich what was imported, by pushing the new candids onto this survey's
    /// enrichment queue. Off by default, so an import does not start GPU work
    /// nobody asked for.
    ///
    /// Only the choice is a parameter. Where the queue and the database are is
    /// this deployment's own configuration: the task backfills into the system
    /// it is running in, so taking its addresses from the submitter would let a
    /// typo write into another deployment, and would put that deployment's
    /// credentials into a stored run.
    #[serde(default)]
    pub enrich: bool,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_n_workers")]
    pub n_workers: usize,
    /// Only stream alerts at or before this Julian date, for importing a fixed
    /// window rather than everything.
    #[serde(default)]
    pub max_jd: Option<f64>,
}

fn default_batch_size() -> usize {
    1_000
}

fn default_n_workers() -> usize {
    4
}

/// Where imported candids go when `enrich` is set. The pipeline's own queue
/// for this survey, so a reprocess and a live alert land in the same place.
const ENRICHMENT_QUEUE: &str = "ZTF_alerts_enrichment_queue";

const MAX_BATCH_SIZE: usize = 100_000;
const MAX_WORKERS: usize = 64;

impl StreamKowalskiParams {
    pub fn validate_params(&self) -> Result<(), String> {
        // The only address a submitter supplies: Kowalski is the one database
        // this deployment's config says nothing about.
        if !(self.kowalski_uri.starts_with("mongodb://")
            || self.kowalski_uri.starts_with("mongodb+srv://"))
        {
            return Err("kowalski_uri is not a valid MongoDB connection URI".to_string());
        }
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        if self.n_workers == 0 || self.n_workers > MAX_WORKERS {
            return Err(format!("n_workers must be between 1 and {MAX_WORKERS}"));
        }
        Ok(())
    }
}

/// Stream the alerts.
pub async fn run(
    ctx: &TaskContext,
    params: StreamKowalskiParams,
) -> Result<serde_json::Value, super::TaskError> {
    let failed = super::TaskError::Failed;

    let kowalski_client = Client::with_uri_str(&params.kowalski_uri)
        .await
        .map_err(|e| super::TaskError::InvalidParams(format!("invalid Kowalski URI: {e}")))?;
    let kowalski_coll: Collection<KowalskiZtfAlert> = kowalski_client
        .database("kowalski")
        .collection("ZTF_alerts");

    let estimated_total = kowalski_coll
        .estimated_document_count()
        .await
        .unwrap_or_else(|e| {
            warn!("estimated_document_count failed: {}", e);
            0
        });

    ctx.info(format!(
        "~{estimated_total} Kowalski alert(s) to stream ({} worker(s), batch_size {})",
        params.n_workers, params.batch_size
    ));

    // Channel capacity: enough for each worker to always have a full batch queued.
    let (sender, receiver) =
        async_channel::bounded::<KowalskiZtfAlert>(params.n_workers * params.batch_size * 2);

    let total_imported = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::with_capacity(params.n_workers);
    for _ in 0..params.n_workers {
        let rx = receiver.clone();
        let kowalski_uri = params.kowalski_uri.clone();
        let batch_size = params.batch_size;
        let enrich = params.enrich;
        let counter = Arc::clone(&total_imported);
        // Cloned rather than borrowed because these are spawned, the same way
        // the queue monitor below takes its own handle.
        let worker_ctx = ctx.clone();
        handles.push(tokio::spawn(async move {
            worker(rx, kowalski_uri, worker_ctx, enrich, batch_size, counter).await
        }));
    }

    drop(receiver); // main never reads; workers hold the live clones

    // The queue depth says which half is the bottleneck -- full means the
    // workers are, empty means the Kowalski cursor is -- which is the first
    // thing anyone watching a slow run wants to know.
    let channel_capacity = params.n_workers * params.batch_size * 2;
    let monitor_sender = sender.clone();
    let monitor_ctx = ctx.clone();
    let monitor_counter = Arc::clone(&total_imported);
    let queue_monitor = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let imported = monitor_counter.load(Ordering::Relaxed);
            monitor_ctx.info(format!(
                "{imported} imported; queue {}/{channel_capacity}",
                monitor_sender.len()
            ));
        }
    });

    let filter = match params.max_jd {
        Some(max_jd) => doc! { "candidate.jd": { "$lte": Bson::Double(max_jd) } },
        None => doc! {},
    };

    // Exclude cutouts from the streaming cursor — they are large and we only
    // need them for the subset of candids that are actually new. Workers fetch
    // cutouts separately after determining which alerts were inserted.
    let projection = doc! {
        "classifications": 0,
        "publisher": 0,
        "schemavsn": 0,
        "coordinates": 0,
        "cutoutScience": 0,
        "cutoutTemplate": 0,
        "cutoutDifference": 0,
    };

    let mut cursor = kowalski_coll
        .find(filter)
        .projection(projection)
        .batch_size(1000)
        .no_cursor_timeout(true)
        .await
        .map_err(|e| failed(format!("failed to open the Kowalski cursor: {e}")))?;

    let mut n_read: u64 = 0;

    let mut last_reported: u64 = 0;
    let mut cursor_error: Option<String> = None;

    while let Some(result) = cursor.next().await {
        // Cancelling in the reader: the workers drain what is queued and finish
        // their batches, so a cancelled run leaves whole alerts imported. The
        // import is idempotent, so resuming means re-running from the start and
        // skipping what is already there.
        if ctx.is_canceled() {
            ctx.warn(format!("canceled after reading {n_read} alert(s)"));
            drop(sender);
            for handle in handles {
                let _ = handle.await;
            }
            queue_monitor.abort();
            return Err(super::TaskError::Canceled);
        }

        match result {
            Ok(alert) => {
                n_read += 1;
                if sender.send(alert).await.is_err() {
                    return Err(failed(
                        "every worker exited; nothing is consuming the stream".to_string(),
                    ));
                }
                if n_read - last_reported >= PROGRESS_EVERY {
                    last_reported = n_read;
                    let imported = total_imported.load(Ordering::Relaxed);
                    ctx.progress(
                        imported,
                        estimated_total.max(n_read),
                        format!("{n_read} read, {imported} imported"),
                    )
                    .await;
                }
            }
            Err(e) => {
                // Stop feeding, but still drain what is queued: the alerts
                // already read are worth keeping, and the run reports the
                // failure rather than a short but clean-looking finish.
                cursor_error = Some(format!("Kowalski cursor error after {n_read} alerts: {e}"));
                break;
            }
        }
    }

    ctx.info(format!("cursor done: {n_read} alert(s) read"));
    queue_monitor.abort();
    drop(sender); // workers drain remaining channel items then exit

    // Every handle is awaited even after one fails, so no worker is left
    // inserting into BOOM after the run has reported its outcome.
    let mut worker_error: Option<String> = None;
    for handle in handles {
        match handle.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => worker_error = worker_error.or(Some(e.to_string())),
            Err(e) => worker_error = worker_error.or(Some(format!("worker panicked: {e}"))),
        }
    }

    let imported = total_imported.load(Ordering::Relaxed);

    // A cursor or worker failure after partial progress is still a failure: the
    // import is resumable, so the honest outcome is what lets someone decide to
    // re-run rather than assume the backfill is complete.
    if let Some(e) = cursor_error.or(worker_error) {
        ctx.error(format!("{imported} alert(s) imported before failing"));
        return Err(failed(e));
    }

    ctx.info(format!(
        "finished: {n_read} Kowalski alert(s) read, {imported} imported into BOOM"
    ));

    ctx.record_mutation(
        MutationTarget {
            database: ctx.db().name().to_string(),
            collection: "ZTF_alerts".to_string(),
            catalog: None,
            survey: Some("ztf".to_string()),
        },
        // Alerts written from an external deployment.
        Operation::Ingest,
        doc! {
            // Redacted on the way in; see tasks::redact. Only Kowalski's, now
            // that the destination is this deployment rather than a parameter.
            "kowalski_uri": &params.kowalski_uri,
            "enriched": params.enrich,
            "alerts_read": n_read as i64,
            "alerts_imported": imported as i64,
            "max_jd": params.max_jd,
            "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                .unwrap_or(Bson::Null),
        },
    )
    .await;

    Ok(serde_json::json!({
        "collection": "ZTF_alerts",
        "alerts_read": n_read,
        "alerts_imported": imported,
    }))
}

/// Alert document streamed from Kowalski — cutouts are excluded from the
/// projection so they are never transferred over the wire at this stage.
#[derive(Debug, Deserialize)]
struct KowalskiZtfAlert {
    candid: i64,
    #[serde(rename = "objectId")]
    object_id: String,
    #[serde(deserialize_with = "deserialize_candidate")]
    candidate: ZtfCandidate,
}

/// Cutout-only document fetched from Kowalski by candid after alert insertion.
#[derive(Debug, Deserialize)]
struct KowalskiZtfCutout {
    candid: i64,
    #[serde(
        rename = "cutoutScience",
        deserialize_with = "deserialize_cutout_as_bytes"
    )]
    cutout_science: Vec<u8>,
    #[serde(
        rename = "cutoutTemplate",
        deserialize_with = "deserialize_cutout_as_bytes"
    )]
    cutout_template: Vec<u8>,
    #[serde(
        rename = "cutoutDifference",
        deserialize_with = "deserialize_cutout_as_bytes"
    )]
    cutout_difference: Vec<u8>,
}

// For projecting _id-only from ZTF_alerts_aux (_id == objectId string).
#[derive(Deserialize)]
struct ObjectIdOnly {
    #[serde(rename = "_id")]
    object_id: String,
}

// ── helpers ───────────────────────────────────────────────────────────────────

// Blocks on the first item, then non-blocking drains up to batch_size total.
// Returns empty only when the channel is closed and empty.
async fn collect_batch(
    rx: &async_channel::Receiver<KowalskiZtfAlert>,
    batch_size: usize,
) -> Vec<KowalskiZtfAlert> {
    let mut batch = Vec::with_capacity(batch_size);
    match rx.recv().await {
        Ok(a) => batch.push(a),
        Err(_) => return batch,
    }
    while batch.len() < batch_size {
        match rx.try_recv() {
            Ok(a) => batch.push(a),
            Err(_) => break,
        }
    }
    batch
}

// Returns the set of objectIds present in ZTF_alerts_aux (_id field).
async fn query_known_obj_ids(
    coll: &Collection<ObjectIdOnly>,
    obj_ids: &[&str],
) -> Result<HashSet<String>> {
    let t = Instant::now();
    let mut cursor = coll
        .find(doc! { "_id": { "$in": obj_ids } })
        .projection(doc! { "_id": 1 })
        .no_cursor_timeout(true)
        .await
        .context("ZTF_alerts_aux query failed")?;
    debug!(
        n = obj_ids.len(),
        elapsed_ms = t.elapsed().as_millis(),
        "aux cursor open"
    );

    let t = Instant::now();
    let mut known = HashSet::with_capacity(obj_ids.len());
    while let Some(result) = cursor.next().await {
        known.insert(
            result
                .context("cursor error in ZTF_alerts_aux query")?
                .object_id,
        );
    }
    debug!(
        n = obj_ids.len(),
        found = known.len(),
        elapsed_ms = t.elapsed().as_millis(),
        "aux cursor drain"
    );
    Ok(known)
}

fn into_boom(alerts: Vec<KowalskiZtfAlert>) -> Vec<ZtfAlert> {
    let now = flare::Time::now().to_jd();
    alerts
        .into_iter()
        .map(|a| {
            let ra = a.candidate.candidate.ra;
            let dec = a.candidate.candidate.dec;
            ZtfAlert {
                candid: a.candid,
                object_id: a.object_id,
                candidate: a.candidate,
                coordinates: Coordinates::new(ra, dec),
                created_at: now,
                updated_at: now,
            }
        })
        .collect()
}

/// Fetch cutouts from Kowalski for a specific set of candids.
async fn fetch_cutouts_from_kowalski(
    coll: &Collection<KowalskiZtfCutout>,
    candids: &[i64],
) -> Result<Vec<AlertCutout>> {
    if candids.is_empty() {
        return Ok(vec![]);
    }
    let t = Instant::now();
    let mut cursor = coll
        .find(doc! { "candid": { "$in": candids } })
        .projection(
            doc! { "candid": 1, "cutoutScience": 1, "cutoutTemplate": 1, "cutoutDifference": 1 },
        )
        .await
        .context("Kowalski cutout query failed")?;
    let mut cutouts = Vec::with_capacity(candids.len());
    while let Some(result) = cursor.next().await {
        let kc = result.context("cursor error fetching Kowalski cutouts")?;
        cutouts.push(AlertCutout {
            candid: kc.candid,
            cutout_science: kc.cutout_science,
            cutout_template: kc.cutout_template,
            cutout_difference: kc.cutout_difference,
        });
    }
    debug!(
        requested = candids.len(),
        fetched = cutouts.len(),
        elapsed_ms = t.elapsed().as_millis(),
        "cutout fetch from Kowalski"
    );
    Ok(cutouts)
}

// insert_many with ordered=false; returns batch indices of E11000 duplicates.
async fn insert_many_skip_dups<T>(
    coll: &Collection<T>,
    docs: Vec<T>,
    opts: &InsertManyOptions,
    label: &str,
) -> Result<Vec<usize>>
where
    T: serde::Serialize + Send + Sync,
{
    if docs.is_empty() {
        return Ok(vec![]);
    }
    match coll.insert_many(docs).with_options(opts.clone()).await {
        Ok(_) => Ok(vec![]),
        Err(e) => {
            if let mongodb::error::ErrorKind::InsertMany(ref ime) = *e.kind {
                if let Some(ref write_errors) = ime.write_errors {
                    let mut dups = Vec::new();
                    for we in write_errors {
                        if we.code == 11000 {
                            dups.push(we.index);
                        } else {
                            return Err(anyhow::anyhow!(
                                "non-duplicate write error inserting {}: {:?}",
                                label,
                                we
                            ));
                        }
                    }
                    return Ok(dups);
                }
            }
            Err(e).context(format!("error inserting {label}"))
        }
    }
}

// ── worker ────────────────────────────────────────────────────────────────────

#[tracing::instrument(skip_all, err)]
// Ten arguments, all of them endpoints and knobs the caller genuinely varies.
// Bundling them into a struct would move the same list rather than shorten it.
#[allow(clippy::too_many_arguments)]
async fn worker(
    rx: async_channel::Receiver<KowalskiZtfAlert>,
    kowalski_uri: String,
    ctx: TaskContext,
    enrich: bool,
    batch_size: usize,
    total_imported: Arc<AtomicU64>,
) -> Result<u64> {
    let kowalski_client = Client::with_uri_str(&kowalski_uri)
        .await
        .context("failed to connect to Kowalski MongoDB")?;
    let kowalski_cutout_coll: Collection<KowalskiZtfCutout> = kowalski_client
        .database("kowalski")
        .collection("ZTF_alerts");

    // The destination is the system this task is running in, so its handle
    // comes from the context. Reusing that client also means the workers share
    // one connection pool instead of opening a second per worker.
    let db = ctx.db().clone();

    let aux_coll: Collection<ObjectIdOnly> = db.collection("ZTF_alerts_aux");
    let alerts_coll: Collection<ZtfAlert> = db.collection("ZTF_alerts");
    let cutouts_coll: Collection<AlertCutout> = db.collection("ZTF_alerts_cutouts");

    let mut redis_conn = if enrich {
        Some(
            ctx.config()
                .build_redis()
                .await
                .context("failed to connect to this deployment's Redis")?,
        )
    } else {
        None
    };

    let insert_opts = InsertManyOptions::builder().ordered(false).build();
    let mut processed: u64 = 0;

    loop {
        let t_wait = Instant::now();
        let batch = collect_batch(&rx, batch_size).await;
        if batch.is_empty() {
            break;
        }

        let batch_len = batch.len() as u64;
        debug!(
            batch = batch_len,
            elapsed_ms = t_wait.elapsed().as_millis(),
            "channel wait"
        );
        let t_batch = Instant::now();

        // Deduplicate objectIds before querying aux.
        let unique_obj_ids: Vec<&str> = {
            let mut seen = HashSet::with_capacity(batch.len());
            batch
                .iter()
                .filter_map(|a| {
                    if seen.insert(a.object_id.as_str()) {
                        Some(a.object_id.as_str())
                    } else {
                        None
                    }
                })
                .collect()
        };

        let known = query_known_obj_ids(&aux_coll, &unique_obj_ids).await?;

        let to_import: Vec<KowalskiZtfAlert> = batch
            .into_iter()
            .filter(|a| known.contains(&a.object_id))
            .collect();

        if to_import.is_empty() {
            debug!(batch = batch_len, "batch: no matching objects in BOOM");
            processed += batch_len;
            continue;
        }

        let n = to_import.len();
        let ztf_alerts = into_boom(to_import);
        let candids: Vec<i64> = ztf_alerts.iter().map(|a| a.candid).collect();

        // Insert alerts first to learn which candids are actually new.
        let t = Instant::now();
        let dup_indices =
            insert_many_skip_dups(&alerts_coll, ztf_alerts, &insert_opts, "alerts").await?;
        let inserted = n - dup_indices.len();
        debug!(
            count = n,
            dups = dup_indices.len(),
            inserted,
            elapsed_ms = t.elapsed().as_millis(),
            "alert insert"
        );

        // Determine exactly which candids were new so we only fetch those cutouts.
        let dup_set: HashSet<usize> = dup_indices.into_iter().collect();
        let inserted_candids: Vec<i64> = candids
            .iter()
            .enumerate()
            .filter(|(i, _)| !dup_set.contains(i))
            .map(|(_, c)| *c)
            .collect();

        if !inserted_candids.is_empty() {
            debug!(
                requested = candids.len(),
                inserted = inserted_candids.len(),
                elapsed_ms = t.elapsed().as_millis(),
                "new alerts to fetch cutouts for"
            );
            // Fetch cutouts from Kowalski only for the newly inserted candids.
            let cutouts =
                fetch_cutouts_from_kowalski(&kowalski_cutout_coll, &inserted_candids).await?;

            // Build set of candids that actually came back so we can filter the
            // enrichment queue below. Enqueueing a candid with no cutout would
            // produce a guaranteed MissingCutouts error in the enrichment worker.
            let fetched_set: HashSet<i64> = cutouts.iter().map(|c| c.candid).collect();
            if fetched_set.len() != inserted_candids.len() {
                warn!(
                    inserted = inserted_candids.len(),
                    fetched = fetched_set.len(),
                    missing = inserted_candids.len() - fetched_set.len(),
                    "cutouts missing for some newly inserted alerts; those candids will not be enqueued"
                );
            }

            let t = Instant::now();
            insert_many_skip_dups(&cutouts_coll, cutouts, &insert_opts, "cutouts").await?;
            info!(
                count = inserted,
                elapsed_ms = t.elapsed().as_millis(),
                "cutout insert"
            );

            if let Some(conn) = redis_conn.as_mut() {
                let queue = ENRICHMENT_QUEUE;
                let to_enqueue: Vec<i64> = inserted_candids
                    .into_iter()
                    .filter(|c| fetched_set.contains(c))
                    .collect();
                let t = Instant::now();
                let enqueued = to_enqueue.len();
                if enqueued == 0 {
                    debug!("Redis push skipped: no newly inserted candids had fetched cutouts");
                } else {
                    conn.lpush::<&str, Vec<i64>, usize>(queue, to_enqueue)
                        .await
                        .context("Redis lpush failed")?;
                    debug!(
                        count = inserted,
                        elapsed_ms = t.elapsed().as_millis(),
                        "Redis push"
                    );
                }
            }
        }

        total_imported.fetch_add(inserted as u64, Ordering::Relaxed);
        processed += batch_len;

        debug!(
            batch = batch_len,
            to_import = n,
            inserted,
            elapsed_ms = t_batch.elapsed().as_millis(),
            "batch complete"
        );
    }

    Ok(processed)
}

// ── main ──────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn params() -> StreamKowalskiParams {
        StreamKowalskiParams {
            kowalski_uri: "mongodb://kowalski/kowalski".into(),
            enrich: false,
            batch_size: default_batch_size(),
            n_workers: default_n_workers(),
            max_jd: None,
        }
    }

    #[test]
    fn the_minimum_submission_validates() {
        assert!(params().validate_params().is_ok());
    }

    #[test]
    fn the_destination_is_not_something_a_submitter_can_name() {
        // The task backfills into the system it runs in, so its database and
        // queue come from that deployment's config. A submission that names
        // them -- an old one, or an attempt to redirect the write -- is
        // accepted and those keys do nothing.
        let submitted = serde_json::json!({
            "kowalski_uri": "mongodb://kowalski/kowalski",
            "boom_uri": "mongodb://elsewhere/boom",
            "boom_db_name": "elsewhere",
            "redis_uri": "redis://elsewhere:6379",
            "enrichment_queue": "elsewhere",
        });
        let parsed: StreamKowalskiParams =
            serde_json::from_value(submitted).expect("still deserializes");
        assert!(parsed.validate_params().is_ok());
        // Nothing on the parsed params can carry those addresses any more; the
        // only thing left to ask for is whether to enrich, which defaults off.
        assert!(!parsed.enrich);
    }

    #[test]
    fn kowalski_uri_must_be_a_mongodb_uri() {
        // The one address a submitter does supply, since nothing in this
        // deployment's config knows where Kowalski is.
        let mut p = params();
        p.kowalski_uri = "redis://kowalski".into();
        assert!(p.validate_params().is_err());
    }

    #[test]
    fn worker_and_batch_counts_are_bounded() {
        let mut p = params();
        p.n_workers = MAX_WORKERS + 1;
        assert!(p.validate_params().is_err());
        p.n_workers = default_n_workers();
        p.batch_size = 0;
        assert!(p.validate_params().is_err());
    }

    #[test]
    fn every_endpoint_is_redacted_on_readback() {
        // Four URIs, any of which may carry a password.
        let submitted = serde_json::json!({
            "kowalski_uri": "mongodb://k:kpass@kowalski/kowalski",
            "boom_uri": "mongodb://b:bpass@boom/boom",
            "boom_cutout_uri": "mongodb://c:cpass@cutouts/boom",
            "redis_uri": "redis://r:rpass@valkey:6379",
            "boom_db_name": "boom",
        });
        let rendered = crate::tasks::redact::redact_params(&submitted).to_string();
        for secret in ["kpass", "bpass", "cpass", "rpass"] {
            assert!(!rendered.contains(secret), "{secret} leaked: {rendered}");
        }
        assert!(rendered.contains("kowalski"), "the endpoint should survive");
    }

    #[test]
    fn the_task_is_registered_and_retryable() {
        // Inserts are unordered and duplicates are skipped, so re-running from
        // the beginning is how an interrupted import resumes.
        assert!(crate::tasks::is_retryable(TASK_TYPE));
    }

    #[test]
    fn single_flight_serializes_every_import() {
        // Two imports would duplicate the whole stream's work, and there is no
        // longer a destination to key on. Asserted for a real submission --
        // keying on `boom_uri` kept passing against a hand-built blob after
        // that parameter was gone, while actual submissions got no key at all.
        let real = serde_json::to_value(params()).expect("serializes");
        assert_eq!(
            crate::tasks::single_flight_key(TASK_TYPE, &real),
            Some(mongodb::bson::doc! {}),
            "a submission must contend with every other import"
        );
    }
}
