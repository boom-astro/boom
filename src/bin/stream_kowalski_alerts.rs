//! Back-fill BOOM by streaming the entire Kowalski ZTF_alerts collection once.
//!
//! Unlike `import_kowalski_alerts`, which first builds a list of objectIds from BOOM
//! and then makes repeated round trips to Kowalski for alerts, this tool opens **one**
//! Kowalski cursor and reads every alert document from start to finish. Alert
//! streaming is sequential from that single cursor, but workers later issue
//! additional per-batch Kowalski queries to fetch cutouts for candids that were
//! newly inserted into BOOM.
//!
//! ## Architecture
//!
//!   Main thread  — streams Kowalski ZTF_alerts (without cutouts) into a bounded channel.
//!   N workers    — each worker pulls up to --batch-size alerts, checks which of
//!                  their objectIds exist in BOOM's ZTF_alerts_aux, drops alerts
//!                  for unknown objects, bulk-inserts the rest into ZTF_alerts,
//!                  then fetches cutouts from Kowalski only for the candids that
//!                  were actually new (not E11000 duplicates), and inserts those.
//!
//! ## Idempotency
//!
//! insert_many calls use ordered=false and silently skip E11000 (duplicate-key)
//! errors, so restarting the tool from the beginning is safe.
//!
//! # Examples
//!
//!   stream_kowalski_alerts \
//!     --kowalski-uri "mongodb://ro:pass@kowalski-host:27017/kowalski" \
//!     --boom-uri     "mongodb://rw:pass@boom-host:27017/boom"
//!
//!   # Cutouts on a separate host:
//!   stream_kowalski_alerts \
//!     --kowalski-uri        "mongodb://ro:pass@kowalski-host:27017/kowalski" \
//!     --boom-uri            "mongodb://rw:pass@boom-host:27017/boom" \
//!     --boom-cutout-uri     "mongodb://rw:pass@cutout-host:27017/boom" \
//!     --boom-cutout-db-name boom \
//!     --redis-uri           "redis://localhost:6379" \
//!     --n-workers           8

use anyhow::{Context, Result};
use boom::utils::cutouts::AlertCutout;
use boom::utils::data::make_progress_bar;
use boom::utils::parser::parse_positive_usize;
use boom::{
    alert::{deserialize_candidate, deserialize_cutout_as_bytes, ZtfAlert, ZtfCandidate},
    utils::o11y::logging::build_subscriber,
    utils::spatial::Coordinates,
};
use clap::Parser;
use futures::StreamExt;
use indicatif::ProgressBar;
use mongodb::{
    bson::{doc, Bson},
    options::InsertManyOptions,
    Client, Collection,
};
use redis::AsyncCommands;
use serde::Deserialize;
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tracing::{debug, error, info, warn};

/// Stream every Kowalski ZTF alert into BOOM, skipping objects not in ZTF_alerts_aux.
///
/// The tool opens a single Kowalski cursor and streams all alert documents
/// sequentially — there are no per-batch round trips to Kowalski.  Workers
/// check ZTF_alerts_aux to decide which alerts belong to objects already known
/// to BOOM, and bulk-insert only those.  Alerts for unknown objects are dropped.
///
/// The operation is idempotent: duplicates are silently skipped,
/// so restarting from the beginning is safe.
///
/// See module-level docs for full examples.
#[derive(Parser)]
#[command(verbatim_doc_comment)]
struct Cli {
    /// Kowalski MongoDB URI.  Format: mongodb://[user:pass@]host:port/database
    /// Read-only credentials are sufficient.
    /// Env: KOWALSKI_MONGODB_URI
    #[arg(long, env = "KOWALSKI_MONGODB_URI")]
    kowalski_uri: String,

    /// BOOM MongoDB URI (alerts written here; cutouts too unless --boom-cutout-uri is set).
    /// Env: BOOM_MONGODB_URI
    #[arg(long, env = "BOOM_MONGODB_URI")]
    boom_uri: String,

    /// BOOM database name within --boom-uri.
    /// Env: BOOM_MONGODB_NAME
    #[arg(long, env = "BOOM_MONGODB_NAME", default_value = "boom")]
    boom_db_name: String,

    /// Optional separate MongoDB URI for ZTF_alerts_cutouts (e.g. a JBOD after migration).
    /// Must be paired with --boom-cutout-db-name.
    /// Env: BOOM_MONGODB_CUTOUT_URI
    #[arg(long, env = "BOOM_MONGODB_CUTOUT_URI")]
    boom_cutout_uri: Option<String>,

    /// Database name on --boom-cutout-uri containing ZTF_alerts_cutouts.
    /// Env: BOOM_MONGODB_CUTOUT_NAME
    #[arg(long, env = "BOOM_MONGODB_CUTOUT_NAME")]
    boom_cutout_db_name: Option<String>,

    /// Optional Redis URI for pushing newly inserted candids to an enrichment queue.
    /// Only candids actually written (not E11000 skips) are enqueued.
    /// Must be paired with --enrichment-queue.
    /// Env: REDIS_URI
    #[arg(long, env = "REDIS_URI")]
    redis_uri: Option<String>,

    /// Redis queue name to push newly inserted candids to.
    /// Must be paired with --redis-uri.
    /// Env: BOOM_ENRICHMENT_QUEUE
    #[arg(long)]
    enrichment_queue: Option<String>,

    /// Number of alerts collected per worker batch.
    /// Each batch issues one ZTF_alerts_aux lookup and one insert_many pair.
    /// Higher values amortise round-trip costs; lower values reduce peak memory.
    #[arg(long, default_value_t = 1000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Number of parallel worker tasks.
    /// Each worker holds its own MongoDB and Redis connections.
    #[arg(long, default_value_t = 4, value_parser = parse_positive_usize)]
    n_workers: usize,

    /// Maximum JD of candidates to stream, to avoid processing new alerts during a long run.
    /// Env: MAX_JD
    #[arg(long, env = "MAX_JD")]
    max_jd: Option<f64>,
}

// ── types ─────────────────────────────────────────────────────────────────────

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
async fn worker(
    rx: async_channel::Receiver<KowalskiZtfAlert>,
    kowalski_uri: String,
    boom_uri: String,
    boom_db_name: String,
    boom_cutout_uri: Option<String>,
    boom_cutout_db_name: Option<String>,
    redis_uri: Option<String>,
    enrichment_queue: Option<String>,
    batch_size: usize,
    total_imported: Arc<AtomicU64>,
    pb: ProgressBar,
) -> Result<u64> {
    let kowalski_client = Client::with_uri_str(&kowalski_uri)
        .await
        .context("failed to connect to Kowalski MongoDB")?;
    let kowalski_cutout_coll: Collection<KowalskiZtfCutout> = kowalski_client
        .database("kowalski")
        .collection("ZTF_alerts");

    let boom_client = Client::with_uri_str(&boom_uri)
        .await
        .context("failed to connect to BOOM MongoDB")?;
    let db = boom_client.database(&boom_db_name);

    let aux_coll: Collection<ObjectIdOnly> = db.collection("ZTF_alerts_aux");
    let alerts_coll: Collection<ZtfAlert> = db.collection("ZTF_alerts");
    let cutouts_coll: Collection<AlertCutout> = if let (Some(uri), Some(name)) =
        (boom_cutout_uri.as_deref(), boom_cutout_db_name.as_deref())
    {
        Client::with_uri_str(uri)
            .await
            .context("failed to connect to BOOM cutout MongoDB")?
            .database(name)
            .collection("ZTF_alerts_cutouts")
    } else {
        db.collection("ZTF_alerts_cutouts")
    };

    let mut redis_conn = match redis_uri {
        Some(ref uri) => {
            let client = redis::Client::open(uri.as_str()).context("invalid Redis URI")?;
            Some(
                client
                    .get_multiplexed_async_connection()
                    .await
                    .context("failed to connect to Redis")?,
            )
        }
        None => None,
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
            pb.inc(batch_len);
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

            if let (Some(ref mut conn), Some(ref queue)) =
                (redis_conn.as_mut(), enrichment_queue.as_ref())
            {
                let to_enqueue: Vec<i64> = inserted_candids
                    .into_iter()
                    .filter(|c| fetched_set.contains(c))
                    .collect();
                let t = Instant::now();
                conn.lpush::<&str, Vec<i64>, usize>(queue.as_str(), to_enqueue)
                    .await
                    .context("Redis lpush failed")?;
                debug!(
                    count = inserted,
                    elapsed_ms = t.elapsed().as_millis(),
                    "Redis push"
                );
            }
        }

        total_imported.fetch_add(inserted as u64, Ordering::Relaxed);
        pb.inc(batch_len);
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

#[tokio::main]
async fn main() {
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    let args = Cli::parse();

    if args.boom_cutout_uri.is_some() != args.boom_cutout_db_name.is_some() {
        eprintln!("error: --boom-cutout-uri and --boom-cutout-db-name must be provided together");
        std::process::exit(1);
    }

    if args.redis_uri.is_some() != args.enrichment_queue.is_some() {
        eprintln!("error: --redis-uri and --enrichment-queue must be provided together");
        std::process::exit(1);
    }

    let kowalski_client = Client::with_uri_str(&args.kowalski_uri)
        .await
        .unwrap_or_else(|e| {
            eprintln!("failed to connect to Kowalski: {}", e);
            std::process::exit(1);
        });
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

    info!(
        "~{} Kowalski alerts to stream ({} workers, batch_size {})",
        estimated_total, args.n_workers, args.batch_size
    );

    // Channel capacity: enough for each worker to always have a full batch queued.
    let (sender, receiver) =
        async_channel::bounded::<KowalskiZtfAlert>(args.n_workers * args.batch_size * 2);

    let total_imported = Arc::new(AtomicU64::new(0));
    let pb = make_progress_bar(estimated_total, "alerts streamed".to_string());

    let mut handles = Vec::with_capacity(args.n_workers);
    for _ in 0..args.n_workers {
        let rx = receiver.clone();
        let kowalski_uri = args.kowalski_uri.clone();
        let boom_uri = args.boom_uri.clone();
        let boom_db_name = args.boom_db_name.clone();
        let boom_cutout_uri = args.boom_cutout_uri.clone();
        let boom_cutout_db_name = args.boom_cutout_db_name.clone();
        let redis_uri = args.redis_uri.clone();
        let enrichment_queue = args.enrichment_queue.clone();
        let batch_size = args.batch_size;
        let counter = Arc::clone(&total_imported);
        let pb = pb.clone();
        handles.push(tokio::spawn(async move {
            worker(
                rx,
                kowalski_uri,
                boom_uri,
                boom_db_name,
                boom_cutout_uri,
                boom_cutout_db_name,
                redis_uri,
                enrichment_queue,
                batch_size,
                counter,
                pb,
            )
            .await
        }));
    }

    drop(receiver); // main never reads; workers hold the live clones

    let channel_capacity = args.n_workers * args.batch_size * 2;
    let monitor_sender = sender.clone();
    let queue_monitor = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let len = monitor_sender.len();
            info!(
                queue_len = len,
                queue_capacity = channel_capacity,
                "channel queue depth (full = workers are the bottleneck, empty = cursor is)"
            );
        }
    });

    let filter = match args.max_jd {
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
        .unwrap_or_else(|e| {
            error!("failed to open Kowalski cursor: {}", e);
            std::process::exit(1);
        });

    let mut n_read: u64 = 0;

    while let Some(result) = cursor.next().await {
        match result {
            Ok(alert) => {
                n_read += 1;
                if sender.send(alert).await.is_err() {
                    error!("all workers have exited, cannot send more alerts");
                    std::process::exit(1);
                }
            }
            Err(e) => {
                error!("Kowalski cursor error: {}", e);
                break;
            }
        }
    }

    info!("cursor done — read {} alerts", n_read);
    queue_monitor.abort();
    drop(sender); // workers drain remaining channel items then exit

    for handle in handles {
        match handle.await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => error!("worker error: {}", e),
            Err(e) => error!("worker join error: {}", e),
        }
    }

    pb.finish();

    info!(
        "finished — {} Kowalski alerts read, {} imported into BOOM",
        n_read,
        total_imported.load(Ordering::Relaxed)
    );
}
