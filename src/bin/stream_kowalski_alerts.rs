//! Back-fill BOOM by streaming the entire Kowalski ZTF_alerts collection once.
//!
//! Unlike `import_kowalski_alerts`, which first builds a list of objectIds from BOOM
//! and then makes repeated round trips to Kowalski, this tool opens **one** Kowalski
//! cursor and reads every alert document from start to finish.  There are no per-batch
//! Kowalski queries — all Kowalski I/O is sequential reads from a single cursor.
//!
//! ## Architecture
//!
//!   Main thread  — streams Kowalski ZTF_alerts into a bounded channel.
//!   N workers    — each worker pulls up to --batch-size alerts, checks which of
//!                  their objectIds exist in BOOM's ZTF_alerts_aux, drops alerts
//!                  for unknown objects, and bulk-inserts the rest into
//!                  ZTF_alerts and ZTF_alerts_cutouts.
//!
//! ## Idempotency
//!
//! insert_many calls use ordered=false and silently skip E11000 (duplicate-key)
//! errors, so restarting the tool from the beginning is safe.
//!
//! ## Resuming an interrupted run
//!
//! When the run ends (either normally or on a cursor error) the last candid read
//! from Kowalski is printed.  Pass it as --min-candid on the next invocation to
//! skip already-processed documents.
//!
//! # Examples
//!
//!   stream_kowalski_alerts \
//!     --kowalski-uri "mongodb://ro:pass@kowalski-host:27017/kowalski" \
//!     --boom-uri     "mongodb://rw:pass@boom-host:27017/boom"
//!
//!   # Resume after interruption at candid 1234567890123456789:
//!   stream_kowalski_alerts \
//!     --kowalski-uri  "mongodb://ro:pass@kowalski-host:27017/kowalski" \
//!     --boom-uri      "mongodb://rw:pass@boom-host:27017/boom" \
//!     --min-candid    1234567890123456789
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
    time::Instant,
};
use tracing::{debug, error, info, warn};

/// Stream every Kowalski ZTF alert into BOOM, skipping objects not in ZTF_alerts_aux.
///
/// The tool opens a single Kowalski cursor and streams all alert documents
/// sequentially — there are no per-batch round trips to Kowalski.  Workers
/// check ZTF_alerts_aux to decide which alerts belong to objects already known
/// to BOOM, and bulk-insert only those.  Alerts for unknown objects are dropped.
///
/// The operation is idempotent: duplicates are silently skipped.  Pass
/// --min-candid to resume from a previous run's last-seen candid.
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

    /// Optional Redis URI for the ZTF enrichment queue (reprocessing).
    /// When set, newly inserted candids are lpush'd to ZTF_alerts_enrichment_queue_reprocess.
    /// Only candids actually written (not E11000 skips) are enqueued.
    /// Env: REDIS_URI
    #[arg(long, env = "REDIS_URI")]
    redis_uri: Option<String>,

    /// Number of alerts collected per worker batch.
    /// Each batch issues one ZTF_alerts_aux lookup and one insert_many pair.
    /// Higher values amortise round-trip costs; lower values reduce peak memory
    /// (cutout images are buffered in memory for the duration of a batch).
    #[arg(long, default_value_t = 1000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Number of parallel worker tasks.
    /// Each worker holds its own MongoDB and Redis connections.
    #[arg(long, default_value_t = 4, value_parser = parse_positive_usize)]
    n_workers: usize,

    /// Only process alerts with candid >= this value.
    /// Use the "last candid" value printed on a previous interrupted run to resume.
    #[arg(long)]
    min_candid: Option<i64>,
}

// ── types ─────────────────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
struct KowalskiZtfAlert {
    candid: i64,
    #[serde(rename = "objectId")]
    object_id: String,
    #[serde(deserialize_with = "deserialize_candidate")]
    candidate: ZtfCandidate,
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
async fn query_known_obj_ids(coll: &Collection<ObjectIdOnly>, obj_ids: &[&str]) -> HashSet<String> {
    let t = Instant::now();
    let mut cursor = match coll
        .find(doc! { "_id": { "$in": obj_ids } })
        .projection(doc! { "_id": 1 })
        .no_cursor_timeout(true)
        .await
    {
        Ok(c) => c,
        Err(e) => {
            error!("ZTF_alerts_aux query failed: {}", e);
            return HashSet::new();
        }
    };
    debug!(
        n = obj_ids.len(),
        elapsed_ms = t.elapsed().as_millis(),
        "aux cursor open"
    );

    let t = Instant::now();
    let mut known = HashSet::with_capacity(obj_ids.len());
    while let Some(result) = cursor.next().await {
        match result {
            Ok(doc) => {
                known.insert(doc.object_id);
            }
            Err(e) => warn!("cursor error in ZTF_alerts_aux query: {}", e),
        }
    }
    debug!(
        n = obj_ids.len(),
        found = known.len(),
        elapsed_ms = t.elapsed().as_millis(),
        "aux cursor drain"
    );
    known
}

fn into_boom(alerts: Vec<KowalskiZtfAlert>) -> (Vec<ZtfAlert>, Vec<AlertCutout>) {
    let now = flare::Time::now().to_jd();
    let n = alerts.len();
    let mut ztf_alerts = Vec::with_capacity(n);
    let mut cutouts = Vec::with_capacity(n);
    for a in alerts {
        let ra = a.candidate.candidate.ra;
        let dec = a.candidate.candidate.dec;
        ztf_alerts.push(ZtfAlert {
            candid: a.candid,
            object_id: a.object_id,
            candidate: a.candidate,
            coordinates: Coordinates::new(ra, dec),
            created_at: now,
            updated_at: now,
        });
        cutouts.push(AlertCutout {
            candid: a.candid,
            cutout_science: a.cutout_science,
            cutout_template: a.cutout_template,
            cutout_difference: a.cutout_difference,
        });
    }
    (ztf_alerts, cutouts)
}

// insert_many with ordered=false; returns batch indices of E11000 duplicates.
async fn insert_many_skip_dups<T>(
    coll: &Collection<T>,
    docs: Vec<T>,
    opts: &InsertManyOptions,
    label: &str,
) -> Vec<usize>
where
    T: serde::Serialize + Send + Sync,
{
    if docs.is_empty() {
        return vec![];
    }
    match coll.insert_many(docs).with_options(opts.clone()).await {
        Ok(_) => vec![],
        Err(e) => {
            if let mongodb::error::ErrorKind::InsertMany(ref ime) = *e.kind {
                if let Some(ref write_errors) = ime.write_errors {
                    let mut dups = Vec::new();
                    for we in write_errors {
                        if we.code == 11000 {
                            dups.push(we.index);
                        } else {
                            error!("non-duplicate write error inserting {}: {:?}", label, we);
                        }
                    }
                    return dups;
                }
            }
            error!("error inserting {}: {}", label, e);
            vec![]
        }
    }
}

// ── worker ────────────────────────────────────────────────────────────────────

async fn worker(
    rx: async_channel::Receiver<KowalskiZtfAlert>,
    boom_uri: String,
    boom_db_name: String,
    boom_cutout_uri: Option<String>,
    boom_cutout_db_name: Option<String>,
    redis_uri: Option<String>,
    batch_size: usize,
    total_imported: Arc<AtomicU64>,
    pb: ProgressBar,
) -> Result<u64> {
    let enrichment_queue_reprocess = "ZTF_alerts_enrichment_queue_reprocess";
    let client = Client::with_uri_str(&boom_uri)
        .await
        .context("failed to connect to BOOM MongoDB")?;
    let db = client.database(&boom_db_name);

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

        let known = query_known_obj_ids(&aux_coll, &unique_obj_ids).await;

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
        let (ztf_alerts, cutouts) = into_boom(to_import);
        let candids: Vec<i64> = ztf_alerts.iter().map(|a| a.candid).collect();

        let t = Instant::now();
        insert_many_skip_dups(&cutouts_coll, cutouts, &insert_opts, "cutouts").await;
        debug!(
            count = n,
            elapsed_ms = t.elapsed().as_millis(),
            "cutout insert"
        );

        let t = Instant::now();
        let dup_indices =
            insert_many_skip_dups(&alerts_coll, ztf_alerts, &insert_opts, "alerts").await;
        let inserted = n - dup_indices.len();
        debug!(
            count = n,
            dups = dup_indices.len(),
            inserted,
            elapsed_ms = t.elapsed().as_millis(),
            "alert insert"
        );

        if let Some(ref mut conn) = redis_conn {
            let dup_set: HashSet<usize> = dup_indices.into_iter().collect();
            let to_enqueue: Vec<i64> = candids
                .iter()
                .enumerate()
                .filter(|(i, _)| !dup_set.contains(i))
                .map(|(_, c)| *c)
                .collect();
            if !to_enqueue.is_empty() {
                let t = Instant::now();
                conn.lpush::<&str, Vec<i64>, usize>(enrichment_queue_reprocess, to_enqueue)
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

    let kowalski_client = Client::with_uri_str(&args.kowalski_uri)
        .await
        .unwrap_or_else(|e| {
            eprintln!("failed to connect to Kowalski: {}", e);
            std::process::exit(1);
        });
    let kowalski_coll: Collection<KowalskiZtfAlert> = kowalski_client
        .database("kowalski")
        .collection("ZTF_alerts");

    let estimated_total = match args.min_candid {
        Some(min) => kowalski_coll
            .count_documents(doc! { "_id": { "$gte": Bson::Int64(min) } })
            .await
            .unwrap_or_else(|e| {
                warn!("count_documents failed: {}", e);
                0
            }),
        None => kowalski_coll
            .estimated_document_count()
            .await
            .unwrap_or_else(|e| {
                warn!("estimated_document_count failed: {}", e);
                0
            }),
    };
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
        let boom_uri = args.boom_uri.clone();
        let boom_db_name = args.boom_db_name.clone();
        let boom_cutout_uri = args.boom_cutout_uri.clone();
        let boom_cutout_db_name = args.boom_cutout_db_name.clone();
        let redis_uri = args.redis_uri.clone();
        let batch_size = args.batch_size;
        let counter = Arc::clone(&total_imported);
        let pb = pb.clone();
        handles.push(tokio::spawn(async move {
            worker(
                rx,
                boom_uri,
                boom_db_name,
                boom_cutout_uri,
                boom_cutout_db_name,
                redis_uri,
                batch_size,
                counter,
                pb,
            )
            .await
        }));
    }

    drop(receiver); // main never reads; workers hold the live clones

    // _id == candid in Kowalski's ZTF_alerts — use the primary index for filter and sort.
    let filter = match args.min_candid {
        Some(min) => doc! { "_id": { "$gte": Bson::Int64(min) } },
        None => doc! {},
    };

    let projection = doc! {
        "classifications": 0,
        "publisher": 0,
        "schemavsn": 0,
        "coordinates": 0,
    };

    let mut cursor = kowalski_coll
        .find(filter)
        .sort(doc! { "_id": 1 })
        .projection(projection)
        .batch_size(1000)
        .no_cursor_timeout(true)
        .await
        .unwrap_or_else(|e| {
            error!("failed to open Kowalski cursor: {}", e);
            std::process::exit(1);
        });

    let mut last_candid: i64 = args.min_candid.unwrap_or(0);
    let mut n_read: u64 = 0;

    while let Some(result) = cursor.next().await {
        match result {
            Ok(alert) => {
                last_candid = alert.candid;
                n_read += 1;
                if sender.send(alert).await.is_err() {
                    error!("all workers stopped; last candid read: {}", last_candid);
                    info!("resume with: --min-candid {}", last_candid);
                    std::process::exit(1);
                }
            }
            Err(e) => {
                error!("Kowalski cursor error: {}", e);
                info!("resume with: --min-candid {}", last_candid);
                break;
            }
        }
    }

    info!(
        "cursor done — read {} alerts, last candid: {}",
        n_read, last_candid
    );
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
    info!(
        "to resume from this point, use: --min-candid {}",
        last_candid
    );
}
