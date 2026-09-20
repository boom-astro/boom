#![recursion_limit = "512"]

use boom::{
    alert::{
        AlertWorker, ProcessAlertStatus, ZtfAlertInput, ZtfAlertWorker, ZtfCandidate,
        ZtfForcedPhot, ZtfPrvCandidate,
    },
    conf::{load_dotenv, AppConfig},
    utils::{
        cutouts::AlertCutout, data::make_progress_bar, o11y::logging::build_subscriber,
        parser::parse_positive_usize,
    },
};

use anyhow::{Context, Result};
use clap::Parser;
use futures::TryStreamExt;
use indicatif::ProgressBar;
use mongodb::{
    bson::{doc, from_document, Document},
    Client, Collection, Database,
};
use redis::AsyncCommands;
use serde::Deserialize;
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};
use tracing::{info, warn};

#[derive(Parser)]
#[command(
    about = "Import a window of ZTF alerts from a healthy peer BOOM instance, recomputing \
             crossmatches locally and queuing the inserted candids for enrich_reprocess \
             instead of the filter workers"
)]
struct Cli {
    #[arg(
        long,
        value_name = "FILE",
        default_value = "config.yaml",
        help = "Configuration of this instance, the import destination: its database, cutout storage, catalogs and redis are the ones written to."
    )]
    config: String,

    #[arg(
        long,
        env = "PEER_MONGODB_URI",
        help = "Peer MongoDB URI holding ZTF_alerts and ZTF_alerts_aux. Read-only credentials are enough."
    )]
    peer_uri: String,

    #[arg(long, default_value = "boom", help = "Database name on --peer-uri.")]
    peer_db: String,

    #[arg(
        long,
        env = "PEER_CUTOUT_MONGODB_URI",
        help = "Peer MongoDB URI holding ZTF_alerts_cutouts, when the peer keeps them on their own mongod. Defaults to --peer-uri."
    )]
    peer_cutout_uri: Option<String>,

    #[arg(
        long,
        help = "Database name on --peer-cutout-uri. Defaults to --peer-db."
    )]
    peer_cutout_db: Option<String>,

    #[arg(
        long,
        value_name = "JD",
        help = "Import the peer's alerts whose candidate.jd is >= this value."
    )]
    jd_min: f64,

    #[arg(
        long,
        value_name = "JD",
        help = "Import the peer's alerts whose candidate.jd is < this value."
    )]
    jd_max: f64,

    #[arg(
        long,
        default_value_t = 30.0,
        help = "Days of peer photometry carried over with each object, counted back from --jd-max, mirroring the history a live packet would have carried."
    )]
    history_days: f64,

    #[arg(
        long,
        default_value = "ZTF_alerts_enrichment_queue_reprocess",
        help = "Redis queue the inserted candids are pushed to, for enrich_reprocess to drain."
    )]
    enrichment_queue: String,

    #[arg(
        long,
        default_value_t = 200,
        value_parser = parse_positive_usize,
        help = "Alerts per batch: one batch is one aux query, one cutout query and one lpush."
    )]
    batch_size: usize,

    #[arg(
        long,
        default_value_t = 4,
        value_parser = parse_positive_usize,
        help = "Number of parallel import workers."
    )]
    n_workers: usize,

    #[arg(long, help = "Report what would be imported without writing anything.")]
    dry_run: bool,
}

#[derive(Deserialize)]
struct PeerAlert {
    #[serde(rename = "_id")]
    candid: i64,
    #[serde(rename = "objectId")]
    object_id: String,
    candidate: ZtfCandidate,
}

#[derive(Deserialize)]
struct PeerAux {
    #[serde(default)]
    prv_candidates: Vec<ZtfPrvCandidate>,
    #[serde(default)]
    prv_nondetections: Vec<ZtfPrvCandidate>,
    #[serde(default)]
    fp_hists: Vec<ZtfForcedPhot>,
}

#[derive(Deserialize)]
struct CandidOnly {
    #[serde(rename = "_id")]
    candid: i64,
}

#[derive(Default)]
struct Counters {
    scanned: AtomicU64,
    already_present: AtomicU64,
    imported: AtomicU64,
    raced: AtomicU64,
    peer_aux_missing: AtomicU64,
    cutout_missing: AtomicU64,
    failed: AtomicU64,
}

#[derive(Clone)]
struct Job {
    config: String,
    peer_uri: String,
    peer_db: String,
    peer_cutout_uri: String,
    peer_cutout_db: String,
    jd_max: f64,
    history_days: f64,
    enrichment_queue: String,
    dry_run: bool,
}

fn aux_window_pipeline(object_ids: Vec<&str>, jd_lo: f64, jd_hi: f64) -> Vec<Document> {
    let window = |field: &str| {
        doc! { "$filter": {
            "input": { "$ifNull": [format!("${}", field), []] },
            "cond": { "$and": [
                { "$gte": ["$$this.jd", jd_lo] },
                { "$lt": ["$$this.jd", jd_hi] },
            ]},
        }}
    };
    vec![
        doc! { "$match": { "_id": { "$in": object_ids } } },
        doc! { "$project": {
            "prv_candidates": window("prv_candidates"),
            "prv_nondetections": window("prv_nondetections"),
            "fp_hists": window("fp_hists"),
        }},
    ]
}

async fn stored_candids(
    collection: &Collection<CandidOnly>,
    candids: &[i64],
) -> Result<HashSet<i64>> {
    let stored: Vec<CandidOnly> = collection
        .find(doc! { "_id": { "$in": candids.to_vec() } })
        .projection(doc! { "_id": 1 })
        .await
        .context("destination ZTF_alerts lookup failed")?
        .try_collect()
        .await
        .context("destination ZTF_alerts cursor failed")?;
    Ok(stored.into_iter().map(|alert| alert.candid).collect())
}

async fn peer_photometry(
    collection: &Collection<Document>,
    object_ids: Vec<&str>,
    jd_lo: f64,
    jd_hi: f64,
) -> Result<HashMap<String, PeerAux>> {
    let mut cursor = collection
        .aggregate(aux_window_pipeline(object_ids, jd_lo, jd_hi))
        .await
        .context("peer ZTF_alerts_aux aggregation failed")?;
    let mut photometry = HashMap::new();
    while let Some(document) = cursor
        .try_next()
        .await
        .context("peer ZTF_alerts_aux cursor failed")?
    {
        let object_id = document.get_str("_id")?.to_string();
        photometry.insert(
            object_id,
            from_document::<PeerAux>(document).context("peer aux document did not deserialize")?,
        );
    }
    Ok(photometry)
}

async fn peer_cutouts(
    collection: &Collection<AlertCutout>,
    candids: &[i64],
) -> Result<HashMap<i64, AlertCutout>> {
    let cutouts: Vec<AlertCutout> = collection
        .find(doc! { "_id": { "$in": candids.to_vec() } })
        .await
        .context("peer ZTF_alerts_cutouts lookup failed")?
        .try_collect()
        .await
        .context("peer ZTF_alerts_cutouts cursor failed")?;
    Ok(cutouts
        .into_iter()
        .map(|cutout| (cutout.candid, cutout))
        .collect())
}

async fn worker(
    receiver: async_channel::Receiver<Vec<PeerAlert>>,
    job: Job,
    counters: Arc<Counters>,
    bar: ProgressBar,
) -> Result<()> {
    let config = AppConfig::from_path(&job.config)?;
    let mut alert_worker = ZtfAlertWorker::new(&job.config).await?;
    let mut redis = config.build_redis().await?;
    let destination: Collection<CandidOnly> = config.build_db().await?.collection("ZTF_alerts");

    let peer: Database = Client::with_uri_str(&job.peer_uri)
        .await
        .context("peer connection failed")?
        .database(&job.peer_db);
    let peer_aux: Collection<Document> = peer.collection("ZTF_alerts_aux");
    let peer_cutout_collection: Collection<AlertCutout> =
        Client::with_uri_str(&job.peer_cutout_uri)
            .await
            .context("peer cutout connection failed")?
            .database(&job.peer_cutout_db)
            .collection("ZTF_alerts_cutouts");

    let jd_lo = job.jd_max - job.history_days;

    while let Ok(batch) = receiver.recv().await {
        bar.inc(batch.len() as u64);
        counters
            .scanned
            .fetch_add(batch.len() as u64, Ordering::Relaxed);

        let candids: Vec<i64> = batch.iter().map(|alert| alert.candid).collect();
        let stored = stored_candids(&destination, &candids).await?;
        counters
            .already_present
            .fetch_add(stored.len() as u64, Ordering::Relaxed);

        let todo: Vec<PeerAlert> = batch
            .into_iter()
            .filter(|alert| !stored.contains(&alert.candid))
            .collect();
        if todo.is_empty() {
            continue;
        }
        let todo_candids: Vec<i64> = todo.iter().map(|alert| alert.candid).collect();

        let mut object_ids: Vec<&str> = todo.iter().map(|alert| alert.object_id.as_str()).collect();
        object_ids.sort_unstable();
        object_ids.dedup();
        let photometry = peer_photometry(&peer_aux, object_ids, jd_lo, job.jd_max).await?;

        if job.dry_run {
            let available = peer_cutout_collection
                .count_documents(doc! { "_id": { "$in": todo_candids.clone() } })
                .await
                .context("peer cutout count failed")?;
            counters.cutout_missing.fetch_add(
                (todo_candids.len() as u64).saturating_sub(available),
                Ordering::Relaxed,
            );
            counters
                .imported
                .fetch_add(todo_candids.len() as u64, Ordering::Relaxed);
            continue;
        }

        let mut cutouts = peer_cutouts(&peer_cutout_collection, &todo_candids).await?;
        let mut inserted: Vec<i64> = Vec::with_capacity(todo.len());
        for alert in todo {
            let Some(cutout) = cutouts.remove(&alert.candid) else {
                counters.cutout_missing.fetch_add(1, Ordering::Relaxed);
                warn!(
                    candid = alert.candid,
                    "no cutout on the peer, alert skipped"
                );
                continue;
            };
            let (prv_candidates, prv_nondetections, fp_hists) =
                match photometry.get(&alert.object_id) {
                    Some(aux) => (
                        aux.prv_candidates.clone(),
                        aux.prv_nondetections.clone(),
                        aux.fp_hists.clone(),
                    ),
                    None => {
                        counters.peer_aux_missing.fetch_add(1, Ordering::Relaxed);
                        (Vec::new(), Vec::new(), Vec::new())
                    }
                };
            let input = ZtfAlertInput {
                candid: alert.candid,
                object_id: alert.object_id,
                candidate: alert.candidate,
                prv_candidates,
                prv_nondetections,
                fp_hists,
                cutout_science: cutout.cutout_science,
                cutout_template: cutout.cutout_template,
                cutout_difference: cutout.cutout_difference,
            };
            match alert_worker.ingest_alert(input).await {
                Ok(ProcessAlertStatus::Added(candid)) => inserted.push(candid),
                Ok(ProcessAlertStatus::Exists(_)) => {
                    counters.raced.fetch_add(1, Ordering::Relaxed);
                }
                Err(error) => {
                    counters.failed.fetch_add(1, Ordering::Relaxed);
                    warn!(%error, candid = alert.candid, "alert import failed");
                }
            }
        }

        if !inserted.is_empty() {
            counters
                .imported
                .fetch_add(inserted.len() as u64, Ordering::Relaxed);
            redis
                .lpush::<&str, Vec<i64>, usize>(job.enrichment_queue.as_str(), inserted)
                .await
                .context("lpush to the enrichment queue failed")?;
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    load_dotenv();
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");

    let args = Cli::parse();
    anyhow::ensure!(
        args.jd_min < args.jd_max,
        "--jd-min must be smaller than --jd-max"
    );
    anyhow::ensure!(args.history_days > 0.0, "--history-days must be > 0");

    let job = Job {
        config: args.config.clone(),
        peer_uri: args.peer_uri.clone(),
        peer_db: args.peer_db.clone(),
        peer_cutout_uri: args
            .peer_cutout_uri
            .clone()
            .unwrap_or_else(|| args.peer_uri.clone()),
        peer_cutout_db: args
            .peer_cutout_db
            .clone()
            .unwrap_or_else(|| args.peer_db.clone()),
        jd_max: args.jd_max,
        history_days: args.history_days,
        enrichment_queue: args.enrichment_queue.clone(),
        dry_run: args.dry_run,
    };

    let peer: Database = Client::with_uri_str(&job.peer_uri)
        .await
        .context("peer connection failed")?
        .database(&job.peer_db);
    let peer_alerts: Collection<PeerAlert> = peer.collection("ZTF_alerts");
    let window = doc! { "candidate.jd": { "$gte": args.jd_min, "$lt": args.jd_max } };

    let total = peer_alerts
        .clone_with_type::<Document>()
        .count_documents(window.clone())
        .await
        .context("peer alert count failed")?;
    info!(
        total,
        jd_min = args.jd_min,
        jd_max = args.jd_max,
        dry_run = job.dry_run,
        "alerts found on the peer for that window"
    );
    let bar = make_progress_bar(total, "importing".to_string());

    let counters = Arc::new(Counters::default());
    let (sender, receiver) = async_channel::bounded::<Vec<PeerAlert>>(args.n_workers * 2);
    let mut handles = Vec::with_capacity(args.n_workers);
    for _ in 0..args.n_workers {
        handles.push(tokio::spawn(worker(
            receiver.clone(),
            job.clone(),
            counters.clone(),
            bar.clone(),
        )));
    }
    drop(receiver);

    let mut cursor = peer_alerts
        .find(window)
        .projection(doc! { "_id": 1, "objectId": 1, "candidate": 1 })
        .no_cursor_timeout(true)
        .await
        .context("peer alert query failed")?;
    let mut batch = Vec::with_capacity(args.batch_size);
    while let Some(alert) = cursor
        .try_next()
        .await
        .context("peer alert cursor failed")?
    {
        batch.push(alert);
        if batch.len() >= args.batch_size {
            sender
                .send(std::mem::replace(
                    &mut batch,
                    Vec::with_capacity(args.batch_size),
                ))
                .await?;
        }
    }
    if !batch.is_empty() {
        sender.send(batch).await?;
    }
    drop(sender);

    let mut failed_workers = 0;
    for handle in handles {
        match handle.await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                failed_workers += 1;
                warn!(%error, "worker stopped early");
            }
            Err(error) => {
                failed_workers += 1;
                warn!(%error, "worker panicked");
            }
        }
    }
    bar.finish_and_clear();

    let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
    info!(
        scanned = get(&counters.scanned),
        already_present = get(&counters.already_present),
        imported = get(&counters.imported),
        raced = get(&counters.raced),
        peer_aux_missing = get(&counters.peer_aux_missing),
        cutout_missing = get(&counters.cutout_missing),
        failed = get(&counters.failed),
        "import done"
    );
    if job.dry_run {
        info!("dry run: nothing was written and nothing was queued");
    } else {
        info!(
            queue = job.enrichment_queue,
            "run enrich_reprocess on that queue to compute ML scores and properties"
        );
    }
    anyhow::ensure!(failed_workers == 0, "{} worker(s) failed", failed_workers);
    Ok(())
}
