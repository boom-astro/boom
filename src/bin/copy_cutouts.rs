use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

use boom::utils::data::make_progress_bar;
use boom::utils::parser::parse_positive_usize;
use boom::{conf::load_dotenv, utils::enums::Survey};
use clap::Parser;
use futures::TryStreamExt;
use mongodb::bson::{doc, Bson};
use mongodb::error::{ErrorKind, InsertManyError};
use mongodb::options::ClientOptions;
use tokio::sync::{mpsc, Mutex};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// Copy alert cutout documents from one MongoDB to another.
///
/// Typical workflow:
///   1. Run without --min-candid for the full copy (~5-7 hours for ZTF).
///      Note the "max candid" printed at the end.
///   2. Run again with --min-candid <that value> to catch up on alerts
///      that arrived during step 1 (takes minutes).
///   3. Flip config and restart boom.
#[derive(Parser)]
struct Cli {
    /// Source MongoDB URI — must include database name
    /// e.g. mongodb://user:pass@host:27017/boom
    #[arg(long)]
    src_uri: String,

    /// Destination MongoDB URI — must include database name
    #[arg(long)]
    dst_uri: String,

    /// Survey name — copies {survey}_alerts_cutouts
    #[arg(long, value_enum, default_value = "ztf")]
    survey: Survey,

    /// Documents per insert_many batch
    #[arg(long, default_value_t = 1000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Incremental mode: only copy documents with _id >= this value.
    /// Pass the "max candid" printed by the previous run.
    #[arg(long)]
    min_candid: Option<i64>,

    /// Number of concurrent write tasks. Overlaps source reads with
    /// destination writes; 4 is a good starting point for a JBOD.
    #[arg(long, default_value_t = 4, value_parser = parse_positive_usize)]
    parallelism: usize,

    /// Print document counts without copying anything
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

async fn connect(uri: &str) -> mongodb::Database {
    let mut opts = match ClientOptions::parse(uri).await {
        Ok(o) => o,
        Err(e) => {
            error!("invalid URI: {}", e);
            std::process::exit(1);
        }
    };
    let db_name = match opts.default_database.take() {
        Some(n) => n,
        None => {
            error!("URI must include a database name: mongodb://...host:port/dbname");
            std::process::exit(1);
        }
    };
    match mongodb::Client::with_options(opts) {
        Ok(client) => client.database(&db_name),
        Err(e) => {
            error!("failed to build client: {}", e);
            std::process::exit(1);
        }
    }
}

/// insert_many with ordered=false, treating E11000 (duplicate key) as skips.
/// Returns (inserted, skipped_as_duplicates) or a fatal Err.
async fn flush_batch(
    dst: &mongodb::Collection<mongodb::bson::Document>,
    batch: &[mongodb::bson::Document],
) -> Result<(u64, u64), mongodb::error::Error> {
    match dst.insert_many(batch).ordered(false).await {
        Ok(result) => Ok((result.inserted_ids.len() as u64, 0)),
        Err(e) => {
            if let ErrorKind::InsertMany(InsertManyError {
                ref write_errors,
                ref write_concern_error,
                ..
            }) = *e.kind
            {
                let has_non_dup = write_errors
                    .as_ref()
                    .map(|errs| errs.iter().any(|we| we.code != 11000))
                    .unwrap_or(false);
                if !has_non_dup && write_concern_error.is_none() {
                    let dups = write_errors.as_ref().map(|e| e.len()).unwrap_or(0) as u64;
                    return Ok((batch.len() as u64 - dups, dups));
                }
            }
            Err(e)
        }
    }
}

#[tokio::main]
async fn main() {
    load_dotenv();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting subscriber failed");

    let args = Cli::parse();

    let src_db = connect(&args.src_uri).await;
    let dst_db = connect(&args.dst_uri).await;

    let collection_name = format!("{}_alerts_cutouts", args.survey);
    let src = src_db.collection::<mongodb::bson::Document>(&collection_name);
    let dst = dst_db.collection::<mongodb::bson::Document>(&collection_name);

    let filter = match args.min_candid {
        Some(min) => doc! { "_id": { "$gte": Bson::Int64(min) } },
        None => doc! {},
    };

    let estimated_total = if args.min_candid.is_some() {
        src.count_documents(filter.clone())
            .await
            .unwrap_or_else(|e| {
                warn!("count_documents failed: {}", e);
                0
            })
    } else {
        src.estimated_document_count().await.unwrap_or_else(|e| {
            warn!("estimated_document_count failed: {}", e);
            0
        })
    };

    if args.dry_run {
        let dst_count = dst.estimated_document_count().await.unwrap_or(0);
        info!(
            "source: ~{} documents to copy | destination: {} documents currently",
            estimated_total, dst_count
        );
        return;
    }

    info!(
        "copying ~{} documents from {}/{} to {}/{} (parallelism={})",
        estimated_total,
        src_db.name(),
        collection_name,
        dst_db.name(),
        collection_name,
        args.parallelism
    );
    if let Some(min) = args.min_candid {
        info!("incremental mode: _id >= {}", min);
    }

    let pb = make_progress_bar(estimated_total, "copy".to_string());

    // Channel carries (batch, batch_start_candid).
    // batch_start_candid lets a writer report a precise resume point on fatal error.
    // Capacity is parallelism*2 so the reader stays slightly ahead of writers.
    let (tx, rx) = mpsc::channel::<(Vec<mongodb::bson::Document>, i64)>(args.parallelism * 2);
    let rx = Arc::new(Mutex::new(rx));

    let total_inserted = Arc::new(AtomicU64::new(0));
    let total_skipped = Arc::new(AtomicU64::new(0));

    let mut writer_handles = Vec::with_capacity(args.parallelism);
    for _ in 0..args.parallelism {
        let rx = Arc::clone(&rx);
        let dst = dst.clone();
        let inserted = Arc::clone(&total_inserted);
        let skipped = Arc::clone(&total_skipped);
        let pb = pb.clone();

        writer_handles.push(tokio::spawn(async move {
            loop {
                // Hold the lock only for recv() — released before flush_batch so all
                // writers can be in flush_batch concurrently.
                let item = {
                    let mut rx = rx.lock().await;
                    rx.recv().await
                };
                let (batch, batch_start) = match item {
                    Some(b) => b,
                    None => break,
                };
                let n = batch.len() as u64;
                match flush_batch(&dst, &batch).await {
                    Ok((ins, dup)) => {
                        inserted.fetch_add(ins, Ordering::Relaxed);
                        skipped.fetch_add(dup, Ordering::Relaxed);
                        pb.inc(n);
                    }
                    Err(e) => {
                        error!("fatal write error: {}", e);
                        error!("resume with --min-candid {}", batch_start);
                        std::process::exit(1);
                    }
                }
            }
        }));
    }

    // Reader runs on the main task, feeding batches into the channel.
    let mut cursor = src
        .find(filter)
        .sort(doc! { "_id": 1 })
        .no_cursor_timeout(true)
        .await
        .unwrap_or_else(|e| {
            error!("failed to open source cursor: {}", e);
            std::process::exit(1);
        });

    let mut batch: Vec<mongodb::bson::Document> = Vec::with_capacity(args.batch_size);
    let mut batch_start: i64 = i64::MIN;
    let mut max_candid_read: i64 = i64::MIN;

    loop {
        match cursor.try_next().await {
            Ok(Some(doc)) => {
                let id = match doc.get("_id") {
                    Some(Bson::Int64(v)) => *v,
                    Some(Bson::Int32(v)) => *v as i64,
                    other => {
                        error!(
                            "_id is missing or not an integer (got {:?}); \
                             collection may have the wrong _id type",
                            other
                        );
                        std::process::exit(1);
                    }
                };
                if batch.is_empty() {
                    batch_start = id;
                }
                if id > max_candid_read {
                    max_candid_read = id;
                }
                batch.push(doc);
                if batch.len() >= args.batch_size {
                    let ready = std::mem::replace(&mut batch, Vec::with_capacity(args.batch_size));
                    let start = batch_start;
                    batch_start = i64::MIN;
                    if tx.send((ready, start)).await.is_err() {
                        // All writers have exited — a fatal write error was already logged
                        error!(
                            "all writer tasks stopped; resume with --min-candid {}",
                            start
                        );
                        std::process::exit(1);
                    }
                }
            }
            Ok(None) => {
                if !batch.is_empty() {
                    let start = batch_start;
                    if tx.send((batch, start)).await.is_err() {
                        error!(
                            "all writer tasks stopped; resume with --min-candid {}",
                            start
                        );
                        std::process::exit(1);
                    }
                }
                break;
            }
            Err(e) => {
                // batch_start is i64::MIN when the error falls between batches;
                // use max_candid_read (last successfully read _id) in that case.
                let resume = if !batch.is_empty() {
                    batch_start
                } else {
                    max_candid_read
                };
                error!("cursor error: {}", e);
                if resume == i64::MIN {
                    error!("no documents read yet; restart without --min-candid");
                } else {
                    error!("resume with --min-candid {}", resume);
                }
                std::process::exit(1);
            }
        }
    }

    drop(tx); // close channel; writers drain remaining batches then exit

    for h in writer_handles {
        h.await.unwrap();
    }

    pb.finish();

    info!(
        "done — inserted: {}, skipped (duplicates): {}",
        total_inserted.load(Ordering::Relaxed),
        total_skipped.load(Ordering::Relaxed)
    );
    info!("max candid copied: {}", max_candid_read);
    info!(
        "for incremental catch-up, run with: --min-candid {}",
        max_candid_read
    );
}
