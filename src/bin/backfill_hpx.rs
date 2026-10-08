//! Write `coordinates.hpx` onto documents that predate the field.
//!
//! The index is a pure function of the position already stored, but HEALPix is
//! not something mongo can evaluate, so each document has to be read, hashed
//! here, and written back. Until this has covered a collection, a MOC range
//! query silently misses everything in it — an absent index is indistinguishable
//! from a position outside the region.
//!
//! `--processes` cuts each collection into shards scanned concurrently.

use boom::conf::{load_dotenv, AppConfig};
use boom::utils::data::{make_progress_bar, spawn_progress_logger};
use boom::utils::db::{
    join_tasks, merge_filters, range_shards, shard_field, TaskError, CURSOR_BATCH_SIZE,
};
use boom::utils::enums::Survey;
use boom::utils::parser::parse_positive_usize;
use boom::utils::spatial::HPX_DEPTH;
use clap::Parser;
use futures::TryStreamExt;
use indicatif::ProgressBar;
use mongodb::bson::{doc, Bson, Document};
use mongodb::options::{UpdateModifications, UpdateOneModel, WriteModel};
use mongodb::Collection;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(about = "Backfill coordinates.hpx on alerts written before the field existed")]
struct Cli {
    /// Path to the configuration file.
    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    /// Survey to process, or "all".
    #[arg(long, default_value = "all")]
    survey: String,

    /// Documents per bulk write.
    #[arg(long, default_value_t = 2000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Number of parallel scan and write shards.
    #[arg(long, default_value_t = 1, value_parser = parse_positive_usize)]
    processes: usize,

    /// Report what would be written without writing it.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

/// Every collection holding positions, alerts and their aux documents alike.
fn collections_for(survey: &Survey) -> Vec<String> {
    let name = survey.as_str();
    vec![format!("{name}_alerts"), format!("{name}_alerts_aux")]
}

#[derive(Default)]
struct ShardStats {
    written: u64,
    skipped: u64,
}

async fn backfill_shard(
    collection: Collection<Document>,
    filter: Document,
    batch_size: usize,
    dry_run: bool,
    pb: ProgressBar,
) -> Result<ShardStats, mongodb::error::Error> {
    let client = collection.client().clone();
    let namespace = collection.namespace();
    let mut cursor = collection
        .find(filter)
        .projection(doc! { "_id": 1, "coordinates.radec_geojson": 1 })
        .no_cursor_timeout(true)
        .batch_size(CURSOR_BATCH_SIZE)
        .await?;

    let mut batch: Vec<WriteModel> = Vec::with_capacity(batch_size);
    let mut stats = ShardStats::default();

    while let Some(doc) = cursor.try_next().await? {
        let Ok(id) = doc.get("_id").ok_or(()) else {
            continue;
        };
        // Positions are stored as [ra - 180, dec].
        let coords = doc
            .get_document("coordinates")
            .ok()
            .and_then(|c| c.get_document("radec_geojson").ok())
            .and_then(|g| g.get_array("coordinates").ok());
        let Some(coords) = coords else {
            stats.skipped += 1;
            continue;
        };
        let (Some(lon), Some(dec)) = (
            coords.first().and_then(Bson::as_f64),
            coords.get(1).and_then(Bson::as_f64),
        ) else {
            stats.skipped += 1;
            continue;
        };
        let ra = lon + 180.0;
        let hpx = cdshealpix::nested::get(HPX_DEPTH).hash(ra.to_radians(), dec.to_radians()) as i64;

        batch.push(WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(namespace.clone())
                .filter(doc! { "_id": id.clone() })
                .update(UpdateModifications::Document(
                    doc! { "$set": { "coordinates.hpx": hpx } },
                ))
                .build(),
        ));

        if batch.len() >= batch_size {
            let n = batch.len() as u64;
            if !dry_run {
                // Replaced rather than drained so the next batch keeps the allocation.
                let full = std::mem::replace(&mut batch, Vec::with_capacity(batch_size));
                client.bulk_write(full).ordered(false).await?;
            } else {
                batch.clear();
            }
            stats.written += n;
            pb.inc(n);
        }
    }

    if !batch.is_empty() {
        let n = batch.len() as u64;
        if !dry_run {
            client.bulk_write(batch).ordered(false).await?;
        }
        stats.written += n;
        pb.inc(n);
    }
    Ok(stats)
}

/// Backfill one collection, returning how many documents were written.
async fn backfill(
    collection: &Collection<Document>,
    batch_size: usize,
    processes: usize,
    dry_run: bool,
) -> Result<u64, TaskError> {
    // Only documents still missing the field, so a resumed run skips its own work.
    let filter = doc! { "coordinates.hpx": { "$exists": false } };
    let name = collection.name().to_string();

    let field = shard_field(collection).await;
    let shards = range_shards(collection, processes, field, &filter).await;
    info!(
        "{}: scanning across {} shard(s) cut on '{}'",
        name,
        shards.len(),
        field
    );

    // An exact count means a collection scan on an unindexed field, which on the
    // larger collections costs more than the pass itself, so the bar is sized by
    // the metadata estimate and reset to the real total once the shards are done.
    let pb = make_progress_bar(collection.estimated_document_count().await?, name.clone());
    pb.enable_steady_tick(std::time::Duration::from_millis(200));
    let logger = spawn_progress_logger(pb.clone(), name.clone());

    let mut handles = Vec::with_capacity(shards.len());
    for shard in shards {
        let collection = collection.clone();
        let filter = merge_filters(&filter, &shard);
        let pb = pb.clone();
        handles.push(tokio::spawn(async move {
            backfill_shard(collection, filter, batch_size, dry_run, pb).await
        }));
    }

    let outcome = join_tasks(handles, "shard").await;
    logger.abort();
    let stats = outcome?;

    let written = stats.iter().map(|s| s.written).sum();
    let skipped: u64 = stats.iter().map(|s| s.skipped).sum();
    pb.set_length(written);
    pb.finish();

    if written == 0 {
        info!("{}: already complete", name);
    }

    if skipped > 0 {
        warn!(
            "{}: {} documents had no usable position and were left alone",
            name, skipped
        );
    }
    Ok(written)
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to set subscriber");
    load_dotenv();

    let args = Cli::parse();
    let config_path = args.config.unwrap_or_else(|| "config.yaml".to_string());
    let config = AppConfig::from_path(&config_path).expect("failed to load config");
    let db = config.build_db().await.expect("failed to connect to mongo");

    let all = [Survey::Ztf, Survey::Lsst, Survey::Decam, Survey::Winter];
    let wanted = args.survey.to_lowercase();
    let surveys: Vec<Survey> = if wanted == "all" {
        all.to_vec()
    } else {
        let picked: Vec<Survey> = all
            .into_iter()
            .filter(|s| s.as_str().eq_ignore_ascii_case(&wanted))
            .collect();
        if picked.is_empty() {
            error!("unknown survey `{}`", args.survey);
            std::process::exit(1);
        }
        picked
    };

    let mut total = 0u64;
    for survey in surveys {
        for name in collections_for(&survey) {
            let collection = db.collection::<Document>(&name);
            // A survey absent from this deployment is not an error.
            if collection.estimated_document_count().await.unwrap_or(0) == 0 {
                continue;
            }
            match backfill(&collection, args.batch_size, args.processes, args.dry_run).await {
                Ok(n) => {
                    info!("{}: {} documents indexed", name, n);
                    total += n;
                }
                Err(e) => {
                    error!("{}: {}", name, e);
                    std::process::exit(1);
                }
            }
        }
    }

    if args.dry_run {
        info!("dry run: {} documents would be indexed", total);
    } else {
        info!("{} documents indexed at depth {}", total, HPX_DEPTH);
    }
}
