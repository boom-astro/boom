use boom::{
    conf::{load_dotenv, AppConfig},
    utils::{data::make_progress_bar, enums::Survey, parser::parse_positive_usize},
};
use clap::Parser;
use futures::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};
use std::io::{BufWriter, Write};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

/// Find every `_id` that exists in `<survey>_alerts_cutouts` but has no
/// matching document in `<survey>_alerts`, and write those ids (one per line)
/// to a text file.
///
/// Both collections key documents by `_id == candid`, so an orphan cutout is
/// simply a cutout `_id` absent from the alerts collection.
///
/// Speed: instead of doing one lookup per cutout, this streams both
/// collections sorted ascending by `_id` and performs a single linear
/// merge-join. Each cursor is a covered, index-only scan (projection
/// `{_id: 1}` served by the default `_id_` index), so the cutout blob
/// documents are never read from disk. Memory use is O(1) (one id buffered
/// per stream) regardless of collection size.
#[derive(Parser)]
struct Cli {
    #[arg(long, value_enum)]
    survey: Survey,

    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    /// Output file for the orphan cutout ids (one id per line).
    #[arg(long, value_name = "FILE", default_value = "orphan_cutout_ids.txt")]
    output: String,

    /// Cursor batch size. Larger values mean fewer network round trips.
    #[arg(long, default_value_t = 100000, value_parser = parse_positive_usize)]
    batch_size: usize,
}

/// Read `_id` as an i64, accepting Int64 / Int32 (candid is always integral).
fn id_as_i64(doc: &Document) -> Option<i64> {
    match doc.get("_id") {
        Some(Bson::Int64(value)) => Some(*value),
        Some(Bson::Int32(value)) => Some(*value as i64),
        _ => None,
    }
}

#[tokio::main]
async fn main() {
    load_dotenv();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Cli::parse();

    let config = match AppConfig::from_path(&args.config) {
        Ok(config) => config,
        Err(e) => {
            error!("error loading config {}: {}", args.config, e);
            std::process::exit(1);
        }
    };

    let db = match config.build_db().await {
        Ok(db) => db,
        Err(e) => {
            error!("error building db: {}", e);
            std::process::exit(1);
        }
    };

    let alerts_name = format!("{}_alerts", args.survey);
    let cutouts_name = format!("{}_alerts_cutouts", args.survey);
    let alerts = db.collection::<Document>(&alerts_name);
    let cutouts = db.collection::<Document>(&cutouts_name);

    let cutouts_total = cutouts.estimated_document_count().await.unwrap_or(0);
    let alerts_total = alerts.estimated_document_count().await.unwrap_or(0);
    info!(
        "scanning {} (~{} docs) against {} (~{} docs)",
        cutouts_name, cutouts_total, alerts_name, alerts_total
    );

    let batch_size = args.batch_size as u32;
    let mut alert_cursor = match alerts
        .find(doc! {})
        .projection(doc! { "_id": 1 })
        .sort(doc! { "_id": 1 })
        .batch_size(batch_size)
        .await
    {
        Ok(cursor) => cursor,
        Err(e) => {
            error!("error opening alerts cursor: {}", e);
            std::process::exit(1);
        }
    };
    let mut cutout_cursor = match cutouts
        .find(doc! {})
        .projection(doc! { "_id": 1 })
        .sort(doc! { "_id": 1 })
        .batch_size(batch_size)
        .await
    {
        Ok(cursor) => cursor,
        Err(e) => {
            error!("error opening cutouts cursor: {}", e);
            std::process::exit(1);
        }
    };

    let file = match std::fs::File::create(&args.output) {
        Ok(file) => file,
        Err(e) => {
            error!("error creating output file {}: {}", args.output, e);
            std::process::exit(1);
        }
    };
    let mut writer = BufWriter::with_capacity(1 << 20, file);

    let progress_bar = make_progress_bar(cutouts_total, "cutouts scanned".to_string());

    let mut current_alert: Option<i64> = match alert_cursor.try_next().await {
        Ok(doc) => doc.and_then(|d| id_as_i64(&d)),
        Err(e) => {
            error!("error reading alerts: {}", e);
            std::process::exit(1);
        }
    };

    let mut orphan_count: u64 = 0;
    let mut scanned: u64 = 0;

    loop {
        let cutout_id = match cutout_cursor.try_next().await {
            Ok(Some(doc)) => match id_as_i64(&doc) {
                Some(id) => id,
                None => continue,
            },
            Ok(None) => break,
            Err(e) => {
                error!("error reading cutouts: {}", e);
                std::process::exit(1);
            }
        };
        scanned += 1;
        if scanned % 100000 == 0 {
            progress_bar.set_position(scanned);
        }

        // Advance the alerts stream until it catches up to this cutout id.
        // Both streams are sorted ascending, so any alert id strictly below
        // the current cutout id has no cutout and can be discarded.
        while matches!(current_alert, Some(alert_id) if alert_id < cutout_id) {
            current_alert = match alert_cursor.try_next().await {
                Ok(doc) => doc.and_then(|d| id_as_i64(&d)),
                Err(e) => {
                    error!("error reading alerts: {}", e);
                    std::process::exit(1);
                }
            };
        }

        // No alert equal to this cutout id (alerts exhausted, or the next
        // alert id is already strictly greater) means the cutout is orphaned.
        match current_alert {
            Some(alert_id) if alert_id == cutout_id => {
                // Matched: consume this alert so it is not reused.
                current_alert = match alert_cursor.try_next().await {
                    Ok(doc) => doc.and_then(|d| id_as_i64(&d)),
                    Err(e) => {
                        error!("error reading alerts: {}", e);
                        std::process::exit(1);
                    }
                };
            }
            _ => {
                orphan_count += 1;
                if let Err(e) = writeln!(writer, "{}", cutout_id) {
                    error!("error writing to output file: {}", e);
                    std::process::exit(1);
                }
            }
        }
    }

    progress_bar.set_position(scanned);
    progress_bar.finish();

    if let Err(e) = writer.flush() {
        error!("error flushing output file: {}", e);
        std::process::exit(1);
    }

    info!(
        "scanned {} cutouts, found {} orphan cutout ids (no matching alert), written to {}",
        scanned, orphan_count, args.output
    );
}
