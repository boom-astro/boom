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

/// Find every `_id` that exists in `<survey>_alerts_aux` but has no alert
/// referencing it in `<survey>_alerts`, and write those object ids (one per
/// line) to a text file.
///
/// In `<survey>_alerts_aux` the `_id` is the `objectId` (a string). In
/// `<survey>_alerts` the `_id` is the `candid` and the link back to the aux
/// document is the `objectId` field. An orphan aux object is therefore an aux
/// `_id` that never appears as `objectId` in the alerts collection.
///
/// Speed: instead of doing one lookup per aux object, this streams both
/// collections sorted ascending and performs a single linear merge-join. The
/// aux cursor is a covered scan of the default `_id_` index (projection
/// `{_id: 1}`). The alerts cursor projects `{_id: 0, objectId: 1}` sorted by
/// `objectId`, served entirely by the `objectId_1` index (covered, index-only
/// scan), so alert blobs are never read from disk. The alerts stream contains
/// one entry per alert (many per object), but the merge simply skips repeated
/// object ids. Memory use is O(1) (one id buffered per stream) regardless of
/// collection size.
#[derive(Parser)]
struct Cli {
    #[arg(long, value_enum)]
    survey: Survey,

    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    /// Output file for the orphan aux object ids (one id per line).
    #[arg(long, value_name = "FILE", default_value = "orphan_aux_object_ids.txt")]
    output: String,

    /// Cursor batch size. Larger values mean fewer network round trips.
    #[arg(long, default_value_t = 100000, value_parser = parse_positive_usize)]
    batch_size: usize,
}

/// Read a string id from `field` (`_id` for aux, `objectId` for alerts).
fn id_as_string(doc: &Document, field: &str) -> Option<String> {
    match doc.get(field) {
        Some(Bson::String(value)) => Some(value.clone()),
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
    let aux_name = format!("{}_alerts_aux", args.survey);
    let alerts = db.collection::<Document>(&alerts_name);
    let aux = db.collection::<Document>(&aux_name);

    let aux_total = aux.estimated_document_count().await.unwrap_or(0);
    let alerts_total = alerts.estimated_document_count().await.unwrap_or(0);
    info!(
        "scanning {} (~{} docs) against {} (~{} docs)",
        aux_name, aux_total, alerts_name, alerts_total
    );

    let batch_size = args.batch_size as u32;
    // Covered by the `objectId_1` index: project objectId only (no _id) and
    // sort by objectId so this is an index-only scan with no document fetch.
    let mut alert_cursor = match alerts
        .find(doc! {})
        .projection(doc! { "_id": 0, "objectId": 1 })
        .sort(doc! { "objectId": 1 })
        .batch_size(batch_size)
        .await
    {
        Ok(cursor) => cursor,
        Err(e) => {
            error!("error opening alerts cursor: {}", e);
            std::process::exit(1);
        }
    };
    let mut aux_cursor = match aux
        .find(doc! {})
        .projection(doc! { "_id": 1 })
        .sort(doc! { "_id": 1 })
        .batch_size(batch_size)
        .await
    {
        Ok(cursor) => cursor,
        Err(e) => {
            error!("error opening aux cursor: {}", e);
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

    let progress_bar = make_progress_bar(aux_total, "aux objects scanned".to_string());

    let mut current_alert: Option<String> = match alert_cursor.try_next().await {
        Ok(doc) => doc.and_then(|d| id_as_string(&d, "objectId")),
        Err(e) => {
            error!("error reading alerts: {}", e);
            std::process::exit(1);
        }
    };

    let mut orphan_count: u64 = 0;
    let mut scanned: u64 = 0;

    loop {
        let aux_id = match aux_cursor.try_next().await {
            Ok(Some(doc)) => match id_as_string(&doc, "_id") {
                Some(id) => id,
                None => continue,
            },
            Ok(None) => break,
            Err(e) => {
                error!("error reading aux: {}", e);
                std::process::exit(1);
            }
        };
        scanned += 1;
        if scanned % 100000 == 0 {
            progress_bar.set_position(scanned);
        }

        // Advance the alerts stream until it catches up to this aux id. Both
        // streams are sorted ascending, so any alert objectId strictly below
        // the current aux id has no aux document and can be discarded. This
        // also collapses the many-alerts-per-object duplicates.
        while matches!(current_alert, Some(ref alert_id) if alert_id.as_str() < aux_id.as_str()) {
            current_alert = match alert_cursor.try_next().await {
                Ok(doc) => doc.and_then(|d| id_as_string(&d, "objectId")),
                Err(e) => {
                    error!("error reading alerts: {}", e);
                    std::process::exit(1);
                }
            };
        }

        // No alert equal to this aux id (alerts exhausted, or the next alert
        // objectId is already strictly greater) means the aux object has no
        // associated alert and is orphaned.
        match current_alert {
            Some(ref alert_id) if alert_id == &aux_id => {
                // Matched: this aux id is referenced by at least one alert.
                // Leave the alert stream as-is; remaining duplicates of this
                // objectId are skipped by the `<` advance on the next aux id.
            }
            _ => {
                orphan_count += 1;
                if let Err(e) = writeln!(writer, "{}", aux_id) {
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
        "scanned {} aux objects, found {} orphan object ids (no associated alert), written to {}",
        scanned, orphan_count, args.output
    );
}
