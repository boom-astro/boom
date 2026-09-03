//! Fit a phase curve per moving object per band, over the whole archive.
//!
//! Enrichment scores a detection against the curve stored here, which is what
//! lets it register activity lasting longer than any trailing window. Rebuild
//! periodically: the fit improves as coverage grows, and an object active for
//! most of its archive will eventually absorb that activity into its own
//! baseline.

use boom::conf::{load_dotenv, AppConfig};
use boom::utils::outburst::{Point, MAX_SEPARATION_ARCSEC};
use boom::utils::parser::parse_positive_usize;
use boom::utils::phase_curve::{baseline_document, fit, PhaseCurve, BASELINES_COLLECTION};
use clap::Parser;
use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use mongodb::options::{ReplaceOneModel, WriteModel};
use std::collections::HashMap;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

const ALERT_COLLECTION: &str = "ZTF_alerts";

#[derive(Parser)]
#[command(about = "Fit per-object phase curves used to score activity")]
struct Cli {
    /// Path to the configuration file.
    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    /// Objects per bulk write.
    #[arg(long, default_value_t = 1_000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Fit and report without writing.
    #[arg(long, default_value_t = false)]
    dry_run: bool,

    /// Stop after this many objects. Unbounded when absent.
    #[arg(long)]
    limit: Option<usize>,
}

/// Fit every band of one object that has enough points to support one.
fn fit_bands(points: &[Point]) -> HashMap<u8, PhaseCurve> {
    let mut by_band: HashMap<u8, Vec<Point>> = HashMap::new();
    for p in points {
        by_band.entry(p.band).or_default().push(*p);
    }
    by_band
        .into_iter()
        .filter_map(|(band, band_points)| Some((band, fit(&band_points)?)))
        .collect()
}

/// One archived detection, or `None` when it is missing photometry or geometry.
fn point_from(doc: &Document) -> Option<(String, Point)> {
    let candidate = doc.get_document("candidate").ok()?;
    let sso = doc
        .get_document("properties")
        .ok()?
        .get_document("sso")
        .ok()?;
    let number = |d: &Document, key: &str| d.get(key).and_then(boom::utils::bson_number);
    Some((
        candidate.get_str("ssnamenr").ok()?.to_string(),
        Point {
            rh: number(sso, "helio_dist")?,
            delta: number(sso, "topo_dist")?,
            phase: number(sso, "phase_angle")?,
            mag: number(candidate, "magpsf")?,
            mag_err: number(candidate, "sigmapsf")?,
            band: number(candidate, "fid")? as u8,
        },
    ))
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

    let alerts: mongodb::Collection<Document> = db.collection(ALERT_COLLECTION);
    let baselines: mongodb::Collection<Document> = db.collection(BASELINES_COLLECTION);
    let now = chrono::Utc::now().timestamp() as f64;

    // Sorted by designation so one object's detections arrive together and only
    // that object is ever held in memory. The same index serves the sort.
    let cursor = alerts
        .find(doc! {
            "candidate.ssnamenr": { "$exists": true },
            "properties.sso.helio_dist": { "$ne": null },
            // A static source near the predicted track carries the object's
            // designation but not its brightness, and would set the baseline
            // that later detections are judged against.
            "candidate.ssdistnr": { "$gte": 0.0, "$lt": MAX_SEPARATION_ARCSEC },
        })
        .projection(doc! {
            "_id": 0,
            "candidate.ssnamenr": 1, "candidate.fid": 1,
            "candidate.magpsf": 1, "candidate.sigmapsf": 1,
            "properties.sso.helio_dist": 1, "properties.sso.topo_dist": 1,
            "properties.sso.phase_angle": 1,
        })
        .sort(doc! { "candidate.ssnamenr": 1 })
        .no_cursor_timeout(true)
        .await;

    let mut cursor = match cursor {
        Ok(cursor) => cursor,
        Err(e) => {
            error!("failed to query {}: {}", ALERT_COLLECTION, e);
            std::process::exit(1);
        }
    };

    let mut pending: Vec<WriteModel> = Vec::with_capacity(args.batch_size);
    let (mut objects, mut fitted, mut written) = (0usize, 0usize, 0usize);
    let mut current: Option<(String, Vec<Point>)> = None;
    let mut done = false;

    loop {
        let next = match cursor.try_next().await {
            Ok(next) => next,
            Err(e) => {
                error!("failed to read {}: {}", ALERT_COLLECTION, e);
                break;
            }
        };
        let exhausted = next.is_none();

        let incoming = next.as_ref().and_then(point_from);
        let boundary = match (&current, &incoming) {
            (Some((name, _)), Some((next_name, _))) => name != next_name,
            (Some(_), None) => exhausted,
            _ => false,
        };

        if boundary || (exhausted && current.is_some()) {
            if let Some((name, points)) = current.take() {
                objects += 1;
                let curves = fit_bands(&points);
                if !curves.is_empty() {
                    fitted += 1;
                    let document = baseline_document(&name, &curves, now);
                    pending.push(WriteModel::ReplaceOne(
                        ReplaceOneModel::builder()
                            .namespace(baselines.namespace())
                            .filter(doc! { "_id": &name })
                            .replacement(document)
                            .upsert(true)
                            .build(),
                    ));
                }
                if args.limit.is_some_and(|limit| objects >= limit) {
                    done = true;
                }
            }
        }

        if pending.len() >= args.batch_size || (done || exhausted) && !pending.is_empty() {
            if args.dry_run {
                written += pending.len();
                pending.clear();
            } else {
                match db.client().bulk_write(std::mem::take(&mut pending)).await {
                    Ok(result) => {
                        written += (result.modified_count + result.upserted_count) as usize
                    }
                    Err(e) => warn!("bulk write failed: {}", e),
                }
            }
            info!(objects, fitted, written, "progress");
        }

        if done || exhausted {
            break;
        }

        if let Some((name, point)) = incoming {
            match &mut current {
                Some((current_name, points)) if *current_name == name => points.push(point),
                _ => current = Some((name, vec![point])),
            }
        }
    }

    info!(objects, fitted, written, dry_run = args.dry_run, "finished");
}
