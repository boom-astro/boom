use std::collections::HashMap;

use boom::{
    conf::{load_dotenv, AppConfig},
    utils::{
        data::{make_progress_bar, spawn_progress_logger},
        db::{join_tasks, TaskError, CURSOR_BATCH_SIZE},
        enums::Survey,
        host::{self, HostGalaxyConfig},
        parser::parse_positive_usize,
    },
};
use clap::Parser;
use futures::TryStreamExt;
use indicatif::ProgressBar;
use mongodb::{
    bson::{doc, to_bson, Bson, Document},
    options::{UpdateOneModel, WriteModel},
    Database, Namespace,
};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

const QUEUE_MULTIPLIER: usize = 2;
/// Sample size for the preflight check that the galaxy catalogs are present.
const CATALOG_PROBE_SAMPLE: i64 = 1_000;
const CROSSMATCH_STATE_COLLECTION: &str = "reprocess_crossmatch_state";
const STATUS_CLEAN: &str = "clean";

/// Fill in `host_galaxy` on a survey's alerts_aux records.
///
/// Association is a pure function of the galaxy cross-matches already stored on
/// each record, so this reads `cross_matches` rather than re-querying the
/// catalogs. Records written before `host_galaxy.enabled` was turned on, or
/// before the galaxy catalogs were added to `crossmatch.<survey>`, have no
/// field at all; a change to the scoring parameters instead leaves a stale one.
///
/// A record whose cross-matches predate the galaxy catalogs needs
/// `reprocess_crossmatch --catalogs NED,LSDR10` first: without those entries
/// there is nothing to associate against and this writes an empty association.
#[derive(Parser)]
struct Cli {
    #[arg(long, value_enum)]
    survey: Survey,

    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    /// Number of records accumulated per worker before a bulk write is issued.
    #[arg(long, default_value_t = 5000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Number of parallel worker tasks.
    #[arg(long, default_value_t = 4, value_parser = parse_positive_usize)]
    processes: usize,

    /// Skip records that already carry a `host_galaxy`, which makes an
    /// interrupted run resumable. Leave it off to rescore everything after a
    /// change to the association parameters.
    #[arg(long, default_value_t = false)]
    skip_existing: bool,

    /// Report what would be written without writing it.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

/// Warn when no sampled record carries a shape column, which produces a run
/// that writes empty associations over the whole collection.
///
/// A bare `cross_matches.NED` is not enough: that key predates host association
/// and its rows were projected without `Diam`, so they carry no extent to score.
/// `Diam` is what says a record has been crossmatched under the current config.
async fn probe_catalogs(
    collection: &mongodb::Collection<Document>,
    config: &HostGalaxyConfig,
) -> Result<(), mongodb::error::Error> {
    let present = vec![
        doc! { format!("cross_matches.{}.Diam", config.ned_catalog): { "$exists": true } },
        doc! { format!("cross_matches.{}", config.ls_dr10_catalog): { "$exists": true } },
    ];
    let found = collection
        .aggregate(vec![
            doc! { "$sample": { "size": CATALOG_PROBE_SAMPLE } },
            doc! { "$match": { "$or": present } },
            doc! { "$limit": 1 },
        ])
        .await?
        .try_next()
        .await?;
    if found.is_none() {
        warn!(
            "none of {} sampled records carry {}.Diam or {}; run reprocess_crossmatch first \
             or every association will be empty",
            CATALOG_PROBE_SAMPLE, config.ned_catalog, config.ls_dr10_catalog
        );
    }
    Ok(())
}

async fn unfinished_reprocesses(
    db: &Database,
    aux_name: &str,
    catalogs: &[&str],
) -> Result<Vec<String>, mongodb::error::Error> {
    let states = db.collection::<Document>(CROSSMATCH_STATE_COLLECTION);
    let mut unfinished = Vec::new();
    for catalog in catalogs {
        let state = states
            .find_one(doc! { "_id": format!("{}:{}", aux_name, catalog) })
            .await?;
        if let Some(state) = state {
            let status = state.get_str("status").unwrap_or("unknown");
            if status != STATUS_CLEAN {
                unfinished.push(format!("{} ({})", catalog, status));
            }
        }
    }
    Ok(unfinished)
}

/// `coordinates.radec_geojson.coordinates` is `[ra - 180, dec]`.
fn position(record: &Document) -> Option<(f64, f64)> {
    let point = record
        .get_document("coordinates")
        .ok()?
        .get_document("radec_geojson")
        .ok()?
        .get_array("coordinates")
        .ok()?;
    let [ra, dec] = point.as_slice() else {
        return None;
    };
    let (ra, dec) = (ra.as_f64()? + 180.0, dec.as_f64()?);
    (ra.is_finite() && dec.is_finite()).then_some((ra, dec))
}

fn galaxy_matches(
    cross_matches: Option<Bson>,
    catalogs: &[&str],
) -> Option<HashMap<String, Vec<Document>>> {
    let mut matches = HashMap::new();
    let Some(cross_matches) = cross_matches else {
        return Some(matches);
    };
    let Bson::Document(mut cross_matches) = cross_matches else {
        return None;
    };
    for catalog in catalogs {
        let Some(rows) = cross_matches.remove(*catalog) else {
            continue;
        };
        let Bson::Array(rows) = rows else {
            return None;
        };
        let rows = rows
            .into_iter()
            .map(|row| match row {
                Bson::Document(row) => Some(row),
                _ => None,
            })
            .collect::<Option<Vec<_>>>()?;
        matches.insert(catalog.to_string(), rows);
    }
    Some(matches)
}

async fn worker(
    rx: async_channel::Receiver<Document>,
    client: mongodb::Client,
    aux_ns: Namespace,
    config: HostGalaxyConfig,
    batch_size: usize,
    dry_run: bool,
    pb: ProgressBar,
) -> Result<u64, mongodb::error::Error> {
    let catalogs = [config.ned_catalog.as_str(), config.ls_dr10_catalog.as_str()];
    let mut batch: Vec<WriteModel> = Vec::with_capacity(batch_size);
    let mut written = 0u64;
    while let Ok(mut record) = rx.recv().await {
        pb.inc(1);
        let Some(object_id) = record.remove("_id") else {
            continue;
        };
        let Some((ra, dec)) = position(&record) else {
            warn!(object_id = %object_id, "unreadable coordinates, left without host_galaxy");
            continue;
        };
        let Some(matches) = galaxy_matches(record.remove("cross_matches"), &catalogs) else {
            warn!(object_id = %object_id, "unreadable cross_matches, left without host_galaxy");
            continue;
        };
        // `enabled` is checked once up front, so this is always `Some`.
        let Some(association) = host::associate_from_xmatches(ra, dec, &matches, &config) else {
            continue;
        };
        let value = match to_bson(&association) {
            Ok(value) => value,
            Err(e) => {
                warn!(object_id = %object_id, error = %e, "failed to encode, left without host_galaxy");
                continue;
            }
        };
        batch.push(WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(aux_ns.clone())
                .filter(doc! { "_id": object_id })
                .update(doc! { "$set": { "host_galaxy": value } })
                .build(),
        ));
        if batch.len() >= batch_size {
            written += batch.len() as u64;
            if !dry_run {
                client
                    .bulk_write(std::mem::take(&mut batch))
                    .ordered(false)
                    .await?;
            } else {
                batch.clear();
            }
        }
    }
    if !batch.is_empty() {
        written += batch.len() as u64;
        if !dry_run {
            client.bulk_write(batch).ordered(false).await?;
        }
    }
    Ok(written)
}

#[tokio::main]
async fn main() {
    load_dotenv();

    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("setting subscriber failed");

    let args = Cli::parse();

    let config = match AppConfig::from_path(&args.config) {
        Ok(c) => c,
        Err(e) => {
            error!("failed to load config from {}: {}", args.config, e);
            std::process::exit(1);
        }
    };

    if !config.host_galaxy.enabled {
        error!(
            "host_galaxy.enabled is false in {}, nothing to do",
            args.config
        );
        std::process::exit(1);
    }

    let db = match config.build_db().await {
        Ok(db) => db,
        Err(e) => {
            error!("failed to build mongo client: {}", e);
            std::process::exit(1);
        }
    };

    let aux_name = format!("{}_alerts_aux", args.survey);
    let catalogs = [
        config.host_galaxy.ned_catalog.as_str(),
        config.host_galaxy.ls_dr10_catalog.as_str(),
    ];
    match unfinished_reprocesses(&db, &aux_name, &catalogs).await {
        Ok(unfinished) if unfinished.is_empty() => {}
        Ok(unfinished) => {
            error!(
                "reprocess_crossmatch has not finished on {} for {}; let it complete first",
                aux_name,
                unfinished.join(", ")
            );
            std::process::exit(1);
        }
        Err(e) => {
            error!("failed to read {}: {}", CROSSMATCH_STATE_COLLECTION, e);
            std::process::exit(1);
        }
    }

    let aux_collection: mongodb::Collection<Document> = db.collection(&aux_name);
    if let Err(e) = probe_catalogs(&aux_collection, &config.host_galaxy).await {
        warn!("catalog probe failed, continuing: {}", e);
    }

    let estimated = aux_collection.estimated_document_count().await.unwrap_or(0);
    let label = format!("host_galaxy→{}", aux_name);
    let pb = make_progress_bar(estimated, label.clone());
    let logger = spawn_progress_logger(pb.clone(), label);

    let queue_capacity = args.processes * args.batch_size * QUEUE_MULTIPLIER;
    let (tx, rx) = async_channel::bounded::<Document>(queue_capacity);

    let aux_ns = Namespace {
        db: db.name().to_string(),
        coll: aux_name.clone(),
    };
    let mut workers = Vec::with_capacity(args.processes);
    for _ in 0..args.processes {
        workers.push(tokio::spawn(worker(
            rx.clone(),
            db.client().clone(),
            aux_ns.clone(),
            config.host_galaxy.clone(),
            args.batch_size,
            args.dry_run,
            pb.clone(),
        )));
    }
    drop(rx);

    let find_filter = if args.skip_existing {
        doc! { "host_galaxy": { "$exists": false } }
    } else {
        doc! {}
    };

    let feed: Result<(), TaskError> = async {
        let mut cursor = aux_collection
            .find(find_filter)
            .projection(doc! {
                "_id": 1,
                "coordinates.radec_geojson.coordinates": 1,
                format!("cross_matches.{}", config.host_galaxy.ned_catalog): 1,
                format!("cross_matches.{}", config.host_galaxy.ls_dr10_catalog): 1,
            })
            .batch_size(CURSOR_BATCH_SIZE)
            .no_cursor_timeout(true)
            .await?;
        while let Some(record) = cursor.try_next().await? {
            if tx.send(record).await.is_err() {
                break;
            }
        }
        Ok(())
    }
    .await;
    drop(tx);

    let outcome = join_tasks(workers, "worker").await;
    logger.abort();
    pb.finish();

    if let Err(e) = feed {
        error!("failed to stream {}: {}", aux_name, e);
        std::process::exit(1);
    }
    match outcome {
        Ok(counts) => {
            let total: u64 = counts.iter().sum();
            if args.dry_run {
                info!("dry run: {} records would have been updated", total);
            } else {
                info!("updated host_galaxy on {} records", total);
            }
        }
        Err(e) => {
            error!("backfill failed: {}", e);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CATALOGS: [&str; 2] = ["NED", "LSDR10"];

    #[test]
    fn test_position_undoes_the_geojson_offset() {
        let record = doc! { "coordinates": { "radec_geojson": { "coordinates": [-170.0, 20.0] } } };
        assert_eq!(position(&record), Some((10.0, 20.0)));
    }

    #[test]
    fn test_position_rejects_a_malformed_point() {
        for coordinates in [
            Bson::Array(vec![Bson::Double(1.0)]),
            Bson::Array(vec![Bson::Double(f64::NAN), Bson::Double(1.0)]),
            Bson::Null,
        ] {
            let record =
                doc! { "coordinates": { "radec_geojson": { "coordinates": coordinates } } };
            assert_eq!(position(&record), None);
        }
    }

    #[test]
    fn test_missing_cross_matches_have_no_rows() {
        assert!(galaxy_matches(None, &CATALOGS).unwrap().is_empty());
        let matches = galaxy_matches(Some(Bson::Document(doc! { "NED": [] })), &CATALOGS).unwrap();
        assert_eq!(matches.get("NED"), Some(&Vec::new()));
        assert!(!matches.contains_key("LSDR10"));
    }

    #[test]
    fn test_malformed_cross_matches_are_unreadable() {
        for cross_matches in [
            Bson::Null,
            Bson::Document(doc! { "NED": Bson::Null }),
            Bson::Document(doc! { "LSDR10": [1] }),
        ] {
            assert!(galaxy_matches(Some(cross_matches), &CATALOGS).is_none());
        }
    }
}
