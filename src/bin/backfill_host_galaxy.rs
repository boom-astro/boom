use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};

use boom::{
    conf::{load_dotenv, AppConfig},
    utils::{
        data::{make_progress_bar, spawn_progress_logger},
        db::{index_cuts, merge_filters, shard_field, shard_filters, TaskError, CURSOR_BATCH_SIZE},
        enums::Survey,
        host::{self, HostGalaxyConfig},
        parser::parse_positive_usize,
    },
};
use clap::Parser;
use flare::Time;
use futures::{future, StreamExt, TryStreamExt};
use indicatif::ProgressBar;
use mongodb::{
    bson::{doc, to_bson, Bson, Document},
    error::Error,
    options::{ReturnDocument, UpdateOneModel, WriteModel},
    Client, Collection, Database,
};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// Sample size for the preflight check that the galaxy catalogs are present.
const CATALOG_PROBE_SAMPLE: i64 = 1_000;
const STATE_COLLECTION: &str = "backfill_host_galaxy_state";
const CROSSMATCH_STATE_COLLECTION: &str = "reprocess_crossmatch_state";
const STATUS_RUNNING: &str = "running";
const STATUS_CLEAN: &str = "clean";
const RECORDS_PER_SHARD: u64 = 100_000;
const MAX_SHARDS: u64 = 1_024;

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

    /// Number of records accumulated per shard before a bulk write is issued.
    #[arg(long, default_value_t = 5000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Number of shards scanned at once.
    #[arg(long, default_value_t = 4, value_parser = parse_positive_usize)]
    processes: usize,

    /// Skip records that already carry a `host_galaxy`; leave it off to rescore everything.
    #[arg(long, default_value_t = false)]
    skip_existing: bool,

    /// Discard the progress of an interrupted run and start over.
    #[arg(long, default_value_t = false)]
    restart: bool,

    /// Report what would be written without writing it.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct RunState {
    #[serde(rename = "_id")]
    id: String,
    status: String,
    run_start_jd: f64,
    skip_existing: bool,
    shard_field: String,
    cuts: Vec<Bson>,
    done: Vec<i64>,
    scanned: i64,
    written: i64,
    unreadable: i64,
    updated_at: f64,
}

#[derive(Default)]
struct ShardStats {
    scanned: u64,
    written: u64,
    unreadable: u64,
}

/// Warn when no sampled record carries a shape column, which produces a run
/// that writes empty associations over the whole collection.
///
/// A bare `cross_matches.NED` is not enough: that key predates host association
/// and its rows were projected without `Diam`, so they carry no extent to score.
/// `Diam` is what says a record has been crossmatched under the current config.
async fn probe_catalogs(
    collection: &Collection<Document>,
    config: &HostGalaxyConfig,
) -> Result<(), Error> {
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
) -> Result<Vec<String>, Error> {
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

async fn start_or_resume(
    aux_collection: &Collection<Document>,
    states: &Collection<RunState>,
    state_id: &str,
    estimated: u64,
    args: &Cli,
    label: &str,
) -> Result<RunState, Error> {
    if !args.restart && !args.dry_run {
        if let Some(run) = states.find_one(doc! { "_id": state_id }).await? {
            if run.status == STATUS_RUNNING {
                info!(
                    "[{}] resuming the run started at JD {}: {} of {} shards already done",
                    label,
                    run.run_start_jd,
                    run.done.len(),
                    run.cuts.len() + 1
                );
                if run.skip_existing != args.skip_existing {
                    warn!(
                        "[{}] keeping --skip-existing = {} from the interrupted run",
                        label, run.skip_existing
                    );
                }
                return Ok(run);
            }
        }
    }

    let parts = (estimated / RECORDS_PER_SHARD).clamp(1, MAX_SHARDS) as usize;
    let field = shard_field(aux_collection).await;
    let cuts = index_cuts(aux_collection, parts, field, estimated).await?;
    if parts > 1 && cuts.is_empty() {
        return Err(Error::custom(format!(
            "could not cut shards on the '{}' index",
            field
        )));
    }
    let now = Time::now().to_jd();
    let run = RunState {
        id: state_id.to_string(),
        status: STATUS_RUNNING.to_string(),
        run_start_jd: now,
        skip_existing: args.skip_existing,
        shard_field: field.to_string(),
        cuts,
        done: Vec::new(),
        scanned: 0,
        written: 0,
        unreadable: 0,
        updated_at: now,
    };
    if !args.dry_run {
        states
            .replace_one(doc! { "_id": state_id }, &run)
            .upsert(true)
            .await?;
    }
    Ok(run)
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
    let mut cross_matches = match cross_matches {
        None => return Some(HashMap::new()),
        Some(Bson::Document(cross_matches)) => cross_matches,
        Some(_) => return None,
    };
    let mut matches = HashMap::new();
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

async fn flush(client: &Client, batch: &mut Vec<WriteModel>, dry_run: bool) -> Result<u64, Error> {
    let batch = std::mem::take(batch);
    let count = batch.len() as u64;
    if count > 0 && !dry_run {
        client.bulk_write(batch).ordered(false).await?;
    }
    Ok(count)
}

#[allow(clippy::too_many_arguments)]
async fn backfill_shard(
    aux_collection: Collection<Document>,
    states: Collection<RunState>,
    state_id: String,
    run_start_jd: f64,
    index: usize,
    filter: Document,
    config: HostGalaxyConfig,
    batch_size: usize,
    dry_run: bool,
    pb: ProgressBar,
) -> Result<ShardStats, Error> {
    let client = aux_collection.client().clone();
    let namespace = aux_collection.namespace();
    let catalogs = [config.ned_catalog.as_str(), config.ls_dr10_catalog.as_str()];
    let mut projection = doc! { "_id": 1, "coordinates.radec_geojson.coordinates": 1 };
    for catalog in catalogs {
        projection.insert(format!("cross_matches.{}", catalog), 1);
    }
    let mut cursor = aux_collection
        .find(filter)
        .projection(projection)
        .batch_size(CURSOR_BATCH_SIZE)
        .no_cursor_timeout(true)
        .await?;

    let mut batch = Vec::with_capacity(batch_size);
    let mut stats = ShardStats::default();
    while let Some(mut record) = cursor.try_next().await? {
        stats.scanned += 1;
        pb.inc(1);
        let Some(object_id) = record.remove("_id") else {
            continue;
        };
        let Some((ra, dec)) = position(&record) else {
            stats.unreadable += 1;
            warn!(object_id = %object_id, "unreadable coordinates, left without host_galaxy");
            continue;
        };
        let Some(matches) = galaxy_matches(record.remove("cross_matches"), &catalogs) else {
            stats.unreadable += 1;
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
                stats.unreadable += 1;
                warn!(object_id = %object_id, error = %e, "failed to encode, left without host_galaxy");
                continue;
            }
        };
        batch.push(WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(namespace.clone())
                .filter(doc! { "_id": object_id })
                .update(doc! { "$set": { "host_galaxy": value } })
                .build(),
        ));
        if batch.len() >= batch_size {
            stats.written += flush(&client, &mut batch, dry_run).await?;
        }
    }
    stats.written += flush(&client, &mut batch, dry_run).await?;

    if !dry_run {
        let recorded = states
            .update_one(
                doc! { "_id": state_id, "run_start_jd": run_start_jd },
                doc! {
                    "$addToSet": { "done": index as i64 },
                    "$inc": {
                        "scanned": stats.scanned as i64,
                        "written": stats.written as i64,
                        "unreadable": stats.unreadable as i64,
                    },
                    "$set": { "updated_at": Time::now().to_jd() },
                },
            )
            .await?;
        if recorded.matched_count == 0 {
            return Err(Error::custom(
                "the run state was replaced by another run".to_string(),
            ));
        }
    }
    Ok(stats)
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
    let label = format!("host_galaxy→{}", aux_name);
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

    let aux_collection: Collection<Document> = db.collection(&aux_name);
    if let Err(e) = probe_catalogs(&aux_collection, &config.host_galaxy).await {
        warn!("catalog probe failed, continuing: {}", e);
    }

    let estimated = match aux_collection.estimated_document_count().await {
        Ok(estimated) => estimated,
        Err(e) => {
            error!("failed to estimate the size of {}: {}", aux_name, e);
            std::process::exit(1);
        }
    };
    let states: Collection<RunState> = db.collection(STATE_COLLECTION);
    let run = match start_or_resume(
        &aux_collection,
        &states,
        &aux_name,
        estimated,
        &args,
        &label,
    )
    .await
    {
        Ok(run) => run,
        Err(e) => {
            error!(
                "failed to load or create the run state in {}, --restart discards it: {}",
                STATE_COLLECTION, e
            );
            std::process::exit(1);
        }
    };

    let mut base_filter = doc! { "created_at": { "$lt": run.run_start_jd } };
    if run.skip_existing {
        base_filter.insert("host_galaxy", doc! { "$exists": false });
    }
    let shards = shard_filters(&run.shard_field, &run.cuts);
    let total = shards.len();
    let pending: Vec<(usize, Document)> = shards
        .into_iter()
        .enumerate()
        .filter(|(index, _)| !run.done.contains(&(*index as i64)))
        .map(|(index, shard)| (index, merge_filters(&base_filter, &shard)))
        .collect();
    info!(
        "[{}] {} of {} shards cut on '{}' to go, over records created before JD {}",
        label,
        pending.len(),
        total,
        run.shard_field,
        run.run_start_jd
    );

    let pb = make_progress_bar(estimated, label.clone());
    pb.set_position(run.scanned as u64);
    let logger = spawn_progress_logger(pb.clone(), label.clone());

    let stop = AtomicBool::new(false);
    let outcome: Result<ShardStats, TaskError> = async {
        let mut totals = ShardStats::default();
        let mut first_error = None;
        let mut completed = total - pending.len();
        let mut running = futures::stream::iter(pending)
            .take_while(|_| future::ready(!stop.load(Ordering::Relaxed)))
            .map(|(index, filter)| {
                tokio::spawn(backfill_shard(
                    aux_collection.clone(),
                    states.clone(),
                    aux_name.clone(),
                    run.run_start_jd,
                    index,
                    filter,
                    config.host_galaxy.clone(),
                    args.batch_size,
                    args.dry_run,
                    pb.clone(),
                ))
            })
            .buffer_unordered(args.processes);
        while let Some(joined) = running.next().await {
            match joined
                .map_err(TaskError::from)
                .and_then(|result| result.map_err(TaskError::from))
            {
                Ok(stats) => {
                    completed += 1;
                    info!("[{}] {} of {} shards done", label, completed, total);
                    totals.scanned += stats.scanned;
                    totals.written += stats.written;
                    totals.unreadable += stats.unreadable;
                }
                Err(e) => {
                    error!("[{}] shard failed: {}", label, e);
                    stop.store(true, Ordering::Relaxed);
                    first_error.get_or_insert(e);
                }
            }
        }
        first_error.map_or(Ok(totals), Err)
    }
    .await;
    logger.abort();
    pb.finish();

    let totals = match outcome {
        Ok(totals) => totals,
        Err(e) => {
            error!("backfill failed, rerun the same command to resume: {}", e);
            std::process::exit(1);
        }
    };

    if args.dry_run {
        info!(
            "dry run: {} records would have been updated, {} unreadable",
            totals.written, totals.unreadable
        );
        return;
    }

    let finished = states
        .find_one_and_update(
            doc! { "_id": &aux_name, "run_start_jd": run.run_start_jd },
            doc! { "$set": { "status": STATUS_CLEAN, "updated_at": Time::now().to_jd() } },
        )
        .return_document(ReturnDocument::After)
        .await;
    match finished {
        Ok(Some(state)) => {
            info!(
                "updated host_galaxy on {} of {} scanned records",
                state.written, state.scanned
            );
            if state.unreadable > 0 {
                warn!(
                    "{} records could not be read and were left without host_galaxy",
                    state.unreadable
                );
            }
        }
        Ok(None) => {
            error!("the run state for {} was replaced by another run", aux_name);
            std::process::exit(1);
        }
        Err(e) => {
            error!("failed to mark the run as complete: {}", e);
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
