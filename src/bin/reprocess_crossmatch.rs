use std::collections::HashMap;

use boom::{
    conf::{load_dotenv, AppConfig, CatalogXmatchConfig},
    utils::{
        data::make_progress_bar,
        enums::Survey,
        parser::parse_positive_usize,
        spatial::{
            cm_radius_arcsec, distance_kpc_from_arcsec, get_f64_from_doc, xmatch, Coordinates,
        },
    },
};
use clap::{Parser, ValueEnum};
use flare::{spatial::great_circle_distance, Time};
use futures::TryStreamExt;
use indicatif::ProgressBar;
use mongodb::{
    bson::{doc, Document},
    options::{UpdateOneModel, WriteModel},
    Namespace,
};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

const QUEUE_MULTIPLIER: usize = 2;

/// Binary for reprocessing crossmatches between a survey's alerts_aux collection and one or more catalogs.
/// The scheduler pipeline only crossmatches at first insert, so adding a catalog to
/// `crossmatch.<survey>` in config.yaml leaves pre-existing alerts_aux records with
/// no entry for it, so this binary fills in those gaps. It can also be used to reprocess existing
/// crossmatches if the matching parameters (e.g. radius) for a catalog are changed.
#[derive(Parser)]
struct Cli {
    #[arg(long, value_enum)]
    survey: Survey,

    /// Each catalog must already be declared under `crossmatch.<survey>` in
    /// config.yaml (radius / projection / etc. are read from there).
    #[arg(long, value_delimiter = ',', num_args = 1..)]
    catalogs: Vec<String>,

    #[arg(long, value_enum, default_value_t = Direction::Auto)]
    direction: Direction,

    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    #[arg(long, default_value_t = 5000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Number of parallel worker tasks. Each worker holds its own DB connection.
    #[arg(long, default_value_t = 1, value_parser = parse_positive_usize)]
    processes: usize,
}

/// Reprocessing can be done in two directions:
/// - Checking the crossmatch catalogs for each alerts_aux record,
/// - Checking the alerts_aux collection for each catalog record.
///
/// To optimize the reprocessing, the binary can loop over either
/// the alerts_aux collection or the catalog collection, depending on which is smaller.
/// If `--direction` is not provided, it checks the estimated document counts
/// of each collection and loops over the smaller one.
#[derive(Copy, Clone, Debug, Eq, PartialEq, ValueEnum)]
enum Direction {
    /// Pick `objects` or `catalog` per catalog based on which side has fewer rows.
    Auto,
    /// Loop over alerts_aux records, query catalog. Best when alerts_aux is smaller.
    Objects,
    /// Loop over catalog rows, query aux. Best when catalog is smaller.
    Catalog,
}

#[derive(serde::Deserialize, serde::Serialize)]
struct AuxIdAndCoords {
    #[serde(rename = "_id")]
    object_id: String,
    coordinates: Coordinates,
}

// -----------------------------------------------------------------------------
// objects-driven: stream alerts_aux records, fan out to N workers running xmatch().
// One pass updates all selected catalogs at once via the existing 1×N xmatch.
// -----------------------------------------------------------------------------
async fn run_objects_driven(
    survey: &Survey,
    catalogs: Vec<CatalogXmatchConfig>,
    db: mongodb::Database,
    batch_size: usize,
    processes: usize,
) -> Result<(), mongodb::error::Error> {
    let aux_collection: mongodb::Collection<AuxIdAndCoords> =
        db.collection(&format!("{}_alerts_aux", survey));
    let estimated = aux_collection.estimated_document_count().await.unwrap_or(0);
    let label = catalogs
        .iter()
        .map(|c| c.catalog.as_str())
        .collect::<Vec<_>>()
        .join(",");
    let pb = make_progress_bar(estimated, format!("objects→{}", label));

    let queue_capacity = processes * batch_size * QUEUE_MULTIPLIER;
    let (tx, rx) = async_channel::bounded::<AuxIdAndCoords>(queue_capacity);

    let mut workers = Vec::with_capacity(processes);
    for _ in 0..processes {
        let rx = rx.clone();
        let pb = pb.clone();
        let survey = survey.clone();
        let db = db.clone();
        let catalogs = catalogs.clone();
        workers.push(tokio::spawn(async move {
            objects_worker(survey, db, catalogs, rx, batch_size, pb).await;
        }));
    }
    drop(rx);

    let mut cursor = aux_collection
        .find(doc! {})
        .projection(doc! { "_id": 1, "coordinates": 1 })
        .no_cursor_timeout(true)
        .await?;
    while let Some(d) = cursor.try_next().await? {
        if tx.send(d).await.is_err() {
            break;
        }
    }
    drop(tx);

    for h in workers {
        if let Err(e) = h.await {
            error!("worker join failed: {}", e);
        }
    }
    pb.finish();
    Ok(())
}

async fn objects_worker(
    survey: Survey,
    db: mongodb::Database,
    catalogs: Vec<CatalogXmatchConfig>,
    rx: async_channel::Receiver<AuxIdAndCoords>,
    batch_size: usize,
    pb: ProgressBar,
) {
    let client = db.client().clone();
    let aux_collection: mongodb::Collection<AuxIdAndCoords> =
        db.collection(&format!("{}_alerts_aux", survey));
    let aux_ns = aux_collection.namespace();

    let mut batch = Vec::with_capacity(batch_size);
    while let Ok(item) = rx.recv().await {
        batch.push(item);
        if batch.len() >= batch_size {
            flush_objects_batch(&db, &client, &aux_ns, &catalogs, &mut batch, &pb).await;
        }
    }
    if !batch.is_empty() {
        flush_objects_batch(&db, &client, &aux_ns, &catalogs, &mut batch, &pb).await;
    }
}

async fn flush_objects_batch(
    db: &mongodb::Database,
    client: &mongodb::Client,
    aux_ns: &Namespace,
    catalogs: &[CatalogXmatchConfig],
    batch: &mut Vec<AuxIdAndCoords>,
    pb: &ProgressBar,
) {
    let mut writes = Vec::with_capacity(batch.len());
    for obj in batch.drain(..) {
        let (ra, dec) = obj.coordinates.get_radec();
        let xmatches = match xmatch(ra, dec, catalogs, db).await {
            Ok(r) => r,
            Err(e) => {
                warn!(object_id = %obj.object_id, error = %e, "xmatch failed, skipping");
                pb.inc(1);
                continue;
            }
        };
        let mut set_doc = Document::new();
        for cat in catalogs {
            let matches = xmatches.get(&cat.catalog).cloned().unwrap_or_default();
            set_doc.insert(format!("cross_matches.{}", cat.catalog), matches);
        }
        writes.push(WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(aux_ns.clone())
                .filter(doc! { "_id": obj.object_id })
                .update(doc! { "$set": set_doc })
                .build(),
        ));
        pb.inc(1);
    }
    if !writes.is_empty() {
        if let Err(e) = client.bulk_write(writes).await {
            error!("bulk_write failed: {}", e);
        }
    }
}

// -----------------------------------------------------------------------------
// catalog-driven: stream catalog rows, fan out to N workers that geo-lookup
// matching alerts_aux records and accumulate $push updates. Uses a temp field
// (`cross_matches_temp.<catalog>`) as a buffer so the `cross_matches.<catalog>` field is
// never empty mid-run.
//
// Concurrency with the live ingest pipeline: every phase is gated on
// `created_at < run_start_jd` so records inserted during the run are left
// completely untouched (the scheduler pipeline already filled their cross_matches).
// Without this guard, phase 4's `$set live = $temp` would overwrite a new record's
// field with a missing/partial temp and silently delete it.
// Phase 5 then cleans up any orphan temp left.
// -----------------------------------------------------------------------------
async fn run_catalog_driven(
    survey: &Survey,
    catalog_config: CatalogXmatchConfig,
    db: mongodb::Database,
    batch_size: usize,
    processes: usize,
) -> Result<(), mongodb::error::Error> {
    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey));
    let cat_collection: mongodb::Collection<Document> = db.collection(&catalog_config.catalog);
    let live_field = format!("cross_matches.{}", catalog_config.catalog);
    let temp_field = format!("cross_matches_temp.{}", catalog_config.catalog);
    let run_start_jd = Time::now().to_jd();
    let existing_records = doc! { "created_at": { "$lt": run_start_jd } };

    // Phase 1: clear temp field on every alerts_aux record that existed at run start
    // so we start from a known empty state. Records inserted later are skipped.
    info!(
        "[catalog→{}] phase 1/5: clearing temp field",
        catalog_config.catalog
    );
    let empty: Vec<Document> = Vec::new();
    aux_collection
        .update_many(
            existing_records.clone(),
            doc! { "$set": { &temp_field: empty } },
        )
        .await?;

    // Phase 2: stream catalog rows through a worker pool, $push matches to temp.
    info!(
        "[catalog→{}] phase 2/5: streaming catalog rows",
        catalog_config.catalog
    );
    let mut cat_projection = catalog_config.projection.clone();
    cat_projection.insert("_id", 1);
    cat_projection.insert("ra", 1);
    cat_projection.insert("dec", 1);
    if let Some(dk) = &catalog_config.distance_key {
        cat_projection.insert(dk.as_str(), 1);
    }

    let cat_estimated = cat_collection.estimated_document_count().await.unwrap_or(0);
    let pb = make_progress_bar(cat_estimated, format!("catalog→{}", catalog_config.catalog));
    let queue_capacity = processes * batch_size * QUEUE_MULTIPLIER;
    let (tx, rx) = async_channel::bounded::<Document>(queue_capacity);

    let mut workers = Vec::with_capacity(processes);
    for _ in 0..processes {
        let rx = rx.clone();
        let pb = pb.clone();
        let survey = survey.clone();
        let db = db.clone();
        let catalog_config = catalog_config.clone();
        let temp_field = temp_field.clone();
        workers.push(tokio::spawn(async move {
            catalog_worker(
                survey,
                db,
                catalog_config,
                temp_field,
                run_start_jd,
                rx,
                batch_size,
                pb,
            )
            .await;
        }));
    }
    drop(rx);

    let mut cursor = cat_collection
        .find(doc! {})
        .projection(cat_projection)
        .no_cursor_timeout(true)
        .await?;
    while let Some(d) = cursor.try_next().await? {
        if tx.send(d).await.is_err() {
            break;
        }
    }
    drop(tx);
    for h in workers {
        if let Err(e) = h.await {
            error!("worker join failed: {}", e);
        }
    }
    pb.finish();

    // Phase 3: sort + trim of accumulated matches per alerts_aux record.
    info!(
        "[catalog→{}] phase 3/5: sorting and trimming temp",
        catalog_config.catalog
    );
    aux_collection
        .update_many(
            doc! {
                "created_at": { "$lt": run_start_jd },
                format!("{}.0", &temp_field): { "$exists": true },
            },
            make_sort_trim_pipeline(&catalog_config, &temp_field),
        )
        .await?;

    // Phase 4: copy temp to live field, then clear temp.
    // Gated on created_at to avoid overwriting new records that arrived during the run.
    info!(
        "[catalog→{}] phase 4/5: swapping temp into live",
        catalog_config.catalog
    );
    aux_collection
        .update_many(
            existing_records,
            vec![
                doc! { "$set": { &live_field: format!("${}", &temp_field) } },
                doc! { "$unset": &temp_field },
            ],
        )
        .await?;

    // Phase 5: clean up any orphan temp fields left on records inserted during the run.
    info!(
        "[catalog→{}] phase 5/5: cleaning orphan temp",
        catalog_config.catalog
    );
    aux_collection
        .update_many(
            doc! { &temp_field: { "$exists": true } },
            doc! { "$unset": { &temp_field: "" } },
        )
        .await?;

    Ok(())
}

async fn catalog_worker(
    survey: Survey,
    db: mongodb::Database,
    catalog_config: CatalogXmatchConfig,
    temp_field: String,
    run_start_jd: f64,
    rx: async_channel::Receiver<Document>,
    batch_size: usize,
    pb: ProgressBar,
) {
    let client = db.client().clone();
    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey));
    let aux_ns = aux_collection.namespace();

    let mut pending: HashMap<String, Vec<Document>> = HashMap::new();
    let mut cat_count = 0u64;

    while let Ok(cat_doc) = rx.recv().await {
        cat_count += 1;
        pb.inc(1);
        if let Err(e) = process_cat_doc(
            &aux_collection,
            &catalog_config,
            run_start_jd,
            &cat_doc,
            &mut pending,
        )
        .await
        {
            warn!(error = %e, "catalog row processing failed, skipping");
        }
        if cat_count % (batch_size as u64) == 0 && !pending.is_empty() {
            flush_pending(&client, &aux_ns, &temp_field, &mut pending).await;
        }
    }
    if !pending.is_empty() {
        flush_pending(&client, &aux_ns, &temp_field, &mut pending).await;
    }
}

async fn process_cat_doc(
    aux_collection: &mongodb::Collection<Document>,
    catalog_config: &CatalogXmatchConfig,
    run_start_jd: f64,
    cat_doc: &Document,
    pending: &mut HashMap<String, Vec<Document>>,
) -> Result<(), mongodb::error::Error> {
    let cat_ra = match get_f64_from_doc(cat_doc, "ra") {
        Some(v) => v,
        None => return Ok(()),
    };
    let cat_dec = match get_f64_from_doc(cat_doc, "dec") {
        Some(v) => v,
        None => return Ok(()),
    };

    let use_distance_data: Option<(f64, f64)> = if catalog_config.use_distance {
        let dk = catalog_config
            .distance_key
            .as_ref()
            .expect("validated in config");
        let z = match get_f64_from_doc(cat_doc, dk) {
            Some(v) => v,
            None => return Ok(()),
        };
        let dmax = catalog_config.distance_max.expect("validated in config");
        let dmax_near = catalog_config
            .distance_max_near
            .expect("validated in config");
        Some((z, cm_radius_arcsec(z, dmax, dmax_near)))
    } else {
        None
    };

    let cat_ra_geojson = cat_ra - 180.0;
    let aux_filter = doc! {
        "coordinates.radec_geojson": {
            "$geoWithin": {
                "$centerSphere": [[cat_ra_geojson, cat_dec], catalog_config.radius]
            }
        },
        "created_at": { "$lt": run_start_jd },
    };
    let mut aux_cursor = aux_collection
        .find(aux_filter)
        .projection(doc! { "_id": 1, "coordinates": 1 })
        .await?;

    while let Some(aux_doc) = aux_cursor.try_next().await? {
        let aux_id = match aux_doc.get_str("_id") {
            Ok(s) => s.to_string(),
            Err(_) => continue,
        };
        let (aux_ra, aux_dec) = match extract_radec(&aux_doc) {
            Some(v) => v,
            None => continue,
        };
        let distance_arcsec = great_circle_distance(aux_ra, aux_dec, cat_ra, cat_dec) * 3600.0;

        let mut match_doc = cat_doc.clone();
        match_doc.insert("distance_arcsec", distance_arcsec);

        if let Some((z, cm_radius)) = use_distance_data {
            if distance_arcsec >= cm_radius {
                continue;
            }
            match_doc.insert("distance_kpc", distance_kpc_from_arcsec(distance_arcsec, z));
        }

        pending.entry(aux_id).or_default().push(match_doc);
    }
    Ok(())
}

async fn flush_pending(
    client: &mongodb::Client,
    aux_ns: &Namespace,
    field: &str,
    pending: &mut HashMap<String, Vec<Document>>,
) {
    let drained: Vec<(String, Vec<Document>)> = pending.drain().collect();
    let models: Vec<WriteModel> = drained
        .into_iter()
        .map(|(aux_id, docs)| {
            WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(aux_ns.clone())
                    .filter(doc! { "_id": aux_id })
                    .update(doc! { "$push": { field: { "$each": docs } } })
                    .build(),
            )
        })
        .collect();
    if !models.is_empty() {
        if let Err(e) = client.bulk_write(models).await {
            error!("bulk_write failed: {}", e);
        }
    }
}

/// `coordinates.radec_geojson.coordinates` is `[ra - 180, dec]`.
fn extract_radec(doc: &Document) -> Option<(f64, f64)> {
    let arr = doc
        .get_document("coordinates")
        .ok()?
        .get_document("radec_geojson")
        .ok()?
        .get_array("coordinates")
        .ok()?;
    if arr.len() != 2 {
        return None;
    }
    let ra_geojson = arr[0].as_f64()?;
    let dec = arr[1].as_f64()?;
    if !ra_geojson.is_finite() || !dec.is_finite() {
        return None;
    }
    Some((ra_geojson + 180.0, dec))
}

/// Mongo-aggregation mirror of the in-Rust sort/trim performed by
/// `utils::spatial::xmatch` (see that function for the source of truth on
/// ordering semantics). `use_distance` and `max_results` are mutually
/// exclusive at config load.
fn make_sort_trim_pipeline(catalog_config: &CatalogXmatchConfig, field: &str) -> Vec<Document> {
    let sort_by = if catalog_config.use_distance {
        doc! { "distance_kpc": 1, "distance_arcsec": 1 }
    } else {
        doc! { "distance_arcsec": 1 }
    };
    let sorted = doc! { "$sortArray": { "input": format!("${}", field), "sortBy": sort_by } };
    let final_value: Document = if let Some(max) = catalog_config.max_results {
        doc! { "$slice": [sorted, max as i64] }
    } else {
        sorted
    };
    vec![doc! { "$set": { field: final_value } }]
}

async fn pick_direction(
    survey: &Survey,
    catalog_config: &CatalogXmatchConfig,
    db: &mongodb::Database,
) -> Direction {
    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey));
    let cat_collection: mongodb::Collection<Document> = db.collection(&catalog_config.catalog);
    let aux_count = aux_collection.estimated_document_count().await.unwrap_or(0);
    let cat_count = cat_collection.estimated_document_count().await.unwrap_or(0);
    info!(
        "auto: catalog '{}' ~{} rows, '{}_alerts_aux' ~{} rows",
        catalog_config.catalog, cat_count, survey, aux_count
    );
    if cat_count < aux_count {
        Direction::Catalog
    } else {
        Direction::Objects
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

    if args.catalogs.is_empty() {
        error!("--catalogs requires at least one catalog name");
        std::process::exit(1);
    }

    let config = match AppConfig::from_path(&args.config) {
        Ok(c) => c,
        Err(e) => {
            error!("failed to load config from {}: {}", args.config, e);
            std::process::exit(1);
        }
    };

    let db = match config.build_db().await {
        Ok(db) => db,
        Err(e) => {
            error!("failed to build mongo client: {}", e);
            std::process::exit(1);
        }
    };

    let survey_configs: &Vec<CatalogXmatchConfig> = match config.crossmatch.get(&args.survey) {
        Some(v) => v,
        None => {
            error!(
                "survey '{}' has no `crossmatch.{}` section in {}",
                args.survey,
                args.survey.to_string().to_lowercase(),
                args.config,
            );
            std::process::exit(1);
        }
    };
    let mut resolved: Vec<CatalogXmatchConfig> = Vec::with_capacity(args.catalogs.len());
    for name in &args.catalogs {
        match survey_configs.iter().find(|c| &c.catalog == name) {
            Some(c) => resolved.push(c.clone()),
            None => {
                error!(
                    "catalog '{}' not declared under crossmatch.{} in {}",
                    name,
                    args.survey.to_string().to_lowercase(),
                    args.config,
                );
                std::process::exit(1);
            }
        }
    }

    // If direction is Auto, split catalogs into two groups based on which collection is smaller.
    let mut objects_catalogs: Vec<CatalogXmatchConfig> = Vec::new();
    let mut catalog_catalogs: Vec<CatalogXmatchConfig> = Vec::new();
    for cat in resolved {
        let direction = match args.direction {
            Direction::Auto => pick_direction(&args.survey, &cat, &db).await,
            d => d,
        };
        match direction {
            Direction::Objects => objects_catalogs.push(cat),
            Direction::Catalog => catalog_catalogs.push(cat),
            Direction::Auto => unreachable!(),
        }
    }

    info!(
        "starting reprocess: survey={} processes={} batch_size={} objects_catalogs={:?} catalog_catalogs={:?}",
        args.survey,
        args.processes,
        args.batch_size,
        objects_catalogs.iter().map(|c| &c.catalog).collect::<Vec<_>>(),
        catalog_catalogs.iter().map(|c| &c.catalog).collect::<Vec<_>>(),
    );

    if !objects_catalogs.is_empty() {
        if let Err(e) = run_objects_driven(
            &args.survey,
            objects_catalogs,
            db.clone(),
            args.batch_size,
            args.processes,
        )
        .await
        {
            error!("objects-driven run failed: {}", e);
            std::process::exit(1);
        }
    }

    for cat in catalog_catalogs {
        let name = cat.catalog.clone();
        if let Err(e) = run_catalog_driven(
            &args.survey,
            cat,
            db.clone(),
            args.batch_size,
            args.processes,
        )
        .await
        {
            error!("catalog-driven run for '{}' failed: {}", name, e);
            std::process::exit(1);
        }
    }

    info!("reprocess_crossmatch complete.");
}
