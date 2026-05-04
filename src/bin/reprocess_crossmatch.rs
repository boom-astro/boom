use std::collections::HashMap;

use boom::{
    conf::{load_dotenv, AppConfig, CatalogXmatchConfig},
    utils::{
        data::make_progress_bar,
        enums::Survey,
        spatial::{cm_radius_arcsec, distance_kpc_from_arcsec, get_f64_from_doc, xmatch},
    },
};
use clap::{Parser, ValueEnum};
use flare::spatial::great_circle_distance;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, Bson, Document},
    options::{UpdateOneModel, WriteModel},
};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// Binary for reprocessing crossmatches between a survey's aux collection and one or more catalogs.
/// The scheduler pipeline only crossmatches at first insert, so adding a catalog to
/// `crossmatch.<survey>` in config.yaml leaves pre-existing aux records with
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

    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    #[arg(long, default_value = "5000")]
    batch_size: usize,
}

/// Reprocessing can be done in two directions:
/// - Checking the crossmatch catalogs for each aux record,
/// - Checking the aux collection for each catalog record.
///
/// To optimize the reprocessing, the binary can loop over either
/// the aux collection or the catalog collection, depending on which is smaller.
/// If `--direction` is not provided, it checks the estimated document counts
/// of each collection and loops over the smaller one.
#[derive(Copy, Clone, Debug, ValueEnum)]
enum Direction {
    /// Pick `objects` or `catalog` per catalog based on which side has fewer rows.
    Auto,
    /// Loop over aux records, query catalog. Best when aux is smaller.
    Objects,
    /// Loop over catalog rows, query aux. Best when catalog is smaller.
    Catalog,
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

async fn run_objects_driven(
    survey: &Survey,
    catalog_config: &CatalogXmatchConfig,
    db: &mongodb::Database,
    batch_size: usize,
) -> Result<u64, mongodb::error::Error> {
    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey));
    let client = db.client();

    let estimated = aux_collection.estimated_document_count().await.unwrap_or(0);
    let pb = make_progress_bar(estimated, format!("objects→{}", catalog_config.catalog));
    let cat_field = format!("cross_matches.{}", catalog_config.catalog);

    let mut cursor = aux_collection
        .find(doc! {})
        .projection(doc! { "_id": 1, "coordinates": 1 })
        .no_cursor_timeout(true)
        .await?;

    let configs = vec![catalog_config.clone()];
    let mut pending_writes: Vec<WriteModel> = Vec::with_capacity(batch_size);
    let mut updated = 0u64;

    while let Some(d) = cursor.try_next().await? {
        pb.inc(1);

        let object_id = match d.get_str("_id") {
            Ok(s) => s.to_string(),
            Err(_) => continue,
        };
        let (ra, dec) = match extract_radec(&d) {
            Some(v) => v,
            None => {
                warn!(object_id = %object_id, "missing/invalid coordinates, skipping");
                continue;
            }
        };

        let xmatch_result = match xmatch(ra, dec, &configs, db).await {
            Ok(r) => r,
            Err(e) => {
                warn!(object_id = %object_id, error = %e, "xmatch failed, skipping");
                continue;
            }
        };
        let matches = xmatch_result
            .get(&catalog_config.catalog)
            .cloned()
            .unwrap_or_default();

        pending_writes.push(WriteModel::UpdateOne(
            UpdateOneModel::builder()
                .namespace(aux_collection.namespace())
                .filter(doc! { "_id": &object_id })
                .update(doc! { "$set": { &cat_field: matches } })
                .build(),
        ));

        if pending_writes.len() >= batch_size {
            let n = pending_writes.len() as u64;
            client
                .bulk_write(std::mem::take(&mut pending_writes))
                .await?;
            updated += n;
        }
    }

    if !pending_writes.is_empty() {
        let n = pending_writes.len() as u64;
        client.bulk_write(pending_writes).await?;
        updated += n;
    }

    pb.finish();
    Ok(updated)
}

async fn run_catalog_driven(
    survey: &Survey,
    catalog_config: &CatalogXmatchConfig,
    db: &mongodb::Database,
    batch_size: usize,
    run_start_jd: f64,
) -> Result<u64, mongodb::error::Error> {
    let aux_collection: mongodb::Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey));
    let cat_collection: mongodb::Collection<Document> = db.collection(&catalog_config.catalog);
    let client = db.client();
    let cat_field = format!("cross_matches.{}", catalog_config.catalog);

    // Skip aux records inserted during this run: the live worker already
    // populated their cross_matches via the live xmatch.
    let pre_existing = doc! { "created_at": { "$lt": run_start_jd } };

    let empty_array: Vec<Document> = Vec::new();
    aux_collection
        .update_many(
            pre_existing.clone(),
            doc! { "$set": { &cat_field: empty_array } },
        )
        .await?;

    let mut cat_projection = catalog_config.projection.clone();
    cat_projection.insert("_id", 1);
    cat_projection.insert("ra", 1);
    cat_projection.insert("dec", 1);
    if let Some(dk) = &catalog_config.distance_key {
        cat_projection.insert(dk.as_str(), 1);
    }

    let cat_estimated = cat_collection.estimated_document_count().await.unwrap_or(0);
    let pb = make_progress_bar(cat_estimated, format!("catalog→{}", catalog_config.catalog));

    let mut cursor = cat_collection
        .find(doc! {})
        .projection(cat_projection)
        .no_cursor_timeout(true)
        .await?;

    let mut pending: HashMap<String, Vec<Document>> = HashMap::new();
    let mut total_pushes = 0u64;
    let mut cat_count = 0u64;

    while let Some(cat_doc) = cursor.try_next().await? {
        cat_count += 1;
        pb.inc(1);

        let cat_ra = match get_f64_from_doc(&cat_doc, "ra") {
            Some(v) => v,
            None => continue,
        };
        let cat_dec = match get_f64_from_doc(&cat_doc, "dec") {
            Some(v) => v,
            None => continue,
        };

        let use_distance_data: Option<(f64, f64)> = if catalog_config.use_distance {
            let dk = catalog_config
                .distance_key
                .as_ref()
                .expect("validated in config");
            let z = match get_f64_from_doc(&cat_doc, dk) {
                Some(v) => v,
                None => continue,
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
                "$geoWithin": { "$centerSphere": [[cat_ra_geojson, cat_dec], catalog_config.radius] }
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
            total_pushes += 1;
        }

        if cat_count % (batch_size as u64) == 0 && !pending.is_empty() {
            flush_pending(client, &aux_collection, &cat_field, &mut pending).await?;
        }
    }

    if !pending.is_empty() {
        flush_pending(client, &aux_collection, &cat_field, &mut pending).await?;
    }
    pb.finish();

    aux_collection
        .update_many(
            doc! { format!("{}.0", &cat_field): { "$exists": true } },
            make_sort_trim_pipeline(catalog_config),
        )
        .await?;

    Ok(total_pushes)
}

async fn flush_pending(
    client: &mongodb::Client,
    aux_collection: &mongodb::Collection<Document>,
    cat_field: &str,
    pending: &mut HashMap<String, Vec<Document>>,
) -> Result<(), mongodb::error::Error> {
    let drained: Vec<(String, Vec<Document>)> = pending.drain().collect();
    let models: Vec<WriteModel> = drained
        .into_iter()
        .map(|(aux_id, docs)| {
            WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(aux_collection.namespace())
                    .filter(doc! { "_id": aux_id })
                    .update(doc! { "$push": { cat_field: { "$each": docs } } })
                    .build(),
            )
        })
        .collect();
    if !models.is_empty() {
        client.bulk_write(models).await?;
    }
    Ok(())
}

/// Mongo-aggregation mirror of the in-Rust sort/trim performed by
/// `utils::spatial::xmatch` (see that function for the source of truth on
/// ordering semantics). `use_distance` and `max_results` are mutually
/// exclusive at config load.
fn make_sort_trim_pipeline(catalog_config: &CatalogXmatchConfig) -> Vec<Document> {
    let cat_field = format!("cross_matches.{}", catalog_config.catalog);
    let sort_by = if catalog_config.use_distance {
        doc! { "distance_kpc": 1, "distance_arcsec": 1 }
    } else {
        doc! { "distance_arcsec": 1 }
    };
    let sorted = doc! { "$sortArray": { "input": format!("${}", &cat_field), "sortBy": sort_by } };
    let final_value = if let Some(max) = catalog_config.max_results {
        Bson::Document(doc! { "$slice": [sorted, max as i64] })
    } else {
        Bson::Document(sorted)
    };
    vec![doc! { "$set": { &cat_field: final_value } }]
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

    let default_config_path = "config.yaml".to_string();
    let config_path = args.config.unwrap_or_else(|| {
        tracing::warn!("no --config provided, using {}", default_config_path);
        default_config_path
    });
    let config = match AppConfig::from_path(&config_path) {
        Ok(c) => c,
        Err(e) => {
            error!("failed to load config from {}: {}", config_path, e);
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
                config_path,
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
                    config_path,
                );
                std::process::exit(1);
            }
        }
    }

    let run_start_jd = flare::Time::now().to_jd();
    info!(
        "starting reprocess: survey={} catalogs={:?} direction={:?} run_start_jd={:.6}",
        args.survey, args.catalogs, args.direction, run_start_jd
    );

    for catalog_config in &resolved {
        let direction = match args.direction {
            Direction::Auto => pick_direction(&args.survey, catalog_config, &db).await,
            d => d,
        };
        info!(
            "=== catalog '{}': direction={:?} ===",
            catalog_config.catalog, direction
        );
        let result = match direction {
            Direction::Auto => unreachable!(),
            Direction::Objects => {
                run_objects_driven(&args.survey, catalog_config, &db, args.batch_size).await
            }
            Direction::Catalog => {
                run_catalog_driven(
                    &args.survey,
                    catalog_config,
                    &db,
                    args.batch_size,
                    run_start_jd,
                )
                .await
            }
        };
        match result {
            Ok(n) => info!(
                "catalog '{}': done ({} updates / pushes)",
                catalog_config.catalog, n
            ),
            Err(e) => {
                error!(
                    "catalog '{}': failed ({}). Stopping.",
                    catalog_config.catalog, e
                );
                std::process::exit(1);
            }
        }
    }

    info!("reprocess_crossmatch complete.");
}
