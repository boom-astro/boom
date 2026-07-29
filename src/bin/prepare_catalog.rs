use boom::{
    api::catalogs::WATCHLIST_PREFIX,
    conf::{load_dotenv, AppConfig},
    utils::{
        data::make_progress_bar, db::create_index, parser::parse_positive_usize,
        spatial::Coordinates,
    },
};
use clap::Parser;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, to_bson, Bson, Document},
    options::{UpdateOneModel, WriteModel},
    Namespace,
};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// Binary for turning a raw collection into a catalog boom can crossmatch against.
///
/// It is meant to run right after importing a file (CSV, JSON, ...) into MongoDB
/// by hand, e.g. with Compass' `Add Data -> Import File`. Such an import only
/// gives flat columns, while crossmatch (and the `cone_search` endpoint) needs
/// the same spatial fields the alert pipeline writes:
///
/// - `ra` / `dec` as numbers (Compass imports them as strings unless the type
///   is set column by column), in degrees
/// - `coordinates.radec_geojson`, a GeoJSON point with longitude shifted to
///   `ra - 180` so it fits the `[-180, 180]` range Mongo requires
/// - `coordinates.l` / `coordinates.b`, the galactic coordinates
/// - a `2dsphere` index on `coordinates.radec_geojson`
///
/// Documents whose ra/dec are missing or out of range are left untouched and
/// reported: the `2dsphere` index would otherwise be rejected by Mongo.
///
/// Idempotent: documents that already carry `coordinates` are skipped unless
/// `--force` is passed, and the index creation is a no-op if it already exists.
///
/// ```bash
/// prepare_catalog --catalog watchlist_supernovas
/// ```
///
/// Once done, declare the catalog under `crossmatch.<survey>` in config.yaml,
/// restart the alert workers, and backfill pre-existing alerts with the
/// `reprocess_crossmatch` binary.
#[derive(Parser)]
struct Cli {
    /// Name of the imported collection, e.g. `watchlist_supernovas`.
    #[arg(long)]
    catalog: String,

    /// Field holding the right ascension, in degrees within [0, 360].
    #[arg(long, default_value = "ra")]
    ra_field: String,

    /// Field holding the declination, in degrees within [-90, 90].
    #[arg(long, default_value = "dec")]
    dec_field: String,

    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,

    #[arg(long, default_value_t = 1000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Recompute `coordinates` for documents that already have it.
    #[arg(long, default_value_t = false)]
    force: bool,

    /// Report what would change without writing anything.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

/// Numeric value of a field, also accepting the string form a Compass import
/// produces when the column type is left untyped.
fn as_f64(value: Option<&Bson>) -> Option<f64> {
    match value? {
        Bson::Double(v) => Some(*v),
        Bson::Int32(v) => Some(*v as f64),
        Bson::Int64(v) => Some(*v as f64),
        Bson::String(v) => v.trim().parse().ok(),
        _ => None,
    }
}

#[derive(Default)]
struct Report {
    updated: u64,
    missing: u64,
    out_of_range: u64,
    samples: Vec<String>,
}

impl Report {
    fn reject(&mut self, id: &Bson, reason: &str) {
        if self.samples.len() < 10 {
            self.samples.push(format!("{} ({})", id, reason));
        }
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
            error!("error loading config from {}: {}", args.config, e);
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

    match db.list_collection_names().await {
        Ok(names) => {
            if !names.iter().any(|n| n == &args.catalog) {
                error!(
                    "collection {} does not exist in database {}, import it first",
                    args.catalog,
                    db.name()
                );
                std::process::exit(1);
            }
        }
        Err(e) => {
            error!("error listing collections: {}", e);
            std::process::exit(1);
        }
    }

    let collection = db.collection::<Document>(&args.catalog);
    let namespace = Namespace {
        db: db.name().to_string(),
        coll: args.catalog.clone(),
    };
    let client = db.client();

    let filter = if args.force {
        doc! {}
    } else {
        doc! { "coordinates": { "$exists": false } }
    };
    let total = match collection.count_documents(filter.clone()).await {
        Ok(total) => total,
        Err(e) => {
            error!("error counting documents: {}", e);
            std::process::exit(1);
        }
    };
    info!(
        "{}: {} document(s) to process ({} in total)",
        args.catalog,
        total,
        collection.estimated_document_count().await.unwrap_or(0)
    );

    let mut report = Report::default();

    if total > 0 {
        let mut cursor = match collection
            .find(filter)
            .projection(doc! { &args.ra_field: 1, &args.dec_field: 1 })
            .no_cursor_timeout(true)
            .await
        {
            Ok(cursor) => cursor,
            Err(e) => {
                error!("error querying documents: {}", e);
                std::process::exit(1);
            }
        };

        let pb = make_progress_bar(total, format!("{} coordinates", args.catalog));
        let mut writes: Vec<WriteModel> = Vec::with_capacity(args.batch_size);

        loop {
            let doc = match cursor.try_next().await {
                Ok(Some(doc)) => doc,
                Ok(None) => break,
                Err(e) => {
                    error!("error reading documents: {}", e);
                    std::process::exit(1);
                }
            };
            pb.inc(1);

            let id = match doc.get("_id") {
                Some(id) => id.clone(),
                None => continue,
            };

            let (ra, dec) = match (
                as_f64(doc.get(&args.ra_field)),
                as_f64(doc.get(&args.dec_field)),
            ) {
                (Some(ra), Some(dec)) => (ra, dec),
                _ => {
                    report.missing += 1;
                    report.reject(&id, "missing or non-numeric ra/dec");
                    continue;
                }
            };
            if !(0.0..=360.0).contains(&ra) || !(-90.0..=90.0).contains(&dec) {
                report.out_of_range += 1;
                report.reject(&id, &format!("ra={} dec={} out of range", ra, dec));
                continue;
            }

            let coordinates = to_bson(&Coordinates::new(ra, dec)).expect("coordinates serialize");
            writes.push(WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(namespace.clone())
                    .filter(doc! { "_id": id })
                    .update(doc! { "$set": { "coordinates": coordinates, "ra": ra, "dec": dec } })
                    .build(),
            ));
            report.updated += 1;

            if writes.len() >= args.batch_size && !args.dry_run {
                if let Err(e) = client.bulk_write(std::mem::take(&mut writes)).await {
                    error!("error writing batch: {}", e);
                    std::process::exit(1);
                }
                writes = Vec::with_capacity(args.batch_size);
            }
        }

        if !writes.is_empty() && !args.dry_run {
            if let Err(e) = client.bulk_write(writes).await {
                error!("error writing final batch: {}", e);
                std::process::exit(1);
            }
        }
        pb.finish();
    }

    if args.dry_run {
        info!("dry run: {} document(s) would be updated", report.updated);
    } else {
        info!("updated {} document(s)", report.updated);
    }
    if report.missing > 0 {
        warn!(
            "{} document(s) skipped: no numeric {} / {}",
            report.missing, args.ra_field, args.dec_field
        );
    }
    if report.out_of_range > 0 {
        warn!(
            "{} document(s) skipped: ra outside [0, 360] or dec outside [-90, 90]",
            report.out_of_range
        );
    }
    for sample in &report.samples {
        warn!("  skipped _id {}", sample);
    }

    if args.dry_run {
        info!("dry run: skipping index creation");
        return;
    }

    if let Err(e) = create_index(
        &collection,
        doc! { "coordinates.radec_geojson": "2dsphere" },
        false,
    )
    .await
    {
        error!("error creating the 2dsphere index: {}", e);
        std::process::exit(1);
    }
    info!("2dsphere index on coordinates.radec_geojson ready");

    info!(
        "{} is ready, declare it under crossmatch.<survey> in {} and backfill \
         existing alerts with: reprocess_crossmatch --survey <survey> --catalogs {}",
        args.catalog, args.config, args.catalog
    );
    if args.catalog.starts_with(WATCHLIST_PREFIX) {
        info!(
            "watchlist catalog: grant access with PATCH /users/{{user_id}}/watchlist_access \
             before it can be queried or bound to a filter"
        );
    }
}
