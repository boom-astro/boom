use boom::{
    conf::{load_dotenv, AppConfig},
    utils::enums::Survey,
};
use clap::Parser;
use futures::TryStreamExt;
use mongodb::bson::{doc, Bson, Document};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

/// Mongo's `$sum` returns Int32 when the value fits and Int64 otherwise, so a
/// plain `get_i64` silently returns 0 for small counts. Accept either width.
fn get_count(doc: &Document, key: &str) -> i64 {
    match doc.get(key) {
        Some(Bson::Int32(v)) => *v as i64,
        Some(Bson::Int64(v)) => *v,
        _ => 0,
    }
}

/// Verify that every alerts_aux record has an entry in `cross_matches` for each
/// catalog declared under `crossmatch.<survey>` in config.yaml. Equivalent to
/// the warning the scheduler emits at startup, but scans the whole collection
/// instead of sampling one record. Reports per-catalog missing counts; if any
/// are non-zero, the user can run `reprocess_crossmatch` to fill the gaps.
///
/// Implementation: a single aggregation pass per survey. For each configured
/// catalog we add a conditional `$sum` to the `$group` stage, so the whole
/// collection is scanned once regardless of catalog count.
#[derive(Parser)]
struct Cli {
    #[arg(long, value_enum)]
    survey: Survey,

    #[arg(long, value_name = "FILE", default_value = "config.yaml")]
    config: String,
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

    let db = match config.build_db().await {
        Ok(db) => db,
        Err(e) => {
            error!("failed to build mongo client: {}", e);
            std::process::exit(1);
        }
    };

    let catalogs = match config.crossmatch.get(&args.survey) {
        Some(v) if !v.is_empty() => v,
        _ => {
            warn!(
                "survey '{}' has no `crossmatch.{}` section (or it is empty) in {}; nothing to check",
                args.survey,
                args.survey.to_string().to_lowercase(),
                args.config,
            );
            return;
        }
    };

    let collection_name = format!("{}_alerts_aux", args.survey);
    let aux_collection: mongodb::Collection<Document> = db.collection(&collection_name);

    // Build a single $group that sums total docs and, per catalog, how many are
    // missing the `cross_matches.<cat>` key. Output field names can't contain
    // dots or special chars, so we key them by catalog index and map back later.
    let mut group = doc! { "_id": mongodb::bson::Bson::Null, "total": { "$sum": 1 } };
    for (idx, cat) in catalogs.iter().enumerate() {
        let path = format!("$cross_matches.{}", cat.catalog);
        group.insert(
            format!("missing_{}", idx),
            doc! {
                "$sum": {
                    "$cond": [
                        { "$eq": [{ "$type": path }, "missing"] },
                        1,
                        0,
                    ]
                }
            },
        );
    }

    info!(
        "scanning {} for {} catalog(s): {}",
        collection_name,
        catalogs.len(),
        catalogs
            .iter()
            .map(|c| c.catalog.as_str())
            .collect::<Vec<_>>()
            .join(", "),
    );

    let mut cursor = match aux_collection
        .aggregate(vec![doc! { "$group": group }])
        .await
    {
        Ok(c) => c,
        Err(e) => {
            error!("aggregation failed: {}", e);
            std::process::exit(1);
        }
    };
    let result = match cursor.try_next().await {
        Ok(Some(d)) => d,
        Ok(None) => {
            info!(
                "collection '{}' is empty, nothing to check",
                collection_name
            );
            return;
        }
        Err(e) => {
            error!("failed to read aggregation result: {}", e);
            std::process::exit(1);
        }
    };

    let total = get_count(&result, "total");
    let mut missing_catalogs: Vec<&str> = Vec::new();
    info!("total alerts_aux records: {}", total);
    for (idx, cat) in catalogs.iter().enumerate() {
        let missing = get_count(&result, &format!("missing_{}", idx));
        if missing == 0 {
            info!("  [OK]      {}: 0 missing", cat.catalog);
        } else {
            warn!(
                "  [MISSING] {}: {} / {} records missing cross_matches",
                cat.catalog, missing, total
            );
            missing_catalogs.push(&cat.catalog);
        }
    }

    if !missing_catalogs.is_empty() {
        warn!(
            "{} catalog(s) have records without cross_matches set. \
             To populate them for existing records, run: \
             `reprocess_crossmatch --survey {} --catalogs {}` \
             with the appropriate processes and batch_size.",
            missing_catalogs.len(),
            args.survey.to_string().to_lowercase(),
            missing_catalogs.join(","),
        );
        std::process::exit(1);
    }

    info!("all catalogs fully populated for survey '{}'", args.survey);
}
