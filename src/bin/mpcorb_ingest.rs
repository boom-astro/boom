//! Refresh the local copy of MPC orbital elements.
//!
//! Downloads MPCORB, parses it, and swaps the result into `MPC_orbits` keyed by
//! unpacked designation. Enrichment looks elements up by designation and derives
//! observing geometry at the alert's own epoch, so nothing here is per-night and
//! the collection is a plain refresh.
//!
//! Intended to run nightly, before the observing night.

use std::io::{BufRead, BufReader};

use boom::conf::{load_dotenv, AppConfig};
use boom::utils::data::download_to_file;
use boom::utils::mpcorb::{parse_line, to_document, ORBITS_COLLECTION};
use boom::utils::parser::parse_positive_usize;
use clap::Parser;
use mongodb::bson::{doc, Document};
use tempfile::NamedTempFile;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

const TARGET_COLLECTION: &str = ORBITS_COLLECTION;
const STAGING_COLLECTION: &str = "MPC_orbits_staging";
const DEFAULT_URL: &str = "https://www.minorplanetcenter.net/iau/MPCORB/MPCORB.DAT";

#[derive(Parser)]
#[command(about = "Refresh MPC orbital elements used to derive solar system geometry")]
struct Cli {
    /// Path to the configuration file.
    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    /// Where to fetch MPCORB from.
    #[arg(long, default_value = DEFAULT_URL)]
    url: String,

    /// Documents per insert batch.
    #[arg(long, default_value_t = 10_000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Parse and report without writing to the database.
    #[arg(long, default_value_t = false)]
    dry_run: bool,
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to set subscriber");
    load_dotenv();

    let args = Cli::parse();

    // A dry run validates the parse alone, so it needs neither a database nor a
    // config full of secrets.
    let db = if args.dry_run {
        None
    } else {
        let config_path = args.config.unwrap_or_else(|| "config.yaml".to_string());
        let config = AppConfig::from_path(&config_path).expect("failed to load config");
        Some(config.build_db().await.expect("failed to connect to mongo"))
    };

    info!("downloading MPCORB from {}", args.url);
    let mut tmp = NamedTempFile::new().expect("failed to create temp file");
    if let Err(e) = download_to_file(tmp.as_file_mut(), &args.url, None, None, true).await {
        error!("failed to download MPCORB: {}", e);
        std::process::exit(1);
    }

    // Write into a staging collection and rename over the target, so readers
    // never observe a half-populated catalogue.
    let staging = match &db {
        Some(db) => {
            let c = db.collection::<Document>(STAGING_COLLECTION);
            let _ = c.drop().await;
            Some(c)
        }
        None => None,
    };

    let file = std::fs::File::open(tmp.path()).expect("failed to reopen download");
    let now = chrono::Utc::now().timestamp() as f64;

    let mut batch: Vec<Document> = Vec::with_capacity(args.batch_size);
    let (mut parsed, mut skipped, mut lines) = (0u64, 0u64, 0u64);
    // Anything record-shaped that fails to parse is a silent data loss, so keep
    // examples rather than only a count.
    let mut rejected_samples: Vec<String> = Vec::new();

    for line in BufReader::new(file).lines() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                warn!("unreadable line: {}", e);
                continue;
            }
        };
        lines += 1;
        // The file opens with a prose header and separates its three sections
        // with blank lines; parse_line rejects both.
        match parse_line(&line) {
            Some(entry) => {
                batch.push(to_document(&entry, now));
                parsed += 1;
            }
            None => {
                skipped += 1;
                // Blank lines and the prose header are expected; a long line is not.
                if line.len() >= 103 && rejected_samples.len() < 5 {
                    rejected_samples.push(line.chars().take(120).collect());
                }
            }
        }

        if batch.len() >= args.batch_size {
            match &staging {
                Some(c) => {
                    if let Err(e) = c.insert_many(std::mem::take(&mut batch)).await {
                        error!("insert failed: {}", e);
                        std::process::exit(1);
                    }
                }
                None => batch.clear(),
            }
            if parsed % 200_000 == 0 {
                info!("parsed {} orbits", parsed);
            }
        }
    }

    if let (Some(c), false) = (&staging, batch.is_empty()) {
        if let Err(e) = c.insert_many(batch).await {
            error!("final insert failed: {}", e);
            std::process::exit(1);
        }
    }

    info!(
        "read {} lines: {} orbits parsed, {} skipped (header/blank/unusable)",
        lines, parsed, skipped
    );
    for sample in &rejected_samples {
        warn!("rejected record-shaped line: {}", sample);
    }

    // A catalogue this far below expectation means a truncated download; leave
    // the existing collection alone rather than swapping in a partial one.
    if parsed < 100_000 {
        error!(
            "only {} orbits parsed, refusing to replace {}",
            parsed, TARGET_COLLECTION
        );
        std::process::exit(1);
    }

    let Some(db) = db else {
        info!("dry run: {} not modified", TARGET_COLLECTION);
        return;
    };

    let admin = db.client().database("admin");
    let from = format!("{}.{}", db.name(), STAGING_COLLECTION);
    let to = format!("{}.{}", db.name(), TARGET_COLLECTION);
    match admin
        .run_command(doc! { "renameCollection": &from, "to": &to, "dropTarget": true })
        .await
    {
        Ok(_) => info!("{} refreshed with {} orbits", TARGET_COLLECTION, parsed),
        Err(e) => {
            error!("failed to swap staging into {}: {}", TARGET_COLLECTION, e);
            std::process::exit(1);
        }
    }
}
