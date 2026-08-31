//! Refresh the local copy of MPC orbital elements, on demand.
//!
//! The ZTF scheduler refreshes this catalogue on its own (see
//! `mpcorb::refresh_orbits`), so this binary exists for a forced refresh or to
//! validate a parse with `--dry-run`. It is not needed for routine operation.

use boom::conf::{load_dotenv, AppConfig};
use boom::utils::mpcorb::{refresh_orbits, DEFAULT_MPCORB_URL, ORBITS_COLLECTION};
use boom::utils::parser::parse_positive_usize;
use clap::Parser;
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(about = "Refresh MPC orbital elements used to derive solar system geometry")]
struct Cli {
    /// Path to the configuration file.
    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    /// Where to fetch MPCORB from.
    #[arg(long, default_value = DEFAULT_MPCORB_URL)]
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

    let now = chrono::Utc::now().timestamp() as f64;
    // Run from a terminal, so a progress bar is useful.
    match refresh_orbits(db.as_ref(), &args.url, args.batch_size, now, true).await {
        Ok(report) => {
            info!(
                "read {} lines: {} orbits parsed, {} skipped (header/blank/unusable)",
                report.lines, report.parsed, report.skipped
            );
            for sample in &report.rejected_samples {
                warn!("rejected record-shaped line: {}", sample);
            }
            match db {
                Some(_) => info!(
                    "{} refreshed with {} orbits",
                    ORBITS_COLLECTION, report.parsed
                ),
                None => info!("dry run: {} not modified", ORBITS_COLLECTION),
            }
        }
        Err(e) => {
            error!("{}", e);
            std::process::exit(1);
        }
    }
}
