//! Recompute SNR fields for ZTF and LSST alerts and their lightcurves.
//!
//! The work lives in `boom::tasks::migrate_snr`; this is a thin wrapper so the
//! migration can still be run directly while the task path beds in.
//!
//! **Prefer the task.** Submitting it through the API records who ran it, with
//! which parameters, under which release, and appends to the data-mutation
//! ledger — none of which happens here. A run started this way also dies with
//! the terminal it was started from, and cannot be cancelled from the admin
//! page. See `docs/task-system.md`.

use boom::conf::{load_dotenv, AppConfig};
use boom::tasks::migrate_snr::{run, MigrateSnrParams, SnrSurvey};
use boom::tasks::TaskContext;
use boom::utils::parser::parse_positive_usize;
use clap::Parser;
use std::sync::Arc;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
#[command(about = "Recompute snr_psf, snr_ap and ZTF apFlux from stored photometry")]
struct Cli {
    /// Which survey(s) to migrate: ztf, lsst, or all
    #[arg(long, default_value = "all")]
    survey: String,

    /// Path to the configuration file
    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    /// Number of document IDs to collect per update_many batch
    #[arg(long, default_value_t = 5000, value_parser = parse_positive_usize)]
    batch_size: usize,

    /// Run the validation pass afterwards. Slow.
    #[arg(long, default_value_t = false)]
    validate: bool,
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();
    tracing::subscriber::set_global_default(subscriber).expect("failed to set subscriber");
    load_dotenv();

    let args = Cli::parse();
    let survey = match args.survey.to_lowercase().as_str() {
        "ztf" => SnrSurvey::Ztf,
        "lsst" => SnrSurvey::Lsst,
        "all" => SnrSurvey::All,
        other => {
            error!("unknown survey {other:?}; use ztf, lsst, or all");
            std::process::exit(1);
        }
    };

    let config_path = args.config.unwrap_or_else(|| "config.yaml".to_string());
    let config = Arc::new(AppConfig::from_path(&config_path).expect("failed to load config"));
    let db = config.build_db().await.expect("failed to connect to mongo");

    let params = MigrateSnrParams {
        survey,
        batch_size: args.batch_size,
        validate: args.validate,
    };
    if let Err(e) = params.validate_params() {
        error!("{}", e);
        std::process::exit(1);
    }

    // Detached: no run to attribute to, so nothing is written to the ledger and
    // nothing can cancel it. That asymmetry is the reason to prefer the task.
    let ctx = TaskContext::detached(db, config);
    match run(&ctx, params).await {
        Ok(report) => info!("{}", report),
        Err(e) => {
            error!("{}", e);
            std::process::exit(1);
        }
    }
}
