use boom::{
    conf,
    scheduler::{get_num_workers, ThreadPool},
    utils::{
        db::initialize_survey_indexes,
        o11y::build_subscriber,
        worker::{check_flag, sig_int_handler, WorkerType},
    },
};
use clap::Parser;
use std::{
    sync::{Arc, Mutex},
    thread,
};
use tracing::{info, instrument, warn};

const INFO: tracing::Level = tracing::Level::INFO;

#[derive(Parser)]
struct Cli {
    #[arg(help = "Name of stream to ingest")]
    stream: String,

    #[arg(long, value_name = "FILE", help = "Path to the configuration file")]
    config: Option<String>,
}

#[instrument(level = INFO, skip_all)]
async fn run(args: Cli) {
    let default_config_path = "config.yaml".to_string();
    let config_path = args.config.unwrap_or_else(|| {
        warn!("no config file provided, using {}", default_config_path);
        default_config_path
    });
    let config = conf::load_config(&config_path).expect("could not load config file");

    // get num workers from config file
    let n_alert = get_num_workers(&config, &args.stream, "alert")
        .expect("could not retrieve number of alert workers");
    let n_ml = get_num_workers(&config, &args.stream, "ml")
        .expect("could not retrieve number of ml workers");
    let n_filter = get_num_workers(&config, &args.stream, "filter")
        .expect("could not retrieve number of filter workers");

    // initialize the indexes for the survey
    let db: mongodb::Database = conf::build_db(&config)
        .await
        .expect("could not create mongodb client");
    initialize_survey_indexes(&args.stream, &db)
        .await
        .expect("could not initialize indexes");

    // setup signal handler thread
    let interrupt = Arc::new(Mutex::new(false));
    sig_int_handler(Arc::clone(&interrupt)).await;

    let alert_pool = ThreadPool::new(
        WorkerType::Alert,
        n_alert as usize,
        args.stream.clone(),
        config_path.clone(),
    );
    let ml_pool = ThreadPool::new(
        WorkerType::ML,
        n_ml as usize,
        args.stream.clone(),
        config_path.clone(),
    );
    let filter_pool = ThreadPool::new(
        WorkerType::Filter,
        n_filter as usize,
        args.stream.clone(),
        config_path.clone(),
    );

    loop {
        info!("heart beat");
        let exit = check_flag(Arc::clone(&interrupt));
        if exit {
            // TODO: fields/context.
            warn!("killed thread(s)");
            drop(alert_pool);
            drop(ml_pool);
            drop(filter_pool);
            break;
        }
        thread::sleep(std::time::Duration::from_secs(1));
    }
}

#[tokio::main]
async fn main() {
    let args = Cli::parse();
    let subscriber = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");
    run(args).await;
}
