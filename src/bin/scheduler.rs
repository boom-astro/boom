use boom::{
    conf,
    scheduler::{get_num_workers, ThreadPool},
    utils::{
        db::initialize_survey_indexes,
        worker::{check_flag, sig_int_handler, WorkerType},
    },
};
use clap::Parser;
use std::{
    sync::{Arc, Mutex},
    thread,
};
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Parser)]
struct Cli {
    #[arg(required = true, help = "Name of stream to ingest")]
    stream: Option<String>,

    #[arg(long, value_name = "FILE", help = "Path to the configuration file")]
    config: Option<String>,
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Cli::parse();

    let stream_name = match args.stream {
        Some(stream) => stream,
        None => {
            warn!("No stream name provided");
            std::process::exit(1);
        }
    };
    info!("Starting scheduler for {} alert processing", stream_name);

    if !args.config.is_some() {
        warn!("No config file provided, using config.yaml");
    }
    let config_path = match args.config {
        Some(path) => path,
        None => "config.yaml".to_string(),
    };
    let config_file = match conf::load_config(&config_path) {
        Ok(config) => config,
        Err(e) => {
            warn!("could not load config file: {}", e);
            std::process::exit(1);
        }
    };

    // get num workers from config file
    let n_alert = match get_num_workers(config_file.to_owned(), &stream_name, "alert") {
        Ok(n) => n,
        Err(e) => {
            warn!("could not retrieve number of alert workers: {}", e);
            std::process::exit(1);
        }
    };
    let n_ml = match get_num_workers(config_file.to_owned(), &stream_name, "ml") {
        Ok(n) => n,
        Err(e) => {
            warn!("could not retrieve number of ml workers: {}", e);
            std::process::exit(1);
        }
    };
    let n_filter = match get_num_workers(config_file.to_owned(), &stream_name, "filter") {
        Ok(n) => n,
        Err(e) => {
            warn!("could not retrieve number of filter workers: {}", e);
            std::process::exit(1);
        }
    };

    // initialize the indexes for the survey
    let db: mongodb::Database = match conf::build_db(&config_file).await {
        Ok(db) => db,
        Err(e) => {
            warn!("could not connect to database: {}", e);
            std::process::exit(1);
        }
    };
    match initialize_survey_indexes(&stream_name, &db).await {
        Ok(_) => info!("initialized indexes for {}", stream_name),
        Err(e) => {
            warn!("could not initialize indexes for {}: {}", stream_name, e);
            std::process::exit(1);
        }
    }

    // setup signal handler thread
    let interrupt = Arc::new(Mutex::new(false));
    sig_int_handler(Arc::clone(&interrupt)).await;

    info!("creating alert, ml, and filter workers...");
    let alert_pool = ThreadPool::new(
        WorkerType::Alert,
        n_alert as usize,
        stream_name.clone(),
        config_path.clone(),
    );
    let ml_pool = ThreadPool::new(
        WorkerType::ML,
        n_ml as usize,
        stream_name.clone(),
        config_path.clone(),
    );
    let filter_pool = ThreadPool::new(
        WorkerType::Filter,
        n_filter as usize,
        stream_name.clone(),
        config_path.clone(),
    );
    info!("created workers");

    loop {
        info!("heart beat (MAIN)");
        let exit = check_flag(Arc::clone(&interrupt));
        if exit {
            warn!("killed thread(s)");
            drop(alert_pool);
            drop(ml_pool);
            drop(filter_pool);
            break;
        }
        thread::sleep(std::time::Duration::from_secs(1));
    }
    std::process::exit(0);
}
