use boom::{
    conf,
    scheduling::ThreadPool,
    utils::worker::{check_flag, sig_int_handler, WorkerType},
};
use clap::Parser;
use config::Config;
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

fn get_num_workers(conf: Config, stream_name: &str, worker_type: &str) -> i64 {
    let table = conf
        .get_table("workers")
        .expect("worker table not found in config");
    let stream_table = table
        .get(stream_name)
        .expect(format!("stream name {} not found in config", stream_name).as_str())
        .to_owned()
        .into_table()
        .unwrap();
    let worker_entry = stream_table
        .get(worker_type)
        .expect(format!("{} not found in config", worker_type).as_str());
    let worker_entry = worker_entry.to_owned().into_table().unwrap();
    let n = worker_entry.get("n_workers").expect(
        format!(
            "n_workers not found for {} entry in worker table",
            worker_type
        )
        .as_str(),
    );
    let n = n
        .to_owned()
        .into_int()
        .expect(format!("n_workers for {} no of type int", worker_type).as_str());
    return n;
}

#[tokio::main]
async fn main() {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let args = Cli::parse();

    let stream_name = args.stream.unwrap();
    info!("Starting scheduler for {} alert processing", stream_name);

    if !args.config.is_some() {
        warn!("No config file provided, using config.yaml");
    }
    let config_path = if args.config.is_some() {
        args.config.unwrap()
    } else {
        String::from("./config.yaml")
    };
    let config_file = conf::load_config(&config_path).unwrap();

    // get num workers from config file
    let n_alert = get_num_workers(config_file.to_owned(), &stream_name, "alert");
    let n_ml = get_num_workers(config_file.to_owned(), &stream_name, "ml");
    let n_filter = get_num_workers(config_file.to_owned(), &stream_name, "filter");

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
