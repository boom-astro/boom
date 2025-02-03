use boom::{conf, scheduling::ThreadPool, worker_util, worker_util::WorkerType};
use config::Config;
use std::{
    env,
    sync::{Arc, Mutex},
    thread,
};
use tracing::{info, warn, Level};
use tracing_subscriber::FmtSubscriber;

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

    // get env::args for stream_name and config_path
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        print!(
            "usage: scheduler <stream_name> <config_path> <topic>, where `config_path` is optional"
        );
        return;
    }

    let stream_name = args[1].to_string();
    let config_path = if args.len() > 2 {
        &args[2]
    } else {
        warn!("No config file provided, using `config.yaml`");
        "./config.yaml"
    }
    .to_string();

    let topic = match args.get(3) {
        Some(topic) => topic.to_string(),
        None => {
            // get the topic of the day, which is ztf_YYYYMMDD_programid
            // where YYYYMMDD is the current UTC date
            let date = chrono::Utc::now().format("%Y%m%d").to_string();
            let topic = format!("ztf_{}_programid1", date);
            warn!("No topic provided, using {}", topic);
            topic
        }
    };

    let config_file = conf::load_config(config_path.as_str()).unwrap();
    // get num workers from config file

    let n_alert = get_num_workers(config_file.to_owned(), stream_name.as_str(), "alert");
    let n_ml = get_num_workers(config_file.to_owned(), stream_name.as_str(), "ml");
    let n_filter = get_num_workers(config_file.to_owned(), stream_name.as_str(), "filter");

    // setup signal handler thread
    let interrupt = Arc::new(Mutex::new(false));
    worker_util::sig_int_handler(Arc::clone(&interrupt)).await;

    info!("creating alert, ml, and filter workers...");
    // note: maybe use &str instead of String for stream_name and config_path to reduce clone calls
    let alert_pool = ThreadPool::new(
        WorkerType::Alert,
        n_alert as usize,
        stream_name.clone(),
        config_path.clone(),
        None,
    );
    let ml_pool = ThreadPool::new(
        WorkerType::ML,
        n_ml as usize,
        stream_name.clone(),
        config_path.clone(),
        None,
    );
    let filter_pool = ThreadPool::new(
        WorkerType::Filter,
        n_filter as usize,
        stream_name.clone(),
        config_path.clone(),
        None,
    );
    info!("created workers");
    let consumer_pool = ThreadPool::new(
        WorkerType::Consumer,
        1,
        stream_name.clone(),
        config_path.clone(),
        Some(topic),
    );

    loop {
        info!("heart beat (MAIN)");
        let exit = worker_util::check_flag(Arc::clone(&interrupt));
        if exit {
            warn!("received termination command, shutting down workers...");
            drop(alert_pool);
            drop(ml_pool);
            drop(filter_pool);
            drop(consumer_pool);
            break;
        }
        thread::sleep(std::time::Duration::from_secs(1));
    }
    std::process::exit(0);
}
