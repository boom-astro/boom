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
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    // get env::args for stream_name and config_path
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        print!("usage: scheduler <stream_name> <config_path>, where `config_path` is optional");
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

    let config_file = conf::load_config(config_path.as_str()).unwrap();
    // get num workers from config file

    let n_alert = get_num_workers(config_file.to_owned(), stream_name.as_str(), "alert");
    let n_ml = get_num_workers(config_file.to_owned(), stream_name.as_str(), "ml");
    let n_filter = get_num_workers(config_file.to_owned(), stream_name.as_str(), "filter");

    // setup signal handler thread
    let interrupt_exit = Arc::new(Mutex::new(false));
    worker_util::sig_int_handler(Arc::clone(&interrupt_exit)).await;
    let interrupt_cmd = Arc::new(Mutex::new("".to_string()));
    let interrupt_cmd_response = Arc::new(Mutex::new("".to_string()));
    worker_util::sig_cmd_handler(
        Arc::clone(&interrupt_cmd),
        Arc::clone(&interrupt_cmd_response),
    )
    .await;

    info!("creating alert, ml, and filter workers...");
    // note: maybe use &str instead of String for stream_name and config_path to reduce clone calls
    let mut alert_pool = ThreadPool::new(
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

    // start a timer
    let mut start = std::time::Instant::now();
    loop {
        // every 60 seconds, print a heart beat
        if start.elapsed().as_secs() > 60 {
            info!("scheduler is running...");
            start = std::time::Instant::now();
        }
        let mut exit = worker_util::check_flag(Arc::clone(&interrupt_exit));

        let cmd = worker_util::check_cmd(Arc::clone(&interrupt_cmd));
        match cmd {
            Some(cmd) => {
                let parts: Vec<&str> = cmd.split(" ").collect();
                if parts.len() > 0 {
                    let parts: Vec<&str> = cmd.split(" ").collect();
                    match parts[0] {
                        "add" => {
                            let worker_type = match parts[1] {
                                "alert" => Some(WorkerType::Alert),
                                "ml" => Some(WorkerType::ML),
                                "filter" => Some(WorkerType::Filter),
                                _ => {
                                    let mut response = interrupt_cmd_response.lock().unwrap();
                                    *response = "invalid worker type".to_string();
                                    drop(response);
                                    None
                                }
                            };
                            match worker_type {
                                Some(WorkerType::Alert) => {
                                    let id = alert_pool.add_worker();
                                    let mut response = interrupt_cmd_response.lock().unwrap();
                                    *response = format!("added alert worker with id: {}", id);
                                    drop(response);
                                }
                                _ => {
                                    let mut response = interrupt_cmd_response.lock().unwrap();
                                    *response = "Not implemented yet".to_string();
                                    drop(response);
                                }
                            }
                        }
                        "remove" => {
                            let worker_type = match parts[1] {
                                "alert" => Some(WorkerType::Alert),
                                "ml" => Some(WorkerType::ML),
                                "filter" => Some(WorkerType::Filter),
                                _ => {
                                    info!("invalid worker type");
                                    let mut response = interrupt_cmd_response.lock().unwrap();
                                    *response = "invalid worker type".to_string();
                                    drop(response);
                                    None
                                }
                            };
                            match worker_type {
                                Some(WorkerType::Alert) => {
                                    let worker_id = parts[2].parse::<String>().unwrap();
                                    // first check if a worker with that id exists
                                    if !alert_pool.senders.contains_key(&worker_id) {
                                        let mut response = interrupt_cmd_response.lock().unwrap();
                                        *response = "worker not found".to_string();
                                        drop(response);
                                        continue;
                                    }

                                    alert_pool.remove_worker(worker_id);
                                    let mut response = interrupt_cmd_response.lock().unwrap();
                                    *response = "removed worker".to_string();
                                    drop(response);
                                }
                                _ => {
                                    info!("Not implemented yet");
                                }
                            }
                        }
                        "ls" => {
                            let worker_type = match parts[1] {
                                "alert" => Some(WorkerType::Alert),
                                "ml" => Some(WorkerType::ML),
                                "filter" => Some(WorkerType::Filter),
                                _ => {
                                    let mut response = interrupt_cmd_response.lock().unwrap();
                                    *response = "invalid worker type".to_string();
                                    drop(response);
                                    None
                                }
                            };
                            match worker_type {
                                Some(WorkerType::Alert) => {
                                    let ids: Vec<String> =
                                        alert_pool.senders.keys().cloned().collect();
                                    let mut response_temp = "".to_string();
                                    let mut i = 0;
                                    for id in ids {
                                        response_temp.push_str(&i.to_string());
                                        response_temp.push_str(" - alert worker: ");
                                        response_temp.push_str(&id);
                                        response_temp.push_str("\n");
                                        i += 1;
                                    }
                                    response_temp.push_str("\ntotal alert workers: ");
                                    response_temp.push_str(&i.to_string());
                                    let mut response = interrupt_cmd_response.lock().unwrap();
                                    *response = response_temp;
                                    drop(response);
                                }
                                _ => {
                                    let mut response = interrupt_cmd_response.lock().unwrap();
                                    *response = "Not implemented yet".to_string();
                                    drop(response);
                                }
                            }
                        }
                        "stop" => {
                            info!("stopping workers...");
                            let mut response = interrupt_cmd_response.lock().unwrap();
                            *response = "stopping workers...".to_string();
                            drop(response);
                            exit = true;
                        }
                        _ => {
                            let mut response = interrupt_cmd_response.lock().unwrap();
                            *response = "invalid command".to_string();
                            drop(response);
                        }
                    }
                }

                // reset cmd
                let mut cmd = interrupt_cmd.lock().unwrap();
                *cmd = "".to_string();
                drop(cmd);
            }
            None => {}
        }

        if exit {
            info!("received exit signal");
            drop(alert_pool);
            drop(ml_pool);
            drop(filter_pool);
            break;
        }
        thread::sleep(std::time::Duration::from_secs(1));
    }
}
