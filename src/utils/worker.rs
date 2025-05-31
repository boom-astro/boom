use crate::utils::o11y::DEBUG;

use config::Config;
use std::{
    fmt,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc::{error::TryRecvError, Receiver};
use tracing::{info, instrument, span, warn, Instrument};

// spawns a thread which listens for interrupt signal. Sets flag to true upon signal interruption
#[instrument(skip_all)]
pub fn spawn_sigint_handler(flag: Arc<Mutex<bool>>) {
    tokio::spawn(
        async move {
            tokio::signal::ctrl_c().await.unwrap();
            warn!("received interrupt signal");
            let mut flag = flag.try_lock().unwrap();
            *flag = true;
        }
        .instrument(span!(DEBUG, "sigint handler")),
    );
}

// checks if a flag is set to true and, if so, exits the program
pub fn check_exit(flag: Arc<Mutex<bool>>) {
    match flag.try_lock() {
        Ok(x) => {
            if *x {
                std::process::exit(0)
            }
        }
        _ => {}
    }
}

// checks returns value of flag
#[instrument(skip_all)]
pub fn check_flag(flag: Arc<Mutex<bool>>) -> bool {
    match flag.try_lock() {
        Ok(x) => {
            if *x {
                true
            } else {
                false
            }
        }
        _ => false,
    }
}

pub fn get_check_command_interval(conf: Config, stream_name: &str) -> i64 {
    let table = conf
        .get_table("workers")
        .expect("worker table not found in config");
    let stream_table = table
        .get(stream_name)
        .expect(format!("stream name {} not found in config", stream_name).as_str())
        .to_owned()
        .into_table()
        .unwrap();
    let check_command_interval = stream_table
        .get("command_interval")
        .expect("command_interval not found in config")
        .to_owned()
        .into_int()
        .unwrap();
    return check_command_interval;
}

#[derive(Clone, Debug)]
pub enum WorkerType {
    Alert,
    Filter,
    ML,
}

impl Copy for WorkerType {}

impl fmt::Display for WorkerType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let enum_str;
        match self {
            WorkerType::Alert => {
                enum_str = "Alert";
            }
            WorkerType::Filter => enum_str = "Filter",
            WorkerType::ML => enum_str = "ML",
        }
        write!(f, "{}", enum_str)
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum WorkerCmd {
    TERM,
}

impl fmt::Display for WorkerCmd {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let enum_str = match self {
            WorkerCmd::TERM => "TERM",
        };
        write!(f, "{}", enum_str)
    }
}

#[instrument(skip_all)]
pub(crate) fn should_terminate(receiver: &mut Receiver<WorkerCmd>) -> bool {
    match receiver.try_recv() {
        Ok(WorkerCmd::TERM) => {
            info!("received termination command");
            true
        }
        Err(TryRecvError::Disconnected) => {
            warn!("disconnected from worker command sender");
            true
        }
        Err(TryRecvError::Empty) => false,
    }
}
