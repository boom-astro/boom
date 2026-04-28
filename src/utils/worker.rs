use config::Config;
use std::{
    fmt,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc::{error::TryRecvError, Receiver};
use tracing::{info, instrument, warn};

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
    Enrichment,
    Filter,
}

impl Copy for WorkerType {}

impl fmt::Display for WorkerType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let enum_str = match self {
            WorkerType::Alert => "Alert",
            WorkerType::Filter => "Filter",
            WorkerType::Enrichment => "Enrichment",
        };
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_type_display() {
        assert_eq!(format!("{}", WorkerType::Alert), "Alert");
        assert_eq!(format!("{}", WorkerType::Enrichment), "Enrichment");
        assert_eq!(format!("{}", WorkerType::Filter), "Filter");
    }

    #[test]
    fn test_worker_type_clone_copy() {
        let wt = WorkerType::Enrichment;
        let wt2 = wt; // Copy
        let wt3 = wt.clone(); // Clone
        assert_eq!(format!("{}", wt2), "Enrichment");
        assert_eq!(format!("{}", wt3), "Enrichment");
    }

    #[test]
    fn test_worker_cmd_display() {
        assert_eq!(format!("{}", WorkerCmd::TERM), "TERM");
    }

    #[tokio::test]
    async fn test_should_terminate_on_term_signal() {
        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        tx.send(WorkerCmd::TERM).await.unwrap();
        assert!(should_terminate(&mut rx));
    }

    #[tokio::test]
    async fn test_should_terminate_empty_channel() {
        let (_tx, mut rx) = tokio::sync::mpsc::channel::<WorkerCmd>(1);
        assert!(!should_terminate(&mut rx));
    }

    #[tokio::test]
    async fn test_should_terminate_disconnected() {
        let (tx, mut rx) = tokio::sync::mpsc::channel::<WorkerCmd>(1);
        drop(tx); // disconnect sender
        assert!(should_terminate(&mut rx));
    }
}
