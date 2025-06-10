use crate::{
    alert::{run_alert_worker, LsstAlertWorker, ZtfAlertWorker},
    filter::{run_filter_worker, LsstFilterWorker, ZtfFilterWorker},
    ml::{run_ml_worker, ZtfMLWorker},
    utils::{
        enums::Survey,
        o11y::{as_error, INFO},
        worker::{WorkerCmd, WorkerType},
    },
};
use std::thread;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tracing::{debug, error, info, instrument, span, warn};

#[derive(thiserror::Error, Debug)]
pub enum SchedulerError {
    #[error("error from config")]
    Config(#[from] config::ConfigError),
}

// get num worker from config file, by stream name and worker type
#[instrument(skip(conf), err)]
pub fn get_num_workers(
    conf: &config::Config,
    survey_name: &Survey,
    worker_type: &str,
) -> Result<i64, SchedulerError> {
    let table = conf.get_table("workers")?;
    let stream_table = table
        .get(&format!("{}", survey_name))
        .ok_or(config::ConfigError::NotFound(
            "survey_name not found in workers table".to_string(),
        ))?
        .to_owned()
        .into_table()?;

    let worker_entry = stream_table
        .get(worker_type)
        .ok_or(config::ConfigError::NotFound(
            "worker_type not found in stream table".to_string(),
        ))?
        .clone()
        .into_table()?;

    let nb_worker = worker_entry
        .get("n_workers")
        .ok_or(config::ConfigError::NotFound(
            "n_workers not found in worker table".to_string(),
        ))?
        .clone()
        .into_int()?;

    Ok(nb_worker)
}

// Thread pool
// allows spawning, killing, and managing of various worker threads through
// the use of a messages
pub struct ThreadPool {
    worker_type: WorkerType,
    survey_name: Survey,
    config_path: String,
    workers: Vec<Worker>,
}

/// Threadpool
///
/// The threadpool manages an array of workers of one type
impl ThreadPool {
    /// Create a new threadpool
    ///
    /// worker_type: a `WorkerType` enum to designate which type of workers this threadpool contains
    /// size: number of workers initially inside of threadpool
    /// survey_name: source stream. e.g. 'ZTF'
    /// config_path: path to config file
    #[instrument(skip(config_path))]
    pub fn new(
        worker_type: WorkerType,
        size: usize,
        survey_name: Survey,
        config_path: String,
    ) -> Self {
        debug!(?config_path);
        let mut thread_pool = ThreadPool {
            worker_type,
            survey_name,
            config_path,
            workers: Vec::new(),
        };
        for _ in 0..size {
            thread_pool.add_worker();
        }
        thread_pool
    }

    /// Send a termination signal to each worker thread.
    #[instrument(skip(self))]
    async fn terminate(&self) {
        for worker in &self.workers {
            let handle = worker
                .handle
                .as_ref()
                .expect("handle already consumed, but that should be impossible");
            let tid = handle.thread().id();
            info!(?tid, "sending termination signal");
            worker.terminate().await.unwrap_or_else(|_| {
                warn!(
                    ?tid,
                    "failed to send termination signal (thread likely already terminated)"
                );
            });
        }
    }

    /// Join all worker threads in the pool.
    #[instrument(skip(self))]
    fn join(&mut self) {
        for worker in &mut self.workers {
            if let Some(handle) = worker.handle.take() {
                let tid = handle.thread().id();
                match handle.join() {
                    Ok(_) => info!(?tid, "successfully shut down worker"),
                    Err(_) => {
                        // NOTE: `JoinHandle::join` produces an error if the
                        // thread panicked. The error value contains the panic
                        // message, but recovering that message is not
                        // straightforward because the error type is opaque.
                        // But, if logging/tracing is enabled for the thread,
                        // then the message will be recorded anyway and we don't
                        // need to worry about capturing it here.
                        warn!(?tid, "worker panicked")
                    }
                }
            }
        }
    }

    /// Add a new worker to the thread pool
    #[instrument(skip(self))]
    fn add_worker(&mut self) {
        self.workers.push(Worker::new(
            self.worker_type,
            self.survey_name.clone(),
            self.config_path.clone(),
        ));
    }
}

// Shut down all workers from the thread pool and drop the threadpool
impl Drop for ThreadPool {
    fn drop(&mut self) {
        futures::executor::block_on(self.terminate());
        self.join();
    }
}

/// Worker Struct
/// The `worker` struct represents a threaded worker which might serve as
/// one of several possible roles in the processing pipeline. A `worker` is
/// controlled completely by a threadpool and has a listening channel through
/// which it listens for commands from it.
pub struct Worker {
    // Needs to be Option because JoinHandle::join() consumes the handle.
    handle: Option<thread::JoinHandle<()>>,
    sender: mpsc::Sender<WorkerCmd>,
}

impl Worker {
    /// Create a new pipeline worker
    ///
    /// worker_type: an instance of enum `WorkerType`
    /// id: unique string identifier
    /// receiver: receiver by which the owning threadpool communicates with the worker
    /// stream_name: name of the stream worker from. e.g. 'ZTF' or 'WINTER'
    /// config_path: path to the config file we are working with
    #[instrument]
    fn new(worker_type: WorkerType, survey_name: Survey, config_path: String) -> Worker {
        let (sender, receiver) = mpsc::channel(1);
        let handle = match worker_type {
            WorkerType::Alert => thread::spawn(move || {
                let tid = std::thread::current().id();
                span!(INFO, "alert worker", ?tid, ?survey_name).in_scope(|| {
                    info!("starting alert worker");
                    debug!(?config_path);
                    let run = match survey_name {
                        Survey::Ztf => run_alert_worker::<ZtfAlertWorker>,
                        Survey::Lsst => run_alert_worker::<LsstAlertWorker>,
                    };
                    run(receiver, &config_path).unwrap_or_else(as_error!("alert worker failed"));
                })
            }),
            WorkerType::Filter => thread::spawn(move || {
                let tid = std::thread::current().id();
                span!(INFO, "filter worker", ?tid, ?survey_name).in_scope(|| {
                    info!("starting filter worker");
                    debug!(?config_path);
                    let run = match survey_name {
                        Survey::Ztf => run_filter_worker::<ZtfFilterWorker>,
                        Survey::Lsst => run_filter_worker::<LsstFilterWorker>,
                    };
                    let key = uuid::Uuid::new_v4().to_string();
                    run(key, receiver, &config_path)
                        .unwrap_or_else(as_error!("filter worker failed"));
                })
            }),
            WorkerType::ML => thread::spawn(move || {
                let tid = std::thread::current().id();
                span!(INFO, "ml worker", ?tid, ?survey_name).in_scope(|| {
                    info!("starting ml worker");
                    debug!(?config_path);
                    let run = match survey_name {
                        Survey::Ztf => run_ml_worker::<ZtfMLWorker>,
                        // we don't have an ML worker for LSST yet
                        Survey::Lsst => {
                            error!("LSST ML worker not implemented");
                            return;
                        }
                    };
                    run(receiver, &config_path).unwrap_or_else(as_error!("ml worker failed"));
                })
            }),
        };

        Worker {
            handle: Some(handle),
            sender,
        }
    }

    /// Send a termination signal to the worker's thread.
    async fn terminate(&self) -> Result<(), SendError<WorkerCmd>> {
        self.sender.send(WorkerCmd::TERM).await
    }
}
