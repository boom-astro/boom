use crate::{
    alert::{run_alert_worker, DecamAlertWorker, LsstAlertWorker, ZtfAlertWorker},
    enrichment::{
        models::{SharedModelPool, SharedModels},
        run_enrichment_worker, LsstEnrichmentWorker, ZtfEnrichmentWorker,
    },
    filter::{run_filter_worker, LsstFilterWorker, ZtfFilterWorker},
    utils::{
        enums::Survey,
        o11y::logging::as_error,
        worker::{WorkerCmd, WorkerType},
    },
};

use std::collections::VecDeque;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::sync::mpsc::error::SendError;
use tracing::{debug, error, info, instrument, warn};
use uuid::Uuid;

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
        .get(&survey_name.to_string().to_lowercase())
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

/// Maximum number of times a single worker slot may be respawned within
/// [`RESTART_WINDOW`] before the supervisor gives up and leaves it dead. A dead
/// slot keeps the pool below its configured size, which surfaces on the
/// `scheduler.worker.live` gauge and fires the Grafana degraded-pool alert
/// instead of hot-looping a deterministically-crashing worker forever.
const MAX_RESTARTS_PER_WINDOW: usize = 5;

/// Sliding window over which restarts are counted (and after which a clean run
/// resets a slot's restart budget).
const RESTART_WINDOW: Duration = Duration::from_secs(300);

/// Base delay for the exponential restart backoff (1s, 2s, 4s, ...).
const RESTART_BACKOFF_BASE: Duration = Duration::from_secs(1);

/// Upper bound on the restart backoff delay.
const RESTART_BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Exponential backoff for the Nth restart within the current window:
/// 1s, 2s, 4s, 8s, 16s, capped at [`RESTART_BACKOFF_MAX`].
fn restart_backoff(prior_restarts: usize) -> Duration {
    let shift = prior_restarts.min(5) as u32;
    RESTART_BACKOFF_BASE
        .saturating_mul(1u32 << shift)
        .min(RESTART_BACKOFF_MAX)
}

/// Per-slot bookkeeping for the restart supervisor.
struct RestartState {
    /// Timestamps of respawns still within [`RESTART_WINDOW`].
    history: VecDeque<Instant>,
    /// Earliest instant at which this slot may be respawned again (backoff).
    next_eligible: Instant,
    /// Set once the slot has exhausted its restart budget; it is then left
    /// dead until its history clears so the degraded pool stays visible.
    given_up: bool,
}

impl RestartState {
    fn new() -> Self {
        RestartState {
            history: VecDeque::new(),
            next_eligible: Instant::now(),
            given_up: false,
        }
    }
}

// Thread pool
// allows spawning, killing, and managing of various worker threads through
// the use of a messages
pub struct ThreadPool {
    worker_type: WorkerType,
    survey_name: Survey,
    config_path: String,
    workers: Vec<Worker>,
    /// Restart bookkeeping, one entry per slot in `workers` (kept in lockstep).
    restart_states: Vec<RestartState>,
    shared_model_pool: Option<Arc<SharedModelPool>>,
}

/// Threadpool
///
/// The threadpool manages an array of workers of one type
impl ThreadPool {
    /// Create a new threadpool
    ///
    /// worker_type: a `WorkerType` enum to designate which type of workers this threadpool contains
    /// size: number of workers initially inside of threadpool
    /// survey_name: source stream. e.g. 'ztf'
    /// config_path: path to config file
    #[instrument(skip(config_path, shared_model_pool))]
    pub fn new(
        worker_type: WorkerType,
        size: usize,
        survey_name: Survey,
        config_path: String,
        shared_model_pool: Option<Arc<SharedModelPool>>,
    ) -> Self {
        debug!(?config_path);
        let mut thread_pool = ThreadPool {
            worker_type,
            survey_name,
            config_path,
            workers: Vec::new(),
            restart_states: Vec::new(),
            shared_model_pool,
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
        let worker = self.spawn_worker();
        self.workers.push(worker);
        self.restart_states.push(RestartState::new());
    }

    /// Construct a fresh worker, drawing the next model set from the shared
    /// pool (round-robin spreads GPU mutex contention across devices).
    fn spawn_worker(&self) -> Worker {
        let shared_models = self
            .shared_model_pool
            .as_ref()
            .map(|pool| pool.next_model_set());
        Worker::new(
            self.worker_type,
            self.survey_name.clone(),
            self.config_path.clone(),
            shared_models,
        )
    }

    /// Respawn any workers whose threads have exited, applying exponential
    /// backoff and giving up after [`MAX_RESTARTS_PER_WINDOW`] restarts within
    /// [`RESTART_WINDOW`]. Giving up (rather than respawning forever) means a
    /// worker that crashes deterministically — e.g. on a poison-pill batch —
    /// leaves the pool degraded and visibly alerting instead of hot-looping.
    /// Call this periodically from the scheduler's supervision tick.
    #[instrument(skip(self), fields(worker_type = ?self.worker_type, survey = ?self.survey_name))]
    pub fn supervise(&mut self) {
        let now = Instant::now();
        for i in 0..self.workers.len() {
            let alive = self.workers[i]
                .handle
                .as_ref()
                .map(|h| !h.is_finished())
                .unwrap_or(false);

            // Drop restarts that have aged out of the window.
            {
                let state = &mut self.restart_states[i];
                while state
                    .history
                    .front()
                    .is_some_and(|t| now.duration_since(*t) > RESTART_WINDOW)
                {
                    state.history.pop_front();
                }
            }

            if alive {
                // A worker that has run cleanly long enough for its restart
                // history to clear earns a fresh budget.
                let state = &mut self.restart_states[i];
                if state.history.is_empty() {
                    state.given_up = false;
                }
                continue;
            }

            // Worker is dead — decide whether to respawn it.
            if self.restart_states[i].given_up || now < self.restart_states[i].next_eligible {
                continue;
            }
            if self.restart_states[i].history.len() >= MAX_RESTARTS_PER_WINDOW {
                self.restart_states[i].given_up = true;
                error!(
                    slot = i,
                    restarts = MAX_RESTARTS_PER_WINDOW,
                    window_secs = RESTART_WINDOW.as_secs(),
                    "worker exhausted its restart budget; leaving slot dead so the pool stays degraded and alerts"
                );
                continue;
            }

            // Reap the dead handle (surfacing any panic) before replacing it.
            if let Some(handle) = self.workers[i].handle.take() {
                let tid = handle.thread().id();
                if handle.join().is_err() {
                    warn!(?tid, "dead worker thread had panicked");
                }
            }

            let backoff = restart_backoff(self.restart_states[i].history.len());
            self.workers[i] = self.spawn_worker();

            let state = &mut self.restart_states[i];
            state.history.push_back(now);
            state.next_eligible = now + backoff;
            warn!(
                slot = i,
                restarts_in_window = state.history.len(),
                backoff_secs = backoff.as_secs(),
                "respawned dead worker"
            );
        }
    }

    /// Get the number of live (non-finished) workers in the pool.
    /// This checks each worker's thread handle to see if it's still running.
    pub fn live_worker_count(&self) -> usize {
        self.workers
            .iter()
            .filter(|w| w.handle.as_ref().map(|h| !h.is_finished()).unwrap_or(false))
            .count()
    }

    /// Get the total number of workers in the pool (including finished ones).
    pub fn total_worker_count(&self) -> usize {
        self.workers.len()
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
    _id: Uuid,
}

impl Worker {
    /// Create a new pipeline worker
    ///
    /// worker_type: an instance of enum `WorkerType`
    /// id: unique string identifier
    /// receiver: receiver by which the owning threadpool communicates with the worker
    /// stream_name: name of the stream worker from. e.g. 'ZTF' or 'WINTER'
    /// config_path: path to the config file we are working with
    #[instrument(skip(shared_models))]
    fn new(
        worker_type: WorkerType,
        survey_name: Survey,
        config_path: String,
        shared_models: Option<Arc<SharedModels>>,
    ) -> Worker {
        let id = Uuid::new_v4();
        let (sender, receiver) = mpsc::channel(1);
        // Each thread body deliberately does NOT wrap the worker `run(...)`
        // call in a long-lived span. Each per-alert call inside the worker
        // is its own short-lived span (and therefore its own trace), which
        // is what we want — a single life-of-the-worker span would make
        // every alert a descendant of one ever-growing root trace and Tempo
        // would reject it. The `?tid`/`?survey_name` info is logged once at
        // startup instead of being attached as span fields.
        let handle = match worker_type {
            WorkerType::Alert => thread::spawn(move || {
                let tid = std::thread::current().id();
                info!(?tid, ?survey_name, "starting alert worker");
                debug!(?config_path);
                let run = match survey_name {
                    Survey::Ztf => run_alert_worker::<ZtfAlertWorker>,
                    Survey::Lsst => run_alert_worker::<LsstAlertWorker>,
                    Survey::Decam => run_alert_worker::<DecamAlertWorker>,
                };
                run(receiver, &config_path, id).unwrap_or_else(as_error!("alert worker failed"));
            }),
            WorkerType::Filter => thread::spawn(move || {
                let tid = std::thread::current().id();
                info!(?tid, ?survey_name, "starting filter worker");
                debug!(?config_path);
                let run = match survey_name {
                    Survey::Ztf => run_filter_worker::<ZtfFilterWorker>,
                    Survey::Lsst => run_filter_worker::<LsstFilterWorker>,
                    _ => {
                        error!(
                            "Filter worker not implemented for survey: {:?}",
                            survey_name
                        );
                        return;
                    }
                };
                run(receiver, &config_path, id).unwrap_or_else(as_error!("filter worker failed"));
            }),
            WorkerType::Enrichment => thread::spawn(move || {
                let tid = std::thread::current().id();
                info!(?tid, ?survey_name, "starting enrichment worker");
                debug!(?config_path);
                let run = match survey_name {
                    Survey::Ztf => run_enrichment_worker::<ZtfEnrichmentWorker>,
                    Survey::Lsst => run_enrichment_worker::<LsstEnrichmentWorker>,
                    _ => {
                        error!(
                            "Enrichment worker not implemented for survey: {:?}",
                            survey_name
                        );
                        return;
                    }
                };
                run(receiver, &config_path, id, shared_models)
                    .unwrap_or_else(as_error!("enrichment worker failed"));
            }),
        };

        Worker {
            handle: Some(handle),
            sender,
            _id: id,
        }
    }

    /// Send a termination signal to the worker's thread.
    async fn terminate(&self) -> Result<(), SendError<WorkerCmd>> {
        self.sender.send(WorkerCmd::TERM).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_backoff_grows_exponentially_then_caps() {
        assert_eq!(restart_backoff(0), Duration::from_secs(1));
        assert_eq!(restart_backoff(1), Duration::from_secs(2));
        assert_eq!(restart_backoff(2), Duration::from_secs(4));
        assert_eq!(restart_backoff(3), Duration::from_secs(8));
        assert_eq!(restart_backoff(4), Duration::from_secs(16));
        // 1s << 5 == 32s, clamped to the 30s ceiling.
        assert_eq!(restart_backoff(5), RESTART_BACKOFF_MAX);
        // Large inputs stay clamped (and never panic via shift overflow).
        assert_eq!(restart_backoff(100), RESTART_BACKOFF_MAX);
    }
}
