//! Runs queued tasks.
//!
//! Claims one run at a time from `task_runs`, holds a lease on it, renews that
//! lease with a heartbeat, and streams progress and logs back to Mongo while
//! the task runs. See [`docs/task-system.md`](../../docs/task-system.md).
//!
//! Deliberately a separate service from the API: these jobs run for hours and
//! are memory-hungry, and an API restart or deploy must not kill one. When this
//! process does go away mid-run, the run's lease lapses and the next worker to
//! start picks it back up -- which is why every task body is written to be
//! resumable.

use boom::conf::{load_dotenv, AppConfig};
use boom::tasks::{
    self,
    models::{self, TaskStatus},
    queue, TaskContext, TaskError,
};
use boom::utils::o11y::logging::build_subscriber;
use clap::Parser;
use mongodb::Database;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

/// How long to wait before looking for work again.
///
/// A couple of seconds is imperceptible for a job that runs for hours, and it
/// keeps the claim query down to a handful of indexed lookups a minute.
const POLL_INTERVAL: Duration = Duration::from_secs(2);

/// Renew well inside the lease so a slow round trip does not cost us the run.
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs((queue::LEASE_SECONDS / 3.0) as u64);

#[derive(Parser)]
#[command(about = "Claim and run queued BOOM tasks")]
struct Cli {
    /// Path to the configuration file.
    #[arg(long, value_name = "FILE")]
    config: Option<String>,

    /// Name this worker reports as. Defaults to the hostname, which is what
    /// makes a stuck run traceable back to a container.
    #[arg(long, env = "BOOM_TASK_WORKER_NAME")]
    name: Option<String>,
}

#[tokio::main]
async fn main() {
    let (subscriber, _guard) = build_subscriber().expect("failed to build subscriber");
    tracing::subscriber::set_global_default(subscriber).expect("failed to install subscriber");
    load_dotenv();
    let args = Cli::parse();

    let config_path = args.config.unwrap_or_else(|| "config.yaml".to_string());
    let config = Arc::new(AppConfig::from_path(&config_path).expect("failed to load config"));
    let db = config.build_db().await.expect("failed to connect to mongo");

    models::initialize_indexes(&db)
        .await
        .expect("failed to create task indexes");

    let worker_name = args.name.unwrap_or_else(|| {
        std::env::var("HOSTNAME").unwrap_or_else(|_| format!("worker-{}", uuid::Uuid::new_v4()))
    });
    info!("task worker {} started", worker_name);

    // Set on SIGTERM/ctrl-c. The running task sees it as a cancellation and
    // stops at its next safe point, and the run goes back on the queue rather
    // than being marked failed -- a deploy is not a failure.
    let shutting_down = Arc::new(AtomicBool::new(false));
    spawn_signal_handler(shutting_down.clone());

    while !shutting_down.load(Ordering::Relaxed) {
        // Before claiming: anything whose worker stopped renewing its lease is
        // ours to pick up. Cheap, and it is what recovers a run orphaned by a
        // crash rather than by a clean shutdown.
        if let Err(e) = queue::requeue_expired(&db).await {
            warn!("failed to requeue expired runs: {}", e);
        }

        let claimed = match queue::claim_next(&db, &worker_name).await {
            Ok(claimed) => claimed,
            Err(e) => {
                error!("failed to claim a run: {}", e);
                tokio::time::sleep(POLL_INTERVAL).await;
                continue;
            }
        };
        let Some(run) = claimed else {
            tokio::time::sleep(POLL_INTERVAL).await;
            continue;
        };

        run_one(&db, &config, &worker_name, run, shutting_down.clone()).await;
    }

    info!("task worker {} stopped", worker_name);
}

/// Execute one claimed run, heartbeating for the duration.
async fn run_one(
    db: &Database,
    config: &Arc<AppConfig>,
    worker_name: &str,
    run: tasks::TaskRun,
    shutting_down: Arc<AtomicBool>,
) {
    info!(
        run_id = %run.id,
        task_type = %run.task_type,
        attempt = run.attempts,
        "starting run requested by {}",
        run.actor.username
    );

    let canceled = Arc::new(AtomicBool::new(false));
    let heartbeat = spawn_heartbeat(
        db.clone(),
        run.id.clone(),
        worker_name.to_string(),
        canceled.clone(),
        shutting_down.clone(),
    );

    let ctx = TaskContext::new(db.clone(), config.clone(), &run.id, canceled.clone());
    ctx.info(format!(
        "run {} claimed by {} (attempt {})",
        run.id, worker_name, run.attempts
    ));

    let result = tasks::dispatch(&ctx, &run.task_type, run.params.clone()).await;
    heartbeat.abort();
    ctx.flush_logs().await;

    // A shutdown is not an outcome: hand the run back queued so the replacement
    // worker resumes it, instead of recording a failure someone has to
    // re-submit by hand.
    if shutting_down.load(Ordering::Relaxed) && result.is_err() {
        info!(run_id = %run.id, "shutting down; returning the run to the queue");
        if let Err(e) = queue::release(db, &run.id, worker_name).await {
            error!("failed to release run {}: {}", run.id, e);
        }
        return;
    }

    let (status, error) = match result {
        Ok(report) => {
            ctx.info(format!("run succeeded: {}", report));
            (TaskStatus::Succeeded, None)
        }
        Err(TaskError::Canceled) => {
            ctx.warn("run canceled");
            (TaskStatus::Canceled, None)
        }
        Err(e) => {
            ctx.error(format!("run failed: {}", e));
            (TaskStatus::Failed, Some(e.to_string()))
        }
    };
    ctx.flush_logs().await;
    if let Err(e) = queue::finish(db, &run.id, status, error).await {
        error!("failed to record the outcome of run {}: {}", run.id, e);
    }
}

/// Renew the lease, and mirror a cancellation request into the flag the task
/// polls.
fn spawn_heartbeat(
    db: Database,
    run_id: String,
    worker_name: String,
    canceled: Arc<AtomicBool>,
    shutting_down: Arc<AtomicBool>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(HEARTBEAT_INTERVAL).await;
            if shutting_down.load(Ordering::Relaxed) {
                canceled.store(true, Ordering::Relaxed);
            }
            match queue::heartbeat(&db, &run_id, &worker_name).await {
                Ok(Some(true)) => {
                    info!(run_id = %run_id, "cancellation requested");
                    canceled.store(true, Ordering::Relaxed);
                }
                Ok(Some(false)) => {}
                // The run is no longer ours: our lease lapsed and another
                // worker claimed it. Two workers running the same task would
                // race on the same collection, so stand down.
                Ok(None) => {
                    warn!(run_id = %run_id, "lost the lease on this run; stopping");
                    canceled.store(true, Ordering::Relaxed);
                    return;
                }
                Err(e) => warn!("heartbeat failed for run {}: {}", run_id, e),
            }
        }
    })
}

fn spawn_signal_handler(shutting_down: Arc<AtomicBool>) {
    tokio::spawn(async move {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to listen for SIGTERM");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => info!("received ctrl-c"),
            _ = term.recv() => info!("received SIGTERM"),
        }
        info!("shutting down; the running task will stop at its next safe point");
        shutting_down.store(true, Ordering::Relaxed);
    });
}
