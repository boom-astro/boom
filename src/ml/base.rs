use crate::{
    conf,
    ml::models::ModelError,
    utils::{
        fits::CutoutError,
        o11y::metrics::SCHEDULER_METER,
        worker::{should_terminate, WorkerCmd},
    },
};

use std::{num::NonZero, sync::LazyLock};

use mongodb::bson::Document;
use opentelemetry::{
    metrics::{Counter, UpDownCounter},
    KeyValue,
};
use redis::AsyncCommands;
use tokio::sync::mpsc;
use tracing::{debug, error, instrument};

// NOTE: Global instruments are defined here because reusing instruments is
// considered a best practice. See boom::alert::base.

// UpDownCounter for the number of alert batches currently being processed by the ML workers.
static ML_WORKER_ACTIVE: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .i64_up_down_counter("ml_worker.active")
        .with_unit("{batch}")
        .with_description("Number of alert batches currently being processed by the ML worker.")
        .build()
});

// Counter for the number of alert batches processed by the ML workers.
static ML_WORKER_BATCH_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .u64_counter("ml_worker.batch.processed")
        .with_unit("{batch}")
        .with_description("Number of alert batches processed by the ML worker.")
        .build()
});

// Counter for the number of alerts processed by the ML workers.
static ML_WORKER_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .u64_counter("ml_worker.alert.processed")
        .with_unit("{alert}")
        .with_description("Number of alerts processed by the ML worker.")
        .build()
});

#[derive(thiserror::Error, Debug)]
pub enum MLWorkerError {
    #[error("failed to access document field")]
    MissingDocumentField(#[from] mongodb::bson::document::ValueAccessError),
    #[error("error from mongodb")]
    Mongodb(#[from] mongodb::error::Error),
    #[error("error from redis")]
    Redis(#[from] redis::RedisError),
    #[error("failed to read config")]
    ReadConfigError(#[from] conf::BoomConfigError),
    #[error("failed to run model")]
    RunModelError(#[from] ModelError),
    #[error("could not access cutout images")]
    CutoutAccessError(#[from] CutoutError),
}

#[async_trait::async_trait]
pub trait MLWorker {
    async fn new(config_path: &str) -> Result<Self, MLWorkerError>
    where
        Self: Sized;
    fn input_queue_name(&self) -> String;
    fn output_queue_name(&self) -> String;
    async fn fetch_alerts(
        &self,
        candids: &[i64], // this is a slice of candids to process
    ) -> Result<Vec<Document>, MLWorkerError>;
    async fn process_alerts(&mut self, alerts: &[i64]) -> Result<Vec<String>, MLWorkerError>;
}

#[tokio::main]
#[instrument(skip_all, err)]
pub async fn run_ml_worker<T: MLWorker>(
    mut receiver: mpsc::Receiver<WorkerCmd>,
    config_path: &str,
    worker_id: String,
) -> Result<(), MLWorkerError> {
    debug!(?config_path);
    let mut ml_worker = T::new(config_path).await?;

    let config = conf::load_config(config_path)?;
    let mut con = conf::build_redis(&config).await?;

    let input_queue = ml_worker.input_queue_name();
    let output_queue = ml_worker.output_queue_name();

    let command_interval: usize = 500;
    let mut command_check_countdown = command_interval;

    let worker_id_attr = KeyValue::new("worker.id", worker_id);
    let ml_worker_active_attrs = [worker_id_attr.clone()];
    let ml_worker_ok_attrs = [worker_id_attr.clone(), KeyValue::new("status", "ok")];
    let ml_worker_input_error_attrs = [
        worker_id_attr.clone(),
        KeyValue::new("status", "error"),
        KeyValue::new("error.type", "input_queue"),
    ];
    let ml_worker_processing_error_attrs = [
        worker_id_attr.clone(),
        KeyValue::new("status", "error"),
        KeyValue::new("error.type", "processing"),
    ];
    let ml_worker_output_error_attrs = [
        worker_id_attr,
        KeyValue::new("status", "error"),
        KeyValue::new("error.type", "output_queue"),
    ];
    loop {
        if command_check_countdown == 0 {
            if should_terminate(&mut receiver) {
                break;
            }
            command_check_countdown = command_interval;
        }

        ML_WORKER_ACTIVE.add(1, &ml_worker_active_attrs);
        let candids: Vec<i64> = con
            .rpop::<&str, Vec<i64>>(&input_queue, NonZero::new(1000))
            .await
            .inspect_err(|_| {
                ML_WORKER_ACTIVE.add(-1, &ml_worker_active_attrs);
                ML_WORKER_BATCH_PROCESSED.add(1, &ml_worker_input_error_attrs);
            })?;

        if candids.is_empty() {
            ML_WORKER_ACTIVE.add(-1, &ml_worker_active_attrs);
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            command_check_countdown = 0;
            continue;
        }

        let batch_size = candids.len() as u64;
        let processed_alerts = ml_worker.process_alerts(&candids).await.inspect_err(|_| {
            ML_WORKER_ACTIVE.add(-1, &ml_worker_active_attrs);
            ML_WORKER_BATCH_PROCESSED.add(1, &ml_worker_processing_error_attrs);
        })?;
        command_check_countdown = command_check_countdown.saturating_sub(candids.len());

        con.lpush::<&str, Vec<String>, usize>(&output_queue, processed_alerts)
            .await
            .inspect_err(|_| {
                ML_WORKER_ACTIVE.add(-1, &ml_worker_active_attrs);
                ML_WORKER_BATCH_PROCESSED.add(1, &ml_worker_output_error_attrs);
            })?;

        let attributes = &ml_worker_ok_attrs;
        ML_WORKER_ACTIVE.add(-1, &ml_worker_active_attrs);
        ML_WORKER_BATCH_PROCESSED.add(1, attributes);
        ML_WORKER_PROCESSED.add(batch_size, attributes);
    }

    Ok(())
}
