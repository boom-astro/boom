use crate::{
    conf,
    ml::models::ModelError,
    utils::{
        fits::CutoutError,
        worker::{should_terminate, WorkerCmd},
    },
};
use mongodb::bson::Document;
use redis::AsyncCommands;
use std::num::NonZero;
use tokio::sync::mpsc;
use tracing::{debug, error, instrument};

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
) -> Result<(), MLWorkerError> {
    debug!(?config_path);
    let mut ml_worker = T::new(config_path).await?;

    let config = conf::load_config(config_path)?;
    let mut con = conf::build_redis(&config).await?;

    let input_queue = ml_worker.input_queue_name();
    let output_queue = ml_worker.output_queue_name();

    let command_interval: usize = 500;
    let mut command_check_countdown = command_interval;

    loop {
        if command_check_countdown == 0 {
            if should_terminate(&mut receiver) {
                break;
            }
            command_check_countdown = command_interval;
        }

        let candids: Vec<i64> = con
            .rpop::<&str, Vec<i64>>(&input_queue, NonZero::new(1000))
            .await?;

        if candids.is_empty() {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            command_check_countdown = 0;
            continue;
        }

        let processed_alerts = ml_worker.process_alerts(&candids).await?;
        command_check_countdown = command_check_countdown.saturating_sub(candids.len());

        con.lpush::<&str, Vec<String>, usize>(&output_queue, processed_alerts)
            .await?;
    }

    Ok(())
}
