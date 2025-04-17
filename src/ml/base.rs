use crate::{conf, ml::models::ModelError, utils::fits::CutoutError, utils::worker::WorkerCmd};
use mongodb::bson::Document;
use redis::AsyncCommands;
use std::num::NonZero;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{error, info, warn};

#[derive(thiserror::Error, Debug)]
pub enum MLWorkerError {
    #[error("failed to connect to database")]
    ConnectMongoError(#[source] mongodb::error::Error),
    #[error("failed to retrieve candidates from database")]
    RetrieveCandidatesDataError(#[source] mongodb::error::Error),
    #[error("failed to connect to redis")]
    ConnectRedisError(#[source] redis::RedisError),
    #[error("failed to retrieve candidates from redis")]
    RedisError(#[source] redis::RedisError),
    #[error("failed to push candidates to redis")]
    RedisPushError(#[source] redis::RedisError),
    #[error("failed to read config")]
    ReadConfigError(#[from] conf::BoomConfigError),
    #[error("failed to pop candids from the input queue")]
    PopCandidsError(#[source] redis::RedisError),
    #[error("failed to push candids to the output queue")]
    PushCandidsError(#[source] redis::RedisError),
    #[error("failed to run model")]
    RunModelError(#[from] ModelError),
    #[error("failed to access document field")]
    MissingDocumentField(#[from] mongodb::bson::document::ValueAccessError),
    #[error("error retrieving alerts")]
    ErrorRetrievingAlerts(#[source] mongodb::error::Error),
    #[error("could not access cutout images")]
    CutoutAccessError(#[from] CutoutError),
    #[error("error saving ml results to database")]
    ErrorSavingResults(#[source] mongodb::error::Error),
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
    async fn process_alerts(&self, alerts: &[i64]) -> Result<Vec<String>, MLWorkerError>;
}

#[tokio::main]
pub async fn run_ml_worker<T: MLWorker>(
    id: String,
    mut receiver: mpsc::Receiver<WorkerCmd>,
    config_path: &str,
) -> Result<(), MLWorkerError> {
    let ml_worker = T::new(config_path).await?;

    // in a never ending loop, loop over the queues
    let client_redis = redis::Client::open("redis://localhost:6379".to_string())
        .map_err(MLWorkerError::ConnectRedisError)?;
    let mut con = client_redis
        .get_multiplexed_async_connection()
        .await
        .map_err(MLWorkerError::ConnectRedisError)?;

    let input_queue = ml_worker.input_queue_name();
    let output_queue = ml_worker.output_queue_name();

    let command_interval: i64 = 500;
    let mut command_check_countdown = command_interval;

    loop {
        if command_check_countdown == 0 {
            match receiver.try_recv() {
                Ok(WorkerCmd::TERM) => {
                    info!("alert worker {} received termination command", &id);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    warn!("alert worker {} receiver disconnected, terminating", &id);
                    break;
                }
                Err(TryRecvError::Empty) => {
                    command_check_countdown = command_interval;
                }
            }
        }
        // if the queue is empty, wait for a bit and continue the loop
        let queue_len: i64 = con
            .llen(&input_queue)
            .await
            .map_err(MLWorkerError::ConnectRedisError)?;
        if queue_len == 0 {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            continue;
        }

        // get candids from redis
        let candids: Vec<i64> = con
            .rpop::<&str, Vec<i64>>(&input_queue, NonZero::new(1000))
            .await
            .map_err(MLWorkerError::PopCandidsError)?;

        let nb_candids = candids.len();
        if nb_candids == 0 {
            // sleep for a bit if no alerts were found
            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
            continue;
        }

        let processed_alerts = ml_worker.process_alerts(&candids).await?;

        // push that back to redis to the output queue
        con.lpush::<&str, Vec<String>, usize>(&output_queue, processed_alerts)
            .await
            .map_err(MLWorkerError::PushCandidsError)?;

        command_check_countdown -= nb_candids as i64;
    }

    Ok(())
}
