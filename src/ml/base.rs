use crate::{conf, utils::worker::WorkerCmd};
use mongodb::bson::Document;
use tokio::sync::mpsc;

#[derive(thiserror::Error, Debug)]
pub enum MLWorkerError {
    #[error("failed to connect to database")]
    ConnectMongoError(#[from] mongodb::error::Error),
    #[error("failed to connect to redis")]
    ConnectRedisError(#[from] redis::RedisError),
    #[error("failed to read config")]
    ReadConfigError(#[from] conf::BoomConfigError),
}

#[async_trait::async_trait]
pub trait MLWorker {
    async fn new(
        id: String,
        receiver: mpsc::Receiver<WorkerCmd>,
        config_path: &str,
    ) -> Result<Self, MLWorkerError>
    where
        Self: Sized;
    async fn fetch_alerts(
        &self,
        candids: &[i64], // this is a slice of candids to process
    ) -> Result<Vec<Document>, MLWorkerError>;
    async fn run(&mut self) -> Result<(), MLWorkerError>;
}

#[tokio::main]
pub async fn run_ml_worker<T: MLWorker>(
    id: String,
    receiver: mpsc::Receiver<WorkerCmd>,
    config_path: &str,
) -> Result<(), MLWorkerError> {
    let mut ml_worker = T::new(id, receiver, config_path).await?;
    ml_worker.run().await?;
    Ok(())
}
