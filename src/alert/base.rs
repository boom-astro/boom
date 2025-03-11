use redis::AsyncCommands;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{error, info, trace, warn};

use crate::utils::worker::WorkerCmd;
use crate::{conf, utils::db::CreateIndexError};

#[derive(thiserror::Error, Debug)]
pub enum SchemaRegistryError {
    #[error("invalid schema")]
    InvalidSchema(#[source] apache_avro::Error),
    #[error("invalid version")]
    InvalidVersion,
    #[error("invalid subject")]
    InvalidSubject,
    #[error("connection error")]
    ConnectionError(#[source] reqwest::Error),
    #[error("parsing error")]
    ParsingError(#[source] reqwest::Error),
    #[error("could not find expected content in response")]
    InvalidResponse,
}

#[derive(thiserror::Error, Debug)]
pub enum AlertError {
    #[error("failed to decode alert")]
    DecodeError(#[source] apache_avro::Error),
    #[error("failed to find candid in the alert collection")]
    FindCandIdError(#[source] mongodb::error::Error),
    #[error("failed to insert into the alert collection")]
    InsertAlertError(#[source] mongodb::error::Error),
    #[error("failed to find objectid in the aux alert collection")]
    FindObjectIdError(#[source] mongodb::error::Error),
    #[error("failed to insert into the alert aux collection")]
    InsertAuxAlertError(#[source] mongodb::error::Error),
    #[error("failed to update the alert aux collection")]
    UpdateAuxAlertError(#[source] mongodb::error::Error),
    #[error("failed to insert into the alert cutout collection")]
    InsertCutoutError(#[source] mongodb::error::Error),
    #[error("failed to retrieve schema for the alert")]
    SchemaError(#[source] apache_avro::Error),
    #[error("avro magic bytes not found")]
    MagicBytesError,
    #[error("schema registry error")]
    SchemaRegistryError(#[from] SchemaRegistryError),
    #[error("alert already exists")]
    AlertExists,
    #[error("missing object_id")]
    MissingObjectId,
    #[error("missing cutout")]
    MissingCutout,
    #[error("missing alert in avro")]
    EmptyAlertError,
    #[error("failed to read avro")]
    AvroReadError(#[source] apache_avro::Error),
    #[error("missing flux psf")]
    MissingFluxPSF,
    #[error("missing flux psf error")]
    MissingFluxPSFError,
}

#[derive(thiserror::Error, Debug)]
pub enum AlertWorkerError {
    #[error("failed to load config")]
    LoadConfigError(#[from] conf::BoomConfigError),
    #[error("failed to create index")]
    CreateIndexError(#[from] CreateIndexError),
    #[error("failed to connect to redis")]
    ConnectRedisError(#[source] redis::RedisError),
    #[error("failed to connect to mongodb")]
    ConnectMongoError(#[source] mongodb::error::Error),
    #[error("failed to get alert schema")]
    GetAlertSchemaError,
    #[error("failed to pop from the alert queue")]
    PopAlertError(#[source] redis::RedisError),
    #[error("failed to get avro bytes from the alert queue")]
    GetAvroBytesError,
    #[error("failed to push candid onto the candid queue")]
    PushCandidError(#[source] redis::RedisError),
    #[error("failed to remove alert from the alert queue")]
    RemoveAlertError(#[source] redis::RedisError),
    #[error("failed to push alert onto the alert queue")]
    PushAlertError(#[source] redis::RedisError),
}

#[async_trait::async_trait]
pub trait AlertWorker {
    async fn new(config_path: &str) -> Result<Self, AlertWorkerError>
    where
        Self: Sized;
    fn stream_name(&self) -> String;
    fn input_queue_name(&self) -> String;
    fn output_queue_name(&self) -> String;
    async fn process_alert(self: &mut Self, avro_bytes: &[u8]) -> Result<i64, AlertError>;
}

#[tokio::main]
pub async fn run_alert_worker<T: AlertWorker>(
    id: String,
    mut receiver: mpsc::Receiver<WorkerCmd>,
    config_path: &str,
) -> Result<(), AlertWorkerError> {
    let mut alert_processor = T::new(config_path).await?;
    let stream_name = alert_processor.stream_name();

    let input_queue_name = alert_processor.input_queue_name();
    let temp_queue_name = format!("{}_temp", input_queue_name);
    let output_queue_name = alert_processor.output_queue_name();

    let client_redis = redis::Client::open("redis://localhost:6379".to_string())
        .map_err(AlertWorkerError::ConnectRedisError)?;
    let mut con = client_redis
        .get_multiplexed_async_connection()
        .await
        .map_err(AlertWorkerError::ConnectRedisError)?;

    let command_interval: i64 = 500;
    let mut command_check_countdown = command_interval;
    let mut count = 0;

    let start = std::time::Instant::now();
    loop {
        // check for command from threadpool
        if command_check_countdown == 0 {
            match receiver.try_recv() {
                Ok(WorkerCmd::TERM) => {
                    info!("alert worker {} received termination command", id);
                    break;
                }
                Err(TryRecvError::Disconnected) => {
                    warn!("alert worker {} receiver disconnected, terminating", id);
                    break;
                }
                Err(TryRecvError::Empty) => {
                    command_check_countdown = command_interval;
                }
            }
        }
        // retrieve candids from redis
        let Some(mut value): Option<Vec<Vec<u8>>> = con
            .rpoplpush(&input_queue_name, &temp_queue_name)
            .await
            .map_err(AlertWorkerError::PopAlertError)?
        else {
            info!("ALERT WORKER {}: Queue is empty", id);
            tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;
            command_check_countdown = 0;
            continue;
        };
        let avro_bytes = (!value.is_empty())
            .then_some(value.remove(0))
            .ok_or(AlertWorkerError::GetAvroBytesError)?;

        let result = alert_processor.process_alert(&avro_bytes).await;
        match result {
            Ok(candid) => {
                // queue the candid for processing by the classifier
                con.lpush::<&str, i64, isize>(&output_queue_name, candid)
                    .await
                    .map_err(AlertWorkerError::PushCandidError)?;
                con.lrem::<&str, Vec<u8>, isize>(&temp_queue_name, 1, avro_bytes)
                    .await
                    .map_err(AlertWorkerError::RemoveAlertError)?;
            }
            Err(error) => match error {
                AlertError::AlertExists => {
                    trace!("Alert already exists");
                    con.lrem::<&str, Vec<u8>, isize>(&temp_queue_name, 1, avro_bytes)
                        .await
                        .map_err(AlertWorkerError::RemoveAlertError)?;
                }
                _ => {
                    warn!(error = %error, "Error processing alert, requeueing");
                    con.lpush::<&str, Vec<u8>, isize>(&input_queue_name, avro_bytes.clone())
                        .await
                        .map_err(AlertWorkerError::PushAlertError)?;
                    con.lrem::<&str, Vec<u8>, isize>(&temp_queue_name, 1, avro_bytes)
                        .await
                        .map_err(AlertWorkerError::RemoveAlertError)?;
                }
            },
        }
        if count % 1000 == 0 {
            let elapsed = start.elapsed().as_secs();
            info!(
                "\nProcessed {} {} alerts in {} seconds, avg: {:.4} alerts/s\n",
                count,
                stream_name,
                elapsed,
                count as f64 / elapsed as f64
            );
        }
        count += 1;
        command_check_countdown -= 1;
    }
    Ok(())
}
