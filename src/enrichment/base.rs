use crate::{
    alert::AlertCutout,
    conf,
    enrichment::models::ModelError,
    utils::{
        fits::CutoutError,
        o11y::metrics::SCHEDULER_METER,
        worker::{should_terminate, WorkerCmd},
    },
};

use std::{num::NonZero, sync::LazyLock};

use futures::StreamExt;
use mongodb::bson::{doc, Document};
use opentelemetry::{
    metrics::{Counter, UpDownCounter},
    KeyValue,
};
use redis::AsyncCommands;
use tokio::sync::mpsc;
use tracing::{debug, error, instrument};
use uuid::Uuid;

// NOTE: Global instruments are defined here because reusing instruments is
// considered a best practice. See boom::alert::base.

// UpDownCounter for the number of alert batches currently being processed by the enrichment workers.
static ACTIVE: LazyLock<UpDownCounter<i64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .i64_up_down_counter("enrichment_worker.active")
        .with_unit("{batch}")
        .with_description(
            "Number of alert batches currently being processed by the enrichment worker.",
        )
        .build()
});

// Counter for the number of alert batches processed by the enrichment workers.
static BATCH_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .u64_counter("enrichment_worker.batch.processed")
        .with_unit("{batch}")
        .with_description("Number of alert batches processed by the enrichment worker.")
        .build()
});

// Counter for the number of alerts processed by the enrichment workers.
static ALERT_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    SCHEDULER_METER
        .u64_counter("enrichment_worker.alert.processed")
        .with_unit("{alert}")
        .with_description("Number of alerts processed by the enrichment worker.")
        .build()
});

#[derive(thiserror::Error, Debug)]
pub enum EnrichmentWorkerError {
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
    #[error("failed to deserialize from MongoDB")]
    MongoDeserializeError(#[from] mongodb::bson::de::Error),
    #[error("missing cutouts for candid {0}")]
    MissingCutouts(i64),
}

#[async_trait::async_trait]
pub trait EnrichmentWorker {
    async fn new(config_path: &str) -> Result<Self, EnrichmentWorkerError>
    where
        Self: Sized;
    fn input_queue_name(&self) -> String;
    fn output_queue_name(&self) -> String;
    async fn process_alerts(
        &mut self,
        alerts: &[i64],
    ) -> Result<Vec<String>, EnrichmentWorkerError>;
}

/// Fetch alerts from the database given a list of candids and an aggregation pipeline.
/// Return a vector of alerts as bson Documents.
///
/// # Arguments
/// * `candids` - A slice of candids to fetch alerts for.
/// * `alert_pipeline` - A reference to a vector of bson Documents representing the aggregation pipeline.
/// * `alert_collection` - A reference to the mongodb collection containing the alerts.
///
/// # Returns
/// A Result containing a vector of bson Documents representing the fetched alerts, or an EnrichmentWorkerError.
#[instrument(skip_all, err)]
pub async fn fetch_alerts<T: for<'a> serde::Deserialize<'a>>(
    candids: &[i64], // this is a slice of candids to process
    alert_pipeline: &Vec<Document>,
    alert_collection: &mongodb::Collection<Document>,
) -> Result<Vec<T>, EnrichmentWorkerError> {
    let mut alert_pipeline = alert_pipeline.clone();
    if let Some(first_stage) = alert_pipeline.first_mut() {
        *first_stage = doc! {
            "$match": {
                "_id": {"$in": candids}
            }
        };
    }
    let mut alert_cursor = alert_collection.aggregate(alert_pipeline).await?;

    let mut alerts: Vec<T> = Vec::new();
    while let Some(result) = alert_cursor.next().await {
        match result {
            Ok(document) => {
                let alert: T = mongodb::bson::from_document(document)?;
                alerts.push(alert);
            }
            _ => {
                continue;
            }
        }
    }

    Ok(alerts)
}

#[instrument(skip_all, err)]
pub async fn fetch_alert_cutouts(
    candids: &[i64],
    alert_cutout_collection: &mongodb::Collection<Document>,
) -> Result<std::collections::HashMap<i64, AlertCutout>, EnrichmentWorkerError> {
    let filter = doc! {
        "_id": {"$in": candids}
    };
    let mut cursor = alert_cutout_collection.find(filter).await?;

    let mut cutouts_map: std::collections::HashMap<i64, AlertCutout> =
        std::collections::HashMap::new();
    while let Some(result) = cursor.next().await {
        match result {
            Ok(document) => {
                let candid = document.get_i64("_id")?;
                let cutout_science = document
                    .get_binary_generic("cutoutScience")
                    .map(|b| b.to_vec())
                    .unwrap_or_default();
                let cutout_template = document
                    .get_binary_generic("cutoutTemplate")
                    .map(|b| b.to_vec())
                    .unwrap_or_default();
                let cutout_difference = document
                    .get_binary_generic("cutoutDifference")
                    .map(|b| b.to_vec())
                    .unwrap_or_default();
                let alert_cutout = AlertCutout {
                    candid,
                    cutout_science,
                    cutout_template,
                    cutout_difference,
                };
                cutouts_map.insert(candid, alert_cutout);
            }
            _ => {
                continue;
            }
        }
    }

    Ok(cutouts_map)
}

#[tokio::main]
#[instrument(skip_all, err)]
pub async fn run_enrichment_worker<T: EnrichmentWorker>(
    mut receiver: mpsc::Receiver<WorkerCmd>,
    config_path: &str,
    worker_id: Uuid,
) -> Result<(), EnrichmentWorkerError> {
    debug!(?config_path);
    let mut enrichment_worker = T::new(config_path).await?;

    let config = conf::load_raw_config(config_path)?;
    let mut con = conf::build_redis(&config).await?;

    let input_queue = enrichment_worker.input_queue_name();
    let output_queue = enrichment_worker.output_queue_name();

    let command_interval: usize = 500;
    let mut command_check_countdown = command_interval;

    let worker_id_attr = KeyValue::new("worker.id", worker_id.to_string());
    let active_attrs = [worker_id_attr.clone()];
    let ok_attrs = [worker_id_attr.clone(), KeyValue::new("status", "ok")];
    let input_error_attrs = [
        worker_id_attr.clone(),
        KeyValue::new("status", "error"),
        KeyValue::new("reason", "input_queue"),
    ];
    let processing_error_attrs = [
        worker_id_attr.clone(),
        KeyValue::new("status", "error"),
        KeyValue::new("reason", "processing"),
    ];
    let output_error_attrs = [
        worker_id_attr,
        KeyValue::new("status", "error"),
        KeyValue::new("reason", "output_queue"),
    ];
    loop {
        if command_check_countdown == 0 {
            if should_terminate(&mut receiver) {
                break;
            }
            command_check_countdown = command_interval;
        }

        ACTIVE.add(1, &active_attrs);
        let candids: Vec<i64> = con
            .rpop::<&str, Vec<i64>>(&input_queue, NonZero::new(1000))
            .await
            .inspect_err(|_| {
                ACTIVE.add(-1, &active_attrs);
                BATCH_PROCESSED.add(1, &input_error_attrs);
            })?;

        if candids.is_empty() {
            ACTIVE.add(-1, &active_attrs);
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
            command_check_countdown = 0;
            continue;
        }

        let processed_alerts: Vec<String> = enrichment_worker
            .process_alerts(&candids)
            .await
            .inspect_err(|_| {
                ACTIVE.add(-1, &active_attrs);
                BATCH_PROCESSED.add(1, &processing_error_attrs);
            })?;
        command_check_countdown = command_check_countdown.saturating_sub(candids.len());

        con.lpush::<&str, Vec<String>, usize>(&output_queue, processed_alerts)
            .await
            .inspect_err(|_| {
                ACTIVE.add(-1, &active_attrs);
                BATCH_PROCESSED.add(1, &output_error_attrs);
            })?;

        let attributes = &ok_attrs;
        ACTIVE.add(-1, &active_attrs);
        BATCH_PROCESSED.add(1, attributes);
        ALERT_PROCESSED.add(candids.len() as u64, attributes);
    }

    Ok(())
}
