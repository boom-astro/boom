use crate::{
    conf::AppConfig,
    utils::{enums::Survey, o11y::metrics::SCHEDULER_METER},
};

use std::{sync::LazyLock, time::Duration};

use mongodb::bson::{doc, Bson, Decimal128, Document};
use opentelemetry::{
    metrics::{Counter, Gauge, Meter},
    KeyValue,
};
use redis::AsyncCommands;
use tokio::task::JoinHandle;
use tracing::warn;

const OBSERVABILITY_POLL_INTERVAL_SECS: u64 = 15;

static QUEUE_DEPTH: LazyLock<Gauge<i64>> = LazyLock::new(|| {
    scheduler_meter()
        .i64_gauge("scheduler.queue.depth")
        .with_description("Current depth of each BOOM scheduler queue.")
        .build()
});

static MONGODB_COLLECTIONS: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    scheduler_meter()
        .f64_gauge("scheduler.mongodb.collections")
        .with_description("Number of MongoDB collections in the BOOM database.")
        .build()
});

static MONGODB_INDEXES: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    scheduler_meter()
        .f64_gauge("scheduler.mongodb.indexes")
        .with_description("Number of MongoDB indexes in the BOOM database.")
        .build()
});

static MONGODB_OBJECTS: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    scheduler_meter()
        .f64_gauge("scheduler.mongodb.objects")
        .with_description("Number of MongoDB objects in the BOOM database.")
        .build()
});

static MONGODB_DATA_SIZE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    scheduler_meter()
        .f64_gauge("scheduler.mongodb.data.size")
        .with_description("Logical MongoDB data size for the BOOM database.")
        .build()
});

static MONGODB_STORAGE_SIZE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    scheduler_meter()
        .f64_gauge("scheduler.mongodb.storage.size")
        .with_description("MongoDB on-disk storage size for the BOOM database.")
        .build()
});

static MONGODB_INDEX_SIZE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    scheduler_meter()
        .f64_gauge("scheduler.mongodb.index.size")
        .with_description("MongoDB index size for the BOOM database.")
        .build()
});

static MONGODB_TOTAL_SIZE: LazyLock<Gauge<f64>> = LazyLock::new(|| {
    scheduler_meter()
        .f64_gauge("scheduler.mongodb.total.size")
        .with_description("Combined MongoDB storage and index size for the BOOM database.")
        .build()
});

static WORKER_LIVE: LazyLock<Gauge<i64>> = LazyLock::new(|| {
    scheduler_meter()
        .i64_gauge("scheduler.worker.live")
        .with_description("Number of currently live scheduler worker threads.")
        .build()
});

static WORKER_TOTAL: LazyLock<Gauge<i64>> = LazyLock::new(|| {
    scheduler_meter()
        .i64_gauge("scheduler.worker.total")
        .with_description("Configured number of scheduler worker threads.")
        .build()
});

static KAFKA_ALERT_PUBLISHED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    scheduler_meter()
        .u64_counter("scheduler.kafka.alert.published")
        .with_unit("{alert}")
        .with_description("Number of alerts published to Kafka by scheduler-owned producers.")
        .build()
});

pub fn record_worker_pool_state(
    survey: &Survey,
    worker_type: &'static str,
    live: usize,
    total: usize,
) {
    let attrs = [
        KeyValue::new("survey", survey.to_string()),
        KeyValue::new("worker_type", worker_type),
    ];
    WORKER_LIVE.record(i64::try_from(live).unwrap_or(i64::MAX), &attrs);
    WORKER_TOTAL.record(i64::try_from(total).unwrap_or(i64::MAX), &attrs);
}

pub fn record_kafka_alert_published(producer: &'static str, survey: &str, topic: &str, count: u64) {
    let attrs = [
        KeyValue::new("producer", producer),
        KeyValue::new("survey", survey.to_string()),
        KeyValue::new("topic", topic.to_string()),
    ];
    KAFKA_ALERT_PUBLISHED.add(count, &attrs);
}

pub fn spawn_observability_poller(config: AppConfig, survey: Survey) -> JoinHandle<()> {
    tokio::spawn(async move {
        let queue_descriptors = queue_descriptors(&survey);
        let database_name = config.database.name.clone();
        let mut redis_connection = None;
        let mut db = None;

        loop {
            if redis_connection.is_none() {
                match config.build_redis().await {
                    Ok(connection) => redis_connection = Some(connection),
                    Err(error) => {
                        warn!(%survey, %error, "failed to connect to valkey for observability polling")
                    }
                }
            }

            if let Some(connection) = redis_connection.as_mut() {
                if let Err(error) =
                    record_queue_depths(connection, &survey, &queue_descriptors).await
                {
                    warn!(%survey, %error, "failed to poll valkey queue depths");
                    redis_connection = None;
                }
            }

            if db.is_none() {
                match config.build_db().await {
                    Ok(database) => db = Some(database),
                    Err(error) => {
                        warn!(%survey, %error, "failed to connect to mongodb for observability polling")
                    }
                }
            }

            if let Some(database) = db.as_ref() {
                if let Err(error) = record_db_stats(database, &database_name).await {
                    warn!(%survey, %error, "failed to poll mongodb dbStats");
                    db = None;
                }
            }

            tokio::time::sleep(Duration::from_secs(OBSERVABILITY_POLL_INTERVAL_SECS)).await;
        }
    })
}

fn scheduler_meter() -> &'static Meter {
    &SCHEDULER_METER
}

async fn record_queue_depths(
    connection: &mut redis::aio::MultiplexedConnection,
    survey: &Survey,
    queue_descriptors: &[QueueDescriptor],
) -> Result<(), redis::RedisError> {
    for queue_descriptor in queue_descriptors {
        let depth: usize = connection.llen(&queue_descriptor.name).await?;
        let queue_attributes = [
            KeyValue::new("survey", survey.to_string()),
            KeyValue::new("stage", queue_descriptor.stage),
            KeyValue::new("queue", queue_descriptor.name.clone()),
        ];
        QUEUE_DEPTH.record(i64::try_from(depth).unwrap_or(i64::MAX), &queue_attributes);
    }

    Ok(())
}

async fn record_db_stats(
    database: &mongodb::Database,
    database_name: &str,
) -> Result<(), mongodb::error::Error> {
    let stats = database.run_command(doc! { "dbStats": 1 }).await?;
    let attributes = [KeyValue::new("database", database_name.to_string())];

    record_db_metric(&MONGODB_COLLECTIONS, &stats, "collections", &attributes);
    record_db_metric(&MONGODB_INDEXES, &stats, "indexes", &attributes);
    record_db_metric(&MONGODB_OBJECTS, &stats, "objects", &attributes);
    record_db_metric(&MONGODB_DATA_SIZE, &stats, "dataSize", &attributes);
    record_db_metric(&MONGODB_STORAGE_SIZE, &stats, "storageSize", &attributes);
    record_db_metric(&MONGODB_INDEX_SIZE, &stats, "indexSize", &attributes);
    record_db_metric(&MONGODB_TOTAL_SIZE, &stats, "totalSize", &attributes);

    Ok(())
}

fn record_db_metric(
    gauge: &Gauge<f64>,
    stats: &Document,
    field_name: &str,
    attributes: &[KeyValue],
) {
    if let Some(value) = stats.get(field_name).and_then(bson_to_f64) {
        gauge.record(value, attributes);
    }
}

fn bson_to_f64(value: &Bson) -> Option<f64> {
    match value {
        Bson::Double(number) => Some(*number),
        Bson::Int32(number) => Some(f64::from(*number)),
        Bson::Int64(number) => Some(*number as f64),
        Bson::Decimal128(number) => decimal128_to_f64(number),
        _ => None,
    }
}

fn decimal128_to_f64(value: &Decimal128) -> Option<f64> {
    value.to_string().parse().ok()
}

fn queue_descriptors(survey: &Survey) -> Vec<QueueDescriptor> {
    let survey_name = survey.to_string();

    vec![
        QueueDescriptor::new("packets", format!("{survey_name}_alerts_packets_queue")),
        QueueDescriptor::new(
            "packets_temp",
            format!("{survey_name}_alerts_packets_queue_temp"),
        ),
        QueueDescriptor::new(
            "enrichment",
            format!("{survey_name}_alerts_enrichment_queue"),
        ),
        QueueDescriptor::new("filter", format!("{survey_name}_alerts_filter_queue")),
    ]
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct QueueDescriptor {
    stage: &'static str,
    name: String,
}

impl QueueDescriptor {
    fn new(stage: &'static str, name: String) -> Self {
        Self { stage, name }
    }
}

#[cfg(test)]
mod tests {
    use super::queue_descriptors;
    use crate::utils::enums::Survey;

    #[test]
    fn builds_expected_queue_names() {
        let queue_descriptors = queue_descriptors(&Survey::Ztf);

        assert_eq!(queue_descriptors[0].name, "ZTF_alerts_packets_queue");
        assert_eq!(queue_descriptors[1].name, "ZTF_alerts_packets_queue_temp");
        assert_eq!(queue_descriptors[2].name, "ZTF_alerts_enrichment_queue");
        assert_eq!(queue_descriptors[3].name, "ZTF_alerts_filter_queue");
    }
}
