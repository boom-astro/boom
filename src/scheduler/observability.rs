use crate::utils::{enums::Survey, o11y::metrics::SCHEDULER_METER};

use std::sync::LazyLock;

use opentelemetry::{
    metrics::{Counter, Gauge, Meter},
    KeyValue,
};

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

fn scheduler_meter() -> &'static Meter {
    &SCHEDULER_METER
}
