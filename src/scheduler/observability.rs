use crate::utils::{enums::Survey, o11y::metrics::SCHEDULER_METER};

use std::sync::{
    atomic::{AtomicU64, Ordering},
    LazyLock,
};

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

static WORKER_RETRY: LazyLock<Counter<u64>> = LazyLock::new(|| {
    scheduler_meter()
        .u64_counter("scheduler.worker.retry")
        .with_unit("{retry}")
        .with_description(
            "Number of transient-error retries performed by scheduler workers \
             (e.g. Valkey or Kafka connection blips) before either succeeding or \
             surfacing the error.",
        )
        .build()
});

static MPC_ORBITS_AGE: LazyLock<Gauge<i64>> = LazyLock::new(|| {
    scheduler_meter()
        .i64_gauge("scheduler.mpc_orbits.age")
        .with_unit("s")
        .with_description(
            "Seconds since MPC_orbits was last refreshed. A stale catalogue degrades \
             quietly, so alert on this rather than on refresh errors.",
        )
        .build()
});

static MPC_ORBITS_COUNT: LazyLock<Gauge<i64>> = LazyLock::new(|| {
    scheduler_meter()
        .i64_gauge("scheduler.mpc_orbits.count")
        .with_unit("{orbit}")
        .with_description("Number of orbits in MPC_orbits after the last refresh.")
        .build()
});

/// Record the state of the MPC orbital element catalogue.
///
/// An absent catalogue is reported as a very large age rather than omitted: a
/// gap in the series would look like a healthy scrape.
pub fn record_mpc_orbits_state(age_seconds: Option<f64>, count: Option<u64>) {
    MPC_ORBITS_AGE.record(
        age_seconds.map_or(i64::MAX, |a| a as i64),
        &[KeyValue::new("present", age_seconds.is_some())],
    );
    if let Some(count) = count {
        MPC_ORBITS_COUNT.record(i64::try_from(count).unwrap_or(i64::MAX), &[]);
    }
}

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

/// Record a single transient-error retry by a worker. `worker_type` is e.g.
/// "enrichment" or "filter"; `operation` is the resource being retried, e.g.
/// "valkey_rpop", "valkey_lpush", or "kafka_send".
pub fn record_worker_retry(worker_type: &'static str, survey: &str, operation: &'static str) {
    let attrs = [
        KeyValue::new("worker_type", worker_type),
        KeyValue::new("survey", survey.to_string()),
        KeyValue::new("operation", operation),
    ];
    WORKER_RETRY.add(1, &attrs);
}

pub fn record_kafka_alert_published(producer: &'static str, survey: &str, topic: &str, count: u64) {
    let attrs = [
        KeyValue::new("producer", producer),
        KeyValue::new("survey", survey.to_string()),
        KeyValue::new("topic", topic.to_string()),
    ];
    KAFKA_ALERT_PUBLISHED.add(count, &attrs);
}

/// Alerts handled by each stage since the last heartbeat, summed over its workers.
#[derive(Debug)]
pub struct HeartbeatCounts {
    pub alert: u64,
    pub enrichment: u64,
    pub filter: u64,
    pub passed: u64,
}

static ALERTS_PROCESSED: AtomicU64 = AtomicU64::new(0);
static ALERTS_ENRICHED: AtomicU64 = AtomicU64::new(0);
static ALERTS_FILTERED: AtomicU64 = AtomicU64::new(0);
static ALERTS_PASSED: AtomicU64 = AtomicU64::new(0);

pub fn count_processed_alerts(count: usize) {
    ALERTS_PROCESSED.fetch_add(count as u64, Ordering::Relaxed);
}

pub fn count_enriched_alerts(count: usize) {
    ALERTS_ENRICHED.fetch_add(count as u64, Ordering::Relaxed);
}

pub fn count_filtered_alerts(filtered: usize, passed: usize) {
    ALERTS_FILTERED.fetch_add(filtered as u64, Ordering::Relaxed);
    ALERTS_PASSED.fetch_add(passed as u64, Ordering::Relaxed);
}

/// Read and reset the counts, so each heartbeat reports only its own interval.
pub fn take_heartbeat_counts() -> HeartbeatCounts {
    HeartbeatCounts {
        alert: ALERTS_PROCESSED.swap(0, Ordering::Relaxed),
        enrichment: ALERTS_ENRICHED.swap(0, Ordering::Relaxed),
        filter: ALERTS_FILTERED.swap(0, Ordering::Relaxed),
        passed: ALERTS_PASSED.swap(0, Ordering::Relaxed),
    }
}

fn scheduler_meter() -> &'static Meter {
    &SCHEDULER_METER
}
