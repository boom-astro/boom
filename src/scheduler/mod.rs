mod base;
mod observability;

pub use base::{get_num_workers, SchedulerError, ThreadPool};
pub use observability::{
    count_enriched_alerts, count_filtered_alerts, count_processed_alerts,
    record_kafka_alert_published, record_mpc_orbits_state, record_worker_pool_state,
    record_worker_retry, take_heartbeat_counts,
};
