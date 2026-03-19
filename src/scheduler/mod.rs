mod base;
mod observability;

pub use base::{get_num_workers, SchedulerError, ThreadPool};
pub use observability::{record_worker_pool_state, spawn_observability_poller};
