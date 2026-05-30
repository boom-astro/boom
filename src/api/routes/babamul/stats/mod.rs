pub mod collections;
pub mod kafka;
pub mod nightly;

pub const STATS_COLLECTION: &str = "stats";

pub use collections::get_collection_stats;
pub use kafka::get_kafka_stats;
pub use nightly::get_nightly_stats;
