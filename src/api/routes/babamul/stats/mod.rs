mod catalogs;
mod kafka;
mod nightly;

pub const STATS_COLLECTION: &str = "stats";

pub use catalogs::get_catalog_stats;
pub use kafka::get_kafka_stats;
pub use nightly::get_nightly_stats;
