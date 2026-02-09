#![recursion_limit = "512"] // for large bson documents in pipelines (default was 256)
pub mod alert;
pub mod api;
pub mod conf;
pub mod enrichment;
pub mod filter;
pub mod fitting;
pub mod kafka;
pub mod scheduler;
pub mod utils;
