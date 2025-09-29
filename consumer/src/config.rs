use crate::sources;
use boom::conf::{BoomConfig, load_config};

use std::path::Path;

use color_eyre::eyre::{OptionExt, Result};
use config::Environment;
use serde::Deserialize;
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub boom: BoomConfig,
    pub consumer: ConsumerConfig,
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let boom = BoomConfig::load(path)?;
        let consumer = ConsumerConfig::load(path)?;
        let config = Self { boom, consumer };
        Ok(config)
    }
}

#[derive(Debug, Deserialize)]
pub struct ConsumerConfig {
    pub app: AppConfig,
    pub task: TaskConfig,
    pub decam: sources::decam::DecamConfig,
    pub lsst: sources::lsst::LsstConfig,
    pub ztf: sources::ztf::ZtfConfig,
}

// This is just a deserialization helper
#[derive(Debug, Deserialize)]
struct FullConfig {
    consumer: ConsumerConfig,
}

impl ConsumerConfig {
    fn load(path: &Path) -> Result<Self> {
        let path_str = path.to_str().ok_or_eyre("invalid unicode")?;
        let config = load_config(path_str)?;

        let full_config: FullConfig = ::config::Config::builder()
            .set_default("consumer.app.instance_id", Uuid::new_v4().to_string())?
            .add_source(config)
            .add_source(Environment::with_prefix("BOOM").separator("__"))
            .build()?
            .try_deserialize()?;
        Ok(full_config.consumer)
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct AppConfig {
    pub instance_id: Uuid,
    pub(crate) deployment_env: String,
    pub(crate) progress_report_interval: u64,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TaskConfig {
    pub(crate) fetch_metadata_timeout: u64,
    pub(crate) timestamp_offset_lookup_timeout: u64,
    pub(crate) receive_timeout: u64,
    pub(crate) enqueue_timeout: u64,
    pub(crate) enqueue_retry_delay: u64,
    pub(crate) queue_soft_limit: usize,
    pub(crate) queue_check_delay: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::path::Path;

    // Guarantees that the "consumer" section of the config file is
    // deserializable, providing confidence as a source of truth.
    #[test]
    fn config_file_is_correct() -> Result<()> {
        let _config = ConsumerConfig::load(Path::new("../config.yaml"))?;
        Ok(())
    }
}
