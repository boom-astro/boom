use super::{parse_date, yesterday};
use crate::Source;

use chrono::NaiveDate;
use clap::Args;
use color_eyre::eyre::Result;
use rdkafka::{ClientConfig, consumer::BaseConsumer};
use serde::Deserialize;

pub struct DecamSource {
    bootstrap_servers: String,
    date: NaiveDate,
}

impl From<(&DecamArgs, &DecamConfig)> for DecamSource {
    fn from((args, config): (&DecamArgs, &DecamConfig)) -> Self {
        Self {
            bootstrap_servers: config.bootstrap_servers.clone(),
            date: args.date,
        }
    }
}

impl Source for DecamSource {
    fn make_base_consumer(&self) -> Result<BaseConsumer> {
        let base_consumer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", self.group_id())
            .set("security.protocol", "PLAINTEXT")
            .create()?;
        Ok(base_consumer)
    }

    fn group_id(&self) -> String {
        "boom_decam_consumer_group".into()
    }

    fn topic(&self) -> String {
        let date = self.date.format("%Y%m%d");
        format!("decam_{date}_programid1")
    }

    fn key(&self) -> String {
        "DECAM_alerts_packets_queue".into()
    }
}

#[derive(Args)]
pub struct DecamArgs {
    /// UTC date for which we want to consume alerts, with format YYYYMMDD or
    /// YYYY-MM-DD
    #[arg(long, value_parser = parse_date, default_value_t = yesterday())]
    date: NaiveDate,
}

#[derive(Debug, Deserialize)]
pub struct DecamConfig {
    bootstrap_servers: String,
}
