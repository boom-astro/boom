use super::{parse_date, yesterday};
use crate::Source;
use boom::utils::enums::ProgramId;

use chrono::NaiveDate;
use clap::Args;
use color_eyre::eyre::Result;
use rdkafka::{ClientConfig, consumer::BaseConsumer};
use serde::Deserialize;

pub struct ZtfSource {
    bootstrap_servers: String,
    date: NaiveDate,
    program_id: ProgramId,
}

impl From<(&ZtfArgs, &ZtfConfig)> for ZtfSource {
    fn from((args, config): (&ZtfArgs, &ZtfConfig)) -> Self {
        Self {
            bootstrap_servers: config.bootstrap_servers.clone(),
            date: args.date,
            program_id: args.program_id.clone(),
        }
    }
}

impl Source for ZtfSource {
    fn make_base_consumer(&self) -> Result<BaseConsumer> {
        let base_consumer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", self.group_id())
            .set("security.protocol", "PLAINTEXT")
            .create()?;
        Ok(base_consumer)
    }

    fn group_id(&self) -> String {
        "boom_ztf_consumer_group".into()
    }

    fn topic(&self) -> String {
        let date = self.date.format("%Y%m%d");
        let program_id = &self.program_id;
        format!("ztf_{date}_programid{program_id}")
    }

    fn key(&self) -> String {
        "ZTF_alerts_packets_queue".into()
    }
}

#[derive(Args)]
pub struct ZtfArgs {
    /// UTC date for which we want to consume alerts, with format YYYYMMDD or
    /// YYYY-MM-DD
    #[arg(long, value_parser = parse_date, default_value_t = yesterday())]
    date: NaiveDate,

    /// ID of the program to consume the alerts
    #[arg(long, default_value_t, value_enum)]
    program_id: ProgramId,
}

#[derive(Debug, Deserialize)]
pub struct ZtfConfig {
    bootstrap_servers: String,
}
