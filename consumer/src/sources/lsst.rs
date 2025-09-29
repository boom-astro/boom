use crate::Source;

use clap::Args;
use color_eyre::eyre::Result;
use rdkafka::{ClientConfig, consumer::BaseConsumer};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;

pub struct LsstSource {
    bootstrap_servers: String,
    schema: String,
    username: SecretString,
    password: SecretString,
}

impl From<(&LsstArgs, &LsstConfig)> for LsstSource {
    fn from((args, config): (&LsstArgs, &LsstConfig)) -> Self {
        Self {
            bootstrap_servers: config.bootstrap_servers.clone(),
            schema: args.schema.clone(),
            username: config.username.clone(),
            password: config.password.clone(),
        }
    }
}

impl Source for LsstSource {
    fn make_base_consumer(&self) -> Result<BaseConsumer> {
        let base_consumer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", self.group_id())
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanisms", "SCRAM-SHA-512")
            .set("sasl.username", self.username.expose_secret())
            .set("sasl.password", self.password.expose_secret())
            .create()?;
        Ok(base_consumer)
    }

    fn group_id(&self) -> String {
        "boom_lsst_consumer_group".into()
    }

    fn topic(&self) -> String {
        // TODO: make sure this is correct
        let suffix = if self.schema.is_empty() {
            "".into()
        } else {
            format!("-{}", self.schema)
        };
        format!("alerts{suffix}")
    }

    fn key(&self) -> String {
        "LSST_alerts_packets_queue".into()
    }
}

#[derive(Args)]
pub struct LsstArgs {
    // TODO: make sure this is correct
    /// Schema version string determining which topic to consume from
    #[arg(long, default_value = "0.0.1")]
    schema: String,
}

#[derive(Debug, Deserialize)]
pub struct LsstConfig {
    bootstrap_servers: String,
    password: SecretString,
    username: SecretString,
}
