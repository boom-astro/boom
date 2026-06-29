use crate::{
    kafka::base::{AlertConsumer, AlertProducer, TopicGenerator},
    utils::{data::count_files_in_dir, enums::Survey},
};
use std::sync::Arc;
use tracing::info;

const WINTER_DEFAULT_NB_PARTITIONS: usize = 15;

/// Topic name for the UTC day containing `timestamp` (seconds). Shared by
/// [`AlertConsumer::topic_names`] and the rollover generator so both stay in
/// sync. As of 2022-08-01 the upstream WINTER naming convention is `winter_%Y%m%d`.
fn winter_topic_names(timestamp: i64) -> Vec<String> {
    let date = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
    vec![format!("winter_{}", date.format("%Y%m%d"))]
}

pub struct WinterAlertConsumer {
    output_queue: String,
}

impl WinterAlertConsumer {
    pub fn new(output_queue: Option<&str>) -> Self {
        let output_queue = output_queue
            .unwrap_or("WINTER_alerts_packets_queue")
            .to_string();

        WinterAlertConsumer { output_queue }
    }
}

#[async_trait::async_trait]
impl AlertConsumer for WinterAlertConsumer {
    fn topic_names(&self, timestamp: i64) -> Vec<String> {
        winter_topic_names(timestamp)
    }
    fn output_queue(&self) -> String {
        self.output_queue.clone()
    }
    fn survey(&self) -> &'static str {
        Survey::Winter.as_str()
    }
    fn rollover_topics(&self) -> Option<TopicGenerator> {
        // WINTER publishes to a new `winter_<date>` topic each UTC day, so roll
        // over to keep consuming as the date advances.
        Some(Arc::new(winter_topic_names))
    }
}

pub struct WinterAlertProducer {
    date: chrono::NaiveDate,
    limit: i64,
    server_url: String,
    verbose: bool,
}

impl WinterAlertProducer {
    pub fn new(date: chrono::NaiveDate, limit: i64, server_url: &str, verbose: bool) -> Self {
        WinterAlertProducer {
            date,
            limit,
            server_url: server_url.to_string(),
            verbose,
        }
    }
}

#[async_trait::async_trait]
impl AlertProducer for WinterAlertProducer {
    fn topic_name(&self) -> String {
        format!("winter_{}", self.date.format("%Y%m%d"))
    }
    fn data_directory(&self) -> String {
        format!("data/alerts/winter/{}", self.date.format("%Y%m%d"))
    }
    fn server_url(&self) -> String {
        self.server_url.clone()
    }
    fn limit(&self) -> i64 {
        self.limit
    }
    fn verbose(&self) -> bool {
        self.verbose
    }
    fn default_nb_partitions(&self) -> usize {
        WINTER_DEFAULT_NB_PARTITIONS
    }
    async fn download_alerts_from_archive(&self) -> Result<i64, Box<dyn std::error::Error>> {
        // there is no public WINTER archive, so we just check if the directory exists
        let data_folder = self.data_directory();
        info!("Checking for WINTER alerts in folder {}", data_folder);
        std::fs::create_dir_all(&data_folder)?;
        let count = count_files_in_dir(&data_folder, Some(&["avro"]))?;
        if count < 1 {
            return Err(format!(
                "WINTER has no public archive to download from, and no alerts found in {}",
                data_folder
            )
            .into());
        }
        Ok(count as i64)
    }
}
