use crate::{
    kafka::base::{AlertConsumer, AlertProducer},
    utils::{data::count_files_in_dir, enums::Survey},
};
use tracing::info;

const DECAM_DEFAULT_NB_PARTITIONS: usize = 15;

pub struct DecamAlertConsumer {
    output_queue: String,
}

impl DecamAlertConsumer {
    pub fn new(output_queue: Option<&str>) -> Self {
        let output_queue = output_queue
            .unwrap_or("DECAM_alerts_packets_queue")
            .to_string();

        DecamAlertConsumer { output_queue }
    }
}

#[async_trait::async_trait]
impl AlertConsumer for DecamAlertConsumer {
    fn topic_name(&self, timestamp: i64) -> String {
        let date = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
        format!("decam_{}_programid{}", date.format("%Y%m%d"), 1)
    }
    fn output_queue(&self) -> String {
        self.output_queue.clone()
    }
    fn survey(&self) -> Survey {
        Survey::Decam
    }
}

pub struct DecamAlertProducer {
    date: chrono::NaiveDate,
    limit: i64,
    server_url: String,
    verbose: bool,
}

impl DecamAlertProducer {
    pub fn new(date: chrono::NaiveDate, limit: i64, server_url: &str, verbose: bool) -> Self {
        DecamAlertProducer {
            date,
            limit,
            server_url: server_url.to_string(),
            verbose,
        }
    }
}

#[async_trait::async_trait]
impl AlertProducer for DecamAlertProducer {
    fn topic_name(&self) -> String {
        format!("decam_{}_programid1", self.date.format("%Y%m%d"))
    }
    fn data_directory(&self) -> String {
        format!("data/alerts/decam/{}", self.date.format("%Y%m%d"))
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
        DECAM_DEFAULT_NB_PARTITIONS
    }
    async fn download_alerts_from_archive(&self) -> Result<i64, Box<dyn std::error::Error>> {
        // there is no public decam archive, so we just check if the directory exists
        let data_folder = self.data_directory();
        info!("Checking for DECAM alerts in folder {}", data_folder);
        std::fs::create_dir_all(&data_folder)?;
        let count = count_files_in_dir(&data_folder, Some(&["avro"]))?;
        if count < 1 {
            return Err(format!(
                "DECAM has no public archive to download from, and no alerts found in {}",
                data_folder
            )
            .into());
        }
        Ok(count as i64)
    }
}
