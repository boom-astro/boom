use crate::{
    kafka::base::{AlertConsumer, AlertProducer},
    utils::{data::count_files_in_dir, enums::Survey},
};
use tracing::info;

const ASKAP_DEFAULT_NB_PARTITIONS: usize = 15;

pub struct AskapAlertConsumer {
    output_queue: String,
}

impl AskapAlertConsumer {
    pub fn new(output_queue: Option<&str>) -> Self {
        let output_queue = output_queue
            .unwrap_or("ASKAP_alerts_packets_queue")
            .to_string();

        AskapAlertConsumer { output_queue }
    }
}

#[async_trait::async_trait]
impl AlertConsumer for AskapAlertConsumer {
    fn topic_names(&self, timestamp: i64) -> Vec<String> {
        let date = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
        vec![format!("askap_{}_programid{}", date.format("%Y%m%d"), 1)]
    }
    fn topic_patterns(&self) -> Vec<String> {
        // POSIX regex (librdkafka): use [0-9]+, not \d.
        vec![r"^askap_[0-9]+_programid[0-9]+$".to_string()]
    }
    fn output_queue(&self) -> String {
        self.output_queue.clone()
    }
    fn survey(&self) -> &'static str {
        Survey::Askap.as_str()
    }
}

pub struct AskapAlertProducer {
    date: chrono::NaiveDate,
    limit: i64,
    server_url: String,
    verbose: bool,
}

impl AskapAlertProducer {
    pub fn new(date: chrono::NaiveDate, limit: i64, server_url: &str, verbose: bool) -> Self {
        AskapAlertProducer {
            date,
            limit,
            server_url: server_url.to_string(),
            verbose,
        }
    }
}

#[async_trait::async_trait]
impl AlertProducer for AskapAlertProducer {
    fn topic_name(&self) -> String {
        format!("askap_{}_programid1", self.date.format("%Y%m%d"))
    }
    fn data_directory(&self) -> String {
        format!("data/alerts/askap/{}", self.date.format("%Y%m%d"))
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
        ASKAP_DEFAULT_NB_PARTITIONS
    }
    async fn download_alerts_from_archive(&self) -> Result<i64, Box<dyn std::error::Error>> {
        // No public ASKAP alert archive; require locally provided files.
        let data_folder = self.data_directory();
        info!("Checking for ASKAP alerts in folder {}", data_folder);
        std::fs::create_dir_all(&data_folder)?;
        let count = count_files_in_dir(&data_folder, Some(&["avro"]))?;
        if count < 1 {
            return Err(format!(
                "ASKAP has no public archive to download from, and no alerts found in {}",
                data_folder
            )
            .into());
        }
        Ok(count as i64)
    }
}
