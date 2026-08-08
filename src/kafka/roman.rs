use crate::{
    kafka::base::{AlertConsumer, AlertProducer},
    utils::{data::count_files_in_dir, enums::Survey},
};
use tracing::info;

const ROMAN_DEFAULT_NB_PARTITIONS: usize = 15;

pub struct RomanAlertConsumer {
    output_queue: String,
}

impl RomanAlertConsumer {
    pub fn new(output_queue: Option<&str>) -> Self {
        let output_queue = output_queue
            .unwrap_or("ROMAN_alerts_packets_queue")
            .to_string();

        RomanAlertConsumer { output_queue }
    }
}

#[async_trait::async_trait]
impl AlertConsumer for RomanAlertConsumer {
    fn topic_names(&self, timestamp: i64) -> Vec<String> {
        let date = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
        vec![format!("roman_{}_programid{}", date.format("%Y%m%d"), 1)]
    }
    fn topic_patterns(&self) -> Vec<String> {
        // Same daily-topic layout as ZTF/DECam. RAPID's production topic naming
        // is not final yet, so this follows the boom convention and should be
        // confirmed against the deployed broker.
        // librdkafka's matcher is POSIX/Thompson-NFA: use `[0-9]+`, not `\d`.
        vec![r"^roman_[0-9]+_programid[0-9]+$".to_string()]
    }
    fn output_queue(&self) -> String {
        self.output_queue.clone()
    }
    fn survey(&self) -> &'static str {
        Survey::Roman.as_str()
    }
}

pub struct RomanAlertProducer {
    date: chrono::NaiveDate,
    limit: i64,
    server_url: String,
    verbose: bool,
}

impl RomanAlertProducer {
    pub fn new(date: chrono::NaiveDate, limit: i64, server_url: &str, verbose: bool) -> Self {
        RomanAlertProducer {
            date,
            limit,
            server_url: server_url.to_string(),
            verbose,
        }
    }
}

#[async_trait::async_trait]
impl AlertProducer for RomanAlertProducer {
    fn topic_name(&self) -> String {
        format!("roman_{}_programid1", self.date.format("%Y%m%d"))
    }
    fn data_directory(&self) -> String {
        format!("data/alerts/roman/{}", self.date.format("%Y%m%d"))
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
        ROMAN_DEFAULT_NB_PARTITIONS
    }
    async fn download_alerts_from_archive(&self) -> Result<i64, Box<dyn std::error::Error>> {
        // RAPID has no public alert archive yet, so we only check that alerts
        // have been staged locally.
        let data_folder = self.data_directory();
        info!("Checking for ROMAN alerts in folder {}", data_folder);
        std::fs::create_dir_all(&data_folder)?;
        let count = count_files_in_dir(&data_folder, Some(&["avro"]))?;
        if count < 1 {
            return Err(format!(
                "ROMAN has no public archive to download from, and no alerts found in {}",
                data_folder
            )
            .into());
        }
        Ok(count as i64)
    }
}
