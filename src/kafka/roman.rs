use crate::{
    kafka::base::{subscription_window, AlertConsumer, AlertProducer},
    utils::{data::count_files_in_dir, enums::Survey},
};
use tracing::info;

const ROMAN_DEFAULT_NB_PARTITIONS: usize = 15;

fn topic_name(date: &chrono::NaiveDate) -> String {
    format!("roman_{}_programid1", date.format("%Y%m%d"))
}

#[derive(Clone)]
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
        vec![topic_name(&date.date_naive())]
    }
    // RAPID has no production broker yet: the topic name is a guess to confirm against it.
    fn subscription_topics(&self, timestamp: i64, window_days: u64) -> Vec<String> {
        subscription_window(timestamp, window_days)
            .iter()
            .map(topic_name)
            .collect()
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
        topic_name(&self.date)
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
