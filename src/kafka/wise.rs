use crate::{
    kafka::base::{subscription_window, AlertConsumer, AlertProducer},
    utils::{data::count_files_in_dir, enums::Survey},
};
use tracing::info;

const WISE_DEFAULT_NB_PARTITIONS: usize = 15;

#[derive(Clone)]
pub struct WiseAlertConsumer {
    output_queue: String,
}

impl WiseAlertConsumer {
    pub fn new(output_queue: Option<&str>) -> Self {
        let output_queue = output_queue
            .unwrap_or("WISE_alerts_packets_queue")
            .to_string();

        WiseAlertConsumer { output_queue }
    }
}

#[async_trait::async_trait]
impl AlertConsumer for WiseAlertConsumer {
    fn topic_names(&self, timestamp: i64) -> Vec<String> {
        let date = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
        vec![format!("wise_{}_programid{}", date.format("%Y%m%d"), 1)]
    }
    fn subscription_topics(&self, timestamp: i64, window_days: u64) -> Vec<String> {
        subscription_window(timestamp, window_days)
            .iter()
            .map(|date| format!("wise_{}_programid{}", date.format("%Y%m%d"), 1))
            .collect()
    }
    fn output_queue(&self) -> String {
        self.output_queue.clone()
    }
    fn survey(&self) -> &'static str {
        Survey::Wise.as_str()
    }
}

pub struct WiseAlertProducer {
    date: chrono::NaiveDate,
    limit: i64,
    server_url: String,
    verbose: bool,
}

impl WiseAlertProducer {
    pub fn new(date: chrono::NaiveDate, limit: i64, server_url: &str, verbose: bool) -> Self {
        WiseAlertProducer {
            date,
            limit,
            server_url: server_url.to_string(),
            verbose,
        }
    }
}

#[async_trait::async_trait]
impl AlertProducer for WiseAlertProducer {
    fn topic_name(&self) -> String {
        format!("wise_{}_programid1", self.date.format("%Y%m%d"))
    }
    fn data_directory(&self) -> String {
        format!("data/alerts/wise/{}", self.date.format("%Y%m%d"))
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
        WISE_DEFAULT_NB_PARTITIONS
    }
    async fn download_alerts_from_archive(&self) -> Result<i64, Box<dyn std::error::Error>> {
        // No public WISE/WTP alert archive; require locally provided files.
        let data_folder = self.data_directory();
        info!("Checking for WISE alerts in folder {}", data_folder);
        std::fs::create_dir_all(&data_folder)?;
        let count = count_files_in_dir(&data_folder, Some(&["avro"]))?;
        if count < 1 {
            return Err(format!(
                "WISE has no public archive to download from, and no alerts found in {}",
                data_folder
            )
            .into());
        }
        Ok(count as i64)
    }
}
