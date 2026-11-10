use crate::{
    kafka::base::{subscription_window, AlertConsumer, AlertProducer},
    utils::{data::count_files_in_dir, enums::Survey},
};
use tracing::info;

const WINTER_DEFAULT_NB_PARTITIONS: usize = 15;

#[derive(Clone)]
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
        // As of 2022-08-01 the upstream WINTER naming convention is `winter_%Y%m%d`.
        let date = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
        vec![format!("winter_{}", date.format("%Y%m%d"))]
    }
    fn subscription_topics(&self, timestamp: i64, window_days: u64) -> Vec<String> {
        // Concrete names over the rollover window rather than a `^winter_[0-9]+$`
        // regex: a pattern also matches every past night the cluster still
        // advertises, whose partitions have already been expired upstream.
        subscription_window(timestamp, window_days)
            .iter()
            .map(|date| format!("winter_{}", date.format("%Y%m%d")))
            .collect()
    }
    fn output_queue(&self) -> String {
        self.output_queue.clone()
    }
    fn survey(&self) -> &'static str {
        Survey::Winter.as_str()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::base::AlertConsumer;

    /// 2026-09-17 00:00:00 UTC.
    const T: i64 = 1_789_603_200;

    #[test]
    fn test_the_window_reaches_back_whole_nights() {
        let consumer = WinterAlertConsumer::new(None);
        let topics = consumer.subscription_topics(T, 2);
        // The day itself plus the preceding two, oldest first.
        assert_eq!(topics.len(), 3);
        assert_eq!(topics.first().unwrap(), "winter_20260915");
        assert_eq!(topics.last().unwrap(), "winter_20260917");
        assert_eq!(consumer.topic_names(T), vec!["winter_20260917".to_string()]);
    }

    /// A one-day window steps over any night a restart sat through, and nothing
    /// subscribes to it afterwards.
    #[test]
    fn test_a_one_day_window_cannot_reach_an_older_night() {
        let consumer = WinterAlertConsumer::new(None);
        let narrow = consumer.subscription_topics(T, 1);
        assert!(!narrow.contains(&"winter_20260915".to_string()));
        assert!(consumer
            .subscription_topics(T, 2)
            .contains(&"winter_20260915".to_string()));
    }

    /// Every deployment gives WINTER more room than the one-day default, or a
    /// restart silently loses a night.
    #[test]
    fn test_deployments_widen_the_winter_window() {
        let root = env!("CARGO_MANIFEST_DIR");
        let mut checked = 0;
        for name in [
            "config.yaml",
            "config/prod/caltech/config.yaml",
            "config/prod/umn/config.yaml",
        ] {
            let text = std::fs::read_to_string(format!("{root}/{name}")).expect(name);
            let Some(block) = text.split("\n    winter:\n").nth(1) else {
                continue;
            };
            // The entry ends at the next key on the survey's own indent.
            let entry: String = block
                .lines()
                .take_while(|l| l.trim().is_empty() || l.starts_with("      "))
                .collect::<Vec<_>>()
                .join("\n");
            checked += 1;
            assert!(
                entry.contains("subscription_window_days:"),
                "{name}: winter consumer leaves the window at the default"
            );
        }
        assert!(checked > 0, "no winter consumer blocks found to check");
    }
}
