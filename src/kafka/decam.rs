use crate::{
    conf,
    kafka::base::{AlertConsumer, AlertProducer, ConsumerError},
    utils::{data::count_files_in_dir, enums::Survey, o11y::as_error},
};
use redis::AsyncCommands;
use tracing::{info, instrument};

const DECAM_DEFAULT_NB_PARTITIONS: usize = 15;

pub struct DecamAlertConsumer {
    output_queue: String,
    n_threads: usize,
    max_in_queue: usize,
    group_id: String,
    config_path: String,
}

impl DecamAlertConsumer {
    pub fn new(
        n_threads: usize,
        max_in_queue: Option<usize>,
        output_queue: Option<&str>,
        group_id: Option<&str>,
        config_path: &str,
    ) -> Self {
        if 15 % n_threads != 0 {
            panic!("Number of threads should be a factor of 15");
        }
        let max_in_queue = max_in_queue.unwrap_or(15000);
        let output_queue = output_queue
            .unwrap_or("DECAM_alerts_packets_queue")
            .to_string();
        let mut group_id = group_id.unwrap_or("example-ck").to_string();

        group_id = format!("{}-{}", "decam", group_id);

        info!(
            "Creating AlertConsumer with {} threads, output_queue: {}, group_id: {}",
            n_threads, output_queue, group_id
        );

        DecamAlertConsumer {
            output_queue,
            n_threads,
            max_in_queue,
            group_id,
            config_path: config_path.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl AlertConsumer for DecamAlertConsumer {
    fn default(config_path: &str) -> Self {
        Self::new(1, None, None, None, config_path)
    }

    fn topic_name(&self, timestamp: i64) -> String {
        let date = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
        format!("decam_{}_programid{}", date.format("%Y%m%d"), 1)
    }
    fn n_threads(&self) -> usize {
        self.n_threads
    }
    fn max_in_queue(&self) -> usize {
        self.max_in_queue
    }
    fn output_queue(&self) -> String {
        self.output_queue.clone()
    }
    fn group_id(&self) -> String {
        self.group_id.clone()
    }
    fn config_path(&self) -> String {
        self.config_path.clone()
    }
    fn survey(&self) -> Survey {
        Survey::Decam
    }

    #[instrument(skip(self))]
    async fn clear_output_queue(&self) -> Result<(), ConsumerError> {
        let config =
            conf::load_config(&self.config_path).inspect_err(as_error!("failed to load config"))?;
        let mut con = conf::build_redis(&config)
            .await
            .inspect_err(as_error!("failed to connect to redis"))?;
        let _: () = con
            .del(&self.output_queue)
            .await
            .inspect_err(as_error!("failed to delete queue"))?;
        info!("Cleared redis queue for DECAM Kafka consumer");
        Ok(())
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
