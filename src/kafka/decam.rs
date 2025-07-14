use crate::{
    conf,
    kafka::base::{initialize_topic, AlertConsumer, AlertProducer, ConsumerError},
    utils::{data::count_files_in_dir, enums::Survey, o11y::as_error},
};
use indicatif::ProgressBar;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use redis::AsyncCommands;
use tracing::{error, info, instrument};

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
    async fn produce(&self, topic: Option<String>) -> Result<i64, Box<dyn std::error::Error>> {
        let date_str = self.date.format("%Y%m%d").to_string();

        let topic_name = match topic {
            Some(t) => t,
            None => format!("decam_{}_programid{}", date_str, 1),
        };

        info!("Initializing DECAM alert kafka producer");
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.server_url)
            .set("message.timeout.ms", "5000")
            // it's best to increase batch.size if the cluster
            // is running on another machine. Locally, lower means less
            // latency, since we are not limited by network speed anyways
            .set("batch.size", "16384")
            .set("linger.ms", "5")
            .set("acks", "1")
            .set("max.in.flight.requests.per.connection", "5")
            .set("retries", "3")
            .create()
            .expect("Producer creation error");

        let _ =
            initialize_topic(&self.server_url, &topic_name, DECAM_DEFAULT_NB_PARTITIONS).await?;

        let data_folder = format!("data/alerts/decam/{}", date_str);
        if !std::path::Path::new(&data_folder).exists() {
            error!("Data folder does not exist: {}", data_folder);
            return Err(format!("Data folder does not exist: {}", data_folder).into());
        }

        let count = count_files_in_dir(&data_folder, Some(&["avro"]))?;

        let total_size = if self.limit > 0 {
            count.min(self.limit as usize) as u64
        } else {
            count as u64
        };

        let progress_bar = ProgressBar::new(total_size)
            .with_message(format!("Pushing alerts to {}", topic_name))
            .with_style(indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} [{elapsed_precise}] {human_pos}/{human_len} ({eta})")?);

        let mut total_pushed = 0;
        let start = std::time::Instant::now();
        for entry in std::fs::read_dir(&data_folder)? {
            if entry.is_err() {
                continue;
            }
            let entry = entry.unwrap();
            let path = entry.path();
            if !path.to_str().unwrap().ends_with(".avro") {
                continue;
            }
            let payload = match std::fs::read(&path) {
                Ok(data) => data,
                Err(e) => {
                    error!("Failed to read file {:?}: {}", path.to_str(), e);
                    continue;
                }
            };

            let record: FutureRecord<'_, (), Vec<u8>> = FutureRecord::to(&topic_name)
                .payload(&payload)
                .timestamp(chrono::Utc::now().timestamp_millis());

            producer
                .send(record, std::time::Duration::from_secs(0))
                .await
                .unwrap();

            total_pushed += 1;
            if self.verbose {
                progress_bar.inc(1);
            }

            if self.limit > 0 && total_pushed >= self.limit {
                info!("Reached limit of {} pushed items", self.limit);
                break;
            }
        }

        info!(
            "Pushed {} alerts to the queue in {:?}",
            total_pushed,
            start.elapsed()
        );

        // close producer
        producer.flush(std::time::Duration::from_secs(1)).unwrap();

        Ok(total_pushed as i64)
    }
}
