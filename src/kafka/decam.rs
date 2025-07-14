use crate::{
    conf,
    kafka::base::{consume_partitions, AlertConsumer, AlertProducer, ConsumerError},
    utils::{
        data::count_files_in_dir,
        enums::Survey,
        o11y::{as_error, log_error},
    },
};
use indicatif::ProgressBar;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use redis::AsyncCommands;
use tracing::{error, info, instrument};

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

    #[instrument(skip(self))]
    async fn consume(&self, timestamp: i64) {
        let partitions_per_thread = 15 / self.n_threads;
        let mut partitions = vec![vec![]; self.n_threads];
        for i in 0..15 {
            partitions[i / partitions_per_thread].push(i as i32);
        }

        // DECAM will use nightly topics. Auth is not implemented yet,
        // so we assume a Kafka cluster without authentication (such as a local one).
        let date = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
        let topic = format!("decam_{}_programid{}", date.format("%Y%m%d"), 1);

        let mut handles = vec![];
        for i in 0..self.n_threads {
            let topic = topic.clone();
            let partitions = partitions[i].clone();
            let max_in_queue = self.max_in_queue;
            let output_queue = self.output_queue.clone();
            let group_id = self.group_id.clone();
            let config_path = self.config_path.clone();
            let handle = tokio::spawn(async move {
                let result = consume_partitions(
                    &i.to_string(),
                    &topic,
                    &group_id,
                    partitions,
                    &output_queue,
                    max_in_queue,
                    timestamp,
                    None,
                    None,
                    &Survey::Decam,
                    &config_path,
                )
                .await;
                if let Err(error) = result {
                    log_error!(error, "failed to consume partitions");
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            if let Err(error) = handle.await {
                log_error!(error, "failed to join task");
            }
        }
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

            let record = FutureRecord::to(&topic_name)
                .payload(&payload)
                .key("decam")
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
