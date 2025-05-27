use crate::{
    conf,
    kafka::base::{consume_partitions, AlertConsumer, AlertProducer},
};
use rdkafka::config::ClientConfig;
use redis::AsyncCommands;
use tracing::{error, info};

use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

const DECAM_SERVER_URL: &str = "localhost:9092";

pub struct DecamAlertConsumer {
    output_queue: String,
    n_threads: usize,
    max_in_queue: usize,
    group_id: String,
    server: String,
    config_path: String,
}

#[async_trait::async_trait]
impl AlertConsumer for DecamAlertConsumer {
    fn new(
        n_threads: usize,
        max_in_queue: Option<usize>,
        topic: Option<&str>,
        output_queue: Option<&str>,
        group_id: Option<&str>,
        server: Option<&str>,
        _program_id: Option<u8>,
        config_path: &str,
    ) -> Self {
        if 15 % n_threads != 0 {
            panic!("Number of threads should be a factor of 15");
        }
        let max_in_queue = max_in_queue.unwrap_or(15000);
        let topic = topic.unwrap_or("decam").to_string();
        let output_queue = output_queue
            .unwrap_or("DECAM_alerts_packets_queue")
            .to_string();
        let mut group_id = group_id.unwrap_or("example-ck").to_string();
        let server = server.unwrap_or(DECAM_SERVER_URL).to_string();

        group_id = format!("{}-{}", "decam", group_id);

        info!(
            "Creating AlertConsumer with {} threads, topic: {}, output_queue: {}, group_id: {}, server: {}",
            n_threads, topic, output_queue, group_id, server
        );

        DecamAlertConsumer {
            output_queue,
            n_threads,
            max_in_queue,
            group_id,
            server,
            config_path: config_path.to_string(),
        }
    }

    fn default(config_path: &str) -> Self {
        Self::new(1, None, None, None, None, None, None, config_path)
    }

    async fn consume(&self, timestamp: i64) -> Result<(), Box<dyn std::error::Error>> {
        let partitions_per_thread = 15 / self.n_threads;
        let mut partitions = vec![vec![]; self.n_threads];
        for i in 0..15 {
            partitions[i / partitions_per_thread].push(i as i32);
        }

        // DECAM uses nightly topics, and no user/pass (IP whitelisting)
        let date = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
        let topic = format!("decam_{}_programid1", date.format("%Y%m%d"));

        let mut handles = vec![];
        for i in 0..self.n_threads {
            let topic = topic.clone();
            let partitions = partitions[i].clone();
            let max_in_queue = self.max_in_queue;
            let output_queue = self.output_queue.clone();
            let group_id = self.group_id.clone();
            let server = self.server.clone();
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
                    &server,
                    None,
                    None,
                    &config_path,
                )
                .await;
                if let Err(e) = result {
                    error!("Error consuming partitions: {:?}", e);
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        Ok(())
    }

    async fn clear_output_queue(&self) -> Result<(), Box<dyn std::error::Error>> {
        let config = conf::load_config(&self.config_path)?;
        let mut con = conf::build_redis(&config).await?;
        let _: () = con.del(&self.output_queue).await.unwrap();
        info!("Cleared redis queued for DECAM Kafka consumer");
        Ok(())
    }
}

pub struct DecamAlertProducer {
    date: String,
    limit: i64,
}

impl DecamAlertProducer {
    // here, let's implement a method where we check that the alerts
    // are on disk for the given date
    // there is no download possible yet, so we just check that
    // the files are there
    fn download_alerts_from_archive(&self) -> Result<i64, Box<dyn std::error::Error>> {
        if self.date.len() != 8 {
            return Err("Invalid date format".into());
        }
        let data_folder = format!("data/alerts/decam/{}", self.date);
        if !std::path::Path::new(&data_folder).exists() {
            return Err(format!(
                "No alerts found for date {} (and no download possible yet for DECAM)",
                self.date
            )
            .into());
        }

        // count how many files are in the directory
        let count = std::fs::read_dir(&data_folder)?.count();
        if count == 0 {
            return Err(format!("No alerts found for date {}", self.date).into());
        }

        Ok(count as i64)
    }
}

#[async_trait::async_trait]
impl AlertProducer for DecamAlertProducer {
    fn new(date: String, limit: i64, _program_id: Option<u8>) -> Self {
        DecamAlertProducer { date, limit }
    }

    async fn produce(&self, topic: Option<String>) -> Result<i64, Box<dyn std::error::Error>> {
        match self.download_alerts_from_archive() {
            Ok(count) => count,
            Err(e) => {
                error!("Error downloading alerts: {}", e);
                return Err(e);
            }
        };

        let topic_name = match topic {
            Some(t) => t,
            None => format!("decam_{}_programid1", self.date),
        };

        info!("Initializing DECAM alert kafka producer");
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", "localhost:9092")
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

        info!("Pushing alerts to {}", topic_name);

        let mut total_pushed = 0;
        let start = std::time::Instant::now();
        for entry in std::fs::read_dir(format!("data/alerts/decam/{}", self.date))? {
            if entry.is_err() {
                continue;
            }
            let entry = entry.unwrap();
            let path = entry.path();
            if !path.to_str().unwrap().ends_with(".avro") {
                continue;
            }
            let payload = std::fs::read(path).unwrap();

            let record = FutureRecord::to(&topic_name)
                .payload(&payload)
                .key("decam")
                .timestamp(chrono::Utc::now().timestamp_millis());

            producer
                .send(record, std::time::Duration::from_secs(0))
                .await
                .unwrap();

            total_pushed += 1;
            if total_pushed % 1000 == 0 {
                info!("Pushed {} items since {:?}", total_pushed, start.elapsed());
            }

            if self.limit > 0 && total_pushed >= self.limit {
                info!("Reached limit of {} pushed items", self.limit);
                break;
            }
        }

        info!("Pushed {} alerts to the queue", total_pushed);

        // close producer
        producer.flush(std::time::Duration::from_secs(1)).unwrap();

        Ok(total_pushed as i64)
    }
}
