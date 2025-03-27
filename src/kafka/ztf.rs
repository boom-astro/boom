use crate::kafka::base::{consume_partitions, AlertConsumer};
use rdkafka::config::ClientConfig;
use redis::AsyncCommands;
use tracing::{error, info};

use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

const ZTF_SERVER_URL: &str = "localhost:9092";

pub struct ZtfAlertConsumer {
    output_queue: String,
    n_threads: usize,
    max_in_queue: usize,
    group_id: String,
    server: String,
}

#[async_trait::async_trait]
impl AlertConsumer for ZtfAlertConsumer {
    fn new(
        n_threads: usize,
        max_in_queue: Option<usize>,
        topic: Option<&str>,
        output_queue: Option<&str>,
        group_id: Option<&str>,
        server: Option<&str>,
    ) -> Self {
        if 15 % n_threads != 0 {
            panic!("Number of threads should be a factor of 15");
        }
        let max_in_queue = max_in_queue.unwrap_or(15000);
        let topic = topic.unwrap_or("ztf").to_string();
        let output_queue = output_queue
            .unwrap_or("ZTF_alerts_packets_queue")
            .to_string();
        let mut group_id = group_id.unwrap_or("example-ck").to_string();
        let server = server.unwrap_or(ZTF_SERVER_URL).to_string();

        group_id = format!("{}-{}", "ztf", group_id);

        info!(
            "Creating AlertConsumer with {} threads, topic: {}, output_queue: {}, group_id: {}, server: {}",
            n_threads, topic, output_queue, group_id, server
        );

        ZtfAlertConsumer {
            output_queue,
            n_threads,
            max_in_queue,
            group_id,
            server,
        }
    }

    fn default() -> Self {
        Self::new(1, None, None, None, None, None)
    }

    async fn consume(&self, timestamp: i64) -> Result<(), Box<dyn std::error::Error>> {
        let partitions_per_thread = 15 / self.n_threads;
        let mut partitions = vec![vec![]; self.n_threads];
        for i in 0..15 {
            partitions[i / partitions_per_thread].push(i as i32);
        }

        // ZTF uses nightly topics, and no user/pass (IP whitelisting)
        let date = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
        let topic = format!("ztf_{}_programid1", date.format("%Y%m%d"));

        let mut handles = vec![];
        for i in 0..self.n_threads {
            let topic = topic.clone();
            let partitions = partitions[i].clone();
            let max_in_queue = self.max_in_queue;
            let output_queue = self.output_queue.clone();
            let group_id = self.group_id.clone();
            let server = self.server.clone();
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
        let client = redis::Client::open("redis://localhost:6379".to_string()).unwrap();
        let mut con = client.get_multiplexed_async_connection().await.unwrap();
        let _: () = con.del(&self.output_queue).await.unwrap();
        info!("Cleared redis queued for ZTF Kafka consumer");
        Ok(())
    }
}

pub fn download_alerts_from_archive(date: &str) -> Result<i64, Box<dyn std::error::Error>> {
    if date.len() != 8 {
        return Err("Invalid date format".into());
    }

    let data_folder = format!("data/alerts/ztf/{}", date);
    std::fs::create_dir_all(&data_folder)?;

    if std::fs::read_dir(&data_folder)?.count() > 0 {
        info!("Alerts already downloaded to {}", data_folder);
        let count = std::fs::read_dir(&data_folder).unwrap().count();
        return Ok(count as i64);
    }

    info!("Downloading alerts for date {}", date);
    let url = format!(
        "https://ztf.uw.edu/alerts/public/ztf_public_{}.tar.gz",
        date
    );
    let output = std::process::Command::new("wget")
        .arg(&url)
        .arg("-P")
        .arg(&data_folder)
        .arg("-O")
        .arg(format!("{}/ztf_public_{}.tar.gz", data_folder, date))
        .output()?;
    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        if error_msg.contains("404 Not Found") {
            return Err("No alerts found for this date".into());
        }
        return Err(format!("Failed to download alerts: {}", error_msg).into());
    } else {
        info!("Downloaded alerts to {}", data_folder);
    }

    let output = std::process::Command::new("tar")
        .arg("-xzf")
        .arg(format!("{}/ztf_public_{}.tar.gz", data_folder, date))
        .arg("-C")
        .arg(&data_folder)
        .output()?;
    if !output.status.success() {
        return Err("Failed to extract alerts".into());
    } else {
        info!("Extracted alerts to {}", data_folder);
    }

    std::fs::remove_file(format!("{}/ztf_public_{}.tar.gz", data_folder, date))?;

    let count = std::fs::read_dir(&data_folder)?.count();

    Ok(count as i64)
}

pub async fn produce_from_archive(
    date: &str,
    limit: i64,
    topic: Option<String>,
) -> Result<i64, Box<dyn std::error::Error>> {
    match download_alerts_from_archive(&date) {
        Ok(count) => count,
        Err(e) => {
            error!("Error downloading alerts: {}", e);
            return Err(e);
        }
    };

    let topic_name = match topic {
        Some(t) => t,
        None => format!("ztf_{}_programid1", &date),
    };

    info!("Initializing producer for topic {}", topic_name);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    info!("Pushing alerts to {}", topic_name);

    let mut total_pushed = 0;
    let start = std::time::Instant::now();
    for entry in std::fs::read_dir(format!("data/alerts/ztf/{}", date))? {
        let entry = entry.unwrap();
        let path = entry.path();
        let payload = std::fs::read(path).unwrap();

        let record = FutureRecord::to(&topic_name)
            .payload(&payload)
            .key("ztf")
            .timestamp(chrono::Utc::now().timestamp_millis());

        producer
            .send(record, std::time::Duration::from_secs(0))
            .await
            .unwrap();

        total_pushed += 1;
        if total_pushed % 1000 == 0 {
            info!("Pushed {} items since {:?}", total_pushed, start.elapsed());
        }

        if limit > 0 && total_pushed >= limit {
            info!("Reached limit of {} pushed items", limit);
            break;
        }
    }

    info!("Pushed {} alerts to the queue", total_pushed);

    // close producer
    producer.flush(std::time::Duration::from_secs(1)).unwrap();

    Ok(total_pushed as i64)
}
