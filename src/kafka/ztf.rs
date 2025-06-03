use crate::{
    conf,
    kafka::base::{consume_partitions, AlertConsumer, AlertProducer},
    utils::data::download_to_file,
    utils::enums::ProgramId,
};
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use redis::AsyncCommands;
use tempfile::NamedTempFile;
use tracing::{error, info};

const ZTF_SERVER_URL: &str = "localhost:9092";

pub struct ZtfAlertConsumer {
    output_queue: String,
    n_threads: usize,
    max_in_queue: usize,
    group_id: String,
    server: String,
    program_id: ProgramId,
    config_path: String,
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
        program_id: Option<ProgramId>,
        config_path: &str,
    ) -> Self {
        if 15 % n_threads != 0 {
            panic!("Number of threads should be a factor of 15");
        }
        let max_in_queue = max_in_queue.unwrap_or(15000);
        let topic = topic
            .unwrap_or(&format!(
                "ztf_{}_programid1",
                chrono::Utc::now().format("%Y%m%d")
            ))
            .to_string();
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

        let program_id = program_id.unwrap_or(ProgramId::Public);

        ZtfAlertConsumer {
            output_queue,
            n_threads,
            max_in_queue,
            group_id,
            server,
            program_id,
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

        // ZTF uses nightly topics, and no user/pass (IP whitelisting)
        let date = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
        let topic = format!(
            "ztf_{}_programid{}",
            date.format("%Y%m%d"),
            self.program_id.as_u8()
        );

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
        info!("Cleared redis queued for ZTF Kafka consumer");
        Ok(())
    }
}

pub struct ZtfAlertProducer {
    date: String,
    program_id: ProgramId,
    limit: i64,
    partnership_archive_username: Option<String>,
    partnership_archive_password: Option<String>,
}

impl ZtfAlertProducer {
    pub async fn download_alerts_from_archive(&self) -> Result<i64, Box<dyn std::error::Error>> {
        if self.date.len() != 8 {
            return Err("Invalid date format".into());
        }

        info!(
            "Downloading alerts for date {} (programid: {:?})",
            self.date, self.program_id
        );

        let (file_name, data_folder, base_url) = match self.program_id {
            ProgramId::Public => (
                format!("ztf_public_{}.tar.gz", self.date),
                format!("data/alerts/ztf/public/{}", self.date),
                "https://ztf.uw.edu/alerts/public/".to_string(),
            ),
            ProgramId::Partnership => (
                format!("ztf_partnership_{}.tar.gz", self.date),
                format!("data/alerts/ztf/partnership/{}", self.date),
                "https://ztf.uw.edu/alerts/partnership/".to_string(),
            ),
            _ => return Err("Invalid program ID".into()),
        };

        std::fs::create_dir_all(&data_folder)?;

        let count = std::fs::read_dir(&data_folder)?
            .filter_map(Result::ok)
            .filter(|entry| entry.path().extension().map_or(false, |ext| ext == "avro"))
            .count();
        if count > 0 {
            info!("Alerts already downloaded to {}{}", data_folder, file_name);
            return Ok(count as i64);
        }

        let mut output_temp_file = NamedTempFile::new_in(&data_folder)
            .map_err(|e| format!("Failed to create temp file: {}", e))?;

        // use download_to_file function to download the file
        match download_to_file(
            &mut output_temp_file,
            &format!("{}{}", base_url, file_name),
            self.partnership_archive_username.as_deref(),
            self.partnership_archive_password.as_deref(),
        )
        .await
        {
            Ok(_) => info!("Downloaded alerts to {}", data_folder),
            Err(e) => {
                if e.to_string().contains("404 Not Found") {
                    return Err("No alerts found for this date".into());
                } else {
                    return Err(e);
                }
            }
        }

        let output_temp_path = output_temp_file.path().to_str().unwrap();

        // when we untar it, the name of the folder should be the same as the file name
        let output = std::process::Command::new("tar")
            .arg("-xzf")
            .arg(output_temp_path)
            .arg("-C")
            .arg(&data_folder)
            .output()?;
        if !output.status.success() {
            return Err("Failed to extract alerts".into());
        } else {
            info!("Extracted alerts to {}", data_folder);
        }

        drop(output_temp_file); // Close the temp file

        let count = std::fs::read_dir(&data_folder)?.count();

        Ok(count as i64)
    }
}

#[async_trait::async_trait]
impl AlertProducer for ZtfAlertProducer {
    fn new(date: String, limit: i64, program_id: Option<ProgramId>) -> Self {
        // if u8 is not provided, default to 1
        let program_id = program_id.unwrap_or(ProgramId::Public);

        // if program_id > 1, check that we have a ZTF_PARTNERSHIP_ARCHIVE_USERNAME
        // and ZTF_PARTNERSHIP_ARCHIVE_PASSWORD set as env variables
        let partnership_archive_username = match std::env::var("ZTF_PARTNERSHIP_ARCHIVE_USERNAME") {
            Ok(username) => Some(username),
            Err(_) => None,
        };
        let partnership_archive_password = match std::env::var("ZTF_PARTNERSHIP_ARCHIVE_PASSWORD") {
            Ok(password) => Some(password),
            Err(_) => None,
        };
        if program_id == ProgramId::Partnership
            && (partnership_archive_username.is_none() || partnership_archive_password.is_none())
        {
            panic!("ZTF_PARTNERSHIP_ARCHIVE_USERNAME and ZTF_PARTNERSHIP_ARCHIVE_PASSWORD environment variables must be set for partnership program ID");
        }

        ZtfAlertProducer {
            date,
            limit,
            program_id,
            partnership_archive_username,
            partnership_archive_password,
        }
    }

    async fn produce(&self, topic: Option<String>) -> Result<i64, Box<dyn std::error::Error>> {
        match self.download_alerts_from_archive().await {
            Ok(count) => count,
            Err(e) => {
                error!("Error downloading alerts: {}", e);
                return Err(e);
            }
        };

        let topic_name = match topic {
            Some(t) => t,
            None => format!("ztf_{}_programid{}", self.date, self.program_id.as_u8()),
        };

        info!("Initializing ZTF alert kafka producer");
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

        let data_folder = match self.program_id {
            ProgramId::Public => format!("data/alerts/ztf/public/{}", self.date),
            ProgramId::Partnership => format!("data/alerts/ztf/partnership/{}", self.date),
            _ => return Err("Invalid program ID".into()),
        };

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
