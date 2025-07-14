use crate::{
    conf,
    kafka::base::{
        check_kafka_topic_partitions, consume_partitions, ensure_kafka_topic_partitions,
        AlertConsumer, AlertProducer, ConsumerError,
    },
    utils::{
        data::{count_files_in_dir, download_to_file},
        enums::{ProgramId, Survey},
        o11y::{as_error, log_error},
    },
};
use indicatif::ProgressBar;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use redis::AsyncCommands;
use tempfile::NamedTempFile;
use tracing::{error, info, instrument};

pub struct ZtfAlertConsumer {
    output_queue: String,
    n_threads: usize,
    max_in_queue: usize,
    group_id: String,
    program_id: ProgramId,
    config_path: String,
}

impl ZtfAlertConsumer {
    #[instrument]
    pub fn new(
        n_threads: usize,
        max_in_queue: Option<usize>,
        output_queue: Option<&str>,
        group_id: Option<&str>,
        server: Option<&str>,
        program_id: ProgramId,
        config_path: &str,
    ) -> Self {
        let max_in_queue = max_in_queue.unwrap_or(15000);
        let output_queue = output_queue
            .unwrap_or("ZTF_alerts_packets_queue")
            .to_string();
        let mut group_id = group_id.unwrap_or("example-ck").to_string();

        group_id = format!("{}-{}", "ztf", group_id);

        info!(
            "Creating ZTF AlertConsumer with {} threads, output_queue: {}, group_id: {}",
            n_threads, output_queue, group_id
        );

        ZtfAlertConsumer {
            output_queue,
            n_threads,
            max_in_queue,
            group_id,
            program_id,
            config_path: config_path.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl AlertConsumer for ZtfAlertConsumer {
    fn default(config_path: &str) -> Self {
        Self::new(1, None, None, None, None, ProgramId::Public, config_path)
    }

    #[instrument(skip(self))]
    async fn consume(&self, timestamp: i64) {
        // ZTF uses nightly topics, and no user/pass (IP whitelisting)
        let date = chrono::DateTime::from_timestamp(timestamp, 0).unwrap();
        let topic = format!("ztf_{}_programid{}", date.format("%Y%m%d"), self.program_id);

        // call check_kafka_topic_partitions to ensure the topic exists (it returns the number of partitions)
        // do so in a while until the topic exists
        let mut nb_partitions = 0;
        let mut topic_exists = false;
        while !topic_exists {
            match check_kafka_topic_partitions(ZTF_SERVER_URL, &topic).await {
                Ok(Some(partitions)) => {
                    nb_partitions = partitions;
                    topic_exists = true;
                }
                Ok(None) => {
                    info!("Topic {} does not exist yet, retrying...", topic);
                    std::thread::sleep(core::time::Duration::from_secs(5));
                }
                Err(e) => {
                    error!("Error checking topic {}: {}", topic, e);
                    return;
                }
            }
        }

        let n_threads = self.n_threads.clone().min(nb_partitions);

        let mut partitions = vec![vec![]; n_threads];
        for i in 0..nb_partitions {
            partitions[i % n_threads].push(i as i32);
        }

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
                    &Survey::Ztf,
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
        info!("Cleared redis queue for ZTF Kafka consumer");
        Ok(())
    }
}

pub struct ZtfAlertProducer {
    date: chrono::NaiveDate,
    program_id: ProgramId,
    limit: i64,
    partnership_archive_username: Option<String>,
    partnership_archive_password: Option<String>,
    server_url: String,
    verbose: bool,
}

impl ZtfAlertProducer {
    pub fn new(
        date: chrono::NaiveDate,
        limit: i64,
        program_id: ProgramId,
        server_url: &str,
        verbose: bool,
    ) -> Self {
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
            server_url: server_url.to_string(),
            verbose,
        }
    }

    pub async fn download_alerts_from_archive(&self) -> Result<i64, Box<dyn std::error::Error>> {
        let date_str = self.date.format("%Y%m%d").to_string();
        info!(
            "Downloading alerts for date {} (programid: {:?})",
            date_str, self.program_id
        );

        let (file_name, data_folder, base_url) = match self.program_id {
            ProgramId::Public => (
                format!("ztf_public_{}.tar.gz", date_str),
                format!("data/alerts/ztf/public/{}", date_str),
                "https://ztf.uw.edu/alerts/public/".to_string(),
            ),
            ProgramId::Partnership => (
                format!("ztf_partnership_{}.tar.gz", date_str),
                format!("data/alerts/ztf/partnership/{}", date_str),
                "https://ztf.uw.edu/alerts/partnership/".to_string(),
            ),
            _ => return Err("Invalid program ID".into()),
        };

        std::fs::create_dir_all(&data_folder)?;

        let count = count_files_in_dir(&data_folder, Some(&["avro"]))?;
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
            self.verbose,
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

        let count = count_files_in_dir(&data_folder, Some(&["avro"]))?;

        Ok(count as i64)
    }
}

#[async_trait::async_trait]
impl AlertProducer for ZtfAlertProducer {
    async fn produce(&self, topic: Option<String>) -> Result<i64, Box<dyn std::error::Error>> {
        let date_str = self.date.format("%Y%m%d").to_string();
        match self.download_alerts_from_archive().await {
            Ok(count) => count,
            Err(e) => {
                error!("Error downloading alerts: {}", e);
                return Err(e);
            }
        };

        let topic_name = match topic {
            Some(t) => t,
            None => format!("ztf_{}_programid{}", date_str, self.program_id),
        };

        info!("Initializing ZTF alert kafka producer");
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
            .set("debug", "broker,topic,msg")
            .create()
            .expect("Producer creation error");

        let nb_partitions =
            ensure_kafka_topic_partitions(ZTF_SERVER_URL, &topic_name, ZTF_NB_PARTITIONS).await?;

        let mut current_key = 0;

        let data_folder = match self.program_id {
            ProgramId::Public => format!("data/alerts/ztf/public/{}", date_str),
            ProgramId::Partnership => format!("data/alerts/ztf/partnership/{}", date_str),
            _ => return Err("Invalid program ID".into()),
        };

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

            let key_string = current_key.to_string();
            let record = FutureRecord::to(&topic_name)
                .payload(&payload)
                .key(&key_string)
                .timestamp(chrono::Utc::now().timestamp_millis());

            producer
                .send(record, std::time::Duration::from_secs(0))
                .await
                .unwrap();

            current_key = (current_key + 1) % nb_partitions as u64;

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
