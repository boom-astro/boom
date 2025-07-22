use crate::{
    conf::{self, SurveyKafkaConfig},
    utils::{
        data::count_files_in_dir,
        o11y::{as_error, log_error},
    },
};
use indicatif::ProgressBar;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    error::KafkaError,
};
use redis::AsyncCommands;
use tracing::{debug, error, info, instrument, trace, warn};

// check that the topic exists and return the number of partitions
pub async fn check_kafka_topic_partitions(
    bootstrap_servers: &str,
    topic_name: &str,
) -> Result<Option<usize>, KafkaError> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;

    let metadata = consumer.fetch_metadata(None, std::time::Duration::from_secs(5))?;

    debug!(
        "Existing topics: {}",
        metadata
            .topics()
            .iter()
            .map(|t| t.name())
            .collect::<Vec<_>>()
            .join(", ")
    );

    let topic_metadata = metadata.topics().iter().find(|t| t.name() == topic_name);
    match topic_metadata.map(|t| t.partitions().len()) {
        Some(partitions) => Ok(Some(partitions)),
        None => Ok(None),
    }
}

pub async fn assign_partitions_to_consumers(
    topic_name: &str,
    nb_consumers: usize,
    kafka_config: &SurveyKafkaConfig,
) -> Result<Vec<Vec<i32>>, ConsumerError> {
    // call check_kafka_topic_partitions to ensure the topic exists (it returns the number of partitions)
    let nb_partitions = loop {
        if let Some(nb_partitions) =
            check_kafka_topic_partitions(&kafka_config.consumer, &topic_name).await?
        {
            break nb_partitions;
        }
        info!("Topic {} does not exist yet, retrying...", &topic_name);
        std::thread::sleep(core::time::Duration::from_secs(5));
    };

    let nb_consumers = nb_consumers.clone().min(nb_partitions);

    let mut partitions = vec![vec![]; nb_consumers];
    for i in 0..nb_partitions {
        partitions[i % nb_consumers].push(i as i32);
    }

    Ok(partitions)
}

pub async fn initialize_topic(
    bootstrap_servers: &str,
    topic_name: &str,
    expected_nb_partitions: usize,
) -> Result<usize, KafkaError> {
    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;

    let nb_partitions = match check_kafka_topic_partitions(bootstrap_servers, topic_name).await? {
        Some(nb_partitions) => {
            if nb_partitions != expected_nb_partitions {
                warn!(
                    "Topic {} exists but has {} partitions instead of expected {}",
                    topic_name, nb_partitions, expected_nb_partitions
                );
            }
            nb_partitions
        }
        None => {
            let opts =
                AdminOptions::new().operation_timeout(Some(std::time::Duration::from_secs(5)));
            info!(
                "Creating topic {} with {} partitions...",
                topic_name, expected_nb_partitions
            );
            admin_client
                .create_topics(
                    &[NewTopic::new(
                        topic_name,
                        expected_nb_partitions as i32,
                        TopicReplication::Fixed(1),
                    )],
                    &opts,
                )
                .await?;
            info!(
                "Topic {} created successfully with {} partitions",
                topic_name, expected_nb_partitions
            );
            expected_nb_partitions
        }
    };
    Ok(nb_partitions)
}

#[async_trait::async_trait]
pub trait AlertProducer {
    fn topic_name(&self) -> String;
    fn data_directory(&self) -> String;
    fn server_url(&self) -> String;
    fn limit(&self) -> i64;
    fn verbose(&self) -> bool {
        false
    }
    async fn download_alerts_from_archive(&self) -> Result<i64, Box<dyn std::error::Error>>;
    fn default_nb_partitions(&self) -> usize;
    async fn produce(&self, topic: Option<String>) -> Result<i64, Box<dyn std::error::Error>> {
        // TODO: refactor this new bit, make it nicer, unify with what's already in this module
        let consumer: BaseConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.server_url())
            .create()?;

        let timeout = std::time::Duration::from_secs(5);
        let metadata = consumer.fetch_metadata(None, timeout)?;
        if let Some(topic_metadata) = metadata
            .topics()
            .iter()
            .find(|t| t.name() == self.topic_name())
        {
            // Topic exists, skip producing if it has the expected number of messages.
            let total_messages = topic_metadata.partitions().iter().try_fold(
                0u32,
                |total_messages, partition_metadata| {
                    let partition_id = partition_metadata.id();
                    consumer
                        .fetch_watermarks(&self.topic_name(), partition_id, timeout)
                        .map(|(low, high)| {
                            debug!(
                                ?partition_id,
                                ?low,
                                ?high,
                                messages = high - low,
                                "watermarks"
                            );
                            total_messages + (high - low) as u32
                        })
                },
            )?;
            debug!(?total_messages);

            // Count the number of Avro files in the data directory
            let avro_count = count_files_in_dir(&self.data_directory(), Some(&["avro"]))?;

            // If they don't match, delete all messages from the topic and
            // start fresh
            if total_messages != avro_count as u32 {
                warn!(
                    "Topic {} already exists with {} messages, but {} Avro files found in data directory",
                    self.topic_name(),
                    total_messages,
                    avro_count
                );
                // Delete all messages from Kafka topic so we start fresh
                let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
                    .set("bootstrap.servers", &self.server_url())
                    .create()?;
                let opts =
                    AdminOptions::new().operation_timeout(Some(std::time::Duration::from_secs(5)));
                admin_client
                    .delete_topics(&[&self.topic_name()], &opts)
                    .await
                    .map_err(|e: KafkaError| {
                        error!("Failed to delete topic {}: {}", self.topic_name(), e);
                        e.into()
                    })?;
                info!("Deleted topic {}", self.topic_name());
            } else {
                info!(
                    "Topic {} already exists with {} messages, no need to produce more",
                    self.topic_name(),
                    total_messages
                );
                return Ok(total_messages as i64);
            }
        }

        match self.download_alerts_from_archive().await {
            Ok(count) => count,
            Err(e) => {
                error!("Error downloading alerts: {}", e);
                return Err(e);
            }
        };

        let topic_name = topic.unwrap_or_else(|| self.topic_name());
        let limit = self.limit();
        let verbose = self.verbose();

        info!("Initializing kafka alert producer");
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.server_url())
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

        let _ = initialize_topic(
            &self.server_url(),
            &topic_name,
            self.default_nb_partitions(),
        )
        .await?;

        let data_folder = self.data_directory();
        let count = count_files_in_dir(&data_folder, Some(&["avro"]))?;

        let total_size = if limit > 0 {
            count.min(limit as usize) as u64
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

            // we do not specify a key for the record, to let kafka distribute messages across partitions
            // across partitions evenly with its built-in round-robin strategy
            let record: FutureRecord<'_, (), Vec<u8>> = FutureRecord::to(&topic_name)
                .payload(&payload)
                .timestamp(chrono::Utc::now().timestamp_millis());

            producer
                .send(record, std::time::Duration::from_secs(0))
                .await
                .unwrap();

            total_pushed += 1;
            if verbose {
                progress_bar.inc(1);
            }

            if limit > 0 && total_pushed >= limit {
                info!("Reached limit of {} pushed items", limit);
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

#[derive(Debug, thiserror::Error)]
pub enum ConsumerError {
    #[error("error from boom::conf")]
    Config(#[from] conf::BoomConfigError),
    #[error("error from rdkafka")]
    Kafka(#[from] rdkafka::error::KafkaError),
    #[error("error from redis")]
    Redis(#[from] redis::RedisError),
    #[error("error from config")]
    ConfigError(#[from] config::ConfigError),
}

#[async_trait::async_trait]
pub trait AlertConsumer: Sized {
    fn topic_name(&self, timestamp: i64) -> String;
    fn output_queue(&self) -> String;
    fn username(&self) -> Option<String> {
        None
    }
    fn password(&self) -> Option<String> {
        None
    }
    fn survey(&self) -> crate::utils::enums::Survey;
    #[instrument(skip(self))]
    async fn consume(
        &self,
        timestamp: i64,
        config_path: &str,
        exit_on_eof: bool,
        n_threads: Option<usize>,
        max_in_queue: Option<usize>,
        group_id: Option<String>,
        topic: Option<String>,
    ) -> Result<(), ConsumerError> {
        let config = conf::load_config(config_path)?;
        let kafka_config = conf::build_kafka_config(&config, &self.survey())?;

        let n_threads = n_threads.unwrap_or(1);
        let max_in_queue = max_in_queue.unwrap_or(15000);
        let group_id = group_id.unwrap_or_else(|| {
            format!(
                "boom_{}_consumer_group",
                self.survey().to_string().to_lowercase()
            )
        });

        let topic = topic.unwrap_or_else(|| self.topic_name(timestamp));
        let partitions = assign_partitions_to_consumers(&topic, n_threads, &kafka_config).await?;

        let mut handles = vec![];
        for i in 0..n_threads {
            let topic = topic.clone();
            let partitions = partitions[i].clone();
            let group_id = group_id.clone();
            let username = self.username();
            let password = self.password();
            let output_queue = self.output_queue();
            let config = config.clone();
            let kafka_config = kafka_config.clone();
            let handle = tokio::spawn(async move {
                let result = consume_partitions(
                    &i.to_string(),
                    &topic,
                    &group_id,
                    partitions,
                    &output_queue,
                    max_in_queue,
                    timestamp,
                    &username,
                    &password,
                    &config,
                    &kafka_config,
                    exit_on_eof,
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

        Ok(())
    }
    #[instrument(skip(self))]
    async fn clear_output_queue(&self, config_path: &str) -> Result<(), ConsumerError> {
        let config =
            conf::load_config(config_path).inspect_err(as_error!("failed to load config"))?;
        let mut con = conf::build_redis(&config)
            .await
            .inspect_err(as_error!("failed to connect to redis"))?;
        let _: () = con
            .del(&self.output_queue())
            .await
            .inspect_err(as_error!("failed to delete queue"))?;
        info!("Cleared redis queue for Kafka consumer");
        Ok(())
    }
}

#[instrument(skip(username, password, survey_config))]
pub async fn consume_partitions(
    id: &str,
    topic: &str,
    group_id: &str,
    partitions: Vec<i32>,
    output_queue: &str,
    max_in_queue: usize,
    timestamp: i64,
    username: &Option<String>,
    password: &Option<String>,
    config: &config::Config,
    survey_config: &SurveyKafkaConfig,
    exit_on_eof: bool,
) -> Result<(), ConsumerError> {
    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", &survey_config.consumer)
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("group.id", group_id)
        .set("debug", "consumer,cgrp,topic,fetch");

    if let (Some(username), Some(password)) = (username, password) {
        client_config
            .set("sasl.mechanisms", "SCRAM-SHA-512")
            .set("sasl.username", username)
            .set("sasl.password", password);
    } else {
        client_config.set("security.protocol", "PLAINTEXT");
    }

    let consumer: BaseConsumer = client_config
        .create()
        .inspect_err(as_error!("failed to create consumer"))?;

    let mut timestamps = rdkafka::TopicPartitionList::new();
    let offset = rdkafka::Offset::Offset(timestamp);
    for i in &partitions {
        timestamps
            .add_partition(topic, *i)
            .set_offset(offset)
            .inspect_err(as_error!("failed to add partition"))?
    }
    let tpl = consumer
        .offsets_for_times(timestamps, std::time::Duration::from_secs(5))
        .inspect_err(as_error!("failed to fetch offsets"))?;

    consumer
        .assign(&tpl)
        .inspect_err(as_error!("failed to assign topic partition list"))?;

    let mut con = conf::build_redis(&config)
        .await
        .inspect_err(as_error!("failed to connect to redis"))?;

    let mut total = 0;

    // start timer
    let start = std::time::Instant::now();
    // poll one message at a time
    loop {
        if max_in_queue > 0 && total % 1000 == 0 {
            loop {
                let nb_in_queue = con
                    .llen::<&str, usize>(&output_queue)
                    .await
                    .inspect_err(as_error!("failed to get queue length"))?;
                if nb_in_queue >= max_in_queue {
                    info!(
                        "{} (limit: {}) items in queue, sleeping...",
                        nb_in_queue, max_in_queue
                    );
                    std::thread::sleep(core::time::Duration::from_millis(500));
                    continue;
                }
                break;
            }
        }
        match consumer.poll(tokio::time::Duration::from_secs(5)) {
            Some(result) => {
                let message = result.inspect_err(as_error!("failed to get message"))?;
                let payload = message.payload().unwrap_or_default();
                con.rpush::<&str, Vec<u8>, usize>(&output_queue, payload.to_vec())
                    .await
                    .inspect_err(as_error!("failed to push message to queue"))?;
                trace!("Pushed message to redis");
                total += 1;
                if total % 1000 == 0 {
                    info!(
                        "Consumer {} pushed {} items since {:?}",
                        id,
                        total,
                        start.elapsed()
                    );
                }
            }
            None => {
                trace!("No message available");
                if exit_on_eof {
                    break;
                }
            }
        }
    }

    Ok(())
}
