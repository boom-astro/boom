use crate::{
    conf::{self, SurveyKafkaConfig},
    utils::{
        data::count_files_in_dir,
        o11y::{
            logging::{as_error, log_error},
            metrics::CONSUMER_METER,
        },
    },
};

use std::sync::LazyLock;

use indicatif::ProgressBar;
use opentelemetry::{metrics::Counter, KeyValue};
use rdkafka::{
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer},
    error::{KafkaError, KafkaResult},
    message::Message,
    producer::{FutureProducer, FutureRecord, Producer},
    TopicPartitionList,
};
use redis::AsyncCommands;
use tracing::{debug, error, info, instrument, trace, warn};

// NOTE: Global instruments are defined here because reusing instruments is
// considered a best practice. See boom::alert::base.

// Counter for the number of alerts processed by the kafka consumer.
static ALERT_PROCESSED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    CONSUMER_METER
        .u64_counter("kafka_consumer.alert.processed")
        .with_unit("{alert}")
        .with_description("Number of alerts processed by the kafka consumer.")
        .build()
});

const MAX_RETRIES_PRODUCER: usize = 6;
const KAFKA_TIMEOUT_SECS: std::time::Duration = std::time::Duration::from_secs(30);

// rdkafka's Metadata type provides *references* to MetadataTopic and
// MetadataPartition values, neither of which implement Clone. We use a custom
// Metadata type to capture the topic and partition information from the rdkafka
// types, which can then can be returned to the caller.
#[instrument(skip_all, err)]
fn get_partition_ids(
    client: &BaseConsumer,
    topic_name: &str,
) -> Result<Option<Vec<i32>>, KafkaError> {
    let cluster_metadata = client.fetch_metadata(Some(topic_name), KAFKA_TIMEOUT_SECS)?;
    let topic = match cluster_metadata
        .topics()
        .iter()
        .find(|metadata_topic| metadata_topic.name() == topic_name)
    {
        Some(topic) => topic,
        None => return Ok(None),
    };
    let partition_ids = topic
        .partitions()
        .iter()
        .map(|metadata_partition| metadata_partition.id())
        .collect::<Vec<_>>();
    Ok(Some(partition_ids))
}

// check that the topic exists and return the number of partitions
#[instrument(skip_all, err)]
pub fn check_kafka_topic_partitions(
    bootstrap_servers: &str,
    topic_name: &str,
    group_id: &str,
    username: Option<String>,
    password: Option<String>,
) -> Result<Option<usize>, KafkaError> {
    let mut client_config = ClientConfig::new();
    client_config
        // Uncomment the following to get logs from kafka (RUST_LOG doesn't work):
        // .set("debug", "consumer,cgrp,topic,fetch")
        .set("bootstrap.servers", bootstrap_servers)
        .set("group.id", group_id);

    if let (Some(username), Some(password)) = (username, password) {
        client_config
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanisms", "SCRAM-SHA-512")
            .set("sasl.username", username)
            .set("sasl.password", password);
    } else {
        client_config.set("security.protocol", "PLAINTEXT");
    }

    let consumer: BaseConsumer = client_config
        .create()
        .inspect_err(as_error!("failed to create consumer"))?;
    let partition_ids = get_partition_ids(&consumer, topic_name)?;
    Ok(partition_ids.map(|ids| ids.len()))
}

#[instrument(skip_all, err)]
pub async fn initialize_topic(
    bootstrap_servers: &str,
    topic_name: &str,
    expected_nb_partitions: usize,
) -> Result<usize, KafkaError> {
    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;

    let nb_partitions = match check_kafka_topic_partitions(
        bootstrap_servers,
        topic_name,
        "producer-topic-check",
        None,
        None,
    )? {
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
            let opts = AdminOptions::new().operation_timeout(Some(KAFKA_TIMEOUT_SECS));
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

#[instrument(skip_all, err)]
pub async fn delete_topic(bootstrap_servers: &str, topic_name: &str) -> Result<(), KafkaError> {
    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;

    let opts = AdminOptions::new().operation_timeout(Some(KAFKA_TIMEOUT_SECS));
    admin_client.delete_topics(&[topic_name], &opts).await?;
    Ok(())
}

#[instrument(skip_all, err)]
pub fn count_messages(
    bootstrap_servers: &str,
    topic_name: &str,
) -> Result<Option<u32>, KafkaError> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;
    match get_partition_ids(&consumer, topic_name) {
        Ok(Some(partition_ids)) => {
            debug!(?topic_name, "topic found");
            let total_messages =
                partition_ids
                    .iter()
                    .try_fold(0u32, |total_messages, &partition_id| {
                        consumer
                            .fetch_watermarks(topic_name, partition_id, KAFKA_TIMEOUT_SECS)
                            .map(|(low, high)| {
                                let count = high - low;
                                debug!(
                                    ?topic_name,
                                    ?partition_id,
                                    ?low,
                                    ?high,
                                    ?count,
                                    "watermarks"
                                );
                                total_messages + count as u32
                            })
                    })?;
            debug!(?topic_name, ?total_messages);
            Ok(Some(total_messages))
        }
        Ok(None) => {
            debug!(?topic_name, "topic not found");
            Ok(None)
        }
        Err(e) => Err(e),
    }
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
    async fn produce(
        &self,
        topic: Option<String>,
    ) -> Result<Option<i64>, Box<dyn std::error::Error>> {
        let topic_name = topic.unwrap_or_else(|| self.topic_name());
        if let Some(total_messages) = count_messages(&self.server_url(), &topic_name)? {
            // Topic exists, skip producing if it has the expected number of
            // messages. Count the number of Avro files in the data directory:
            if let Some(avro_count) = count_files_in_dir(&self.data_directory(), Some(&["avro"]))
                .map(|count| Some(count))
                .or_else(|error| match error.kind() {
                    std::io::ErrorKind::NotFound => Ok(None),
                    _ => Err(error),
                })?
            {
                if avro_count == 0 {
                    warn!("data directory {} is empty", self.data_directory());
                }
                debug!(
                    "{} avro files found in {}",
                    avro_count,
                    self.data_directory()
                );
                // If the counts match, then nothing to do, return early.
                if total_messages == avro_count as u32 {
                    info!(
                        "Topic {} already exists with {} messages, no need to produce",
                        topic_name, total_messages
                    );
                    return Ok(None);
                } else {
                    warn!(
                        "Topic {} already exists with {} messages, but {} Avro files found in data directory",
                        topic_name,
                        total_messages,
                        avro_count
                    );
                }
            } else {
                warn!(
                    "Topic {} already exists, but data directory not found",
                    topic_name,
                );
            }
            // The topic and data directory are inconsistent. Delete the topic
            // to start fresh:
            warn!("recreating topic {}", topic_name);
            delete_topic(&self.server_url(), &topic_name).await?;
        }

        match self.download_alerts_from_archive().await {
            Ok(count) => count,
            Err(e) => {
                error!("Error downloading alerts: {}", e);
                return Err(e);
            }
        };

        let limit = self.limit();
        let verbose = self.verbose();

        info!("Initializing kafka alert producer");
        let producer: FutureProducer = ClientConfig::new()
            // Uncomment the following to get logs from kafka (RUST_LOG doesn't work):
            // .set("debug", "broker,topic,msg")
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
            // use retries in case of transient errors
            let mut n_retries = MAX_RETRIES_PRODUCER;
            while n_retries > 0 {
                let record: FutureRecord<'_, (), Vec<u8>> = FutureRecord::to(&topic_name)
                    .payload(&payload)
                    .timestamp(chrono::Utc::now().timestamp_millis());
                let status = producer.send(record, KAFKA_TIMEOUT_SECS).await;
                match status {
                    Ok(_) => {
                        break;
                    }
                    Err((e, _)) => {
                        error!(
                            "Failed to deliver message: {:?}, retrying ({} left)",
                            e,
                            n_retries - 1
                        );
                    }
                }
                n_retries -= 1;
            }

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
        producer.flush(KAFKA_TIMEOUT_SECS)?;

        Ok(Some(total_pushed as i64))
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
    fn survey(&self) -> crate::utils::enums::Survey;
    #[instrument(skip(self))]
    async fn clear_output_queue(&self, config_path: &str) -> Result<(), ConsumerError> {
        let config =
            conf::load_raw_config(config_path).inspect_err(as_error!("failed to load config"))?;
        let mut con = conf::build_redis(&config)
            .await
            .inspect_err(as_error!("failed to connect to redis"))?;
        let _: () = con
            .del(&self.output_queue())
            .await
            .inspect_err(as_error!("failed to delete queue"))?;
        info!(
            "Cleared redis queue {} for Kafka consumer",
            self.output_queue()
        );
        Ok(())
    }
    #[instrument(skip(self))]
    async fn consume(
        &self,
        topic: Option<String>,
        timestamp: i64,
        kafka_config: Option<SurveyKafkaConfig>,
        n_threads: Option<usize>,
        max_in_queue: Option<usize>,
        exit_on_eof: bool,
        config_path: &str,
    ) -> Result<(), ConsumerError> {
        let config = conf::load_raw_config(config_path)?;

        let topic = topic.unwrap_or_else(|| self.topic_name(timestamp));
        let kafka_config = match kafka_config {
            Some(cfg) => cfg,
            None => conf::build_kafka_config(&config, &self.survey())?,
        };

        let n_threads = n_threads.unwrap_or(1);
        let max_in_queue = max_in_queue.unwrap_or(15000);

        let mut handles = vec![];
        for i in 0..n_threads {
            let topic = topic.clone();
            let output_queue = self.output_queue();
            let config = config.clone();
            let kafka_config = kafka_config.clone();
            let handle = tokio::spawn(async move {
                let result = consumer(
                    &i.to_string(),
                    &topic,
                    &output_queue,
                    max_in_queue,
                    timestamp,
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
}

fn seek_to_timestamp(consumer: &BaseConsumer, timestamp_ms: i64) -> KafkaResult<()> {
    // Get current assignment
    let assignment = consumer.assignment()?;

    // Create a new TopicPartitionList with offsets to look up
    let mut tpl_with_timestamps = TopicPartitionList::new();

    for elem in assignment.elements() {
        tpl_with_timestamps.add_partition_offset(
            elem.topic(),
            elem.partition(),
            rdkafka::Offset::Offset(timestamp_ms), // Use timestamp as offset for lookup, in ms
        )?;
    }

    // Query offsets for the given timestamp
    let offsets = consumer.offsets_for_times(tpl_with_timestamps, KAFKA_TIMEOUT_SECS)?;

    // Seek to the resolved offsets
    for elem in offsets.elements() {
        debug!(
            "Seeking partition {} to offset for timestamp {}",
            elem.partition(),
            timestamp_ms
        );
        if let rdkafka::Offset::Offset(offset) = elem.offset() {
            consumer.seek(
                elem.topic(),
                elem.partition(),
                rdkafka::Offset::Offset(offset),
                KAFKA_TIMEOUT_SECS,
            )?;
            debug!(
                "Seeked partition {} to offset {} for timestamp {}",
                elem.partition(),
                offset,
                timestamp_ms
            );
        } else {
            debug!(
                "Seeking partition {} to end as no offset found for timestamp {}",
                elem.partition(),
                timestamp_ms
            );
            // If we didn't find an offset for the timestamp, seek to the end
            consumer.seek(
                elem.topic(),
                elem.partition(),
                rdkafka::Offset::End,
                KAFKA_TIMEOUT_SECS,
            )?;
        }
    }

    Ok(())
}

#[instrument(skip(config, survey_config))]
pub async fn consumer(
    id: &str,
    topic: &str,
    output_queue: &str,
    max_in_queue: usize,
    timestamp: i64,
    config: &config::Config,
    survey_config: &SurveyKafkaConfig,
    exit_on_eof: bool,
) -> Result<(), ConsumerError> {
    let server = survey_config.consumer.server.clone();
    let group_id = survey_config.consumer.group_id.clone();
    let username = survey_config.consumer.username.clone();
    let password = survey_config.consumer.password.clone();

    let mut client_config = ClientConfig::new();
    client_config
        // Uncomment the following to get logs from kafka (RUST_LOG doesn't work):
        // .set("debug", "consumer,cgrp,topic,fetch")
        .set("bootstrap.servers", &server)
        .set("group.id", &group_id)
        .set("auto.offset.reset", "earliest")
        // Important: disable automatic offset storage so offsets
        // can be manually controlled during the seek operation
        .set("enable.auto.commit", "true")
        .set("enable.auto.offset.store", "false");

    if let (Some(username), Some(password)) = (username, password) {
        client_config
            .set("security.protocol", "SASL_PLAINTEXT")
            .set("sasl.mechanisms", "SCRAM-SHA-512")
            .set("sasl.username", username)
            .set("sasl.password", password);
    } else {
        client_config.set("security.protocol", "PLAINTEXT");
    }

    let consumer: BaseConsumer = client_config
        .create()
        .inspect_err(as_error!("failed to create consumer"))?;

    // Subscribe to topic(s) - broker will handle partition assignment
    consumer
        .subscribe(&[topic])
        .inspect_err(as_error!("failed to subscribe to topic"))?;

    // Wait for initial assignment
    debug!("Waiting for partition assignment...");

    // Poll once to trigger rebalance and get assignment
    loop {
        match consumer.poll(KAFKA_TIMEOUT_SECS) {
            Some(Ok(_msg)) => {
                debug!("Received initial message, seeking to timestamp...");
                // Now seek all assigned partitions to the target timestamp
                seek_to_timestamp(&consumer, timestamp * 1000)?;
                break;
            }
            Some(Err(e)) => {
                error!("Error during initial poll: {:?}", e);
                // sleep and retry
                tokio::time::sleep(core::time::Duration::from_secs(1)).await;
            }
            None => {
                debug!("No message received yet, polling again...");
                // sleep and retry
                tokio::time::sleep(core::time::Duration::from_secs(1)).await;
            }
        }
    }

    // OTel attributes informed by https://opentelemetry.io/docs/specs/semconv/messaging/kafka/
    let consumer_attrs = [
        KeyValue::new("messaging.system", "kafka"),
        KeyValue::new("messaging.destination.name", topic.to_string()),
        KeyValue::new("messaging.consumer.group.name", group_id.to_string()),
        KeyValue::new("messaging.operation.name", "poll"),
        KeyValue::new("messaging.operation.type", "receive"),
        KeyValue::new("messaging.client.id", id.to_string()),
    ];
    let ok_attrs: Vec<KeyValue> = consumer_attrs
        .iter()
        .cloned()
        .chain([KeyValue::new("status", "ok")])
        .collect();
    let input_error_attrs: Vec<KeyValue> = consumer_attrs
        .iter()
        .cloned()
        .chain([
            KeyValue::new("status", "error"),
            KeyValue::new("reason", "kafka_poll"),
        ])
        .collect();
    let output_error_attrs: Vec<KeyValue> = consumer_attrs
        .iter()
        .cloned()
        .chain([
            KeyValue::new("status", "error"),
            KeyValue::new("reason", "kafka_send"),
        ])
        .collect();

    let mut con = conf::build_redis(&config)
        .await
        .inspect_err(as_error!("failed to connect to redis"))?;

    let mut total = 0;
    // start timer
    let start = std::time::Instant::now();

    debug!("Starting Kafka consumer loop...");

    // Process the rest normally
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
                    tokio::time::sleep(core::time::Duration::from_millis(500)).await;
                    continue;
                }
                break;
            }
        }
        match consumer.poll(KAFKA_TIMEOUT_SECS) {
            Some(Ok(message)) => {
                let payload = message.payload().unwrap_or_default();
                con.rpush::<&str, Vec<u8>, usize>(&output_queue, payload.to_vec())
                    .await
                    .inspect_err(|error| {
                        log_error!(error, "failed to push message to queue");
                        ALERT_PROCESSED.add(1, &output_error_attrs);
                    })?;
                trace!("Pushed message to redis");
                ALERT_PROCESSED.add(1, &ok_attrs);
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
            Some(Err(e)) => {
                error!("Error while consuming from Kafka, retrying: {}", e);
                ALERT_PROCESSED.add(1, &input_error_attrs);
                tokio::time::sleep(core::time::Duration::from_secs(1)).await;
                continue;
            }
            None => {
                debug!("No message available");
                if exit_on_eof {
                    info!("No more messages, exiting consumer {}", id);
                    break;
                }
            }
        }
    }

    Ok(())
}
