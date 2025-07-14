use crate::{
    conf,
    utils::{enums::Survey, o11y::as_error},
};

use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, Producer};
use redis::AsyncCommands;
use tracing::{debug, error, info, instrument, trace};

#[async_trait::async_trait]
pub trait AlertProducer {
    async fn produce(&self, topic: Option<String>) -> Result<i64, Box<dyn std::error::Error>>;
}

pub async fn ensure_kafka_topic_partitions(
    bootstrap_servers: &str,
    topic_name: &str,
    expected_nb_partitions: usize,
) -> Result<usize, Box<dyn std::error::Error>> {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;

    let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;

    // Fetch all topics
    let metadata = producer
        .client()
        .fetch_metadata(None, std::time::Duration::from_secs(5))?;
    let existing_topics = metadata
        .topics()
        .iter()
        .map(|t| t.name().to_string())
        .collect::<Vec<String>>();

    let topic_exists = existing_topics.contains(&topic_name.to_string());

    let nb_partitions = if topic_exists {
        info!("Topic {} exists, using existing partitions", topic_name);
        metadata
            .topics()
            .iter()
            .find(|t| t.name() == topic_name)
            .map(|t| t.partitions().len())
            .unwrap_or(expected_nb_partitions)
    } else {
        info!("Topic {} does not exist, creating it", topic_name);
        let opts = AdminOptions::new().operation_timeout(Some(std::time::Duration::from_secs(5)));
        admin_client
            .create_topics(
                &[NewTopic::new(
                    topic_name,
                    expected_nb_partitions as i32,
                    TopicReplication::Fixed(1),
                )],
                &opts,
            )
            .await
            .map_err(|e| format!("Failed to create topic {}: {}", topic_name, e))?;
        info!("Topic {} created successfully", topic_name);
        expected_nb_partitions
    };

    Ok(nb_partitions)
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
    fn default(config_path: &str) -> Self;
    async fn consume(&self, timestamp: i64);
    async fn clear_output_queue(&self) -> Result<(), ConsumerError>;
}

// same as ensure_kafka_topic_partitions, but do not attempt to create the topic
// simply check that the topic exists and return the number of partitions
pub async fn check_kafka_topic_partitions(
    bootstrap_servers: &str,
    topic_name: &str,
) -> Result<Option<usize>, Box<dyn std::error::Error>> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .create()?;

    let metadata = consumer
        .fetch_metadata(Some(topic_name), std::time::Duration::from_secs(5))
        .map_err(|e| format!("Failed to fetch metadata for topic {}: {}", topic_name, e))?;

    let topic_metadata = metadata.topics().iter().find(|t| t.name() == topic_name);
    match topic_metadata.map(|t| t.partitions().len()) {
        Some(partitions) => Ok(Some(partitions)),
        None => Ok(None),
    }
}

#[instrument(skip(username, password, config_path))]
pub async fn consume_partitions(
    id: &str,
    topic: &str,
    group_id: &str,
    partitions: Vec<i32>,
    output_queue: &str,
    max_in_queue: usize,
    timestamp: i64,
    username: Option<&str>,
    password: Option<&str>,
    survey: &Survey,
    config_path: &str,
) -> Result<(), ConsumerError> {
    debug!(?config_path);
    let config = conf::load_config(config_path).inspect_err(as_error!("failed to load config"))?;

    let kafka_config = conf::build_kafka_config(&config, survey)
        .inspect_err(as_error!("failed to build kafka config"))?;

    info!("Consuming from bootstrap server: {}", kafka_config.consumer);

    let mut client_config = ClientConfig::new();
    client_config
        .set("bootstrap.servers", kafka_config.consumer)
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
            }
        }
    }
}
