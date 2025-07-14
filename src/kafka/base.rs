use crate::{
    conf,
    utils::{enums::Survey, o11y::as_error},
};

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use redis::AsyncCommands;
use tracing::{debug, error, info, instrument, trace};

#[async_trait::async_trait]
pub trait AlertProducer {
    async fn produce(&self, topic: Option<String>) -> Result<i64, Box<dyn std::error::Error>>;
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
