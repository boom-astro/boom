use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use redis::AsyncCommands;
use tracing::{error, info, trace};

use crate::conf;

#[async_trait::async_trait]
pub trait AlertProducer {
    async fn produce(&self, topic: Option<String>) -> Result<i64, Box<dyn std::error::Error>>;
}

pub async fn consume_partitions(
    id: &str,
    topic: &str,
    group_id: &str,
    partitions: Vec<i32>,
    output_queue: &str,
    max_in_queue: usize,
    timestamp: i64,
    server: &str,
    username: Option<&str>,
    password: Option<&str>,
    config_path: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut producer_config = ClientConfig::new();
    producer_config
        .set("bootstrap.servers", server)
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("group.id", group_id);

    if let (Some(username), Some(password)) = (username, password) {
        producer_config
            .set("sasl.mechanisms", "SCRAM-SHA-512")
            .set("sasl.username", username)
            .set("sasl.password", password);
    } else {
        producer_config.set("security.protocol", "PLAINTEXT");
    }

    let consumer: BaseConsumer = producer_config.create().unwrap();

    consumer.subscribe(&[topic]).unwrap();

    let mut timestamps = rdkafka::TopicPartitionList::new();
    let offset = rdkafka::Offset::Offset(timestamp);
    for i in &partitions {
        let result = timestamps.add_partition(topic, *i).set_offset(offset);
        if let Err(e) = result {
            return Err(format!("Error adding partition: {:?}", e).into());
        }
    }
    let result = consumer.offsets_for_times(timestamps, std::time::Duration::from_secs(5));
    if let Err(e) = result {
        return Err(format!("Error fetching offsets: {:?}", e).into());
    }
    let tpl = result.unwrap();

    consumer.assign(&tpl).unwrap();

    let config = conf::load_config(config_path)?;
    let mut con = conf::build_redis(&config).await?;

    let mut total = 0;

    // start timer
    let start = std::time::Instant::now();
    // poll one message at a time
    loop {
        if max_in_queue > 0 && total % 1000 == 0 {
            loop {
                let nb_in_queue = con.llen::<&str, usize>(&output_queue).await.unwrap();
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
        let message = consumer.poll(tokio::time::Duration::from_secs(5));
        match message {
            Some(Ok(msg)) => {
                let payload = msg.payload().unwrap();
                con.rpush::<&str, Vec<u8>, usize>(&output_queue, payload.to_vec())
                    .await
                    .unwrap();
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
            Some(Err(err)) => {
                error!("Error: {:?}", err);
            }
            None => {
                trace!("No message available");
            }
        }
    }
}

#[async_trait::async_trait]
pub trait AlertConsumer: Sized {
    fn default(config_path: &str) -> Self;
    async fn consume(&self, timestamp: i64) -> Result<(), Box<dyn std::error::Error>>;
    async fn clear_output_queue(&self) -> Result<(), Box<dyn std::error::Error>>;
}
