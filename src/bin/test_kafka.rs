use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use std::time::Duration;

async fn get_topic_message_count(topic_name: &str, brokers: &str) -> Result<i64, KafkaError> {
    // Create consumer
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "temp-counter-group")
        .create()?;

    // Get topic metadata
    let metadata = consumer.fetch_metadata(Some(topic_name), Duration::from_secs(10))?;

    let topic_metadata = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic_name)
        .ok_or_else(|| {
            KafkaError::AdminOp(rdkafka::error::RDKafkaErrorCode::UnknownTopicOrPartition)
        })?;

    let mut total_messages = 0i64;

    // Iterate through all partitions
    for partition in topic_metadata.partitions() {
        let partition_id = partition.id();

        // Get watermarks (low and high offsets)
        let (low, high) =
            consumer.fetch_watermarks(topic_name, partition_id, Duration::from_secs(10))?;

        let messages_in_partition = high - low;
        total_messages += messages_in_partition;

        // println!("Partition {}: {} messages (low: {}, high: {})",
        //          partition_id, messages_in_partition, low, high);
    }

    Ok(total_messages)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let brokers = "localhost:9092";

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", "temp-list-group")
        .create()?;
    let metadata = consumer.fetch_metadata(None, Duration::from_secs(10))?;

    println!("Available topics:");
    for topic in metadata.topics() {
        println!(" - {}", topic.name());
        match get_topic_message_count(topic.name(), brokers).await {
            Ok(count) => println!("Total messages in topic '{}': {}", topic.name(), count),
            Err(e) => eprintln!("Error getting message count: {}", e),
        }
    }

    Ok(())
}
