use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use redis::AsyncCommands;
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::TRACE)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("setting default subscriber failed");

    let topic = "alerts-simulated";

    let max_in_queue: usize = 10000;

    let server = "usdf-alert-stream-dev.lsst.cloud:9094".to_string();
    let username = std::env::var("KAFKA_USERNAME").unwrap();
    let password = std::env::var("KAFKA_PASSWORD").unwrap();
    let group_id = format!("{}-example-ck-test", username);

    let queue_name = "LSST_alerts_packets_queue".to_string();

    // drop the queue in redis
    let client = redis::Client::open("redis://localhost:6379".to_string()).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();
    let _: () = con.del(&queue_name).await.unwrap();

    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", &server)
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanisms", "SCRAM-SHA-512")
        .set("sasl.username", &username)
        .set("sasl.password", &password)
        .set("group.id", group_id)
        // we want to poll multiple messages at a time, so increase the fetch_max_bytes
        // .set("fetch.max.bytes", "104857600") // 100MB
        .create()
        .unwrap();

    consumer.subscribe(&[topic]).unwrap();

    println!("Subscribed to topic {}", topic);

    let timestamp: i64 = 1740696960000;

    let mut timestamps = rdkafka::TopicPartitionList::new();
    let offset = rdkafka::Offset::Offset(timestamp);
    for i in 0..45 {
        let result = timestamps.add_partition(topic, i).set_offset(offset);
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

    println!("Assigned partitions, reading from timestamp {}", timestamp);

    let mut total = 0;

    info!("Reading from kafka topic and pushing to the queue");
    // start timer
    let start = std::time::Instant::now();
    // poll one message at a time
    loop {
        if max_in_queue > 0 && total % 1000 == 0 {
            loop {
                let nb_in_queue = con.llen::<&str, usize>(&queue_name).await.unwrap();
                if nb_in_queue >= max_in_queue {
                    info!(
                        "{} (limit: {}) items in queue, sleeping...",
                        nb_in_queue, max_in_queue
                    );
                    std::thread::sleep(core::time::Duration::from_secs(1));
                    continue;
                }
                break;
            }
        }
        let message = consumer.poll(tokio::time::Duration::from_secs(5));
        match message {
            Some(Ok(msg)) => {
                let payload = msg.payload().unwrap();
                con.rpush::<&str, Vec<u8>, usize>(&queue_name, payload.to_vec())
                    .await
                    .unwrap();
                info!("Pushed message to redis");
                total += 1;
                if total % 1000 == 0 {
                    info!("Pushed {} items since {:?}", total, start.elapsed());
                }
            }
            Some(Err(err)) => {
                error!("Error: {:?}", err);
            }
            None => {
                info!("No message available");
            }
        }
    }
}
