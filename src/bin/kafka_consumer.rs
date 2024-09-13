use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use redis::AsyncCommands;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = redis::Client::open(
        "redis://localhost:6379".to_string()
    )?;
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // empty the queue
    con.del::<&str, usize>("ZTF_alerts_packet_queue").await.unwrap();

    let mut total = 0;
    // generate a random group id every time
    let consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", &Uuid::new_v4().to_string())
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()?;

    consumer.subscribe(&["ztf_20240519_programid1"]).unwrap();

    // check how many messages are in the queue
    let mut count = 0;
    println!("Reading from kafka topic and pushing to the queue");
    // start timer
    let start = std::time::Instant::now();
    // poll one message at a time
    loop {
        count += 1;
        if count % 1000 == 0 {
            loop {
                let in_queue = con.llen::<&str, usize>("ZTF_alerts_packet_queue").await;
                match in_queue {
                    Ok(in_queue) => {
                        if in_queue < 10000 {
                            break;
                        } else {
                            println!("Queue has {} items, sleeping for 5 seconds", in_queue);
                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
                        }
                    }
                    Err(err) => {
                        println!("Error: {:?}", err);
                    }
                }
            }
            count = 0;
        }
        let message = consumer.poll(tokio::time::Duration::from_secs(30));
        match message {
            Some(Ok(msg)) => {
                let payload = msg.payload().unwrap();
                con.rpush::<&str, Vec<u8>, usize>("ZTF_alerts_packet_queue", payload.to_vec()).await.unwrap();
                total += 1;
                if total % 1000 == 0 {
                    println!("Pushed {} items since {:?}", total, start.elapsed());
                }
            }
            Some(Err(err)) => {
                println!("Error: {:?}", err);
            }
            None => {
                println!("No message");
            }
        }
    }
}