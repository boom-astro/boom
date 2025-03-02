use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::Message;
use redis::AsyncCommands;
use tracing::{error, info};

use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

pub fn download_alerts_from_archive(date: &str) -> Result<i64, Box<dyn std::error::Error>> {
    if date.len() != 8 {
        return Err("Invalid date format".into());
    }

    let data_folder = format!("data/alerts/ztf/{}", date);

    if std::path::Path::new(&data_folder).exists() && std::fs::read_dir(&data_folder)?.count() > 0 {
        info!("Alerts already downloaded to {}", data_folder);
        let count = std::fs::read_dir(&data_folder).unwrap().count();
        return Ok(count as i64);
    }

    std::fs::create_dir_all(&data_folder)?;

    info!("Downloading alerts for date {}", date);
    let url = format!(
        "https://ztf.uw.edu/alerts/public/ztf_public_{}.tar.gz",
        date
    );
    let output = std::process::Command::new("wget")
        .arg(&url)
        .arg("-P")
        .arg(&data_folder)
        .output()?;
    if !output.status.success() {
        let error_msg = String::from_utf8_lossy(&output.stderr);
        if error_msg.contains("404 Not Found") {
            return Err("No alerts found for this date".into());
        }
        return Err(format!("Failed to download alerts: {}", error_msg).into());
    } else {
        info!("Downloaded alerts to {}", data_folder);
    }

    let output = std::process::Command::new("tar")
        .arg("-xzf")
        .arg(format!("{}/ztf_public_{}.tar.gz", data_folder, date))
        .arg("-C")
        .arg(&data_folder)
        .output()?;
    if !output.status.success() {
        return Err("Failed to extract alerts".into());
    } else {
        info!("Extracted alerts to {}", data_folder);
    }

    std::fs::remove_file(format!("{}/ztf_public_{}.tar.gz", data_folder, date))?;

    let count = std::fs::read_dir(&data_folder)?.count();

    Ok(count as i64)
}

pub async fn consume_alerts(
    topic: &str,
    group_id: Option<String>,
    exit_on_eof: bool,
    max_in_queue: usize,
    queue_name: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    let group_id = match group_id {
        Some(id) => id,
        None => uuid::Uuid::new_v4().to_string(),
    };

    let queue_name = match queue_name {
        Some(q) => q,
        None => "ZTF_alerts_packets_queue".to_string(),
    };

    let consumer: BaseConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", "localhost:9092")
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        .set("enable.auto.commit", "true")
        .set("auto.offset.reset", "earliest")
        .create()
        .unwrap();

    consumer.subscribe(&[topic]).unwrap();

    let metadata = match consumer.fetch_metadata(Some(&topic), std::time::Duration::from_secs(1)) {
        Ok(m) => m,
        Err(e) => {
            return Err(e.into());
        }
    };

    let metadata = metadata.topics().iter().next().unwrap();

    let client = redis::Client::open("redis://localhost:6379".to_string()).unwrap();
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

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
                let mut total_available = 0;
                for partition in metadata.partitions().iter() {
                    let (low, high) = consumer
                        .fetch_watermarks(
                            metadata.name(),
                            partition.id(),
                            std::time::Duration::from_secs(1),
                        )
                        .unwrap();
                    total_available += high - low;
                }
                if exit_on_eof && (total_available == 0 || total_available == total) {
                    info!("Read all available messages, exiting");
                    break;
                }
            }
        }
    }

    Ok(())
}

pub async fn produce_from_archive(
    date: &str,
    limit: i64,
    topic: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    match download_alerts_from_archive(&date) {
        Ok(count) => count,
        Err(e) => {
            error!("Error downloading alerts: {}", e);
            return Err(e);
        }
    };

    let topic_name = match topic {
        Some(t) => t,
        None => format!("ztf_{}_programid1", &date),
    };

    info!("Initializing producer for topic {}", topic_name);
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", "localhost:9092")
        .set("message.timeout.ms", "5000")
        .create()
        .expect("Producer creation error");

    info!("Pushing alerts to {}", topic_name);

    let mut total_pushed = 0;
    let start = std::time::Instant::now();
    for entry in std::fs::read_dir(format!("data/alerts/ztf/{}", date))? {
        let entry = entry.unwrap();
        let path = entry.path();
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

        if limit > 0 && total_pushed >= limit {
            info!("Reached limit of {} pushed items", limit);
            break;
        }
    }

    info!("Pushed {} alerts to the queue", total_pushed);

    // close producer
    producer.flush(std::time::Duration::from_secs(1)).unwrap();

    Ok(())
}
