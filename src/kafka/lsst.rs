use crate::kafka::base::{consume_partitions, AlertConsumer};
use redis::AsyncCommands;
use tracing::{error, info};

const LSST_SERVER_URL: &str = "usdf-alert-stream-dev.lsst.cloud:9094";

pub struct LsstAlertConsumer {
    topic: String,
    output_queue: String,
    n_threads: usize,
    max_in_queue: usize,
    group_id: String,
    username: String,
    password: String,
    server: String,
}

#[async_trait::async_trait]
impl AlertConsumer for LsstAlertConsumer {
    fn new(
        n_threads: usize,
        max_in_queue: Option<usize>,
        topic: Option<&str>,
        output_queue: Option<&str>,
        group_id: Option<&str>,
        server_url: Option<&str>,
    ) -> LsstAlertConsumer {
        // 45 should be divisible by n_threads
        if 45 % n_threads != 0 {
            panic!("Number of threads should be a factor of 45");
        }
        let max_in_queue = max_in_queue.unwrap_or(15000);
        let topic = topic.unwrap_or("alerts-simulated").to_string();
        let output_queue = output_queue
            .unwrap_or("LSST_alerts_packets_queue")
            .to_string();
        let mut group_id = group_id.unwrap_or("example-ck").to_string();
        let server = server_url.unwrap_or(LSST_SERVER_URL).to_string();

        info!(
            "Creating AlertConsumer with {} threads, topic: {}, output_queue: {}, group_id: {}, server: {}",
            n_threads, topic, output_queue, group_id, server
        );

        // we check that the username and password are set
        let username = std::env::var("LSST_KAFKA_USERNAME");
        if username.is_err() {
            panic!("LSST_KAFKA_USERNAME environment variable not set");
        }
        let password = std::env::var("LSST_KAFKA_PASSWORD");
        if password.is_err() {
            panic!("LSST_KAFKA_PASSWORD environment variable not set");
        }

        // to the groupid, we prepend the username
        group_id = format!("{}-{}", username.as_ref().unwrap(), group_id);

        LsstAlertConsumer {
            topic,
            output_queue,
            n_threads,
            max_in_queue,
            group_id,
            username: username.unwrap(),
            password: password.unwrap(),
            server,
        }
    }

    fn default() -> Self {
        Self::new(1, None, None, None, None, None)
    }
    async fn consume(&self, timestamp: i64) -> Result<(), Box<dyn std::error::Error>> {
        // divide the 45 LSST partitions for the n_threads that will read them
        let partitions_per_thread = 45 / self.n_threads;
        let mut partitions = vec![vec![]; self.n_threads];
        for i in 0..45 {
            partitions[i / partitions_per_thread].push(i as i32);
        }

        // spawn n_threads to consume each partitions subset
        let mut handles = vec![];
        for i in 0..self.n_threads {
            let topic = self.topic.clone();
            let partitions = partitions[i].clone();
            let max_in_queue = self.max_in_queue;
            let output_queue = self.output_queue.clone();
            let group_id = self.group_id.clone();
            let username = self.username.clone();
            let password = self.password.clone();
            let server = self.server.clone();
            let handle = tokio::spawn(async move {
                let result = consume_partitions(
                    &i.to_string(),
                    &topic,
                    &group_id,
                    partitions,
                    &output_queue,
                    max_in_queue,
                    timestamp,
                    &server,
                    Some(&username),
                    Some(&password),
                )
                .await;
                if let Err(e) = result {
                    error!("Error consuming partitions: {:?}", e);
                }
            });
            handles.push(handle);
        }

        // sleep until all threads are done
        for handle in handles {
            handle.await.unwrap();
        }
        Ok(())
    }

    async fn clear_output_queue(&self) -> Result<(), Box<dyn std::error::Error>> {
        let client = redis::Client::open("redis://localhost:6379".to_string()).unwrap();
        let mut con = client.get_multiplexed_async_connection().await.unwrap();
        let _: () = con.del(&self.output_queue).await.unwrap();
        info!("Cleared redis queued for LSST Kafka consumer");
        Ok(())
    }
}
