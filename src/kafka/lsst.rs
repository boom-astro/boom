use crate::{
    conf,
    kafka::base::{consume_partitions, AlertConsumer, ConsumerError},
    utils::{
        enums::Survey,
        o11y::{as_error, log_error},
    },
};
use redis::AsyncCommands;
use tracing::{info, instrument};

pub struct LsstAlertConsumer {
    output_queue: String,
    n_threads: usize,
    max_in_queue: usize,
    group_id: String,
    username: String,
    password: String,
    config_path: String,
    simulated: bool,
}

impl LsstAlertConsumer {
    #[instrument]
    pub fn new(
        n_threads: usize,
        max_in_queue: Option<usize>,
        output_queue: Option<&str>,
        group_id: Option<&str>,
        server_url: Option<&str>,
        simulated: bool,
        config_path: &str,
    ) -> Self {
        // 45 should be divisible by n_threads
        if 45 % n_threads != 0 {
            panic!("Number of threads should be a factor of 45");
        }
        let max_in_queue = max_in_queue.unwrap_or(15000);
        let output_queue = output_queue
            .unwrap_or("LSST_alerts_packets_queue")
            .to_string();
        let mut group_id = group_id.unwrap_or("example-ck").to_string();

        info!(
            "Creating LSST AlertConsumer with {} threads, output_queue: {}, group_id: {} (simulated data: {})",
            n_threads, output_queue, group_id, simulated
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
            output_queue,
            n_threads,
            max_in_queue,
            group_id,
            username: username.unwrap(),
            password: password.unwrap(),
            config_path: config_path.to_string(),
            simulated,
        }
    }
}

#[async_trait::async_trait]
impl AlertConsumer for LsstAlertConsumer {
    fn default(config_path: &str) -> Self {
        Self::new(1, None, None, None, None, true, config_path)
    }

    #[instrument(skip(self))]
    async fn consume(&self, timestamp: i64) {
        let topic = if self.simulated {
            "alerts-simulated".to_string()
        } else {
            "alerts".to_string()
        };
        // divide the 45 LSST partitions for the n_threads that will read them
        let partitions_per_thread = 45 / self.n_threads;
        let mut partitions = vec![vec![]; self.n_threads];
        for i in 0..45 {
            partitions[i / partitions_per_thread].push(i as i32);
        }

        // spawn n_threads to consume each partitions subset
        let mut handles = vec![];
        for i in 0..self.n_threads {
            let topic = topic.clone();
            let partitions = partitions[i].clone();
            let max_in_queue = self.max_in_queue;
            let output_queue = self.output_queue.clone();
            let group_id = self.group_id.clone();
            let username = self.username.clone();
            let password = self.password.clone();
            let config_path = self.config_path.clone();
            let handle = tokio::spawn(async move {
                let result = consume_partitions(
                    &i.to_string(),
                    &topic,
                    &group_id,
                    partitions,
                    &output_queue,
                    max_in_queue,
                    timestamp,
                    Some(&username),
                    Some(&password),
                    &Survey::Lsst,
                    &config_path,
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
    }

    #[instrument(skip(self))]
    async fn clear_output_queue(&self) -> Result<(), ConsumerError> {
        let config =
            conf::load_config(&self.config_path).inspect_err(as_error!("failed to load config"))?;
        let mut con = conf::build_redis(&config)
            .await
            .inspect_err(as_error!("failed to connect to redis"))?;
        let _: () = con
            .del(&self.output_queue)
            .await
            .inspect_err(as_error!("failed to delete queue"))?;
        info!("Cleared redis queue for LSST Kafka consumer");
        Ok(())
    }
}
