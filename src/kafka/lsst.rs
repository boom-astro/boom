use crate::{
    conf,
    kafka::base::{AlertConsumer, ConsumerError},
    utils::{enums::Survey, o11y::as_error},
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

    fn topic_name(&self, _timestamp: i64) -> String {
        if self.simulated {
            "alerts-simulated".to_string()
        } else {
            "alerts".to_string()
        }
    }
    fn n_threads(&self) -> usize {
        self.n_threads
    }
    fn output_queue(&self) -> String {
        self.output_queue.clone()
    }
    fn max_in_queue(&self) -> usize {
        self.max_in_queue
    }
    fn group_id(&self) -> String {
        self.group_id.clone()
    }
    fn config_path(&self) -> String {
        self.config_path.clone()
    }
    fn survey(&self) -> Survey {
        Survey::Lsst
    }
    fn username(&self) -> Option<String> {
        Some(self.username.clone())
    }
    fn password(&self) -> Option<String> {
        Some(self.password.clone())
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
