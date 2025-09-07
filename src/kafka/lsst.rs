use crate::{kafka::base::AlertConsumer, utils::enums::Survey};
use tracing::instrument;

pub struct LsstAlertConsumer {
    output_queue: String,
    username: String,
    password: String,
    simulated: bool,
}

impl LsstAlertConsumer {
    #[instrument]
    pub fn new(output_queue: Option<&str>, simulated: bool) -> Self {
        let output_queue = output_queue
            .unwrap_or("LSST_alerts_packets_queue")
            .to_string();

        // we check that the username and password are set
        let username = std::env::var("LSST_KAFKA_USERNAME");
        if username.is_err() {
            panic!("LSST_KAFKA_USERNAME environment variable not set");
        }
        let password = std::env::var("LSST_KAFKA_PASSWORD");
        if password.is_err() {
            panic!("LSST_KAFKA_PASSWORD environment variable not set");
        }

        LsstAlertConsumer {
            output_queue,
            username: username.unwrap(),
            password: password.unwrap(),
            simulated,
        }
    }
}

#[async_trait::async_trait]
impl AlertConsumer for LsstAlertConsumer {
    fn topic_name(&self, _timestamp: i64) -> String {
        if self.simulated {
            "alerts-simulated".to_string()
        } else {
            "lsst-alerts-v9.0".to_string()
        }
    }
    fn output_queue(&self) -> String {
        self.output_queue.clone()
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
}
