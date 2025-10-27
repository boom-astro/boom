use crate::{kafka::base::AlertConsumer, utils::enums::Survey};
use tracing::instrument;

pub struct LsstAlertConsumer {
    output_queue: String,
    simulated: bool,
}

impl LsstAlertConsumer {
    #[instrument]
    pub fn new(output_queue: Option<&str>, simulated: bool) -> Self {
        let output_queue = output_queue
            .unwrap_or("LSST_alerts_packets_queue")
            .to_string();

        LsstAlertConsumer {
            output_queue,
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
}
