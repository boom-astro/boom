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
    fn topic_names(&self, _timestamp: i64) -> Vec<String> {
        if self.simulated {
            vec!["alerts-simulated".to_string()]
        } else {
            vec!["lsst-alerts-v11".to_string()]
        }
    }
    fn topic_patterns(&self) -> Vec<String> {
        // LSST uses a single static topic (not date-partitioned), so there is
        // nothing to roll over: subscribe to the literal topic name.
        self.topic_names(0)
    }
    fn output_queue(&self) -> String {
        self.output_queue.clone()
    }
    fn survey(&self) -> &'static str {
        Survey::Lsst.as_str()
    }
}
