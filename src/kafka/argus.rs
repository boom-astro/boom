use std::sync::Arc;

use crate::{
    alert::{decode_argus_packet, ArgusLane},
    kafka::base::{AlertConsumer, QueueRouter},
    utils::enums::Survey,
};

#[derive(Clone)]
pub struct ArgusAlertConsumer {
    lane: Option<ArgusLane>,
    output_queue: String,
}

impl ArgusAlertConsumer {
    pub fn new(lane: ArgusLane) -> Self {
        ArgusAlertConsumer {
            lane: Some(lane),
            output_queue: lane.alert_queue(),
        }
    }

    pub fn mixed() -> Self {
        ArgusAlertConsumer {
            lane: None,
            output_queue: Survey::Argus.alert_input_queue(),
        }
    }

    pub fn lane(&self) -> Option<ArgusLane> {
        self.lane
    }
}

#[async_trait::async_trait]
impl AlertConsumer for ArgusAlertConsumer {
    fn topic_names(&self, _timestamp: i64) -> Vec<String> {
        self.lane.map(|lane| lane.topic()).into_iter().collect()
    }
    fn subscription_topics(&self, _timestamp: i64, _window_days: u64) -> Vec<String> {
        self.topic_names(0)
    }
    fn output_queue(&self) -> String {
        self.output_queue.clone()
    }
    fn router(&self) -> Option<QueueRouter> {
        if self.lane.is_some() {
            return None;
        }
        Some(QueueRouter {
            queues: ArgusLane::ALL.map(|lane| lane.alert_queue()).to_vec(),
            route: Arc::new(|payload| {
                let lane = decode_argus_packet(payload)
                    .map(|alert| alert.lane())
                    .unwrap_or(ArgusLane::Second);
                ArgusLane::ALL
                    .iter()
                    .position(|candidate| *candidate == lane)
                    .unwrap_or(0)
            }),
        })
    }
    fn survey(&self) -> &'static str {
        Survey::Argus.as_str()
    }
}
