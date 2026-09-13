//! Server-side PostHog product analytics for the Babamul API; see `docs/analytics.md`.

use crate::conf::PostHogConfig;
use crate::utils::o11y::metrics::API_METER;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use opentelemetry::metrics::Counter;
use opentelemetry::KeyValue;
use serde::Serialize;
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;

const MAX_BATCH_SIZE: usize = 250;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

static EVENTS_DROPPED: LazyLock<Counter<u64>> = LazyLock::new(|| {
    API_METER
        .u64_counter("api.analytics.event.dropped")
        .with_unit("{event}")
        .with_description(
            "Number of analytics events dropped because the PostHog queue was full \
             or the batch could not be delivered.",
        )
        .build()
});

static EVENTS_SENT: LazyLock<Counter<u64>> = LazyLock::new(|| {
    API_METER
        .u64_counter("api.analytics.event.sent")
        .with_unit("{event}")
        .with_description("Number of analytics events successfully delivered to PostHog.")
        .build()
});

#[derive(Debug, Clone, Serialize)]
pub struct AnalyticsEvent {
    pub event: String,
    pub distinct_id: String,
    pub properties: Map<String, Value>,
    pub timestamp: String,
}

impl AnalyticsEvent {
    pub fn new(event: impl Into<String>, distinct_id: impl Into<String>) -> Self {
        Self {
            event: event.into(),
            distinct_id: distinct_id.into(),
            properties: Map::new(),
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    pub fn with(mut self, key: &str, value: impl Serialize) -> Self {
        if let Ok(value) = serde_json::to_value(value) {
            self.properties.insert(key.to_string(), value);
        }
        self
    }

    pub fn with_opt(self, key: &str, value: Option<impl Serialize>) -> Self {
        match value {
            Some(value) => self.with(key, value),
            None => self,
        }
    }

    /// Opts the event out of person profiles, which PostHog would otherwise create.
    pub fn anonymous(self) -> Self {
        self.with("$process_person_profile", false)
    }
}

#[derive(Clone)]
pub struct AnalyticsClient {
    inner: Option<Arc<Sender>>,
}

struct Sender {
    tx: mpsc::Sender<AnalyticsEvent>,
    dropped: AtomicU64,
}

impl AnalyticsClient {
    pub fn disabled() -> Self {
        Self { inner: None }
    }

    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Spawns the background flush task, or returns a disabled client when no key is set.
    pub fn from_config(config: &PostHogConfig) -> Self {
        if !config.is_enabled() {
            tracing::info!("PostHog analytics are DISABLED (no project API key configured)");
            return Self::disabled();
        }

        // `mpsc::channel(0)` panics: a config typo must not take the API down at startup.
        let capacity = config.queue_capacity.max(1);
        if capacity != config.queue_capacity {
            tracing::warn!(
                configured = config.queue_capacity,
                capacity,
                "posthog.queue_capacity must be at least 1; overriding"
            );
        }
        let (tx, rx) = mpsc::channel(capacity);
        let client = Self {
            inner: Some(Arc::new(Sender {
                tx,
                dropped: AtomicU64::new(0),
            })),
        };

        tokio::spawn(flush_loop(
            rx,
            config.host.trim_end_matches('/').to_string(),
            config.project_api_key.clone(),
            Duration::from_secs(config.flush_interval_seconds.max(1)),
        ));

        tracing::info!(host = %config.host, "PostHog analytics are ENABLED");
        client
    }

    /// Never blocks and never fails the caller: a full queue drops the event.
    pub fn capture(&self, event: AnalyticsEvent) -> bool {
        let Some(inner) = self.inner.as_ref() else {
            return false;
        };

        let (reason, message) = match inner.tx.try_send(event) {
            Ok(()) => return true,
            Err(mpsc::error::TrySendError::Full(_)) => (
                "queue_full",
                "PostHog analytics queue is full; dropping events. \
                 Increase posthog.queue_capacity or check PostHog availability.",
            ),
            Err(mpsc::error::TrySendError::Closed(_)) => (
                "queue_closed",
                "PostHog analytics queue is closed; the flush task is no longer \
                 running and events will be dropped until the service restarts.",
            ),
        };

        let dropped = inner.dropped.fetch_add(1, Ordering::Relaxed) + 1;
        EVENTS_DROPPED.add(1, &[KeyValue::new("reason", reason)]);
        if dropped == 1 || dropped % 1000 == 0 {
            tracing::warn!(dropped, "{}", message);
        }
        false
    }
}

async fn flush_loop(
    mut rx: mpsc::Receiver<AnalyticsEvent>,
    host: String,
    api_key: String,
    flush_interval: Duration,
) {
    let http = match reqwest::Client::builder().timeout(REQUEST_TIMEOUT).build() {
        Ok(http) => http,
        Err(error) => {
            tracing::error!(%error, "failed to build the PostHog HTTP client; analytics disabled");
            return;
        }
    };
    let endpoint = format!("{}/batch/", host);

    let mut ticker = tokio::time::interval(flush_interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    let mut batch: Vec<AnalyticsEvent> = Vec::new();
    loop {
        tokio::select! {
            received = rx.recv() => {
                let Some(event) = received else {
                    send_batch(&http, &endpoint, &api_key, std::mem::take(&mut batch)).await;
                    return;
                };
                batch.push(event);
                while batch.len() < MAX_BATCH_SIZE {
                    let Ok(event) = rx.try_recv() else { break };
                    batch.push(event);
                }
                if batch.len() >= MAX_BATCH_SIZE {
                    send_batch(&http, &endpoint, &api_key, std::mem::take(&mut batch)).await;
                }
            }
            _ = ticker.tick() => {
                send_batch(&http, &endpoint, &api_key, std::mem::take(&mut batch)).await;
            }
        }
    }
}

/// Best-effort: a failed batch is counted and dropped, never retried.
async fn send_batch(
    http: &reqwest::Client,
    endpoint: &str,
    api_key: &str,
    batch: Vec<AnalyticsEvent>,
) {
    if batch.is_empty() {
        return;
    }
    let count = batch.len() as u64;
    let body = json!({ "api_key": api_key, "batch": batch });

    match http.post(endpoint).json(&body).send().await {
        Ok(response) if response.status().is_success() => {
            EVENTS_SENT.add(count, &[]);
        }
        Ok(response) => {
            let status = response.status();
            EVENTS_DROPPED.add(count, &[KeyValue::new("reason", "http_error")]);
            tracing::warn!(
                %status,
                count,
                "PostHog rejected an analytics batch; events dropped"
            );
        }
        Err(error) => {
            EVENTS_DROPPED.add(count, &[KeyValue::new("reason", "request_failed")]);
            tracing::warn!(%error, count, "failed to deliver an analytics batch to PostHog");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_client_swallows_events() {
        let client = AnalyticsClient::disabled();
        assert!(!client.is_enabled());
        // Must not panic even though there is no receiver.
        client.capture(AnalyticsEvent::new("test", "user-1"));
    }

    #[test]
    fn from_config_without_key_is_disabled() {
        let config = PostHogConfig::default();
        assert!(!AnalyticsClient::from_config(&config).is_enabled());
    }

    #[tokio::test]
    async fn zero_queue_capacity_does_not_panic() {
        let config = PostHogConfig {
            project_api_key: "phc_test".to_string(),
            queue_capacity: 0,
            ..PostHogConfig::default()
        };
        let client = AnalyticsClient::from_config(&config);
        assert!(client.is_enabled());
        client.capture(AnalyticsEvent::new("test", "user-1"));
    }

    #[test]
    fn with_opt_skips_none() {
        let event = AnalyticsEvent::new("test", "user-1")
            .with("a", 1)
            .with_opt("b", None::<String>)
            .with_opt("c", Some("yes"));
        assert!(event.properties.contains_key("a"));
        assert!(!event.properties.contains_key("b"));
        assert_eq!(event.properties.get("c").unwrap(), "yes");
    }

    #[test]
    fn anonymous_events_opt_out_of_person_profiles() {
        let event = AnalyticsEvent::new("test", "anon").anonymous();
        assert_eq!(
            event.properties.get("$process_person_profile").unwrap(),
            false
        );
    }
}
