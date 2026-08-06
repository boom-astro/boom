//! Server-side PostHog product analytics for the Babamul API.
//!
//! # Why server-side
//!
//! The Babamul Python package deliberately does not embed an analytics SDK —
//! shipping one in a library that runs on users' own machines is a privacy
//! problem we don't want. Instead the package identifies itself with a
//! non-identifying `User-Agent`, and *this* service — which already
//! authenticates every request and already stores the user record — is what
//! reports usage to PostHog.
//!
//! # Identity
//!
//! `distinct_id` is the Babamul user `_id`. The web app identifies PostHog
//! persons by exactly the same value (see `frontend/src/pages/Login.tsx`), so
//! web activity, API activity and Kafka consumption all merge into one person
//! rather than three. Requests that aren't authenticated (signup, activate,
//! the public stats endpoints) are reported against a stable per-source
//! anonymous id instead, and are flagged `$process_person_profile: false` so
//! they don't create person profiles in PostHog.
//!
//! # Delivery
//!
//! Events go onto a bounded channel and are drained by a background task that
//! POSTs them to PostHog's `/batch/` endpoint. Capture is therefore always
//! non-blocking: if the queue is full (PostHog slow or down) events are
//! dropped and counted, never awaited. Analytics must not be able to add
//! latency to, or fail, a user's API request.

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

/// Maximum number of events sent to PostHog in a single `/batch/` request.
const MAX_BATCH_SIZE: usize = 250;

/// How long to wait on the PostHog HTTP call before giving up on a batch.
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

/// A single PostHog capture event, shaped for the `/batch/` endpoint.
#[derive(Debug, Clone, Serialize)]
pub struct AnalyticsEvent {
    pub event: String,
    pub distinct_id: String,
    pub properties: Map<String, Value>,
    /// RFC 3339 timestamp of when the event happened (not when it was flushed).
    pub timestamp: String,
}

impl AnalyticsEvent {
    /// Build an event for the given name and distinct id, stamped now.
    pub fn new(event: impl Into<String>, distinct_id: impl Into<String>) -> Self {
        AnalyticsEvent {
            event: event.into(),
            distinct_id: distinct_id.into(),
            properties: Map::new(),
            timestamp: chrono::Utc::now().to_rfc3339(),
        }
    }

    /// Attach a property, ignoring values that fail to serialize.
    pub fn with(mut self, key: &str, value: impl Serialize) -> Self {
        if let Ok(value) = serde_json::to_value(value) {
            self.properties.insert(key.to_string(), value);
        }
        self
    }

    /// Attach a property only when it is `Some`.
    pub fn with_opt(self, key: &str, value: Option<impl Serialize>) -> Self {
        match value {
            Some(value) => self.with(key, value),
            None => self,
        }
    }

    /// Mark this event as belonging to an anonymous actor, so PostHog does not
    /// create or update a person profile for it.
    pub fn anonymous(self) -> Self {
        self.with("$process_person_profile", false)
    }
}

/// Handle used to enqueue analytics events.
///
/// Cloning is cheap and clones share one queue. A handle built by
/// [`AnalyticsClient::disabled`] silently discards everything, which is what
/// tests and unconfigured deployments get.
#[derive(Clone)]
pub struct AnalyticsClient {
    inner: Option<Arc<Sender>>,
}

struct Sender {
    tx: mpsc::Sender<AnalyticsEvent>,
    dropped: AtomicU64,
}

impl AnalyticsClient {
    /// A client that discards every event.
    pub fn disabled() -> Self {
        AnalyticsClient { inner: None }
    }

    /// Whether events enqueued on this client will actually be delivered.
    pub fn is_enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Build a client from config, spawning the background flush task.
    ///
    /// Returns a disabled client when no project key is configured, so callers
    /// never have to branch on whether analytics are turned on.
    pub fn from_config(config: &PostHogConfig) -> Self {
        if !config.is_enabled() {
            tracing::info!("PostHog analytics are DISABLED (no project API key configured)");
            return Self::disabled();
        }

        let (tx, rx) = mpsc::channel(config.queue_capacity);
        let client = AnalyticsClient {
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

    /// Enqueue an event. Never blocks and never fails the caller.
    pub fn capture(&self, event: AnalyticsEvent) {
        let Some(inner) = self.inner.as_ref() else {
            return;
        };

        if inner.tx.try_send(event).is_err() {
            // Full (or closed) queue: drop rather than apply backpressure to an
            // in-flight API request. Log the first drop and then every 1000th,
            // so a sustained outage doesn't flood the logs.
            let dropped = inner.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            EVENTS_DROPPED.add(1, &[KeyValue::new("reason", "queue_full")]);
            if dropped == 1 || dropped % 1000 == 0 {
                tracing::warn!(
                    dropped,
                    "PostHog analytics queue is full; dropping events. \
                     Increase posthog.queue_capacity or check PostHog availability."
                );
            }
        }
    }
}

/// Drain the queue and POST batches to PostHog until the channel closes.
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
                match received {
                    Some(event) => {
                        batch.push(event);
                        // Drain whatever else is already queued so a burst goes
                        // out in one request instead of one per tick.
                        while batch.len() < MAX_BATCH_SIZE {
                            match rx.try_recv() {
                                Ok(event) => batch.push(event),
                                Err(_) => break,
                            }
                        }
                        if batch.len() >= MAX_BATCH_SIZE {
                            send_batch(&http, &endpoint, &api_key, std::mem::take(&mut batch)).await;
                        }
                    }
                    None => {
                        // Channel closed: flush what's left and stop.
                        if !batch.is_empty() {
                            send_batch(&http, &endpoint, &api_key, std::mem::take(&mut batch)).await;
                        }
                        return;
                    }
                }
            }
            _ = ticker.tick() => {
                if !batch.is_empty() {
                    send_batch(&http, &endpoint, &api_key, std::mem::take(&mut batch)).await;
                }
            }
        }
    }
}

/// POST one batch. Failures are logged and counted, never retried — analytics
/// are best-effort and a retry queue would risk unbounded memory growth.
async fn send_batch(
    http: &reqwest::Client,
    endpoint: &str,
    api_key: &str,
    batch: Vec<AnalyticsEvent>,
) {
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
