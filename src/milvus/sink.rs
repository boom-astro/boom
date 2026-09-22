//! Failure-tolerant wrapper around [`MilvusClient`] for the enrichment path.
//!
//! Milvus is an optional add-on: Mongo is the source of truth for enriched
//! alerts, and the embeddings are only needed for similarity search. So a
//! Milvus outage must never stop enrichment. This wrapper degrades instead:
//! it keeps the pipeline running and pauses uploads while Milvus is unhealthy.

use std::time::{Duration, Instant};

use tracing::{debug, info, warn};

use super::client::{MilvusClient, MilvusError};
use super::insert::EmbeddingRow;
use crate::conf::MilvusConfig;

/// Pause after the first failed attempt. Each further failure doubles it.
const COOLDOWN_BASE: Duration = Duration::from_secs(30);

/// Upper bound on the pause between attempts.
const COOLDOWN_MAX: Duration = Duration::from_secs(300);

/// Pause before the Nth consecutive retry: 30s, 60s, 120s, 240s, capped at
/// [`COOLDOWN_MAX`].
fn cooldown(consecutive_failures: u32) -> Duration {
    let shift = consecutive_failures.saturating_sub(1).min(5);
    COOLDOWN_BASE
        .saturating_mul(1u32 << shift)
        .min(COOLDOWN_MAX)
}

/// An optional Milvus connection guarded by a circuit breaker.
///
/// Every failure — at connect or at upsert — trips the breaker, so a single
/// slow batch costs one `milvus.timeout_seconds` timeout rather than one per
/// batch for as long as the outage lasts. The connection is rebuilt on the
/// first attempt after the cooldown, so recovery needs no worker restart.
pub struct MilvusSink {
    config: MilvusConfig,
    /// `None` while disabled, or while disconnected after a failure.
    client: Option<MilvusClient>,
    consecutive_failures: u32,
    /// While in the future, uploads are skipped without touching the network.
    retry_at: Option<Instant>,
}

impl MilvusSink {
    /// Connect if `milvus.enabled`, degrading to a paused sink if Milvus is
    /// unreachable rather than failing the caller.
    ///
    /// This is deliberately infallible: an operator enabling Milvus must not be
    /// able to take enrichment down by pointing it at a server that is off.
    pub async fn connect_or_degrade(config: &MilvusConfig) -> Self {
        let mut sink = Self {
            config: config.clone(),
            client: None,
            consecutive_failures: 0,
            retry_at: None,
        };

        if !config.enabled {
            return sink;
        }

        // A failure here is recorded like any other, so the first batch does not
        // immediately redial a server we already know is down.
        match MilvusClient::connect(config).await {
            Ok(client) => sink.client = Some(client),
            Err(e) => sink.record_failure("connect to milvus at startup", e),
        }
        sink
    }

    /// Whether the integration is switched on at all. False means callers can
    /// skip collecting embeddings entirely; it does not track breaker state,
    /// which is deliberate — an outage should not silently change what the
    /// worker computes, only where it ends up.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Whether an upload would be attempted right now.
    fn is_ready(&self) -> bool {
        self.config.enabled && self.retry_at.is_none_or(|at| Instant::now() >= at)
    }

    fn record_failure(&mut self, what: &str, e: MilvusError) {
        // Drop the channel so the next attempt redials; a broken connection
        // never recovers on its own.
        self.client = None;
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        let pause = cooldown(self.consecutive_failures);
        self.retry_at = Some(Instant::now() + pause);
        warn!(
            failures = self.consecutive_failures,
            pause_secs = pause.as_secs(),
            "failed to {}: {}; pausing embedding uploads",
            what,
            e
        );
    }

    fn record_success(&mut self) {
        if self.consecutive_failures > 0 {
            info!(
                failures = self.consecutive_failures,
                "milvus recovered; resuming embedding uploads"
            );
        }
        self.consecutive_failures = 0;
        self.retry_at = None;
    }

    /// Upsert a batch, skipping the attempt while the breaker is open.
    ///
    /// Never returns an error: failures are recorded and logged, because the
    /// alerts these embeddings came from are already persisted in Mongo.
    pub async fn upsert(&mut self, rows: &[EmbeddingRow]) {
        if rows.is_empty() || !self.is_ready() {
            return;
        }

        if self.client.is_none() {
            match MilvusClient::connect(&self.config).await {
                Ok(client) => {
                    self.client = Some(client);
                }
                Err(e) => {
                    self.record_failure("reconnect to milvus", e);
                    return;
                }
            }
        }

        let client = self
            .client
            .as_mut()
            .expect("client was just connected above");
        match client.upsert_embeddings(rows).await {
            Ok(count) => {
                debug!("upserted {} fusion embeddings to milvus", count);
                self.record_success();
            }
            Err(e) => {
                let what = format!("upsert {} fusion embeddings", rows.len());
                self.record_failure(&what, e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Built through serde so the test sees the same defaults a deployment gets.
    fn config(enabled: bool) -> MilvusConfig {
        serde_json::from_value(serde_json::json!({ "enabled": enabled }))
            .expect("milvus config defaults must deserialize")
    }

    fn sink(enabled: bool) -> MilvusSink {
        MilvusSink {
            config: config(enabled),
            client: None,
            consecutive_failures: 0,
            retry_at: None,
        }
    }

    #[test]
    fn cooldown_grows_then_caps() {
        assert_eq!(cooldown(1), Duration::from_secs(30));
        assert_eq!(cooldown(2), Duration::from_secs(60));
        assert_eq!(cooldown(3), Duration::from_secs(120));
        assert_eq!(cooldown(4), Duration::from_secs(240));
        // 30s << 4 == 480s, clamped to the ceiling.
        assert_eq!(cooldown(5), COOLDOWN_MAX);
        // Large inputs stay clamped, and never panic via shift overflow.
        assert_eq!(cooldown(u32::MAX), COOLDOWN_MAX);
    }

    /// A disabled sink must never dial, so it is never "ready" and callers can
    /// skip building embedding rows at all.
    #[test]
    fn disabled_sink_is_inert() {
        let sink = sink(false);
        assert!(!sink.is_enabled());
        assert!(!sink.is_ready());
    }

    /// The point of the breaker: after a failure the next batch is skipped
    /// without a network call, so an outage costs one timeout, not one per batch.
    #[test]
    fn a_failure_pauses_further_attempts() {
        let mut sink = sink(true);
        assert!(sink.is_ready(), "a fresh enabled sink attempts uploads");

        sink.record_failure("connect", MilvusError::NotEnabled);
        assert!(!sink.is_ready());
        assert_eq!(sink.consecutive_failures, 1);

        // Still enabled — the outage changes where embeddings go, not whether
        // the worker computes them.
        assert!(sink.is_enabled());
    }

    /// Recovery must not need a worker restart.
    #[test]
    fn the_breaker_closes_once_the_cooldown_passes() {
        let mut sink = sink(true);
        sink.record_failure("connect", MilvusError::NotEnabled);
        assert!(!sink.is_ready());

        sink.retry_at = Some(Instant::now() - Duration::from_secs(1));
        assert!(sink.is_ready(), "the cooldown has elapsed");
    }

    /// Consecutive failures back off; a success resets the budget so a later,
    /// unrelated blip starts from the short pause again.
    #[test]
    fn success_resets_the_backoff() {
        let mut sink = sink(true);
        sink.record_failure("connect", MilvusError::NotEnabled);
        sink.record_failure("connect", MilvusError::NotEnabled);
        assert_eq!(sink.consecutive_failures, 2);

        sink.record_success();
        assert_eq!(sink.consecutive_failures, 0);
        assert!(sink.retry_at.is_none());
        assert!(sink.is_ready());
    }

    /// A failure drops the channel; a broken tonic channel does not heal, so
    /// the next attempt has to redial.
    #[test]
    fn a_failure_drops_the_connection() {
        let mut sink = sink(true);
        sink.record_failure("upsert", MilvusError::NotEnabled);
        assert!(sink.client.is_none());
    }
}
