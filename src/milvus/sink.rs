//! Failure-tolerant wrapper around [`MilvusClient`] for the enrichment path.
//!
//! Milvus is an optional add-on: Mongo is the source of truth for enriched
//! alerts, and the embeddings are only needed for similarity search. So a
//! Milvus outage must never stop enrichment. This wrapper degrades instead:
//! it keeps the pipeline running and pauses uploads while Milvus is unhealthy.

use std::time::{Duration, Instant};

use tracing::{info, warn};

use super::backup::BackupQueue;
use super::client::{MilvusClient, MilvusError};
use super::insert::EmbeddingRow;
use crate::conf::MilvusConfig;

/// Pause after the first failed attempt. Each further failure doubles it.
const COOLDOWN_BASE: Duration = Duration::from_secs(30);

/// Upper bound on the pause between attempts, held until a success resets it.
const COOLDOWN_MAX: Duration = Duration::from_secs(60 * 60);

/// How long an idle worker waits before checking the backup queue again
/// after finding it empty, so idle workers do not poll Valkey every tick.
const IDLE_DRAIN_PAUSE: Duration = Duration::from_secs(30);

/// Pause before the Nth consecutive retry: 30s, 1m, 2m, 4m, 8m, 16m, 32m,
/// then [`COOLDOWN_MAX`] for every retry after that.
fn cooldown(consecutive_failures: u32) -> Duration {
    let shift = consecutive_failures.saturating_sub(1).min(7);
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
    /// Holds embeddings the breaker turned away. `None` when the backup
    /// queue is off, in which case rejected rows are dropped.
    queue: Option<BackupQueue>,
    /// While in the future, idle drains skip the queue: it was last found
    /// empty or unreadable.
    idle_drain_at: Option<Instant>,
}

impl MilvusSink {
    /// Connect if `milvus.enabled`, degrading to a paused sink if Milvus is
    /// unreachable rather than failing the caller.
    ///
    /// This is deliberately infallible: an operator enabling Milvus must not be
    /// able to take enrichment down by pointing it at a server that is off.
    pub async fn connect_or_degrade(config: &MilvusConfig, queue: Option<BackupQueue>) -> Self {
        let mut sink = Self {
            config: config.clone(),
            client: None,
            consecutive_failures: 0,
            retry_at: None,
            queue,
            idle_drain_at: None,
        };

        if !config.enabled {
            return sink;
        }

        // A failure here is recorded like any other, so the first batch does not
        // immediately redial a server we already know is down.
        match MilvusClient::connect(config).await {
            Ok(client) => {
                sink.client = Some(client);
                sink.log_connected();
            }
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

    /// Time left before the breaker allows another attempt; zero once closed.
    fn retry_in(&self) -> Duration {
        self.retry_at.map_or(Duration::ZERO, |at| {
            at.saturating_duration_since(Instant::now())
        })
    }

    fn log_connected(&self) {
        info!(
            endpoint = %self.config.endpoint(),
            timeout_secs = self.config.timeout_seconds,
            "milvus connection active"
        );
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
            timeout_secs = self.config.timeout_seconds,
            "failed to {}: {}; milvus breaker open, pausing embedding uploads",
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

    /// Buffer rows the sink could not upload, logging the breaker state and
    /// how many rows Valkey now holds. A queue failure is swallowed: Valkey
    /// being down too does not justify failing enrichment.
    async fn buffer(&mut self, rows: &[EmbeddingRow]) {
        let failures = self.consecutive_failures;
        let retry_in_secs = self.retry_in().as_secs();
        let timeout_secs = self.config.timeout_seconds;
        let Some(queue) = self.queue.as_mut() else {
            warn!(
                rows = rows.len(),
                failures,
                retry_in_secs,
                timeout_secs,
                "milvus breaker open and the backup queue is off; dropped embeddings"
            );
            return;
        };
        match queue.push(rows).await {
            Ok(queued) => warn!(
                rows = rows.len(),
                queued,
                failures,
                retry_in_secs,
                timeout_secs,
                "milvus breaker open; buffered embeddings in valkey"
            ),
            Err(e) => warn!("could not buffer {} embeddings: {}", rows.len(), e),
        }
    }

    /// Upsert a batch, buffering it instead while the breaker is open.
    ///
    /// Never returns an error: failures are recorded and logged, because the
    /// alerts these embeddings came from are already persisted in Mongo.
    pub async fn upsert(&mut self, rows: &[EmbeddingRow]) {
        if rows.is_empty() || !self.config.enabled {
            return;
        }

        // Breaker open: hold the rows rather than pay a timeout.
        if !self.is_ready() {
            self.buffer(rows).await;
            return;
        }

        let Some(client) = self.connected().await else {
            self.buffer(rows).await;
            return;
        };
        match client.upsert_embeddings(rows).await {
            Ok(count) => {
                self.record_success();
                self.drain_some().await;
                let queued = self.queue_len().await;
                info!(upserted = count, queued, "milvus upsert succeeded");
            }
            Err(e) => {
                let what = format!("upsert {} fusion embeddings", rows.len());
                self.record_failure(&what, e);
                self.buffer(rows).await;
            }
        }
    }

    /// The live client, redialing if a failure dropped it. A failed redial
    /// trips the breaker and returns `None`.
    async fn connected(&mut self) -> Option<&mut MilvusClient> {
        if self.client.is_none() {
            match MilvusClient::connect(&self.config).await {
                Ok(client) => {
                    self.client = Some(client);
                    self.log_connected();
                }
                Err(e) => {
                    self.record_failure("reconnect to milvus", e);
                    return None;
                }
            }
        }
        self.client.as_mut()
    }

    /// Replay part of the backlog while the worker has no live alerts.
    /// Once the queue is found empty, further idle drains wait [`IDLE_DRAIN_PAUSE`].
    pub async fn drain_when_idle(&mut self) {
        if self.queue.is_none() || !self.is_ready() {
            return;
        }
        if self.idle_drain_at.is_some_and(|at| Instant::now() < at) {
            return;
        }
        if self.connected().await.is_none() {
            return;
        }
        if !self.drain_some().await {
            let queued = self.queue_len().await;
            info!(
                queued,
                next_check_secs = IDLE_DRAIN_PAUSE.as_secs(),
                "nothing to replay from the milvus backup queue"
            );
            self.idle_drain_at = Some(Instant::now() + IDLE_DRAIN_PAUSE);
        }
    }

    /// Replay a batch of embeddings from the Valkey backup queue.
    ///
    /// Returns whether the queue had rows to replay. True even when the replay
    /// fails, since the rows are re-buffered and the breaker paces the retry.
    /// False when nothing was found: the queue was empty or unreadable, or
    /// there was no queue or client to drain with.
    async fn drain_some(&mut self) -> bool {
        let (Some(queue), Some(client)) = (self.queue.as_mut(), self.client.as_mut()) else {
            return false;
        };

        let batch = match queue.take(self.config.backup_queue.drain_rows).await {
            Ok(batch) if batch.is_empty() => return false,
            Ok(batch) => batch,
            Err(e) => {
                warn!("could not read the milvus backup queue: {}", e);
                return false;
            }
        };

        match client.upsert_embeddings(&batch).await {
            Ok(count) => {
                // An idle drain can be the first call after an outage.
                self.record_success();
                let remaining = self.queue_len().await;
                info!(
                    replayed = count,
                    queued = remaining,
                    "milvus upsert succeeded for embeddings drained from valkey"
                );
            }
            Err(e) => {
                let what = format!("replay {} buffered embeddings", batch.len());
                self.record_failure(&what, e);
                self.buffer(&batch).await;
            }
        }
        true
    }

    /// Rows still waiting. Reports 0 on failure; only used for a log line.
    async fn queue_len(&mut self) -> usize {
        match self.queue.as_mut() {
            Some(queue) => queue.pending().await.unwrap_or(0),
            None => 0,
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
            queue: None,
            idle_drain_at: None,
        }
    }

    #[test]
    fn cooldown_grows_then_caps() {
        // A failure count of 0 never reaches here, but must not underflow.
        assert_eq!(cooldown(0), Duration::from_secs(30));
        assert_eq!(cooldown(1), Duration::from_secs(30));
        assert_eq!(cooldown(2), Duration::from_secs(60));
        assert_eq!(cooldown(3), Duration::from_secs(120));
        assert_eq!(cooldown(4), Duration::from_secs(240));
        assert_eq!(cooldown(7), Duration::from_secs(32 * 60));
        // 30s << 7 == 64m, clamped to the ceiling.
        assert_eq!(cooldown(8), COOLDOWN_MAX);
        // Held at the ceiling for as long as the failures continue.
        assert_eq!(cooldown(9), COOLDOWN_MAX);
        assert_eq!(cooldown(100), COOLDOWN_MAX);
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
        assert!(
            sink.is_ready(),
            "a fresh enabled sink should attempt uploads"
        );

        sink.record_failure("connect", MilvusError::NotEnabled);
        assert!(!sink.is_ready());
        assert_eq!(sink.consecutive_failures, 1);

        // Still enabled — the outage changes where embeddings go, not whether
        // the worker computes them.
        assert!(sink.is_enabled());
    }

    #[test]
    fn the_breaker_closes_once_the_cooldown_passes() {
        let mut sink = sink(true);
        sink.record_failure("connect", MilvusError::NotEnabled);
        assert!(!sink.is_ready());

        sink.retry_at = Some(Instant::now() - Duration::from_secs(1));
        assert!(
            sink.is_ready(),
            "the breaker should close once the cooldown has elapsed"
        );
    }

    /// Consecutive failures back off; a success resets the budget so a later,
    /// unrelated blip starts from the first pause again.
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

    /// Idle workers tick every 500ms; with nothing to replay they must not
    /// dial Milvus, or an outage would trip the breaker with no live traffic.
    #[tokio::test]
    async fn an_idle_drain_without_a_queue_never_dials() {
        let mut sink = sink(true);
        sink.drain_when_idle().await;
        assert!(sink.client.is_none(), "an idle drain should not connect");
        assert_eq!(
            sink.consecutive_failures, 0,
            "an idle drain should not record a failure"
        );
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
