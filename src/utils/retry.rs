//! Small, dependency-free retry helper for transient infrastructure errors.
//!
//! Worker loops talk to external systems (Valkey, Kafka, MongoDB) that can
//! return short-lived connection errors — a broker restart, a dropped
//! connection, a momentary timeout. Without retries, any such blip propagates
//! out of the worker loop and kills the worker thread. With the scheduler's
//! restart supervision that is recoverable, but bouncing the whole worker for a
//! sub-second hiccup is wasteful and loses the in-flight batch. Retrying the
//! individual operation a few times with backoff keeps the worker alive through
//! transient faults and only surfaces an error when the dependency is genuinely
//! unavailable.

use std::future::Future;
use std::time::Duration;

use tracing::warn;

/// Default number of retries for transient worker I/O (Valkey/Kafka). With
/// [`DEFAULT_BASE_BACKOFF`] this spans ~6s of backoff before giving up, after
/// which the error propagates and the scheduler's restart supervision takes
/// over for longer outages.
pub const DEFAULT_MAX_RETRIES: u32 = 5;

/// Base backoff for transient worker I/O retries (200ms, 400ms, 800ms, …).
pub const DEFAULT_BASE_BACKOFF: Duration = Duration::from_millis(200);

/// Whether a [`redis::RedisError`] looks transient (worth retrying) rather than
/// a logic/usage error that would fail identically on retry.
pub fn is_transient_redis_error(error: &redis::RedisError) -> bool {
    error.is_io_error()
        || error.is_connection_dropped()
        || error.is_connection_refusal()
        || error.is_timeout()
        || error.is_cluster_error()
}

/// Retry an async operation while it fails with a *transient* error.
///
/// Makes up to `max_retries + 1` attempts: the initial try plus `max_retries`
/// retries, each preceded by an exponential backoff starting at `base_backoff`
/// (`base_backoff`, `2×`, `4×`, … capped at 32×). Every retry is logged at WARN
/// and reported through `on_retry` (used to bump a metric). A non-transient
/// error, or the final error once retries are exhausted, is returned to the
/// caller unchanged.
///
/// * `operation` — short stable label for logs (e.g. `"valkey_rpop"`).
/// * `is_transient` — classifies an error as retryable.
/// * `on_retry` — invoked once per retry attempt (e.g. to record a metric).
pub async fn retry_transient<T, E, Fut>(
    operation: &'static str,
    max_retries: u32,
    base_backoff: Duration,
    is_transient: impl Fn(&E) -> bool,
    mut on_retry: impl FnMut(),
    mut op: impl FnMut() -> Fut,
) -> Result<T, E>
where
    Fut: Future<Output = Result<T, E>>,
    E: std::fmt::Display,
{
    let mut attempt: u32 = 0;
    loop {
        match op().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                if attempt >= max_retries || !is_transient(&error) {
                    return Err(error);
                }
                let backoff = base_backoff.saturating_mul(1u32 << attempt.min(5));
                warn!(
                    operation,
                    attempt = attempt + 1,
                    max_retries,
                    backoff_ms = backoff.as_millis() as u64,
                    error = %error,
                    "transient error; retrying after backoff"
                );
                on_retry();
                tokio::time::sleep(backoff).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    // Use a zero base backoff so the tests don't actually sleep.
    const NO_BACKOFF: Duration = Duration::from_millis(0);

    #[tokio::test]
    async fn succeeds_after_transient_failures() {
        let calls = Cell::new(0u32);
        let retries = Cell::new(0u32);
        let result: Result<&str, &str> = retry_transient(
            "test",
            5,
            NO_BACKOFF,
            |_| true, // everything transient
            || retries.set(retries.get() + 1),
            || {
                let n = calls.get() + 1;
                calls.set(n);
                async move {
                    if n < 3 {
                        Err("temporary")
                    } else {
                        Ok("ok")
                    }
                }
            },
        )
        .await;
        assert_eq!(result, Ok("ok"));
        assert_eq!(calls.get(), 3, "should attempt until success");
        assert_eq!(retries.get(), 2, "two retries before the third attempt");
    }

    #[tokio::test]
    async fn gives_up_after_max_retries() {
        let calls = Cell::new(0u32);
        let result: Result<(), &str> = retry_transient(
            "test",
            2,
            NO_BACKOFF,
            |_| true,
            || {},
            || {
                calls.set(calls.get() + 1);
                async move { Err("always") }
            },
        )
        .await;
        assert_eq!(result, Err("always"));
        // initial attempt + 2 retries == 3 calls
        assert_eq!(calls.get(), 3);
    }

    #[tokio::test]
    async fn does_not_retry_non_transient() {
        let calls = Cell::new(0u32);
        let result: Result<(), &str> = retry_transient(
            "test",
            5,
            NO_BACKOFF,
            |_| false, // nothing is transient
            || {},
            || {
                calls.set(calls.get() + 1);
                async move { Err("fatal") }
            },
        )
        .await;
        assert_eq!(result, Err("fatal"));
        assert_eq!(calls.get(), 1, "non-transient error must not be retried");
    }
}
