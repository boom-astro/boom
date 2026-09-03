//! Common observability utilities.
//!
//! This module provides a collection of tools for tracing, logging, and metrics
//! throughout the application.
//!
pub mod logging;
pub mod metrics;
pub mod tracing;

/// Returns true when the OTel SDK should be fully disabled.
///
/// Honors the standard `OTEL_SDK_DISABLED` env var (per the OTel spec) — when
/// set to `true`, `init_tracing` and `init_metrics` return `None`, the OTel
/// `tracing` layer is not installed, and global meter/tracer providers are left
/// as the SDK's built-in no-ops. All `Counter::add`, `Histogram::record`, and
/// similar instrument calls compile down to no-ops; `#[instrument]` spans still
/// participate in the fmt layer subject to `RUST_LOG`.
///
/// Use this in throughput-sensitive contexts where the OTLP pipeline is not
/// available (e.g. the throughput-test compose, or a prod deployment that opts
/// out of telemetry).
pub fn is_otel_disabled() -> bool {
    std::env::var("OTEL_SDK_DISABLED")
        .ok()
        .is_some_and(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "true" | "1"))
}
