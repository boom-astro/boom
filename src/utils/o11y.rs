//! Common observability utilities.
//!
//! This module provides a collection of tools for instrumentation and logging
//! throughout the application.
//!
use tracing::Subscriber;
use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, EnvFilter, Layer};

/// The error type returned when building a subscriber.
#[derive(Debug, thiserror::Error)]
pub enum BuildSubscriberError {
    #[error("failed to parse filtering directive")]
    Parse(#[from] tracing_subscriber::filter::ParseError),
}

/// Build a tracing subscriber.
pub fn build_subscriber() -> Result<impl Subscriber, BuildSubscriberError> {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_thread_names(true)
        .with_file(true)
        .with_line_number(true)
        .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE);
    let env_filter = EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("info"))?;
    Ok(tracing_subscriber::registry().with(fmt_layer.with_filter(env_filter)))
}
