//! Common logging utilities.
use std::{fmt, fs::File, io::BufWriter, iter::successors};

use opentelemetry::trace::TraceContextExt;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tracing::{Event, Subscriber};
use tracing_flame::{FlameLayer, FlushGuard};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::{
    fmt::{
        format::{FmtSpan, Format, Writer},
        FmtContext, FormatEvent, FormatFields,
    },
    layer::SubscriberExt,
    registry::LookupSpan,
    EnvFilter, Layer,
};

use crate::utils::o11y::tracing::otel_layer;

/// Iterate over the `Display` representations of the sources of the given error
/// by recursively calling `std::error::Error::source()`.
pub fn iter_sources<T: std::error::Error>(
    error: &T,
) -> impl std::iter::Iterator<Item = String> + use<'_, T> {
    successors(error.source(), |&error| error.source()).map(ToString::to_string)
}

/// Alias for `tracing::Level::ERROR`.
pub const ERROR: tracing::Level = tracing::Level::ERROR;

/// Alias for `tracing::Level::WARN`.
pub const WARN: tracing::Level = tracing::Level::WARN;

/// Alias for `tracing::Level::INFO`.
pub const INFO: tracing::Level = tracing::Level::INFO;

/// Alias for `tracing::Level::DEBUG`.
pub const DEBUG: tracing::Level = tracing::Level::DEBUG;

/// Alias for `tracing::Level::TRACE`.
pub const TRACE: tracing::Level = tracing::Level::TRACE;

/// Default separator used by `log_error` to format error sources.
pub const SEP: &str = " | ";

/// Create a `tracing::event!` wrapper to standardize the creation of events for
/// errors.
///
/// The created event has fields for both the `Display` and `Debug`
/// representations of the error as well as a field showing the error's sources.
///
/// Examples:
///
/// ```no_run
/// use boom::utils::o11y::logging::{log_error, WARN};
/// use std::io::{Error, ErrorKind};
///
/// let error = Error::new(ErrorKind::Other, "borked");
///
/// log_error!(error); // An ERROR with just the details of the error.
/// log_error!(error, "oh no"); // With added context
///
/// let n = 5;
/// log_error!(error, "oh no: {} attempts", n); // Context with format args
///
/// // Optionaly, a level may be provided to get an event other than ERROR:
/// log_error!(WARN, error);
/// log_error!(WARN, error, "oh no");
/// log_error!(WARN, error, "oh no: {} attempts", n);
/// ```
#[macro_export]
macro_rules! log_error {
    // NOTE: The order of the patterns seems to matter, don't scramble them.
    // TODO: add support for fields?

    // Error + Format string + args
    ($error:expr, $fmt:literal, $($arg:tt)*) => {
        tracing::event!(
            $crate::utils::o11y::logging::ERROR,
            error = %$error,
            source = %$crate::utils::o11y::logging::iter_sources(&$error).collect::<Vec<_>>().join($crate::utils::o11y::logging::SEP),
            debug = ?$error,
            $fmt,
            $($arg)*
        )
    };

    // Level + Error + Format string + args
    ($lvl:expr, $error:expr, $fmt:literal, $($arg:tt)*) => {
        tracing::event!(
            $lvl,
            error = %$error,
            source = %$crate::utils::o11y::logging::iter_sources(&$error).collect::<Vec<_>>().join($crate::utils::o11y::logging::SEP),
            debug = ?$error,
            $fmt,
            $($arg)*
        )
    };

    // Error + Message string
    ($error:expr, $msg:literal) => {
        tracing::event!(
            $crate::utils::o11y::logging::ERROR,
            error = %$error,
            source = %$crate::utils::o11y::logging::iter_sources(&$error).collect::<Vec<_>>().join($crate::utils::o11y::logging::SEP),
            debug = ?$error,
            $msg
        )
    };

    // Level + Error + Message string
    ($lvl:expr, $error:expr, $msg:literal) => {
        tracing::event!(
            $lvl,
            error = %$error,
            source = %$crate::utils::o11y::logging::iter_sources(&$error).collect::<Vec<_>>().join($crate::utils::o11y::logging::SEP),
            debug = ?$error,
            $msg
        )
    };

    // Error
    ($error:expr) => {
        tracing::event!(
            $crate::utils::o11y::logging::ERROR,
            error = %$error,
            source = %$crate::utils::o11y::logging::iter_sources(&$error).collect::<Vec<_>>().join($crate::utils::o11y::logging::SEP),
            debug = ?$error
        )
    };

    // Level + Error
    ($lvl:expr, $error:expr) => {
        tracing::event!(
            $lvl,
            error = %$error,
            source = %$crate::utils::o11y::logging::iter_sources(&$error).collect::<Vec<_>>().join($crate::utils::o11y::logging::SEP),
            debug = ?$error
        )
    };
}

pub use log_error;

/// Create a closure that takes an error and emits it as an event (as an ERROR
/// event by default) using `log_error`.
///
/// This macro has exactly the same interface as `log_error` except it doesn't
/// take the error as an argument (the error is instead passed to the closure).
/// It is particularly useful in `Result` methods like `unwrap_or_else` and
/// `inspect_err`.
///
/// Examples:
///
/// ```no_run
/// use boom::utils::o11y::logging::{as_error, log_error, WARN};
/// use std::io::{Error, ErrorKind};
///
/// fn f() -> Result<(), Error> {
///     Err(Error::new(ErrorKind::Other, "borked"))
/// }
///
/// f().unwrap_or_else(as_error!());
/// let _ = f().inspect_err(as_error!(WARN, "oh no"));
///
/// // The above are equivalent to,
/// f().unwrap_or_else(|error| log_error!(error));
/// let _ = f().inspect_err(|error| log_error!(WARN, error, "oh no"));
/// ```
#[macro_export]
macro_rules! as_error {
    // Format string + args
    ($fmt:literal, $($arg:tt)*) => {
        |error| $crate::utils::o11y::logging::log_error!(error, $fmt, $($arg)*)
    };

    // Level + Format string + args
    ($lvl:expr, $fmt:literal, $($arg:tt)*) => {
        |error| $crate::utils::o11y::logging::log_error!($lvl, error, $fmt, $($arg)*)
    };

    // Message string
    ($msg:literal) => {
        |error| $crate::utils::o11y::logging::log_error!(error, $msg)
    };

    // Level + Message string
    ($lvl:expr, $msg:literal) => {
        |error| $crate::utils::o11y::logging::log_error!($lvl, error, $msg)
    };

    // Nullary
    () => {
        |error| $crate::utils::o11y::logging::log_error!(error)
    };

    // Level
    ($lvl:expr) => {
        |error| $crate::utils::o11y::logging::log_error!($lvl, error)
    };
}

pub use as_error;

/// The error type returned when building a subscriber.
#[derive(Debug, thiserror::Error)]
pub enum BuildSubscriberError {
    #[error("failed to parse filtering directive")]
    Parse(#[from] tracing_subscriber::filter::ParseError),
    #[error("failed to build flame layer")]
    Flame(#[from] tracing_flame::Error),
}

fn parse_span_events(env_var: &str) -> FmtSpan {
    std::env::var(env_var)
        .ok()
        .and_then(|string| {
            string
                .split(',')
                .filter_map(|part| match part.trim().to_lowercase().as_str() {
                    "new" => Some(FmtSpan::NEW),
                    "enter" => Some(FmtSpan::ENTER),
                    "exit" => Some(FmtSpan::EXIT),
                    "close" => Some(FmtSpan::CLOSE),
                    "none" => Some(FmtSpan::NONE),
                    "active" => Some(FmtSpan::ACTIVE),
                    "full" => Some(FmtSpan::FULL),
                    _ => None,
                })
                .reduce(|lhs, rhs| lhs | rhs)
        })
        .unwrap_or(FmtSpan::NONE)
}

/// Build a tracing subscriber.
///
/// The Ok value is a tuple containing the subscriber and an optional flush
/// guard. The flush guard is created if the subscriber includes a flame graph
/// layer and should be kept in scope until the program ends to ensure all of
/// the flame graph data are flushed to it. If the subscriber does not include a
/// flame graph layer, then the second value in the tuple is None (no flush
/// guard).
///
/// The inclusion of a flame graph layer depends on the environment variable
/// BOOM_SPAN_EVENTS. If set, a flame graph layer is added to the subscriber and
/// the value is used as the path where the raw flame graph data are to be
/// written. If unset, then the returned subscriber will not include a flame
/// graph layer.
/// `FormatEvent` adapter that prefixes each formatted log line with
/// `trace_id=<32-hex> span_id=<16-hex> ` when an OTel span context is active.
///
/// This is what makes Loki → Tempo derived-field linking work: the matcher
/// regex on the Loki datasource (`trace_id=([a-f0-9]{32})`) needs the trace
/// id to actually appear in the log line. `tracing-opentelemetry` adds the
/// OTel context to spans as an extension but does not surface it as a
/// formatted field, so we read it ourselves at format time.
pub struct OtelTraceFormatter<F = Format> {
    inner: F,
}

impl<F> OtelTraceFormatter<F> {
    pub fn new(inner: F) -> Self {
        Self { inner }
    }
}

impl<S, N, F> FormatEvent<S, N> for OtelTraceFormatter<F>
where
    S: Subscriber + for<'a> LookupSpan<'a>,
    N: for<'a> FormatFields<'a> + 'static,
    F: FormatEvent<S, N>,
{
    fn format_event(
        &self,
        ctx: &FmtContext<'_, S, N>,
        mut writer: Writer<'_>,
        event: &Event<'_>,
    ) -> fmt::Result {
        // The OTel span context lives on the *currently entered* tracing
        // span, which `tracing::Span::current()` returns. We use the
        // `OpenTelemetrySpanExt::context` extension to extract it.
        let cx = tracing::Span::current().context();
        let span_cx = cx.span().span_context().clone();
        if span_cx.is_valid() {
            write!(
                writer,
                "trace_id={} span_id={} ",
                span_cx.trace_id(),
                span_cx.span_id()
            )?;
        }
        self.inner.format_event(ctx, writer, event)
    }
}

pub fn build_subscriber() -> Result<
    (
        // Return a boxed subscriber because the subscriber type is different
        // depending on whether it has a flame graph layer.
        Box<dyn Subscriber + Send + Sync>,
        Option<FlushGuard<BufWriter<File>>>,
    ),
    BuildSubscriberError,
> {
    build_subscriber_with_otel(None, "boom")
}

/// Like `build_subscriber`, but if `tracer_provider` is `Some`, also adds a
/// `tracing-opentelemetry` layer that forwards `tracing` spans to the OTLP
/// pipeline. `service_name` is used as the tracer name for the OTel layer.
pub fn build_subscriber_with_otel(
    tracer_provider: Option<&SdkTracerProvider>,
    service_name: &str,
) -> Result<
    (
        Box<dyn Subscriber + Send + Sync>,
        Option<FlushGuard<BufWriter<File>>>,
    ),
    BuildSubscriberError,
> {
    let env_filter =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("info,ort=error"))?;

    // Build the base `Format` with our line-shape options and reuse it whether
    // or not OTel is active. When OTel tracing is on, wrap the format in
    // `OtelTraceFormatter` so each log line gets a `trace_id=<hex>` prefix —
    // that's what the Loki datasource's derived field matches on to link
    // logs back to traces in Tempo. ANSI is disabled because the dominant log
    // sink is Loki/Grafana (and `docker compose logs`), where escape codes
    // render as visual noise.
    let format = Format::default()
        .with_target(false)
        .with_file(true)
        .with_line_number(true)
        .with_ansi(false);
    let fmt_base =
        tracing_subscriber::fmt::layer().with_span_events(parse_span_events("BOOM_SPAN_EVENTS"));
    let fmt_layer = if tracer_provider.is_some() {
        fmt_base
            .event_format(OtelTraceFormatter::new(format))
            .boxed()
    } else {
        fmt_base.event_format(format).boxed()
    };

    // Compose optional layers (flame + OTel) by type-erasing each into a
    // boxed dyn Layer so we can attach them in a single shared layered chain.
    let (flame_layer, guard) = match std::env::var("BOOM_FLAME_FILE") {
        Ok(path) => {
            let (layer, guard) = FlameLayer::with_file(path)?;
            (Some(layer.boxed()), Some(guard))
        }
        Err(_) => (None, None),
    };

    let otel = tracer_provider.map(|provider| otel_layer(provider, service_name).boxed());

    let subscriber = tracing_subscriber::registry()
        .with(fmt_layer.with_filter(env_filter))
        .with(flame_layer)
        .with(otel);

    Ok((Box::new(subscriber), guard))
}
