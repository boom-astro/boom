//! Common observability utilities.
//!
//! This module provides a collection of tools for instrumentation and logging
//! throughout the application.
//!
use std::{fs::File, io::BufWriter, iter::successors};

use tracing::Subscriber;
use tracing_flame::{FlameLayer, FlushGuard};
use tracing_subscriber::{fmt::format::FmtSpan, layer::SubscriberExt, EnvFilter, Layer};

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
/// use boom::utils::o11y::{log_error, WARN};
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
            $crate::utils::o11y::ERROR,
            error = %$error,
            source = %$crate::utils::o11y::iter_sources(&$error).collect::<Vec<_>>().join($crate::utils::o11y::SEP),
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
            source = %$crate::utils::o11y::iter_sources(&$error).collect::<Vec<_>>().join($crate::utils::o11y::SEP),
            debug = ?$error,
            $fmt,
            $($arg)*
        )
    };

    // Error + Message string
    ($error:expr, $msg:literal) => {
        tracing::event!(
            $crate::utils::o11y::ERROR,
            error = %$error,
            source = %$crate::utils::o11y::iter_sources(&$error).collect::<Vec<_>>().join($crate::utils::o11y::SEP),
            debug = ?$error,
            $msg
        )
    };

    // Level + Error + Message string
    ($lvl:expr, $error:expr, $msg:literal) => {
        tracing::event!(
            $lvl,
            error = %$error,
            source = %$crate::utils::o11y::iter_sources(&$error).collect::<Vec<_>>().join($crate::utils::o11y::SEP),
            debug = ?$error,
            $msg
        )
    };

    // Error
    ($error:expr) => {
        tracing::event!(
            $crate::utils::o11y::ERROR,
            error = %$error,
            source = %$crate::utils::o11y::iter_sources(&$error).collect::<Vec<_>>().join($crate::utils::o11y::SEP),
            debug = ?$error
        )
    };

    // Level + Error
    ($lvl:expr, $error:expr) => {
        tracing::event!(
            $lvl,
            error = %$error,
            source = %$crate::utils::o11y::iter_sources(&$error).collect::<Vec<_>>().join($crate::utils::o11y::SEP),
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
/// use boom::utils::o11y::{as_error, log_error, WARN};
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
        |error| $crate::utils::o11y::log_error!(error, $fmt, $($arg)*)
    };

    // Level + Format string + args
    ($lvl:expr, $fmt:literal, $($arg:tt)*) => {
        |error| $crate::utils::o11y::log_error!($lvl, error, $fmt, $($arg)*)
    };

    // Message string
    ($msg:literal) => {
        |error| $crate::utils::o11y::log_error!(error, $msg)
    };

    // Level + Message string
    ($lvl:expr, $msg:literal) => {
        |error| $crate::utils::o11y::log_error!($lvl, error, $msg)
    };

    // Nullary
    () => {
        |error| $crate::utils::o11y::log_error!(error)
    };

    // Level
    ($lvl:expr) => {
        |error| $crate::utils::o11y::log_error!($lvl, error)
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
pub fn build_subscriber() -> Result<
    (
        // Return a boxed subscriber because the subscriber type is different
        // depending on whether it has a flame graph layer.
        Box<dyn Subscriber + Send + Sync>,
        Option<FlushGuard<BufWriter<File>>>,
    ),
    BuildSubscriberError,
> {
    let fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_file(true)
        .with_line_number(true)
        .with_span_events(parse_span_events("BOOM_SPAN_EVENTS"));

    let env_filter =
        EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("info,ort=error"))?;

    let subscriber = tracing_subscriber::registry().with(fmt_layer.with_filter(env_filter));

    match std::env::var("BOOM_FLAME_FILE") {
        Ok(path) => {
            let (flame_layer, guard) = FlameLayer::with_file(path)?;
            Ok((Box::new(subscriber.with(flame_layer)), Some(guard)))
        }
        Err(_) => Ok((Box::new(subscriber), None)),
    }
}
