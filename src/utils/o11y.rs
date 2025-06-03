//! Common observability utilities.
//!
//! This module provides a collection of tools for instrumentation and logging
//! throughout the application.
//!
use std::iter::successors;

use tracing::Subscriber;
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
}

/// Build a tracing subscriber.
pub fn build_subscriber() -> Result<impl Subscriber, BuildSubscriberError> {
    let mut fmt_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .with_file(true)
        .with_line_number(true);

    if let Some(kind) = std::env::var("RUST_LOG_SPAN_EVENTS")
        .ok()
        .and_then(|string| {
            string
                .split(',')
                .map(|part| match part.trim().to_lowercase().as_str() {
                    "new" => Some(FmtSpan::NEW),
                    "enter" => Some(FmtSpan::ENTER),
                    "exit" => Some(FmtSpan::EXIT),
                    "close" => Some(FmtSpan::CLOSE),
                    "none" => Some(FmtSpan::NONE),
                    "active" => Some(FmtSpan::ACTIVE),
                    "full" => Some(FmtSpan::FULL),
                    _ => None,
                })
                .flatten()
                .reduce(|lhs, rhs| lhs | rhs)
        })
    {
        fmt_layer = fmt_layer.with_span_events(kind);
    }

    let env_filter = EnvFilter::try_from_default_env().or_else(|_| EnvFilter::try_new("info"))?;
    Ok(tracing_subscriber::registry().with(fmt_layer.with_filter(env_filter)))
}
