use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use std::{
    collections::VecDeque,
    io::{Seek, SeekFrom, Write},
    time::{Duration, Instant},
};
use tracing::{info, warn};

const PROGRESS_LOG_SECS: u64 = 60;
const RATE_WINDOW_SAMPLES: usize = 10;

/// Standard progress bar used by long-running maintenance binaries
/// (`reprocess_crossmatch`, `migrate_*`).
pub fn make_progress_bar(total: u64, label: String) -> ProgressBar {
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::with_template(
            "{msg} {bar:40} {pos}/{len} [{elapsed_precise} < {eta_precise}]",
        )
        .unwrap(),
    );
    pb.set_message(label);
    pb
}

fn windowed_rate(window: &VecDeque<(f64, u64)>, at: f64, pos: u64) -> f64 {
    match window.front() {
        Some(&(then, then_pos)) if at > then => pos.saturating_sub(then_pos) as f64 / (at - then),
        _ => 0.0,
    }
}

pub fn format_duration(seconds: u64) -> String {
    format!("{}h{:02}m", seconds / 3600, (seconds % 3600) / 60)
}

pub fn format_eta(remaining: u64, rate: f64) -> String {
    if remaining == 0 {
        return format_duration(0);
    }
    if rate <= 0.0 {
        return "unknown".to_string();
    }
    format_duration((remaining as f64 / rate) as u64)
}

/// Logs progress every `PROGRESS_LOG_SECS`, with a rate measured over the last
/// `RATE_WINDOW_SAMPLES` samples rather than over the whole run.
pub fn spawn_progress_logger(pb: ProgressBar, label: String) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(PROGRESS_LOG_SECS));
        ticker.tick().await;
        let mut window: VecDeque<(f64, u64)> = VecDeque::with_capacity(RATE_WINDOW_SAMPLES);
        window.push_back((pb.elapsed().as_secs_f64(), pb.position()));
        loop {
            ticker.tick().await;
            let pos = pb.position();
            let len = pb.length().unwrap_or(0);
            let elapsed = pb.elapsed().as_secs_f64();

            let rate = windowed_rate(&window, elapsed, pos);
            window.push_back((elapsed, pos));
            if window.len() > RATE_WINDOW_SAMPLES {
                window.pop_front();
            }

            let pct = if len > 0 {
                pos as f64 * 100.0 / len as f64
            } else {
                0.0
            };
            info!(
                "[{}] {}/{} ({:.2}%) {:.0} docs/s, eta {}",
                label,
                pos,
                len,
                pct,
                rate,
                format_eta(len.saturating_sub(pos), rate),
            );
        }
    })
}

pub fn spawn_elapsed_logger(label: String, activity: &'static str) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let started = Instant::now();
        let mut ticker = tokio::time::interval(Duration::from_secs(PROGRESS_LOG_SECS));
        ticker.tick().await;
        loop {
            ticker.tick().await;
            info!(
                "[{}] {}, {} elapsed",
                label,
                activity,
                format_duration(started.elapsed().as_secs())
            );
        }
    })
}

/// How many times to ask for the body, counting the first.
const DOWNLOAD_ATTEMPTS: u32 = 4;

/// A failed download, and whether asking again could plausibly work.
enum DownloadFailure {
    /// The request never completed, or the server said "not now".
    Transient(Box<dyn std::error::Error + Send + Sync>),
    /// The server answered, and the answer will not change.
    Permanent(Box<dyn std::error::Error + Send + Sync>),
}

impl DownloadFailure {
    /// `Send + Sync` so a failure can be held across the retry's sleep without
    /// making every caller's future non-`Send`.
    fn into_inner(self) -> Box<dyn std::error::Error + Send + Sync> {
        match self {
            Self::Transient(e) | Self::Permanent(e) => e,
        }
    }
}

/// Downloads `url` into `file`, retrying a transient failure.
///
/// Takes a `File` rather than a `Write` because a retry has to undo the partial
/// body the last attempt wrote: rewinding alone would leave a stale tail if the
/// second body were shorter, so the file is truncated as well.
///
/// A 4xx is not retried. The ZTF producer reads "404 Not Found" out of the
/// error to mean "no alerts for this date", and asking four times would turn a
/// quick answer into a slow one.
pub async fn download_to_file(
    file: &mut std::fs::File,
    url: &str,
    username: Option<&str>,
    password: Option<&str>,
    show_progress: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::builder().build()?;
    let mut attempt = 0;
    loop {
        attempt += 1;
        match download_once(&client, file, url, username, password, show_progress).await {
            Ok(()) => return Ok(()),
            Err(failure) => {
                let transient = matches!(failure, DownloadFailure::Transient(_));
                let error = failure.into_inner();
                if !transient || attempt >= DOWNLOAD_ATTEMPTS {
                    let error: Box<dyn std::error::Error> = error;
                    return Err(error);
                }
                let backoff = Duration::from_secs(2u64.pow(attempt));
                warn!(
                    attempt,
                    url,
                    backoff_s = backoff.as_secs(),
                    error = %error,
                    "download failed, retrying"
                );
                file.set_len(0)?;
                file.seek(SeekFrom::Start(0))?;
                tokio::time::sleep(backoff).await;
            }
        }
    }
}

/// 5xx and 429 are the server declining for now; every other failing status
/// will say the same thing however many times it is asked.
fn status_is_transient(status: reqwest::StatusCode) -> bool {
    status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS
}

async fn download_once(
    client: &reqwest::Client,
    file: &mut std::fs::File,
    url: &str,
    username: Option<&str>,
    password: Option<&str>,
    show_progress: bool,
) -> Result<(), DownloadFailure> {
    let mut request_builder = client.get(url);
    if let (Some(user), Some(pass)) = (username, password) {
        request_builder = request_builder.basic_auth(user, Some(pass));
    }
    let response = request_builder
        .send()
        .await
        // The request never reached a verdict -- connection refused, DNS, a
        // reset partway through the handshake.
        .map_err(|e| DownloadFailure::Transient(e.into()))?;
    let status = response.status();
    if !status.is_success() {
        let error: Box<dyn std::error::Error + Send + Sync> =
            format!("Failed to download file: {}", status).into();
        return Err(if status_is_transient(status) {
            DownloadFailure::Transient(error)
        } else {
            DownloadFailure::Permanent(error)
        });
    }

    let total_size = response.content_length().unwrap_or(0);
    let mut stream = response.bytes_stream();

    if show_progress {
        let progress_bar = ProgressBar::new(total_size)
            .with_message("Downloading file")
            .with_style(indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} [{elapsed_precise}] {bytes}/{total_bytes} ({eta})")
                .map_err(|e| DownloadFailure::Permanent(e.into()))?);
        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.map_err(|e| DownloadFailure::Transient(e.into()))?;
            file.write_all(&chunk)
                .map_err(|e| DownloadFailure::Permanent(e.into()))?;
            progress_bar.inc(chunk.len() as u64);
        }
        progress_bar.finish();
    } else {
        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result.map_err(|e| DownloadFailure::Transient(e.into()))?;
            file.write_all(&chunk)
                .map_err(|e| DownloadFailure::Permanent(e.into()))?;
        }
    }

    Ok(())
}

pub fn count_files_in_dir(dir: &str, extensions: Option<&[&str]>) -> Result<usize, std::io::Error> {
    let count = match extensions {
        Some(extensions) => std::fs::read_dir(dir)?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry
                    .path()
                    .extension()
                    .map_or(false, |ext| extensions.contains(&ext.to_str().unwrap()))
            })
            .count(),
        None => std::fs::read_dir(dir)?.filter_map(Result::ok).count(),
    };
    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_ignores_progress_older_than_the_window() {
        // 10k docs in the first minute, then 100 per minute for nine more.
        let mut window: VecDeque<(f64, u64)> = VecDeque::new();
        window.push_back((0.0, 0));
        for i in 1..10 {
            window.push_back((60.0 * i as f64, 10_000 + 100 * (i - 1) as u64));
        }

        let cumulative = windowed_rate(&window, 600.0, 10_900);
        assert!(
            (cumulative - 10_900.0 / 600.0).abs() < 1e-9,
            "got {cumulative}"
        );

        window.pop_front();
        let windowed = windowed_rate(&window, 600.0, 10_900);
        assert!((windowed - 900.0 / 540.0).abs() < 1e-9, "got {windowed}");
    }

    #[test]
    fn rate_is_zero_without_a_usable_span() {
        assert_eq!(windowed_rate(&VecDeque::new(), 60.0, 100), 0.0);

        let window = VecDeque::from(vec![(60.0, 100)]);
        assert_eq!(windowed_rate(&window, 60.0, 500), 0.0);
    }

    #[test]
    fn eta_follows_the_windowed_rate() {
        assert_eq!(format_eta(7200, 1.0), "2h00m");
        assert_eq!(format_eta(5400, 1.0), "1h30m");
        assert_eq!(format_eta(0, 0.0), "0h00m");
        assert_eq!(format_eta(1000, 0.0), "unknown");
    }
}

#[cfg(test)]
mod download_retry_tests {
    use super::status_is_transient;
    use reqwest::StatusCode;

    /// The ZTF producer reads "404 Not Found" out of the error to mean "no
    /// alerts for this date", so retrying it would turn a quick answer into a
    /// slow one -- and it would still be 404.
    #[test]
    fn a_client_error_is_not_retried() {
        assert!(!status_is_transient(StatusCode::NOT_FOUND));
        assert!(!status_is_transient(StatusCode::UNAUTHORIZED));
        assert!(!status_is_transient(StatusCode::FORBIDDEN));
    }

    #[test]
    fn a_server_saying_not_now_is_retried() {
        assert!(status_is_transient(StatusCode::INTERNAL_SERVER_ERROR));
        assert!(status_is_transient(StatusCode::BAD_GATEWAY));
        assert!(status_is_transient(StatusCode::SERVICE_UNAVAILABLE));
        assert!(status_is_transient(StatusCode::GATEWAY_TIMEOUT));
        assert!(status_is_transient(StatusCode::TOO_MANY_REQUESTS));
    }
}
