use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use std::collections::VecDeque;
use std::io::Write;
use tracing::info;

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

fn format_eta(remaining: u64, rate: f64) -> String {
    if remaining == 0 {
        return "0h00m".to_string();
    }
    if rate <= 0.0 {
        return "unknown".to_string();
    }
    let secs = (remaining as f64 / rate) as u64;
    format!("{}h{:02}m", secs / 3600, (secs % 3600) / 60)
}

/// Logs progress every `PROGRESS_LOG_SECS`, with a rate measured over the last
/// `RATE_WINDOW_SAMPLES` samples rather than over the whole run.
pub fn spawn_progress_logger(pb: ProgressBar, label: String) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(PROGRESS_LOG_SECS));
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

// let's make this more generic so we can take any file type, not just a NamedTempFile
pub async fn download_to_file(
    file: &mut impl Write,
    url: &str,
    username: Option<&str>,
    password: Option<&str>,
    show_progress: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let client = reqwest::Client::builder().build()?;
    let mut request_builder = client.get(url);
    if let (Some(user), Some(pass)) = (username, password) {
        request_builder = request_builder.basic_auth(user, Some(pass));
    }
    let response = request_builder.send().await?;
    if !response.status().is_success() {
        return Err(format!("Failed to download file: {}", response.status()).into());
    }

    let total_size = response.content_length().unwrap_or(0);
    let mut stream = response.bytes_stream();

    if show_progress {
        let progress_bar = ProgressBar::new(total_size)
            .with_message("Downloading file")
            .with_style(indicatif::ProgressStyle::default_bar()
                .template("{spinner:.green} {msg} {wide_bar} [{elapsed_precise}] {bytes}/{total_bytes} ({eta})")?);
        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result?;
            file.write_all(&chunk)?;
            progress_bar.inc(chunk.len() as u64);
        }
        progress_bar.finish();
    } else {
        while let Some(chunk_result) = stream.next().await {
            let chunk = chunk_result?;
            file.write_all(&chunk)?;
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
