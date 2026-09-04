use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use std::io::Write;
use tracing::info;

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

const PROGRESS_LOG_SECS: u64 = 60;

/// The indicatif bar hides itself when stderr is not a terminal, i.e. whenever the run
/// is piped to a log file, so mirror it into the tracing output.
pub fn spawn_progress_logger(pb: ProgressBar, label: String) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(PROGRESS_LOG_SECS));
        ticker.tick().await;
        loop {
            ticker.tick().await;
            let pos = pb.position();
            let len = pb.length().unwrap_or(0);
            let elapsed = pb.elapsed().as_secs_f64();
            let rate = if elapsed > 0.0 {
                pos as f64 / elapsed
            } else {
                0.0
            };
            let eta_secs = if rate > 0.0 && len > pos {
                ((len - pos) as f64 / rate) as u64
            } else {
                0
            };
            let pct = if len > 0 {
                pos as f64 * 100.0 / len as f64
            } else {
                0.0
            };
            info!(
                "[{}] {}/{} ({:.2}%) {:.0} docs/s, eta {}h{:02}m",
                label,
                pos,
                len,
                pct,
                rate,
                eta_secs / 3600,
                (eta_secs % 3600) / 60,
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
