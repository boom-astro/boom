use futures::StreamExt;
use indicatif::ProgressBar;
use std::io::Write;

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
