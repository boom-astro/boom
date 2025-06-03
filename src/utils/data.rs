use futures::StreamExt;
use std::io::Write;

// let's make this more generic so we can take any file type, not just a NamedTempFile
pub async fn download_to_file(
    file: &mut impl Write,
    url: &str,
    username: Option<&str>,
    password: Option<&str>,
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
    let mut stream = response.bytes_stream();

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result?;
        file.write_all(&chunk)?;
    }

    Ok(())
}
