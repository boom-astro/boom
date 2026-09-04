//! Sourcing catalog files, by way of the `boompy` Python package.
//!
//! Catalog sources are not uniform: 2MASS is an Apache directory index, NED is
//! one resumable gigabyte, AllWISE is HEALPix partitions behind LSDB. The
//! Python astronomy stack already speaks all of that, so every catalog is
//! sourced through one subprocess interface rather than half in `reqwest` and
//! half in Python.
//!
//! The interface is two commands, both printing one JSON object to stdout and
//! logging to stderr:
//!
//! ```text
//! python -m boompy.catalogs list-chunks <catalog>
//! python -m boompy.catalogs fetch-chunk <catalog> --chunk <id> --dest <dir>
//! ```

use serde::Deserialize;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tracing::instrument;

/// One independently fetchable, independently ingestable piece of a catalog.
///
/// The unit of both resumability and disk pressure: a chunk is downloaded,
/// ingested, and deleted before the next one starts, so peak disk is one chunk
/// rather than one catalog. Catalogs published as a single file have exactly
/// one.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub struct Chunk {
    /// Stable across runs -- it is what a resumed run matches against the
    /// already-done list, so it must not embed a timestamp or an ordinal.
    pub id: String,
    /// Human-readable, for logs and the eventual admin page.
    #[serde(default)]
    pub label: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListChunksOutput {
    chunks: Vec<Chunk>,
}

#[derive(Debug, Deserialize)]
struct FetchChunkOutput {
    files: Vec<PathBuf>,
}

#[derive(thiserror::Error, Debug)]
pub enum DownloadError {
    #[error("failed to run {0}: {1}")]
    Spawn(String, std::io::Error),
    #[error("boompy {command} for {catalog} failed with {status}: {stderr}")]
    Failed {
        command: &'static str,
        catalog: String,
        status: String,
        stderr: String,
    },
    #[error("could not parse boompy {command} output: {source}; output was {output}")]
    Parse {
        command: &'static str,
        output: String,
        source: serde_json::Error,
    },
    #[error("boompy reported fetching {path}, which does not exist")]
    MissingFile { path: PathBuf },
}

/// How to invoke boompy.
#[derive(Debug, Clone)]
pub struct Boompy {
    /// Directory holding boompy's `pyproject.toml`.
    project_dir: PathBuf,
}

impl Boompy {
    pub fn new(project_dir: impl Into<PathBuf>) -> Self {
        Self {
            project_dir: project_dir.into(),
        }
    }

    /// `uv` resolves and caches the environment itself, so there is no separate
    /// install step and no interpreter to keep in sync with the image.
    fn command(&self) -> Command {
        let mut cmd = Command::new("uv");
        cmd.arg("run")
            .arg("--project")
            .arg(&self.project_dir)
            .arg("--quiet")
            .arg("python")
            .arg("-m")
            .arg("boompy.catalogs");
        cmd
    }

    /// Every chunk of `catalog`, in the order they should be ingested.
    #[instrument(skip(self), err)]
    pub async fn list_chunks(&self, catalog: &str) -> Result<Vec<Chunk>, DownloadError> {
        let mut cmd = self.command();
        cmd.arg("list-chunks").arg(catalog);
        let output: ListChunksOutput = self.run(cmd, "list-chunks", catalog).await?;
        Ok(output.chunks)
    }

    /// Fetch one chunk into `dest`, returning the files it wrote.
    #[instrument(skip(self), fields(dest = %dest.display()), err)]
    pub async fn fetch_chunk(
        &self,
        catalog: &str,
        chunk: &str,
        dest: &Path,
    ) -> Result<Vec<PathBuf>, DownloadError> {
        let mut cmd = self.command();
        cmd.arg("fetch-chunk")
            .arg(catalog)
            .arg("--chunk")
            .arg(chunk)
            .arg("--dest")
            .arg(dest);
        let output: FetchChunkOutput = self.run(cmd, "fetch-chunk", catalog).await?;
        // Trusting the exit status alone would let an empty fetch look like an
        // empty catalog, which the ingest would happily record as done.
        for path in &output.files {
            if !path.exists() {
                return Err(DownloadError::MissingFile { path: path.clone() });
            }
        }
        Ok(output.files)
    }

    /// Run one boompy command, forwarding its stderr into the log as it arrives
    /// and parsing its stdout as JSON.
    async fn run<T: serde::de::DeserializeOwned>(
        &self,
        mut cmd: Command,
        command: &'static str,
        catalog: &str,
    ) -> Result<T, DownloadError> {
        cmd.stdout(Stdio::piped()).stderr(Stdio::piped());
        let mut child = cmd
            .spawn()
            .map_err(|e| DownloadError::Spawn(format!("uv run boompy {command}"), e))?;

        // Streamed rather than collected at exit so a multi-hour download
        // reports progress while it is running, not once it is over.
        let stderr = child.stderr.take().expect("stderr was piped");
        let catalog_owned = catalog.to_string();
        let stderr_task = tokio::spawn(async move {
            let mut tail = Vec::new();
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::info!(catalog = %catalog_owned, "boompy: {}", line);
                // Only the tail is kept for the error message; a failing
                // download can produce a great deal of output.
                tail.push(line);
                if tail.len() > 20 {
                    tail.remove(0);
                }
            }
            tail
        });

        let output = child
            .wait_with_output()
            .await
            .map_err(|e| DownloadError::Spawn(format!("uv run boompy {command}"), e))?;
        let stderr_tail = stderr_task.await.unwrap_or_default().join("\n");

        if !output.status.success() {
            return Err(DownloadError::Failed {
                command,
                catalog: catalog.to_string(),
                status: output.status.to_string(),
                stderr: stderr_tail,
            });
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        serde_json::from_str(&stdout).map_err(|source| DownloadError::Parse {
            command,
            output: stdout.chars().take(500).collect(),
            source,
        })
    }
}
