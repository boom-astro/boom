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
use std::sync::Arc;
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
    #[error("boompy returned {path}, which is outside the staging directory {dest}")]
    OutsideDest { path: PathBuf, dest: PathBuf },
}

/// Where boompy's own output should go, besides the process log.
///
/// A download is most of the wall time of an ingest, and its only account of
/// itself is what boompy writes to stderr. That reaches the process log and so
/// Loki, but the task's log is a different stream -- fed by explicit calls, not
/// by `tracing` -- and the task page reads the latter. Without this the page
/// shows a row counter and nothing about the download that counter is waiting
/// on.
pub type LogLine = Arc<dyn Fn(String) + Send + Sync>;

/// How to invoke boompy.
#[derive(Clone)]
pub struct Boompy {
    /// Directory holding boompy's `pyproject.toml`.
    project_dir: PathBuf,
    forward: Option<LogLine>,
}

impl std::fmt::Debug for Boompy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Boompy")
            .field("project_dir", &self.project_dir)
            .field("forward", &self.forward.is_some())
            .finish()
    }
}

impl Boompy {
    pub fn new(project_dir: impl Into<PathBuf>) -> Self {
        Self {
            project_dir: project_dir.into(),
            forward: None,
        }
    }

    /// Send each line boompy writes to `forward` as well as to the log.
    pub fn forwarding_to(mut self, forward: LogLine) -> Self {
        self.forward = Some(forward);
        self
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
            // The ingest deletes these files when it is done with them, so a
            // path outside the staging directory is a delete outside it. Taking
            // the subprocess at its word here would turn a path-join mistake in
            // boompy into data loss somewhere else on the host.
            let (resolved, root) = (path.canonicalize(), dest.canonicalize());
            let contained = match (&resolved, &root) {
                (Ok(resolved), Ok(root)) => resolved.starts_with(root),
                _ => false,
            };
            if !contained {
                return Err(DownloadError::OutsideDest {
                    path: path.clone(),
                    dest: dest.to_path_buf(),
                });
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
        let forward = self.forward.clone();
        let stderr_task = tokio::spawn(async move {
            let mut tail = Vec::new();
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                tracing::info!(catalog = %catalog_owned, "boompy: {}", line);
                if let Some(forward) = &forward {
                    forward(format!("boompy: {line}"));
                }
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
