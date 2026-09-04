//! Fixed-column and delimited plain-text catalogs, optionally gzipped.
//!
//! 2MASS and VSX publish this way: one record per line, no header, parsed by a
//! per-catalog [`FromAsciiRow`] impl rather than by serde.

use super::ingest::{HasCoordinates, IngestError, IngestReport, Inserter};
use serde::Serialize;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::Path;
use tracing::instrument;

/// Parse one line of a catalog's published text format.
pub trait FromAsciiRow: Sized {
    fn from_line(line: &str) -> Result<Self, String>;
}

/// Open a text file, transparently decompressing `.gz`.
fn open_lines(path: &Path) -> Result<Box<dyn BufRead>, std::io::Error> {
    let file = File::open(path)?;
    if path.extension().is_some_and(|e| e == "gz") {
        Ok(Box::new(BufReader::new(flate2::read::GzDecoder::new(file))))
    } else {
        Ok(Box::new(BufReader::new(file)))
    }
}

/// How many unparseable lines to tolerate before giving up on a file.
///
/// A handful of bad rows in a hundred-million-row catalog is upstream noise; a
/// file that is mostly rejects means the format changed, and ingesting the
/// remainder would quietly install a half-empty catalog.
const MAX_PARSE_ERRORS: u64 = 100;

#[instrument(skip(inserter), fields(path = %path.display()), err)]
pub async fn ingest_ascii<T>(inserter: &Inserter, path: &Path) -> Result<IngestReport, IngestError>
where
    T: Serialize + HasCoordinates + FromAsciiRow + Send + 'static,
{
    let (sender, workers) = inserter.start::<T>();
    let mut report = IngestReport::default();
    let mut first_error: Option<String> = None;

    let reader = open_lines(path).map_err(|e| IngestError::Read(e.to_string()))?;
    for (line_number, line) in reader.lines().enumerate() {
        let line = match line {
            Ok(l) => l,
            Err(e) => {
                report.skipped += 1;
                first_error.get_or_insert_with(|| format!("line {}: {}", line_number + 1, e));
                continue;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        match T::from_line(&line) {
            Ok(record) => {
                report.read += 1;
                // A send failure means every worker is gone, which `finish`
                // below will report properly; stop feeding a dead channel.
                if sender.send(record).await.is_err() {
                    break;
                }
            }
            Err(e) => {
                report.skipped += 1;
                first_error.get_or_insert_with(|| format!("line {}: {}", line_number + 1, e));
                if report.skipped > MAX_PARSE_ERRORS {
                    drop(sender);
                    inserter.finish(workers).await?;
                    return Err(IngestError::Read(format!(
                        "{}: gave up after {} unparseable lines, first was {}",
                        path.display(),
                        report.skipped,
                        first_error.unwrap_or_default()
                    )));
                }
            }
        }
    }

    drop(sender);
    report.inserted = inserter.finish(workers).await?;
    if let Some(e) = first_error {
        tracing::warn!(
            skipped = report.skipped,
            "skipped unparseable lines in {}, first was {}",
            path.display(),
            e
        );
    }
    Ok(report)
}
