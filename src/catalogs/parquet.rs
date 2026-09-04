//! Parquet catalogs, read a row group at a time.
//!
//! Behind the `catalogs` feature: polars is a slow build and only the ingest
//! path needs it.

use super::ingest::{HasCoordinates, IngestError, IngestReport, Inserter};
use polars::prelude::*;
use serde::Serialize;
use std::path::Path;
use tracing::instrument;

/// Build records from one slice of a parquet file.
///
/// A batch rather than a row at a time because polars is columnar: pulling a
/// typed column once and indexing it is far cheaper than materializing rows.
pub trait FromDataFrame: Sized {
    fn from_dataframe(df: &DataFrame) -> Result<Vec<Self>, PolarsError>;
}

/// Rows read into memory at once.
///
/// The whole point of chunked ingest is bounded memory, so the file is sliced
/// rather than collected -- a partition of a large HATS catalog can be millions
/// of rows.
const SLICE_ROWS: usize = 100_000;

#[instrument(skip(inserter), fields(path = %path.display()), err)]
pub async fn ingest_parquet<T>(
    inserter: &Inserter,
    path: &Path,
) -> Result<IngestReport, IngestError>
where
    T: Serialize + HasCoordinates + FromDataFrame + Send + 'static,
{
    let total_rows = {
        let file = std::fs::File::open(path).map_err(|e| IngestError::Read(e.to_string()))?;
        ParquetReader::new(file)
            .num_rows()
            .map_err(|e| IngestError::Read(format!("{}: {}", path.display(), e)))?
    };

    let (sender, workers) = inserter.start::<T>();
    let mut report = IngestReport::default();
    let mut offset = 0usize;

    while offset < total_rows {
        let length = SLICE_ROWS.min(total_rows - offset);
        let file = std::fs::File::open(path).map_err(|e| IngestError::Read(e.to_string()))?;
        let frame = ParquetReader::new(file)
            .with_slice(Some((offset, length)))
            .finish()
            .map_err(|e| IngestError::Read(format!("{}: {}", path.display(), e)))?;
        let records = T::from_dataframe(&frame)
            .map_err(|e| IngestError::Read(format!("{}: {}", path.display(), e)))?;

        for record in records {
            report.read += 1;
            if sender.send(record).await.is_err() {
                break;
            }
        }
        offset += length;
    }

    drop(sender);
    report.inserted = inserter.finish(workers).await?;
    Ok(report)
}
