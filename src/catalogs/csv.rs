//! Delimited catalogs with a header row, optionally gzipped, deserialized
//! straight into the record type by serde.

use super::ingest::{HasCoordinates, IngestError, IngestReport, Inserter};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::File;
use std::io::{BufReader, Read};
use std::path::Path;
use tracing::instrument;

fn open_csv(path: &Path) -> Result<csv::Reader<Box<dyn Read>>, std::io::Error> {
    let file = File::open(path)?;
    let reader: Box<dyn Read> = if path.extension().is_some_and(|e| e == "gz") {
        Box::new(BufReader::new(flate2::read::GzDecoder::new(file)))
    } else {
        Box::new(BufReader::new(file))
    };
    Ok(csv::ReaderBuilder::new()
        .comment(Some(b'#'))
        .has_headers(true)
        .from_reader(reader))
}

#[instrument(skip(inserter), fields(path = %path.display()), err)]
pub async fn ingest_csv<T>(inserter: &Inserter, path: &Path) -> Result<IngestReport, IngestError>
where
    T: Serialize + DeserializeOwned + HasCoordinates + Send + 'static,
{
    let (sender, workers) = inserter.start::<T>();
    let mut report = IngestReport::default();

    let mut reader = open_csv(path).map_err(|e| IngestError::Read(e.to_string()))?;
    for (row, result) in reader.deserialize::<T>().enumerate() {
        // Unlike the ascii engine there is no tolerance here: a serde failure
        // against a declared header means the published schema moved, and every
        // subsequent row will fail the same way.
        let record = result.map_err(|e| {
            IngestError::Read(format!("{}: row {}: {}", path.display(), row + 1, e))
        })?;
        report.read += 1;
        if sender.send(record).await.is_err() {
            break;
        }
    }

    drop(sender);
    report.inserted = inserter.finish(workers).await?;
    Ok(report)
}
