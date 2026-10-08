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
    let mut reader = open_csv(path).map_err(|e| IngestError::Read(e.to_string()))?;

    let (sender, workers) = inserter.start::<T>();
    let mut report = IngestReport::default();
    // The reader's errors are held rather than returned on the spot. Returning
    // with `?` here drops the workers' join handles without awaiting them,
    // which detaches tasks that go on inserting after the chunk has been
    // reported failed -- and loses the counts of what they did write. Every
    // exit from this function goes through the `finish` below.
    let mut failure: Option<IngestError> = None;

    for (row, result) in reader.deserialize::<T>().enumerate() {
        // Unlike the ascii engine there is no tolerance here: a serde failure
        // against a declared header means the published schema moved, and every
        // subsequent row will fail the same way.
        let record = match result {
            Ok(record) => record,
            Err(e) => {
                failure = Some(IngestError::Read(format!(
                    "{}: row {}: {}",
                    path.display(),
                    row + 1,
                    e
                )));
                break;
            }
        };
        report.read += 1;
        if sender.send(record).await.is_err() {
            break;
        }
    }

    drop(sender);
    let tally = inserter.finish(workers).await;
    // The read error is the cause, so it is reported in preference to whatever
    // the workers then made of a truncated stream.
    if let Some(e) = failure {
        return Err(e);
    }
    let tally = tally?;
    report.inserted = tally.inserted;
    report.skipped += tally.skipped;
    Ok(report)
}
