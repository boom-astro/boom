//! Newline-delimited JSON, optionally gzipped: one document per line.
//!
//! This is the shape a Mongo collection dumps to. `mongoexport` writes it and
//! `mongoimport` reads it, so an export BOOM produces can be loaded either by
//! BOOM's own ingest or by the mongo tools, and nothing needs a column list.
//!
//! Numeric types and nested values survive a round trip here in a way they
//! cannot through a CSV cell, where an integer, a float and a string all look
//! alike and an absent value is indistinguishable from an empty one.

use super::ingest::{HasCoordinates, IngestError, IngestReport, Inserter};
use serde::{de::DeserializeOwned, Serialize};
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::path::Path;
use tracing::instrument;

fn open_lines(path: &Path) -> Result<Box<dyn BufRead>, std::io::Error> {
    let file = File::open(path)?;
    let reader: Box<dyn Read> = if path.extension().is_some_and(|e| e == "gz") {
        Box::new(flate2::read::GzDecoder::new(file))
    } else {
        Box::new(file)
    };
    Ok(Box::new(BufReader::new(reader)))
}

#[instrument(skip(inserter), fields(path = %path.display()), err)]
pub async fn ingest_jsonl<T>(inserter: &Inserter, path: &Path) -> Result<IngestReport, IngestError>
where
    T: Serialize + DeserializeOwned + HasCoordinates + Send + 'static,
{
    let (sender, workers) = inserter.start::<T>();
    let mut report = IngestReport::default();

    let reader = open_lines(path).map_err(|e| IngestError::Read(e.to_string()))?;
    for (index, line) in reader.lines().enumerate() {
        let line = line.map_err(|e| {
            IngestError::Read(format!("{}: line {}: {}", path.display(), index + 1, e))
        })?;
        if line.trim().is_empty() {
            continue;
        }
        // No tolerance for a bad line: a record that does not deserialize means
        // the export's schema and this release's record type disagree, and
        // every following line will disagree the same way.
        let record: T = serde_json::from_str(&line).map_err(|e| {
            IngestError::Read(format!("{}: line {}: {}", path.display(), index + 1, e))
        })?;
        report.read += 1;
        if sender.send(record).await.is_err() {
            break;
        }
    }

    drop(sender);
    let tally = inserter.finish(workers).await?;
    report.inserted = tally.inserted;
    report.skipped += tally.skipped;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// The line `export_catalog` writes for an `LSPSC` row: relaxed extended
    /// JSON, with the GeoJSON `coordinates` BOOM adds on ingest still in it.
    const LINE: &str = r#"{"_id":10995475402457455,"ra":150.000443,"dec":2.200123,"score":0.98,"mag_white":21.4,"coordinates":{"radec_geojson":{"type":"Point","coordinates":[-29.99,2.2]}}}"#;

    #[test]
    fn a_line_from_the_exporter_round_trips_into_its_record_type() {
        // The round trip that matters: what export_catalog writes is what the
        // ingest side reads back, including an i64 id that a CSV cell would
        // have handed back as a string.
        let row: super::super::types::Lspsc = serde_json::from_str(LINE).unwrap();
        assert_eq!(row.id, 10_995_475_402_457_455);
        assert_eq!(row.ra, 150.000443);
        assert_eq!(row.score, Some(0.98));
    }

    #[test]
    fn a_null_stays_absent_rather_than_becoming_a_value() {
        // CSV could not tell these apart; both were the empty cell.
        let row: super::super::types::Lspsc =
            serde_json::from_str(r#"{"_id":1,"ra":1.0,"dec":2.0,"score":null}"#).unwrap();
        assert_eq!(row.score, None);
        assert_eq!(row.mag_white, None);
    }

    #[test]
    fn gzipped_and_plain_files_read_alike() {
        let dir = std::env::temp_dir().join(format!("boom-jsonl-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();

        let plain = dir.join("part-0000.jsonl");
        std::fs::write(&plain, format!("{LINE}\n{LINE}\n")).unwrap();

        let gz = dir.join("part-0000.jsonl.gz");
        let mut enc = flate2::write::GzEncoder::new(
            std::fs::File::create(&gz).unwrap(),
            flate2::Compression::fast(),
        );
        enc.write_all(format!("{LINE}\n{LINE}\n").as_bytes())
            .unwrap();
        enc.finish().unwrap();

        for path in [&plain, &gz] {
            let lines: Vec<String> = open_lines(path)
                .unwrap()
                .lines()
                .map(|l| l.unwrap())
                .collect();
            assert_eq!(lines.len(), 2, "{}", path.display());
            assert!(serde_json::from_str::<super::super::types::Lspsc>(&lines[0]).is_ok());
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}
