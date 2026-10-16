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
    let reader = open_lines(path).map_err(|e| IngestError::Read(e.to_string()))?;

    let (sender, workers) = inserter.start::<T>();
    let mut report = IngestReport::default();
    // The reader's errors are held rather than returned on the spot. Returning
    // with `?` here drops the workers' join handles without awaiting them,
    // which detaches tasks that go on inserting after the chunk has been
    // reported failed -- and loses the counts of what they did write. Every
    // exit from this function goes through the `finish` below.
    let mut failure: Option<IngestError> = None;

    for (index, line) in reader.lines().enumerate() {
        let line = match line {
            Ok(line) => line,
            Err(e) => {
                failure = Some(IngestError::Read(format!(
                    "{}: line {}: {}",
                    path.display(),
                    index + 1,
                    e
                )));
                break;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        // No tolerance for a bad line: a record that does not deserialize means
        // the export's schema and this release's record type disagree, and
        // every following line will disagree the same way.
        let record: T = match serde_json::from_str(&line) {
            Ok(record) => record,
            Err(e) => {
                failure = Some(IngestError::Read(format!(
                    "{}: line {}: {}",
                    path.display(),
                    index + 1,
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

    /// A failure part way through a file must still leave the records already
    /// sent in the collection.
    ///
    /// That is what proves the insert workers were awaited rather than
    /// abandoned. Returning from the read loop through `?` drops their join
    /// handles, which detaches the tasks: they keep writing after the chunk has
    /// been reported failed, their counts are lost, and on a `drop_existing`
    /// retry they can write into a collection that has just been emptied.
    #[tokio::test]
    async fn a_bad_line_still_leaves_the_good_ones_written() {
        let db = crate::conf::get_test_db().await;
        let name = "test_jsonl_partial_ingest";
        let collection = db.collection::<mongodb::bson::Document>(name);
        collection
            .delete_many(mongodb::bson::doc! {})
            .await
            .unwrap();

        let dir = std::env::temp_dir().join("boom_jsonl_partial");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("part-0000.jsonl");
        let mut file = std::fs::File::create(&path).unwrap();
        writeln!(file, "{LINE}").unwrap();
        writeln!(file, "{{\"not\": \"an lspsc row\"}}").unwrap();
        writeln!(file, "{LINE}").unwrap();
        drop(file);

        let inserter = Inserter::new(db.clone(), name, 2, 1, 4);
        let outcome = ingest_jsonl::<super::super::types::Lspsc>(&inserter, &path).await;

        assert!(outcome.is_err(), "the bad line must fail the file");
        assert_eq!(
            collection
                .count_documents(mongodb::bson::doc! {})
                .await
                .unwrap(),
            1,
            "the line before the failure should be in the collection, which it \
             is only if the workers were awaited"
        );

        collection.drop().await.unwrap();
        let _ = std::fs::remove_file(&path);
    }

    /// One `Inserter` serves every chunk of a run, so the off-sphere cap has to
    /// be scoped to the file rather than to the inserter.
    ///
    /// Counted run-wide it was an upstream-noise budget for the whole catalog:
    /// Pan-STARRS lists thousands of files, and a steady handful of bad
    /// positions per file -- well inside what any one file tolerates -- would
    /// spend the allowance in the first few chunks and then fail every
    /// subsequent run at whichever chunk happened to cross the line.
    #[tokio::test]
    async fn the_off_sphere_cap_is_spent_per_file_rather_than_per_run() {
        let db = crate::conf::get_test_db().await;
        let name = "test_jsonl_off_sphere_cap";
        let collection = db.collection::<mongodb::bson::Document>(name);
        collection
            .delete_many(mongodb::bson::doc! {})
            .await
            .unwrap();

        let dir = std::env::temp_dir().join("boom_jsonl_off_sphere");
        std::fs::create_dir_all(&dir).unwrap();
        // A row that parses and then turns out not to be on the sphere, which
        // is the case the cap counts -- an unparseable line is `ascii.rs`'s.
        let off_sphere = |id: u64| format!(r#"{{"_id":{id},"ra":150.0,"dec":91.0}}"#);
        let write = |path: &std::path::Path, rows: u64, first_id: u64| {
            let mut file = std::fs::File::create(path).unwrap();
            for i in 0..rows {
                writeln!(file, "{}", off_sphere(first_id + i)).unwrap();
            }
            writeln!(file, "{LINE}").unwrap();
        };

        // At the cap, twice over, through the one inserter a run would use.
        let inserter = Inserter::new(db.clone(), name, 2, 1, 4);
        for (n, first_id) in [(0u64, 0u64), (1, 1_000)] {
            let path = dir.join(format!("part-{n:04}.jsonl"));
            write(&path, super::super::ingest::MAX_OFF_SPHERE, first_id);
            let report = ingest_jsonl::<super::super::types::Lspsc>(&inserter, &path)
                .await
                .unwrap_or_else(|e| panic!("file {n} should be inside the cap: {e}"));
            assert_eq!(report.skipped, super::super::ingest::MAX_OFF_SPHERE);
            let _ = std::fs::remove_file(&path);
        }

        // And the cap still bites within a file.
        let path = dir.join("part-0002.jsonl");
        write(&path, super::super::ingest::MAX_OFF_SPHERE + 1, 2_000);
        let outcome = ingest_jsonl::<super::super::types::Lspsc>(&inserter, &path).await;
        assert!(
            outcome.is_err(),
            "a file that is mostly off the sphere must still fail"
        );
        let _ = std::fs::remove_file(&path);

        collection.drop().await.unwrap();
    }
}
