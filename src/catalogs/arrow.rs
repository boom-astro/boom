//! Columnar catalogs, read a record batch at a time.
//!
//! Every catalog that is not plain text arrives here as parquet, because that
//! is the one columnar format worth teaching Rust to read. Awkward source
//! formats are converted on the Python side, where the library that reads them
//! already lives: `astropy` turns a FITS table into parquet, `lsdb` hands us
//! parquet directly. See `boompy/README.md`.
//!
//! The alternative -- a format engine in Rust per source format -- meant a
//! dataframe engine and a C library linked into every BOOM binary to do work
//! that amounts to "give me this column as f64".

use super::ingest::{HasCoordinates, IngestError, IngestReport, Inserter};
use arrow::array::{Array, RecordBatch};
use arrow::datatypes::DataType;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Serialize;
use std::path::Path;
use tracing::instrument;

/// Rows held in memory at once. Bounded because a HATS partition or a converted
/// FITS table can be millions of rows and the point of chunked ingest is that
/// memory does not scale with the catalog.
const BATCH_ROWS: usize = 50_000;

#[derive(thiserror::Error, Debug)]
pub enum ColumnError {
    #[error("column {0:?} is missing")]
    Missing(String),
    #[error("column {name:?} is {actual:?}, which cannot be read as {wanted}")]
    WrongType {
        name: String,
        actual: DataType,
        wanted: &'static str,
    },
}

/// Build records from one batch of columns.
///
/// A batch rather than a row at a time because the layout is columnar: pulling
/// a typed column once and indexing it is far cheaper than materializing rows.
pub trait FromRecordBatch: Sized {
    fn from_batch(batch: &RecordBatch) -> Result<Vec<Self>, ColumnError>;
}

/// Read a column as `f64`, whatever width it was written at.
///
/// Writers disagree: `lsdb` emits f64, a converted FITS table may hold f32, and
/// a column that is all-integer can come back as an integer type. Accepting one
/// of those and rejecting the rest would make ingest depend on which tool wrote
/// the file.
///
/// Absent values are `None`, and so are NaN and the floating-point extremes:
/// FITS has no null, so a missing magnitude arrives as NaN or as the type's
/// limit, and storing either would let a crossmatch treat a missing redshift as
/// a real one.
pub fn f64_column(batch: &RecordBatch, name: &str) -> Result<Vec<Option<f64>>, ColumnError> {
    use arrow::array::{Float32Array, Float64Array, Int32Array, Int64Array};
    let column = column(batch, name)?;
    let finite = |v: f64| (v.is_finite() && v > f64::MIN && v < f64::MAX).then_some(v);

    macro_rules! collect {
        ($ty:ty, $cast:expr) => {{
            let array = column
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| wrong_type(name, column, "a number"))?;
            Ok((0..array.len())
                .map(|i| {
                    if array.is_null(i) {
                        None
                    } else {
                        #[allow(clippy::redundant_closure_call)]
                        finite($cast(array.value(i)))
                    }
                })
                .collect())
        }};
    }

    match column.data_type() {
        DataType::Float64 => collect!(Float64Array, |v: f64| v),
        DataType::Float32 => collect!(Float32Array, |v: f32| v as f64),
        DataType::Int64 => collect!(Int64Array, |v: i64| v as f64),
        DataType::Int32 => collect!(Int32Array, |v: i32| v as f64),
        _ => Err(wrong_type(name, column, "a number")),
    }
}

/// Read a column as `i64`, whatever integer width it was written at.
pub fn i64_column(batch: &RecordBatch, name: &str) -> Result<Vec<Option<i64>>, ColumnError> {
    use arrow::array::{Int16Array, Int32Array, Int64Array, UInt32Array, UInt64Array};
    let column = column(batch, name)?;

    macro_rules! collect {
        ($ty:ty, $cast:expr) => {{
            let array = column
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| wrong_type(name, column, "an integer"))?;
            Ok((0..array.len())
                .map(|i| {
                    if array.is_null(i) {
                        None
                    } else {
                        #[allow(clippy::redundant_closure_call)]
                        Some($cast(array.value(i)))
                    }
                })
                .collect())
        }};
    }

    match column.data_type() {
        DataType::Int64 => collect!(Int64Array, |v: i64| v),
        DataType::Int32 => collect!(Int32Array, |v: i32| v as i64),
        DataType::Int16 => collect!(Int16Array, |v: i16| v as i64),
        // Legacy Survey ids and similar identifiers are written unsigned but
        // fit in an i64, which is what BSON can store.
        DataType::UInt64 => collect!(UInt64Array, |v: u64| v as i64),
        DataType::UInt32 => collect!(UInt32Array, |v: u32| v as i64),
        _ => Err(wrong_type(name, column, "an integer")),
    }
}

/// Read a string column, trimming surrounding whitespace.
///
/// Trimmed because FITS pads fixed-width character columns out to their
/// declared length, and an object name with trailing spaces will not match one
/// without them.
pub fn string_column(batch: &RecordBatch, name: &str) -> Result<Vec<Option<String>>, ColumnError> {
    use arrow::array::{LargeStringArray, StringArray, StringViewArray};
    let column = column(batch, name)?;

    macro_rules! collect {
        ($ty:ty) => {{
            let array = column
                .as_any()
                .downcast_ref::<$ty>()
                .ok_or_else(|| wrong_type(name, column, "a string"))?;
            Ok((0..array.len())
                .map(|i| (!array.is_null(i)).then(|| array.value(i).trim().to_string()))
                .collect())
        }};
    }

    match column.data_type() {
        DataType::Utf8 => collect!(StringArray),
        DataType::LargeUtf8 => collect!(LargeStringArray),
        DataType::Utf8View => collect!(StringViewArray),
        _ => Err(wrong_type(name, column, "a string")),
    }
}

/// Read a boolean column. Integer columns are accepted as 0/non-zero, because
/// a FITS logical converted through pandas can land as an integer.
pub fn bool_column(batch: &RecordBatch, name: &str) -> Result<Vec<Option<bool>>, ColumnError> {
    use arrow::array::BooleanArray;
    let column = column(batch, name)?;
    match column.data_type() {
        DataType::Boolean => {
            let array = column
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| wrong_type(name, column, "a boolean"))?;
            Ok((0..array.len())
                .map(|i| (!array.is_null(i)).then(|| array.value(i)))
                .collect())
        }
        _ => Ok(i64_column(batch, name)?
            .into_iter()
            .map(|v| v.map(|v| v != 0))
            .collect()),
    }
}

fn column<'a>(batch: &'a RecordBatch, name: &str) -> Result<&'a dyn Array, ColumnError> {
    batch
        .column_by_name(name)
        .map(|c| c.as_ref())
        .ok_or_else(|| ColumnError::Missing(name.to_string()))
}

fn wrong_type(name: &str, column: &dyn Array, wanted: &'static str) -> ColumnError {
    ColumnError::WrongType {
        name: name.to_string(),
        actual: column.data_type().clone(),
        wanted,
    }
}

#[instrument(skip(inserter), fields(path = %path.display()), err)]
pub async fn ingest_parquet<T>(
    inserter: &Inserter,
    path: &Path,
) -> Result<IngestReport, IngestError>
where
    T: Serialize + HasCoordinates + FromRecordBatch + Send + 'static,
{
    let file = std::fs::File::open(path).map_err(|e| IngestError::Read(e.to_string()))?;
    // Streamed rather than sliced: the reader walks row groups itself, so the
    // file is opened once and only one batch is resident at a time.
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| IngestError::Read(format!("{}: {}", path.display(), e)))?
        .with_batch_size(BATCH_ROWS)
        .build()
        .map_err(|e| IngestError::Read(format!("{}: {}", path.display(), e)))?;

    let (sender, workers) = inserter.start::<T>();
    let mut report = IngestReport::default();

    for batch in reader {
        let batch = batch.map_err(|e| IngestError::Read(format!("{}: {}", path.display(), e)))?;
        let records = T::from_batch(&batch)
            .map_err(|e| IngestError::Read(format!("{}: {}", path.display(), e)))?;
        // A row the reader rejected -- no id, no position -- is skipped rather
        // than failing the chunk, but it is counted so a file that is mostly
        // rejects is visible in the report.
        report.skipped += (batch.num_rows() - records.len()) as u64;
        for record in records {
            report.read += 1;
            if sender.send(record).await.is_err() {
                break;
            }
        }
    }

    drop(sender);
    report.inserted = inserter.finish(workers).await?;
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Float32Array, Float64Array, Int32Array, StringArray, StringViewArray,
        UInt64Array,
    };
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn batch(name: &str, array: arrow::array::ArrayRef) -> RecordBatch {
        let schema = Schema::new(vec![Field::new(name, array.data_type().clone(), true)]);
        RecordBatch::try_new(Arc::new(schema), vec![array]).expect("valid batch")
    }

    #[test]
    fn f64_column_reads_f64_and_f32_alike() {
        // Which one a file holds depends on the tool that wrote it, and ingest
        // must not depend on that.
        let wide = batch("ra", Arc::new(Float64Array::from(vec![1.5, 2.5])));
        let narrow = batch("ra", Arc::new(Float32Array::from(vec![1.5f32, 2.5])));
        assert_eq!(f64_column(&wide, "ra").unwrap(), vec![Some(1.5), Some(2.5)]);
        assert_eq!(
            f64_column(&narrow, "ra").unwrap(),
            vec![Some(1.5), Some(2.5)]
        );
    }

    #[test]
    fn f64_column_reads_an_all_integer_column() {
        // A float column whose values happen to be whole can round-trip through
        // a converter as an integer type.
        let b = batch("z", Arc::new(Int32Array::from(vec![0, 3])));
        assert_eq!(f64_column(&b, "z").unwrap(), vec![Some(0.0), Some(3.0)]);
    }

    #[test]
    fn f64_column_treats_nan_and_extremes_as_absent() {
        // FITS has no null, so a missing quantity arrives as NaN or as the
        // type's limit. Storing either would let a crossmatch read a missing
        // redshift as a real one.
        let b = batch(
            "z",
            Arc::new(Float64Array::from(vec![
                f64::NAN,
                f64::INFINITY,
                f64::MIN,
                f64::MAX,
                0.07,
            ])),
        );
        assert_eq!(
            f64_column(&b, "z").unwrap(),
            vec![None, None, None, None, Some(0.07)]
        );
    }

    #[test]
    fn f64_column_preserves_real_nulls() {
        let b = batch(
            "w1mpro",
            Arc::new(Float64Array::from(vec![Some(15.0), None])),
        );
        assert_eq!(f64_column(&b, "w1mpro").unwrap(), vec![Some(15.0), None]);
    }

    #[test]
    fn i64_column_accepts_unsigned_identifiers() {
        // Legacy Survey ids are written unsigned but fit in an i64, which is
        // what BSON can store.
        let b = batch("lsid", Arc::new(UInt64Array::from(vec![9_000_000_000u64])));
        assert_eq!(i64_column(&b, "lsid").unwrap(), vec![Some(9_000_000_000)]);
    }

    #[test]
    fn string_column_trims_fits_padding() {
        // FITS pads fixed-width character columns; an object name with trailing
        // spaces would not match the same name without them.
        let b = batch(
            "objname",
            Arc::new(StringArray::from(vec!["NGC 1234    ", "  M31"])),
        );
        assert_eq!(
            string_column(&b, "objname").unwrap(),
            vec![Some("NGC 1234".to_string()), Some("M31".to_string())]
        );
    }

    #[test]
    fn string_column_reads_the_view_encoding() {
        // Newer arrow writers emit Utf8View; rejecting it would make ingest
        // depend on the writer's version.
        let b = batch("objtype", Arc::new(StringViewArray::from(vec!["G"])));
        assert_eq!(
            string_column(&b, "objtype").unwrap(),
            vec![Some("G".to_string())]
        );
    }

    #[test]
    fn bool_column_accepts_booleans_and_integers() {
        // A FITS logical converted through pandas can land as an integer.
        let native = batch("z_qual", Arc::new(BooleanArray::from(vec![true, false])));
        let as_int = batch("z_qual", Arc::new(Int32Array::from(vec![1, 0])));
        assert_eq!(
            bool_column(&native, "z_qual").unwrap(),
            vec![Some(true), Some(false)]
        );
        assert_eq!(
            bool_column(&as_int, "z_qual").unwrap(),
            vec![Some(true), Some(false)]
        );
    }

    #[test]
    fn a_missing_column_names_itself() {
        // The whole drift guard now: boompy's projection dropping a column has
        // to fail loudly, with the column name, rather than ingesting nulls.
        let b = batch("ra", Arc::new(Float64Array::from(vec![1.0])));
        let err = f64_column(&b, "dec").unwrap_err();
        assert!(err.to_string().contains("\"dec\""), "{err}");
    }

    #[test]
    fn a_column_of_the_wrong_kind_names_itself() {
        let b = batch("ra", Arc::new(StringArray::from(vec!["not a number"])));
        let err = f64_column(&b, "ra").unwrap_err();
        assert!(err.to_string().contains("\"ra\""), "{err}");
        assert!(err.to_string().contains("number"), "{err}");
    }
}
