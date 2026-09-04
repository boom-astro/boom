//! FITS binary-table catalogs, read a row range at a time.
//!
//! Behind the `catalogs` feature: fitsio links cfitsio, which has to be present
//! at both compile and run time.
//!
//! Note this is catalog *tables*; `utils::fits` handles cutout images and the
//! two share nothing but the format's name.

use super::ingest::{HasCoordinates, IngestError, IngestReport, Inserter};
use fitsio::{hdu::HduInfo, FitsFile};
use serde::Serialize;
use std::path::Path;
use tracing::instrument;

/// Read one row range of a FITS binary table into records.
pub trait FromFitsRows: Sized {
    fn read_rows(
        hdu: &fitsio::hdu::FitsHdu,
        fptr: &mut FitsFile,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<Self>, fitsio::errors::Error>;
}

/// Rows pulled out of the table at once. cfitsio reads column-wise, so this is
/// also the width of every column buffer held in memory.
const ROWS_PER_READ: usize = 50_000;

#[instrument(skip(inserter), fields(path = %path.display()), err)]
pub async fn ingest_fits<T>(inserter: &Inserter, path: &Path) -> Result<IngestReport, IngestError>
where
    T: Serialize + HasCoordinates + FromFitsRows + Send + 'static,
{
    let mut fptr = FitsFile::open(path)
        .map_err(|e| IngestError::Read(format!("{}: {}", path.display(), e)))?;
    // Catalog tables live in HDU 1; HDU 0 is the (empty) primary image header.
    let hdu = fptr
        .hdu(1)
        .map_err(|e| IngestError::Read(format!("{}: no table HDU: {}", path.display(), e)))?;
    let num_rows = match hdu.info {
        HduInfo::TableInfo { num_rows, .. } => num_rows,
        _ => {
            return Err(IngestError::Read(format!(
                "{}: HDU 1 is not a table",
                path.display()
            )))
        }
    };

    let (sender, workers) = inserter.start::<T>();
    let mut report = IngestReport::default();

    for start in (0..num_rows).step_by(ROWS_PER_READ) {
        let end = (start + ROWS_PER_READ).min(num_rows);
        let rows = T::read_rows(&hdu, &mut fptr, start..end).map_err(|e| {
            IngestError::Read(format!(
                "{}: rows {}..{}: {}",
                path.display(),
                start,
                end,
                e
            ))
        })?;
        for record in rows {
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
