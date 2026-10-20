//! Rewrites stored DECam cutouts north up: the first month of live alerts arrived mirrored.

use super::context::TaskContext;
use super::TaskError;
use crate::utils::enums::Survey;
use futures::TryStreamExt;
use mongodb::bson::{doc, spec::BinarySubtype, Binary, Bson, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use utoipa::ToSchema;

pub const TASK_TYPE: &str = "reorient_decam_cutouts";

const MARKER: &str = "reoriented";
const STAMPS: [&str; 3] = ["cutoutScience", "cutoutTemplate", "cutoutDifference"];
const FITS_BLOCK: usize = 2880;
const FITS_CARD: usize = 80;
const MAX_BATCH_SIZE: usize = 5_000;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ReorientDecamCutoutsParams {
    /// First candid to rewrite, inclusive.
    pub first_candid: i64,
    /// Last candid to rewrite, inclusive.
    pub last_candid: i64,
    /// Documents rewritten per batch.
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_batch_size() -> usize {
    500
}

impl ReorientDecamCutoutsParams {
    pub fn validate(&self) -> Result<(), String> {
        if self.first_candid > self.last_candid {
            return Err("first_candid must not be greater than last_candid".to_string());
        }
        if self.batch_size == 0 || self.batch_size > MAX_BATCH_SIZE {
            return Err(format!("batch_size must be between 1 and {MAX_BATCH_SIZE}"));
        }
        Ok(())
    }
}

#[derive(thiserror::Error, Debug, PartialEq)]
pub enum StampError {
    #[error("stamp is not a FITS image: {0}")]
    NotFits(&'static str),
    #[error("stamp is {0}x{1}, only square stamps can be reflected in place")]
    NotSquare(usize, usize),
    #[error("stamp could not be decompressed or compressed")]
    Gzip,
}

fn header_value(card: &[u8], key: &str) -> Option<i64> {
    let name = std::str::from_utf8(&card[..8]).ok()?.trim_end();
    if name != key || card.get(8..10) != Some(b"= ") {
        return None;
    }
    let value = std::str::from_utf8(&card[10..]).ok()?;
    value.split('/').next()?.trim().parse().ok()
}

pub fn reflect_stamp(stamp: &[u8]) -> Result<Vec<u8>, StampError> {
    let gzipped = stamp.starts_with(&[0x1f, 0x8b]);
    let mut fits = if gzipped {
        let mut raw = Vec::new();
        flate2::read::GzDecoder::new(stamp)
            .read_to_end(&mut raw)
            .map_err(|_| StampError::Gzip)?;
        raw
    } else {
        stamp.to_vec()
    };

    let (mut bitpix, mut naxis1, mut naxis2, mut header_end) = (None, None, None, None);
    for (index, card) in fits.chunks(FITS_CARD).enumerate() {
        if card.len() < FITS_CARD {
            break;
        }
        if card.starts_with(b"END     ") {
            header_end = Some((index + 1) * FITS_CARD);
            break;
        }
        bitpix = bitpix.or_else(|| header_value(card, "BITPIX"));
        naxis1 = naxis1.or_else(|| header_value(card, "NAXIS1"));
        naxis2 = naxis2.or_else(|| header_value(card, "NAXIS2"));
    }
    let header_end = header_end.ok_or(StampError::NotFits("no END card"))?;
    let data_start = header_end.div_ceil(FITS_BLOCK) * FITS_BLOCK;
    let width = naxis1.ok_or(StampError::NotFits("no NAXIS1"))? as usize;
    let height = naxis2.ok_or(StampError::NotFits("no NAXIS2"))? as usize;
    let pixel = (bitpix
        .ok_or(StampError::NotFits("no BITPIX"))?
        .unsigned_abs()
        / 8) as usize;
    if width != height {
        return Err(StampError::NotSquare(width, height));
    }
    let n = width;
    let data_end = data_start + n * n * pixel;
    if pixel == 0 || fits.len() < data_end {
        return Err(StampError::NotFits("data shorter than its header says"));
    }

    let data = fits[data_start..data_end].to_vec();
    for row in 0..n {
        for col in 0..n {
            let from = ((n - 1 - col) * n + (n - 1 - row)) * pixel;
            let to = data_start + (row * n + col) * pixel;
            fits[to..to + pixel].copy_from_slice(&data[from..from + pixel]);
        }
    }

    if !gzipped {
        return Ok(fits);
    }
    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&fits).map_err(|_| StampError::Gzip)?;
    encoder.finish().map_err(|_| StampError::Gzip)
}

fn candid_of(document: &Document) -> Option<i64> {
    match document.get("_id")? {
        Bson::Int64(v) => Some(*v),
        Bson::Int32(v) => Some(*v as i64),
        _ => None,
    }
}

pub async fn run(
    ctx: &TaskContext,
    params: ReorientDecamCutoutsParams,
) -> Result<serde_json::Value, TaskError> {
    let failed = |e: &dyn std::fmt::Display| TaskError::Failed(e.to_string());
    let storage = ctx
        .config()
        .build_cutout_storage(&Survey::Decam)
        .await
        .map_err(|e| failed(&e))?;
    let collection = storage.mongo_collection().ok_or_else(|| {
        TaskError::Failed("only cutouts stored in MongoDB can be rewritten".to_string())
    })?;
    let namespace = collection.namespace();
    let client = collection.client().clone();

    let pending = doc! {
        "_id": { "$gte": params.first_candid, "$lte": params.last_candid },
        MARKER: { "$exists": false },
    };
    let total = collection
        .count_documents(pending.clone())
        .await
        .map_err(|e| failed(&e))?;
    ctx.info(format!(
        "{total} DECam cutouts left to rewrite between candids {} and {}",
        params.first_candid, params.last_candid
    ));

    let (mut rewritten, mut skipped) = (0u64, 0u64);
    loop {
        if ctx.is_canceled() {
            return Err(TaskError::Canceled);
        }
        let batch: Vec<Document> = collection
            .find(pending.clone())
            .sort(doc! { "_id": 1 })
            .limit(params.batch_size as i64)
            .await
            .map_err(|e| failed(&e))?
            .try_collect()
            .await
            .map_err(|e| failed(&e))?;
        if batch.is_empty() {
            break;
        }

        let mut writes = Vec::with_capacity(batch.len());
        for document in &batch {
            let Some(candid) = candid_of(document) else {
                skipped += 1;
                continue;
            };
            let mut set = doc! { MARKER: true };
            for stamp in STAMPS {
                match document.get_binary_generic(stamp).map(|b| reflect_stamp(b)) {
                    Ok(Ok(bytes)) => {
                        set.insert(
                            stamp,
                            Binary {
                                subtype: BinarySubtype::Generic,
                                bytes,
                            },
                        );
                    }
                    Ok(Err(e)) => ctx.warn(format!("candid {candid}: {stamp} left as is: {e}")),
                    Err(_) => ctx.warn(format!("candid {candid}: no {stamp}")),
                }
            }
            writes.push(WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(namespace.clone())
                    .filter(doc! { "_id": candid, MARKER: { "$exists": false } })
                    .update(doc! { "$set": set })
                    .build(),
            ));
        }
        if writes.is_empty() {
            return Err(TaskError::Failed(format!(
                "{skipped} documents without an integer _id in the range"
            )));
        }
        let batch_len = writes.len() as u64;
        client
            .bulk_write(writes)
            .ordered(false)
            .await
            .map_err(|e| failed(&e))?;
        rewritten += batch_len;
        ctx.progress(
            rewritten,
            total,
            format!("{rewritten} of {total} cutouts rewritten"),
        )
        .await;
    }

    ctx.record_mutation(
        super::ledger::MutationTarget {
            database: collection.namespace().db,
            collection: collection.name().to_string(),
            catalog: None,
            survey: Some(Survey::Decam.to_string()),
        },
        super::ledger::Operation::Recompute,
        doc! {
            "first_candid": params.first_candid,
            "last_candid": params.last_candid,
            "rewritten": rewritten as i64,
            "transform": "reflect across the anti-diagonal",
            "code_version": mongodb::bson::to_bson(&super::ledger::CodeVersion::current())
                .unwrap_or(Bson::Null),
        },
    )
    .await;

    Ok(serde_json::json!({ "rewritten": rewritten, "skipped": skipped }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn card(text: &str) -> Vec<u8> {
        format!("{text:<80}").into_bytes()
    }

    fn fits_stamp(n: usize, pixels: &[f32]) -> Vec<u8> {
        let mut fits = Vec::new();
        for text in [
            "SIMPLE  =                    T".to_string(),
            "BITPIX  =                  -32".to_string(),
            "NAXIS   =                    2".to_string(),
            format!("NAXIS1  = {n:>20}"),
            format!("NAXIS2  = {n:>20}"),
            "END".to_string(),
        ] {
            fits.extend(card(&text));
        }
        fits.resize(FITS_BLOCK, b' ');
        for p in pixels {
            fits.extend(p.to_be_bytes());
        }
        fits.resize(fits.len().div_ceil(FITS_BLOCK) * FITS_BLOCK, 0);
        fits
    }

    fn pixels_of(fits: &[u8], n: usize) -> Vec<f32> {
        fits[FITS_BLOCK..FITS_BLOCK + n * n * 4]
            .chunks(4)
            .map(|b| f32::from_be_bytes(b.try_into().unwrap()))
            .collect()
    }

    #[test]
    fn a_stamp_is_reflected_across_its_anti_diagonal() {
        let stamp = fits_stamp(3, &[1., 2., 3., 4., 5., 6., 7., 8., 9.]);
        let reflected = reflect_stamp(&stamp).unwrap();
        assert_eq!(
            pixels_of(&reflected, 3),
            vec![9., 6., 3., 8., 5., 2., 7., 4., 1.]
        );
        assert_eq!(reflected[..FITS_BLOCK], stamp[..FITS_BLOCK]);
        assert_eq!(reflect_stamp(&reflected).unwrap(), stamp);
    }

    #[test]
    fn a_gzipped_stamp_stays_gzipped() {
        let stamp = fits_stamp(2, &[1., 2., 3., 4.]);
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder.write_all(&stamp).unwrap();
        let reflected = reflect_stamp(&encoder.finish().unwrap()).unwrap();
        assert!(reflected.starts_with(&[0x1f, 0x8b]));
        let mut raw = Vec::new();
        flate2::read::GzDecoder::new(&reflected[..])
            .read_to_end(&mut raw)
            .unwrap();
        assert_eq!(pixels_of(&raw, 2), vec![4., 2., 3., 1.]);
    }

    #[test]
    fn a_stamp_that_is_not_square_is_refused() {
        let mut stamp = fits_stamp(2, &[1., 2., 3., 4.]);
        stamp[4 * FITS_CARD..5 * FITS_CARD].copy_from_slice(&card(&format!("NAXIS2  = {:>20}", 1)));
        assert_eq!(reflect_stamp(&stamp), Err(StampError::NotSquare(2, 1)));
    }

    #[test]
    fn parameters_are_validated() {
        let params = |first, last, batch_size| ReorientDecamCutoutsParams {
            first_candid: first,
            last_candid: last,
            batch_size,
        };
        assert!(params(1, 2, 500).validate().is_ok());
        assert!(params(3, 2, 500).validate().is_err());
        assert!(params(1, 2, 0).validate().is_err());
    }
}
