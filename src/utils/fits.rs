// we use zune_inflate as a replacement for flate2
// which is a slightly faster alternative
use crate::alert::AlertCutout;
use zune_inflate::{DeflateDecoder, DeflateOptions};

const NAXIS1_BYTES: &[u8] = "NAXIS1  =".as_bytes();
const NAXIS2_BYTES: &[u8] = "NAXIS2  =".as_bytes();
const NAXIS1_BYTES_LEN: usize = NAXIS1_BYTES.len();
const NAXIS2_BYTES_LEN: usize = NAXIS2_BYTES.len();
const FITS_HEADER_LEN: usize = 2880; // FITS headers are in blocks of 2880 bytes

#[derive(thiserror::Error, Debug)]
pub enum CutoutError {
    #[error("failed to access document field")]
    MissingDocumentField(#[from] mongodb::bson::document::ValueAccessError),
    #[error("decode error from zune_inflate")]
    DecodeGzip(#[from] zune_inflate::errors::InflateDecodeErrors),
    #[error("integer parsing error")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("Cutout size or cropped size is invalid")]
    InvalidCutoutOrCropSize(String),
}

fn u8_to_f32_vec(v: &[u8]) -> Vec<f32> {
    v.chunks_exact(4)
        .map(TryInto::try_into)
        .map(Result::unwrap)
        .map(f32::from_be_bytes)
        .collect()
}

/// Converts a buffer of bytes from a gzipped FITS file to a vector of flattened 2D image data
pub fn buffer_to_image(buffer: &[u8], naxis_standard: usize) -> Result<Vec<f32>, CutoutError> {
    let mut decoder = DeflateDecoder::new_with_options(
        buffer,
        DeflateOptions::default()
            .set_confirm_checksum(false)
            .set_size_hint(20160),
    );
    let decompressed_data = decoder.decode_gzip()?;

    let subset = &decompressed_data[0..FITS_HEADER_LEN];

    let mut naxis1_key_start = 0;
    let mut naxis1_val_start = 0;
    let mut naxis1_val_end = 0;
    for i in 0..subset.len() - NAXIS1_BYTES_LEN {
        if &subset[i..(i + NAXIS1_BYTES_LEN)] == NAXIS1_BYTES {
            naxis1_key_start = i;
            break;
        }
    }
    for i in (naxis1_key_start + NAXIS1_BYTES_LEN)..subset.len() {
        if subset[i] != b' ' {
            naxis1_val_start = i;
            break;
        }
    }
    for i in naxis1_val_start..subset.len() {
        if subset[i] == b' ' {
            naxis1_val_end = i;
            break;
        }
    }
    let naxis1 =
        String::from_utf8_lossy(&subset[naxis1_val_start..naxis1_val_end]).parse::<usize>()?; // Parse the value of NAXIS1 from the FITS header

    let mut naxis2_key_start = naxis1_val_end;
    let mut naxis2_val_start = 0;
    let mut naxis2_val_end = 0;
    for i in naxis2_key_start..subset.len() - NAXIS2_BYTES_LEN {
        if &subset[i..(i + NAXIS2_BYTES_LEN)] == NAXIS2_BYTES {
            naxis2_key_start = i + NAXIS2_BYTES_LEN;
            break;
        }
    }
    for i in naxis2_key_start..subset.len() {
        if subset[i] != b' ' {
            naxis2_val_start = i;
            break;
        }
    }
    for i in naxis2_val_start..subset.len() {
        if subset[i] == b' ' {
            naxis2_val_end = i;
            break;
        }
    }
    let naxis2 =
        String::from_utf8_lossy(&subset[naxis2_val_start..naxis2_val_end]).parse::<usize>()?; // Parse the value of NAXIS2 from the FITS header

    let mut image_data = u8_to_f32_vec(
        &decompressed_data[FITS_HEADER_LEN..(FITS_HEADER_LEN + (naxis1 * naxis2 * 4) as usize)],
    ); // 32 BITPIX / 8 bits per byte = 4

    // if NAXIS1 and NAXIS2 are not NAXIS_STANDARD, we need to pad the image with zeros
    // we can't just add zeros to the end of the image_data vector, because it's a 2D array we flattened into a 1D vector
    // so if NAXIS1 is not NAXIS_STANDARD, we need to add NAXIS_STANDARD - NAXIS1 zeros to the start and end of each row
    // and if NAXIS2 is not NAXIS_STANDARD, we need to add NAXIS_STANDARD - NAXIS2 zeros to the start and end of the vector

    let offset1 = ((naxis_standard - naxis1) as f32 / 2.0).ceil() as usize;
    let offset2 = ((naxis_standard - naxis2) as f32 / 2.0).ceil() as usize;
    if (offset1, offset2) != (0, 0) {
        let mut new_image_data = vec![0.0; naxis_standard * naxis_standard];
        for i in 0..naxis2 {
            for j in 0..naxis1 {
                let k = i * naxis1 + j;
                let k_new = (i + offset2) * naxis_standard + (j + offset1);
                new_image_data[k_new] = image_data[k];
            }
        }
        image_data = new_image_data;
    }

    Ok(image_data)
}

/// Normalizes a flattened 2D image
pub fn normalize_image(image: Vec<f32>) -> Result<Vec<f32>, CutoutError> {
    // replace all NaNs with 0s and clamp all values to the range [f32::MIN, f32::MAX]
    let mut normalized: Vec<f32> = image
        .into_iter()
        .map(|pixel| {
            if pixel.is_nan() {
                0.0
            } else {
                pixel.clamp(f32::MIN, f32::MAX)
            }
        })
        .collect();

    // then, compute the norm of the vector, which is the Frobenius norm of this array
    // so a 2-norm of a vector is the square root of the sum of the squares of the elements (in absolute value)
    let norm: f32 = normalized.iter().map(|x| x.powi(2)).sum::<f32>().sqrt();

    normalized = normalized.iter().map(|x| x / norm).collect();
    Ok(normalized)
}

pub fn crop_center(image: Vec<f32>, image_size: usize, crop_size: usize) -> Vec<f32> {
    let mut cropped = vec![0.0; crop_size * crop_size];
    let offset = (image_size - crop_size) / 2;
    for i in 0..crop_size {
        for j in 0..crop_size {
            let k = (i + offset) * image_size + (j + offset);
            let k_crop = i * crop_size + j;
            cropped[k_crop] = image[k];
        }
    }
    cropped
}

/// Prepares a cutout image for ML models
/// It reads the image from the alert document
/// decompresses it, normalizes it and returns it as a flattened 2D array of floats
fn prepare_cutout(
    cutout: &[u8],
    cutout_size: usize,
    cropped_size: usize,
) -> Result<Vec<f32>, CutoutError> {
    let cutout = buffer_to_image(cutout, cutout_size)?;
    let cutout = normalize_image(cutout)?;
    if cropped_size < cutout_size {
        Ok(crop_center(cutout, cutout_size, cropped_size))
    } else {
        Ok(cutout)
    }
}

/// Prepares a triplet of cutouts for ML models
pub fn prepare_triplet(
    alert_cutouts: &AlertCutout,
    cutout_size: usize,
    cropped_size: usize,
) -> Result<(Vec<f32>, Vec<f32>, Vec<f32>), CutoutError> {
    // the cutout size and cropped size need to be odd numbers, otherwise there is no central pixel
    if cutout_size % 2 == 0 || cropped_size % 2 == 0 {
        return Err(CutoutError::InvalidCutoutOrCropSize(format!(
            "cutout_size and cropped_size must be odd numbers, got cutout_size={} and cropped_size={}",
            cutout_size, cropped_size
        )));
    }
    let cutout_science = prepare_cutout(&alert_cutouts.cutout_science, cutout_size, cropped_size)?;
    let cutout_template =
        prepare_cutout(&alert_cutouts.cutout_template, cutout_size, cropped_size)?;
    let cutout_difference =
        prepare_cutout(&alert_cutouts.cutout_difference, cutout_size, cropped_size)?;

    Ok((cutout_science, cutout_template, cutout_difference))
}
