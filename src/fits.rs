use flate2::read::GzDecoder;
use std::io::Read;

const NAXIS1_BYTES: &[u8] = "NAXIS1  =".as_bytes();
const NAXIS2_BYTES: &[u8] = "NAXIS2  =".as_bytes();
const NAXIS1_BYTES_LEN: usize = NAXIS1_BYTES.len();
const NAXIS2_BYTES_LEN: usize = NAXIS2_BYTES.len();
const FITS_HEADER_LEN: usize = 2880; // FITS headers are in blocks of 2880 bytes
const NAXIS_STANDARD: usize = 63;
const NB_PIXELS: usize = NAXIS_STANDARD * NAXIS_STANDARD;

fn u8_to_f32_vec(v: &[u8]) -> Vec<f32> {
    v.chunks_exact(4)
        .map(TryInto::try_into)
        .map(Result::unwrap)
        .map(f32::from_be_bytes)
        .collect()
}

/// Converts a buffer of bytes from a gzipped FITS file to a vector of flattened 2D image data
pub fn buffer_to_image(buffer: &[u8]) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
    let mut decoder = GzDecoder::new(buffer);

    let mut decompressed_data = Vec::new();
    let _ = decoder.read_to_end(&mut decompressed_data);

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
    let naxis1 = String::from_utf8_lossy(&subset[naxis1_val_start..naxis1_val_end])
        .parse::<usize>()
        .unwrap();

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
    let naxis2 = String::from_utf8_lossy(&subset[naxis2_val_start..naxis2_val_end])
        .parse::<usize>()
        .unwrap();

    let mut image_data = u8_to_f32_vec(
        &decompressed_data[FITS_HEADER_LEN..(FITS_HEADER_LEN + (naxis1 * naxis2 * 4) as usize)],
    ); // 32 BITPIX / 8 bits per byte = 4

    // if NAXIS1 and NAXIS2 are not NAXIS_STANDARD, we need to pad the image with zeros
    // we can't just add zeros to the end of the image_data vector, because it's a 2D array we flattened into a 1D vector
    // so if NAXIS1 is not NAXIS_STANDARD, we need to add NAXIS_STANDARD - NAXIS1 zeros to the start and end of each row
    // and if NAXIS2 is not NAXIS_STANDARD, we need to add NAXIS_STANDARD - NAXIS2 zeros to the start and end of the vector

    let offset1 = (NAXIS_STANDARD - naxis1) / 2; // Assuming naxis1 and naxis2 are both usize
    let offset2 = (NAXIS_STANDARD - naxis2) / 2;
    if (offset1, offset2) != (0, 0) {
        let mut new_image_data = vec![0.0; NB_PIXELS];
        for i in 0..naxis2 {
            for j in 0..naxis1 {
                let k = i * naxis1 + j;
                let k_new = (i + offset2) * NAXIS_STANDARD + (j + offset1);
                new_image_data[k_new] = image_data[k];
            }
        }
        image_data = new_image_data;
    }

    Ok(image_data)
}

/// Normalizes a flattened 2D image
pub fn normalize_image(image: Vec<f32>) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
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

/// Prepares a cutout image for ML models
/// It reads the image from the alert document
/// decompresses it, normalizes it and returns it as a flattened 2D array of floats
fn prepare_cutout(cutout: &[u8]) -> Result<Vec<f32>, Box<dyn std::error::Error>> {
    let cutout = buffer_to_image(cutout).unwrap();
    let cutout = normalize_image(cutout).unwrap();
    Ok(cutout)
}

/// Prepares a triplet of cutouts for ML models
pub fn prepare_triplet(
    alert_doc: &mongodb::bson::Document,
) -> Result<(Vec<f32>, Vec<f32>, Vec<f32>), Box<dyn std::error::Error>> {
    let cutout_science = alert_doc
        .get_binary_generic("cutoutScience")
        .unwrap()
        .to_vec();
    let cutout_science = prepare_cutout(&cutout_science).unwrap();

    let cutout_template = alert_doc
        .get_binary_generic("cutoutTemplate")
        .unwrap()
        .to_vec();
    let cutout_template = prepare_cutout(&cutout_template).unwrap();

    let cutout_difference = alert_doc
        .get_binary_generic("cutoutDifference")
        .unwrap()
        .to_vec();
    let cutout_difference = prepare_cutout(&cutout_difference).unwrap();

    Ok((cutout_science, cutout_template, cutout_difference))
}
