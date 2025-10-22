use mongodb::bson::{doc, Document};
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none};

pub const ZP_AB: f32 = 8.90; // Zero point for AB magnitudes
pub const SNT: f32 = 3.0; // Signal-to-noise threshold for detection

pub fn flux2mag(flux: f32, flux_err: f32, zp: f32) -> (f32, f32) {
    let mag = -2.5 * (flux).log10() + zp;
    let sigma = (2.5 / 10.0_f32.ln()) * (flux_err / flux);

    (mag, sigma)
}

pub fn fluxerr2diffmaglim(flux_err: f32, zp: f32) -> f32 {
    -2.5 * (5.0 * flux_err).log10() + zp
}

#[derive(Debug, Clone)]
pub struct PhotometryMag {
    pub time: f64,
    pub mag: f32,
    pub mag_err: f32,
    pub band: String,
}

// we want a function that takes a vec of bson::Document and returns a Vec<PhotometryMag>
pub fn parse_photometry(
    docs: &[mongodb::bson::Bson],
    jd_field: &str,
    mag_field: &str,
    mag_err_field: &str,
    band_field: &str,
    max_time: f64,
) -> Vec<PhotometryMag> {
    docs.iter()
        .filter_map(|doc| {
            if let mongodb::bson::Bson::Document(doc) = doc {
                if let (Some(time), Some(mag), Some(mag_err), Some(band)) = (
                    doc.get_f64(jd_field).ok(),
                    doc.get_f64(mag_field).ok(),
                    doc.get_f64(mag_err_field).ok(),
                    doc.get_str(band_field).ok(),
                ) {
                    // ensure time is within the max_time limit
                    if time > max_time {
                        return None;
                    }
                    // convert to PhotometryMag
                    return Some(PhotometryMag {
                        time,
                        mag: mag as f32,
                        mag_err: mag_err as f32,
                        band: band.to_string(),
                    });
                } else {
                    None
                }
            } else {
                None
            }
        })
        .collect()
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct PhotometryProperties {
    pub peak_index: i32,
    pub linear_fit_before: [f32; 3], // slope, intercept, r-squared
    pub linear_fit_after: [f32; 3],  // slope, intercept, r-squared
}

pub fn prepare_photometry(photometry: &mut Vec<PhotometryMag>) {
    // sort by time
    photometry.sort_by(|a, b| a.time.partial_cmp(&b.time).unwrap());

    // remove duplicates (same time and band)
    photometry.dedup_by(|a, b| a.time == b.time && a.band == b.band);
}

// we want a function that takes a Vec of PhotometryMag and:
// - sort by time (ascending)
// - divide it by band
// - identifies the index of the peak (minimum magnitude) for each band
// - for each band, do a linear fit of the data before the peak and after the peak independently
// - return a vec of PhotometryProperties
pub fn analyze_photometry(sorted_photometry: &[PhotometryMag]) -> (Document, Document, bool) {
    let stationary = sorted_photometry.len() > 0
        && (sorted_photometry.last().unwrap().time - sorted_photometry[0].time) > 0.01;

    let mut global_peak_jd = sorted_photometry[0].time;
    let mut global_peak_mag = sorted_photometry[0].mag;
    let mut global_peak_mag_err = sorted_photometry[0].mag_err;
    let mut global_peak_band = sorted_photometry[0].band.clone();
    let mut global_faintest_jd = sorted_photometry[0].time;
    let mut global_faintest_mag = sorted_photometry[0].mag;
    let mut global_faintest_mag_err = sorted_photometry[0].mag_err;
    let mut global_faintest_band = sorted_photometry[0].band.clone();
    let first_jd = sorted_photometry[0].time;
    let last_jd = sorted_photometry.last().unwrap().time;

    // group by band
    let mut bands: std::collections::HashMap<String, Vec<&PhotometryMag>> =
        std::collections::HashMap::new();
    for mag in sorted_photometry {
        bands
            .entry(mag.band.clone())
            .or_insert_with(Vec::new)
            .push(mag);
    }

    let mut results = doc! {};
    for (band, mags) in bands {
        if mags.is_empty() {
            continue;
        }
        // find the peak index (minimum magnitude) and faintest index (maximum magnitude)
        let (peak_index, faintest_index) =
            mags.iter()
                .enumerate()
                .fold((0, 0), |(peak, faintest), (i, mag)| {
                    if mag.mag < mags[peak].mag {
                        (i, faintest)
                    } else if mag.mag > mags[faintest].mag {
                        (peak, i)
                    } else {
                        (peak, faintest)
                    }
                });

        let peak_jd = mags[peak_index].time;
        let peak_mag = mags[peak_index].mag;
        let peak_mag_err = mags[peak_index].mag_err;

        if peak_mag < global_peak_mag {
            global_peak_jd = peak_jd;
            global_peak_mag = peak_mag;
            global_peak_mag_err = peak_mag_err;
            global_peak_band = band.clone();
        }

        let faintest_jd = mags[faintest_index].time;
        let faintest_mag = mags[faintest_index].mag;
        let faintest_mag_err = mags[faintest_index].mag_err;

        if faintest_mag > global_faintest_mag {
            global_faintest_jd = faintest_jd;
            global_faintest_mag = faintest_mag;
            global_faintest_mag_err = faintest_mag_err;
            global_faintest_band = band.clone();
        }

        let mut properties_per_band = doc! {
            "peak_jd": peak_jd,
            "peak_mag": peak_mag,
            "peak_mag_err": peak_mag_err,
        };

        // before is from 0 to peak_index inclusive, after is from peak_index inclusive to the end
        let before = &mags[0..=peak_index];
        let after = &mags[peak_index..];

        // if there isn't enough time separation between the first and last point, we cannot do a linear fit
        // so we will just return empty arrays
        if before.len() > 1 && before.last().unwrap().time - before.first().unwrap().time > 0.01 {
            let mut before_time = Vec::new();
            let mut before_mag = Vec::new();
            let mut before_mag_err = Vec::new();
            let mut min_mag = f32::INFINITY;
            let mut min_mag_err = f32::INFINITY;
            let mut max_mag = f32::NEG_INFINITY;
            let mut max_mag_err = f32::NEG_INFINITY;

            // get the min time for the before and after segments
            let before_min_time = before[0].time;

            for m in before {
                before_time.push((m.time - before_min_time) as f32);
                before_mag.push(m.mag);
                before_mag_err.push(m.mag_err);
                if m.mag < min_mag {
                    min_mag = m.mag;
                    min_mag_err = m.mag_err;
                }
                if m.mag > max_mag {
                    max_mag = m.mag;
                    max_mag_err = m.mag_err;
                }
            }
            // if min_mag + min_mag_err >= max_mag - max_mag_err, then we cannot do a linear fit
            // as the data is just within the noise
            // so when that happens, empty the before arrays
            if min_mag + min_mag_err >= max_mag - max_mag_err {
                before_time.clear();
                before_mag.clear();
                before_mag_err.clear();
            }

            let linear_fit_before = linear_fit(before_time, before_mag, before_mag_err);
            if let Some(lfb) = linear_fit_before {
                properties_per_band.insert(
                    "rising",
                    doc! {
                        "rate": lfb.slope,
                        // "intercept": lfb.intercept,
                        "r_squared": lfb.r_squared,
                        "nb_data": lfb.nb_data,
                    },
                );
            }
        }

        // same for after
        if after.len() > 1 && after.last().unwrap().time - after.first().unwrap().time > 0.01 {
            let mut after_time = Vec::new();
            let mut after_mag = Vec::new();
            let mut after_mag_err = Vec::new();
            let mut min_mag = f32::INFINITY;
            let mut min_mag_err = f32::INFINITY;
            let mut max_mag = f32::NEG_INFINITY;
            let mut max_mag_err = f32::NEG_INFINITY;

            let after_min_time = after[0].time;

            for m in after {
                after_time.push((m.time - after_min_time) as f32);
                after_mag.push(m.mag);
                after_mag_err.push(m.mag_err);
                if m.mag < min_mag {
                    min_mag = m.mag;
                    min_mag_err = m.mag_err;
                }
                if m.mag > max_mag {
                    max_mag = m.mag;
                    max_mag_err = m.mag_err;
                }
            }
            // if min_mag + min_mag_err >= max_mag - max_mag_err, then we cannot do a linear fit
            // as the data is just within the noise
            // so when that happens, empty the after arrays
            if min_mag + min_mag_err >= max_mag - max_mag_err {
                after_time.clear();
                after_mag.clear();
                after_mag_err.clear();
            }

            let linear_fit_after = linear_fit(after_time, after_mag, after_mag_err);
            if let Some(lfa) = linear_fit_after {
                properties_per_band.insert(
                    "fading",
                    doc! {
                        "rate": lfa.slope,
                        // "intercept": lfa.intercept,
                        "r_squared": lfa.r_squared,
                        "nb_data": lfa.nb_data,
                    },
                );
            }
        }

        results.insert(band, properties_per_band);
    }

    let all_bands_properties = doc! {
        "peak_jd": global_peak_jd,
        "peak_mag": global_peak_mag,
        "peak_mag_err": global_peak_mag_err,
        "peak_band": global_peak_band,
        "faintest_jd": global_faintest_jd,
        "faintest_mag": global_faintest_mag,
        "faintest_mag_err": global_faintest_mag_err,
        "faintest_band": global_faintest_band,
        "first_jd": first_jd,
        "last_jd": last_jd,
    };

    (results, all_bands_properties, stationary)
}

struct LinearFitResult {
    slope: f32,
    // intercept: f32,
    r_squared: f32,
    nb_data: i32,
}

// have a version of linear_fit that takes time, mag, and mag_err as separate vectors
fn linear_fit(time: Vec<f32>, mag: Vec<f32>, mag_err: Vec<f32>) -> Option<LinearFitResult> {
    if time.len() < 2 || mag.len() < 2 || mag_err.len() < 2 {
        return None; // not enough data for a fit
    }

    let n = time.len() as f32;
    let sum_x: f32 = time.iter().sum();
    let sum_y: f32 = mag.iter().sum();
    let sum_xy: f32 = time.iter().zip(mag.iter()).map(|(x, y)| x * y).sum();
    let sum_xx: f32 = time.iter().map(|x| x.powi(2)).sum();

    let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_xx - sum_x.powi(2));
    let intercept = (sum_y - slope * sum_x) / n;

    // calculate r-squared
    let ss_total: f32 = mag.iter().map(|y| (y - (sum_y / n)).powi(2)).sum();
    let ss_residual: f32 = mag
        .iter()
        .zip(time.iter())
        .map(|(y, x)| (y - (slope * x + intercept)).powi(2))
        .sum();
    let r_squared = 1.0 - (ss_residual / ss_total);

    Some(LinearFitResult {
        slope,
        // intercept,
        r_squared,
        nb_data: n as i32,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flux2mag() {
        let flux = 200.0;
        let flux_err = 20.0;
        let zp = 23.9;
        let (mag, mag_err) = flux2mag(flux, flux_err, zp);
        assert!((mag - 18.147425).abs() < 1e-5);
        assert!((mag_err - 0.108574).abs() < 1e-5);

        // let's change the zp to make sure that it is being used correctly
        let zp = 25.0;
        let (mag, mag_err) = flux2mag(flux, flux_err, zp);
        assert!((mag - 19.247425).abs() < 1e-5);
        assert!((mag_err - 0.108574).abs() < 1e-5);

        // test with flux = 0, should return inf
        let flux = 0.0;
        let (mag, mag_err) = flux2mag(flux, flux_err, zp);
        assert!(mag.is_infinite());
        assert!(mag_err.is_infinite());
    }

    #[test]
    fn test_fluxerr2diffmaglim() {
        let zp = 23.9;
        let flux_err = 20.0;
        let diffmaglim = fluxerr2diffmaglim(flux_err, zp);
        assert!((diffmaglim - 18.9).abs() < 1e-5);
    }

    #[test]
    fn test_parse_photometry() {
        let docs = vec![
            doc! { // a valid detection
                "jd": 2459000.5,
                "magpsf": 20.0,
                "sigmapsf": 0.1,
                "band": "r",
            },
            doc! { // NOT a valid detection
                "jd": 2459001.5,
                "sigmapsf": 0.2,
                "band": "r",
            },
        ];
        let photometry = parse_photometry(
            &docs
                .iter()
                .map(|d| mongodb::bson::Bson::Document(d.clone()))
                .collect::<Vec<_>>(),
            "jd",
            "magpsf",
            "sigmapsf",
            "band",
            2459001.0,
        );
        assert_eq!(photometry.len(), 1);
        assert_eq!(photometry[0].time, 2459000.5);
        assert_eq!(photometry[0].mag, 20.0);
        assert_eq!(photometry[0].mag_err, 0.1);
        assert_eq!(photometry[0].band, "r");
    }

    #[test]
    fn test_prepare_photometry() {
        let mut photometry = vec![
            PhotometryMag {
                time: 2459001.5,
                mag: 19.5,
                mag_err: 0.1,
                band: "r".to_string(),
            }, // later point that should be sorted down
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: "r".to_string(),
            }, // earlier point that should be sorted up
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: "r".to_string(),
            }, // duplicate that should be removed
        ];
        prepare_photometry(&mut photometry);
        assert_eq!(photometry.len(), 2);
        assert_eq!(photometry[0].time, 2459000.5);
        assert_eq!(photometry[1].time, 2459001.5);
    }

    #[test]
    fn test_analyze_photometry() {
        // Test case 1: only one data point
        let mut data = vec![PhotometryMag {
            time: 2459000.5,
            mag: 20.0,
            mag_err: 0.1,
            band: "r".to_string(),
        }];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, false);
        assert_eq!(results.len(), 1);
        let r_stats = results.get_document("r").unwrap();
        let r_peak_jd = r_stats.get_f64("peak_jd").unwrap();
        let r_peak_mag = r_stats.get_f64("peak_mag").unwrap() as f32;
        let r_peak_mag_err = r_stats.get_f64("peak_mag_err").unwrap() as f32;
        assert!((data[0].time - r_peak_jd).abs() < 1e-6);
        assert!((data[0].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[0].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert_eq!(r_stats.contains_key("rising"), false);
        assert_eq!(r_stats.contains_key("fading"), false);

        // the all band properties should also just match the one data point we have
        let peak_jd = all_bands_props.get_f64("peak_jd").unwrap();
        let peak_mag = all_bands_props.get_f64("peak_mag").unwrap() as f32;
        let peak_mag_err = all_bands_props.get_f64("peak_mag_err").unwrap() as f32;
        let peak_band = all_bands_props.get_str("peak_band").unwrap();
        assert!((data[0].time - peak_jd).abs() < 1e-6);
        assert!((data[0].mag - peak_mag).abs() < 1e-6);
        assert!((data[0].mag_err - peak_mag_err).abs() < 1e-6);
        assert_eq!(data[0].band, peak_band);

        // Test case 2: 2 data points in the same band, rising
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: "r".to_string(),
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.0,
                mag_err: 0.1,
                band: "r".to_string(),
            },
        ];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, true);
        assert_eq!(results.len(), 1);
        let r_stats = results.get_document("r").unwrap();
        let r_peak_jd = r_stats.get_f64("peak_jd").unwrap();
        let r_peak_mag = r_stats.get_f64("peak_mag").unwrap() as f32;
        let r_peak_mag_err = r_stats.get_f64("peak_mag_err").unwrap() as f32;
        assert!((data[1].time - r_peak_jd).abs() < 1e-6);
        assert!((data[1].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[1].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert_eq!(r_stats.contains_key("rising"), true);
        assert_eq!(r_stats.contains_key("fading"), false);
        let rising_stats = r_stats.get_document("rising").unwrap();
        let rising_rate = rising_stats.get_f64("rate").unwrap();
        let r_squared = rising_stats.get_f64("r_squared").unwrap();
        let nb_data = rising_stats.get_i32("nb_data").unwrap();
        assert!((rising_rate + 1.0).abs() < 1e-6); // should be -1 mag/day
        assert!((r_squared - 1.0).abs() < 1e-6); // perfect fit
        assert_eq!(nb_data, 2);

        // the all band properties should also just match the one data point we have
        let peak_jd = all_bands_props.get_f64("peak_jd").unwrap();
        let peak_mag = all_bands_props.get_f64("peak_mag").unwrap() as f32;
        let peak_mag_err = all_bands_props.get_f64("peak_mag_err").unwrap() as f32;
        let peak_band = all_bands_props.get_str("peak_band").unwrap();
        assert!((data[1].time - peak_jd).abs() < 1e-6);
        assert!((data[1].mag - peak_mag).abs() < 1e-6);
        assert!((data[1].mag_err - peak_mag_err).abs() < 1e-6);
        assert_eq!(data[1].band, peak_band);

        // Test case 3: 2 data points in the same band, fading
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 19.0,
                mag_err: 0.1,
                band: "r".to_string(),
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 20.0,
                mag_err: 0.1,
                band: "r".to_string(),
            },
        ];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, true);
        assert_eq!(results.len(), 1);
        let r_stats = results.get_document("r").unwrap();
        let r_peak_jd = r_stats.get_f64("peak_jd").unwrap();
        let r_peak_mag = r_stats.get_f64("peak_mag").unwrap() as f32;
        let r_peak_mag_err = r_stats.get_f64("peak_mag_err").unwrap() as f32;
        assert!((data[0].time - r_peak_jd).abs() < 1e-6);
        assert!((data[0].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[0].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert_eq!(r_stats.contains_key("rising"), false);
        assert_eq!(r_stats.contains_key("fading"), true);
        let fading_stats = r_stats.get_document("fading").unwrap();
        let fading_rate = fading_stats.get_f64("rate").unwrap();
        let r_squared = fading_stats.get_f64("r_squared").unwrap();
        let nb_data = fading_stats.get_i32("nb_data").unwrap();
        assert!((fading_rate - 1.0).abs() < 1e-6); // should be 1 mag/day
        assert!((r_squared - 1.0).abs() < 1e-6); // perfect fit
        assert_eq!(nb_data, 2);
        // the all band properties should also just match the one data point we have
        let peak_jd = all_bands_props.get_f64("peak_jd").unwrap();
        let peak_mag = all_bands_props.get_f64("peak_mag").unwrap() as f32;
        let peak_mag_err = all_bands_props.get_f64("peak_mag_err").unwrap() as f32;
        let peak_band = all_bands_props.get_str("peak_band").unwrap();
        assert!((data[0].time - peak_jd).abs() < 1e-6);
        assert!((data[0].mag - peak_mag).abs() < 1e-6);
        assert!((data[0].mag_err - peak_mag_err).abs() < 1e-6);
        assert_eq!(data[0].band, peak_band);

        // Test case 4: 3 data points in the same band, rising then fading
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: "r".to_string(),
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.0,
                mag_err: 0.1,
                band: "r".to_string(),
            },
            PhotometryMag {
                time: 2459002.5,
                mag: 20.0,
                mag_err: 0.1,
                band: "r".to_string(),
            },
        ];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, true);
        assert_eq!(results.len(), 1);
        let r_stats = results.get_document("r").unwrap();
        let r_peak_jd = r_stats.get_f64("peak_jd").unwrap();
        let r_peak_mag = r_stats.get_f64("peak_mag").unwrap() as f32;
        let r_peak_mag_err = r_stats.get_f64("peak_mag_err").unwrap() as f32;
        assert!((data[1].time - r_peak_jd).abs() < 1e-6);
        assert!((data[1].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[1].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert_eq!(r_stats.contains_key("rising"), true);
        assert_eq!(r_stats.contains_key("fading"), true);
        let rising_stats = r_stats.get_document("rising").unwrap();
        let rising_rate = rising_stats.get_f64("rate").unwrap();
        let rising_r_squared = rising_stats.get_f64("r_squared").unwrap();
        let rising_nb_data = rising_stats.get_i32("nb_data").unwrap();
        assert!((rising_rate + 1.0).abs() < 1e-6); // should be -1 mag/day
        assert!((rising_r_squared - 1.0).abs() < 1e-6); // perfect fit
        assert_eq!(rising_nb_data, 2);
        let fading_stats = r_stats.get_document("fading").unwrap();
        let fading_rate = fading_stats.get_f64("rate").unwrap();
        let fading_r_squared = fading_stats.get_f64("r_squared").unwrap();
        let fading_nb_data = fading_stats.get_i32("nb_data").unwrap();
        assert!((fading_rate - 1.0).abs() < 1e-6); // should be 1 mag/day
        assert!((fading_r_squared - 1.0).abs() < 1e-6); // perfect fit
        assert_eq!(fading_nb_data, 2);
        // the all band properties should also just match the one data point we have
        let peak_jd = all_bands_props.get_f64("peak_jd").unwrap();
        let peak_mag = all_bands_props.get_f64("peak_mag").unwrap() as f32;
        let peak_mag_err = all_bands_props.get_f64("peak_mag_err").unwrap() as f32;
        let peak_band = all_bands_props.get_str("peak_band").unwrap();
        assert!((data[1].time - peak_jd).abs() < 1e-6);
        assert!((data[1].mag - peak_mag).abs() < 1e-6);
        assert!((data[1].mag_err - peak_mag_err).abs() < 1e-6);
        assert_eq!(data[1].band, peak_band);

        // Test case 5: multiple bands
        // - rising and fading in r band (3 points)
        // - only rising in g band (2 points)
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: "r".to_string(),
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.0,
                mag_err: 0.1,
                band: "r".to_string(),
            },
            PhotometryMag {
                time: 2459002.5,
                mag: 20.0,
                mag_err: 0.1,
                band: "r".to_string(),
            },
            PhotometryMag {
                time: 2459000.5,
                mag: 21.0,
                mag_err: 0.1,
                band: "g".to_string(),
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 20.0,
                mag_err: 0.1,
                band: "g".to_string(),
            },
        ];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, true);
        assert_eq!(results.len(), 2);
        // r band
        let r_stats = results.get_document("r").unwrap();
        let r_peak_jd = r_stats.get_f64("peak_jd").unwrap();
        let r_peak_mag = r_stats.get_f64("peak_mag").unwrap() as f32;
        let r_peak_mag_err = r_stats.get_f64("peak_mag_err").unwrap() as f32;
        // the original array was sorted and deduplicated,
        // so the r-band peak is now at index 2 (not 1)
        assert!((data[2].time - r_peak_jd).abs() < 1e-6);
        assert!((data[2].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[2].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert_eq!(r_stats.contains_key("rising"), true);
        assert_eq!(r_stats.contains_key("fading"), true);
        // check the rising stats in r band
        let rising_stats = r_stats.get_document("rising").unwrap();
        let rising_rate = rising_stats.get_f64("rate").unwrap();
        let rising_r_squared = rising_stats.get_f64("r_squared").unwrap();
        let rising_nb_data = rising_stats.get_i32("nb_data").unwrap();
        assert!((rising_rate + 1.0).abs() < 1e-6); // should be -1 mag/day
        assert!((rising_r_squared - 1.0).abs() < 1e-6); // perfect fit
        assert_eq!(rising_nb_data, 2);
        // check the fading stats in r band
        let fading_stats = r_stats.get_document("fading").unwrap();
        let fading_rate = fading_stats.get_f64("rate").unwrap();
        let fading_r_squared = fading_stats.get_f64("r_squared").unwrap();
        let fading_nb_data = fading_stats.get_i32("nb_data").unwrap();
        assert!((fading_rate - 1.0).abs() < 1e-6); // should be 1 mag/day
        assert!((fading_r_squared - 1.0).abs() < 1e-6); // perfect fit
        assert_eq!(fading_nb_data, 2);
        // g band
        let g_stats = results.get_document("g").unwrap();
        let g_peak_jd = g_stats.get_f64("peak_jd").unwrap();
        let g_peak_mag = g_stats.get_f64("peak_mag").unwrap() as f32;
        let g_peak_mag_err = g_stats.get_f64("peak_mag_err").unwrap() as f32;
        // the original array was sorted and deduplicated,
        // so the g-band peak is now at index 3 (not 4)
        assert!((data[3].time - g_peak_jd).abs() < 1e-6);
        assert!((data[3].mag - g_peak_mag).abs() < 1e-6);
        assert!((data[3].mag_err - g_peak_mag_err).abs() < 1e-6);
        assert_eq!(g_stats.contains_key("rising"), true);
        assert_eq!(g_stats.contains_key("fading"), false);
        // check the rising stats in g band
        let rising_stats = g_stats.get_document("rising").unwrap();
        let rising_rate = rising_stats.get_f64("rate").unwrap();
        let rising_r_squared = rising_stats.get_f64("r_squared").unwrap();
        let rising_nb_data = rising_stats.get_i32("nb_data").unwrap();
        assert!((rising_rate + 1.0).abs() < 1e-6); // should be -1 mag/day
        assert!((rising_r_squared - 1.0).abs() < 1e-6); // perfect fit
        assert_eq!(rising_nb_data, 2);

        // the all band properties should match the peak in r band
        let peak_jd = all_bands_props.get_f64("peak_jd").unwrap();
        let peak_mag = all_bands_props.get_f64("peak_mag").unwrap() as f32;
        let peak_mag_err = all_bands_props.get_f64("peak_mag_err").unwrap() as f32;
        let peak_band = all_bands_props.get_str("peak_band").unwrap();
        assert!((data[2].time - peak_jd).abs() < 1e-6);
        assert!((data[2].mag - peak_mag).abs() < 1e-6);
        assert!((data[2].mag_err - peak_mag_err).abs() < 1e-6);
        assert_eq!(data[2].band, peak_band);

        // Edge case 1: duplicated points (same time and band, different mag)
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: "r".to_string(),
            },
            // duplicate of the first point
            PhotometryMag {
                time: 2459000.5,
                mag: 20.5,
                mag_err: 0.1,
                band: "r".to_string(),
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.0,
                mag_err: 0.1,
                band: "r".to_string(),
            },
        ];
        prepare_photometry(&mut data);
        let (results, _, stationary) = analyze_photometry(&data);
        // make sure that only 2 points were used (the duplicate should be removed)
        assert_eq!(stationary, true);
        assert_eq!(results.len(), 1);
        let r_stats = results.get_document("r").unwrap();
        let r_peak_jd = r_stats.get_f64("peak_jd").unwrap();
        let r_peak_mag = r_stats.get_f64("peak_mag").unwrap() as f32;
        // the original array was sorted and deduplicated,
        // so the r-band peak is now at index 1 (not 2)
        assert!((data[1].time - r_peak_jd).abs() < 1e-6);
        assert!((data[1].mag - r_peak_mag).abs() < 1e-6);
        let rising_stats = r_stats.get_document("rising").unwrap();
        let rising_nb_data = rising_stats.get_i32("nb_data").unwrap();
        assert_eq!(rising_nb_data, 2);
    }
}
