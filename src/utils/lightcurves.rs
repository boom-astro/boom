use apache_avro_derive::AvroSchema;
use apache_avro_macros::serdavro;
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use utoipa::ToSchema;

pub const ZP_AB: f32 = 8.90; // Zero point for AB magnitude
pub const SNT: f32 = 3.0; // Signal-to-noise threshold for detection
const FACTOR: f32 = 1.0857362047581294; // where 1.0857362047581294 = 2.5 / np.log(10)

// now survey-specific values:
pub const ZTF_ZP: f32 = 23.9;
pub const LSST_ZP_AB_NJY: f32 = ZP_AB + 22.5; // ZP + nJy to Jy conversion factor, as 2.5 * log10(1e9) = 22.5

pub fn flux2mag(flux: f32, flux_err: f32, zp: f32) -> (f32, f32) {
    let mag = -2.5 * (flux).log10() + zp;
    let sigma = (2.5 / 10.0_f32.ln()) * (flux_err / flux);

    (mag, sigma)
}

pub fn fluxerr2diffmaglim(flux_err: f32, zp: f32) -> f32 {
    -2.5 * (5.0 * flux_err).log10() + zp
}

pub fn mag2flux(mag: f32, mag_err: f32, zp: f32) -> (f32, f32) {
    let flux = 10.0_f32.powf(-0.4 * (mag - zp));
    let fluxerr = mag_err / FACTOR * flux;
    (flux, fluxerr)
}

pub fn diffmaglim2fluxerr(diffmaglim: f32, zp: f32) -> f32 {
    10.0_f32.powf((diffmaglim - zp) / -2.5) / 5.0
}

#[apache_avro_macros::serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Eq, Hash, ToSchema)]
pub enum Band {
    #[serde(rename = "g")]
    G,
    #[serde(rename = "r")]
    R,
    #[serde(rename = "i")]
    I,
    #[serde(rename = "z")]
    Z,
    #[serde(rename = "y")]
    Y,
    #[serde(rename = "u")]
    U,
}

impl std::fmt::Display for Band {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Band::G => write!(f, "g"),
            Band::R => write!(f, "r"),
            Band::I => write!(f, "i"),
            Band::Z => write!(f, "z"),
            Band::Y => write!(f, "y"),
            Band::U => write!(f, "u"),
        }
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema)]
pub struct PhotometryMag {
    #[serde(alias = "jd")]
    pub time: f64,
    #[serde(alias = "magpsf")]
    pub mag: f32,
    #[serde(alias = "sigmapsf")]
    pub mag_err: f32,
    pub band: Band,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, AvroSchema, ToSchema)]
pub struct BandRateProperties {
    pub rate: f32,
    pub rate_error: f32,
    pub red_chi2: f32,
    pub nb_data: i32,
    pub dt: f32,
}

// TODO: avro serialization fail when we use skip_serializing_none,
// since the optional fields are not just None but simply missing
// (this needs to be fixed in the apache_avro-related crates)
// #[serde_as]
// #[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, AvroSchema, ToSchema)]
pub struct BandProperties {
    pub peak_jd: f64,
    pub peak_mag: f32,
    pub peak_mag_err: f32,
    pub dt: f32,
    pub rising: Option<BandRateProperties>,
    pub fading: Option<BandRateProperties>,
}

// TODO: avro serialization fail when we use skip_serializing_none,
// since the optional fields are not just None but simply missing
// (this needs to be fixed in the apache_avro-related crates)
// #[serde_as]
// #[skip_serializing_none]
#[serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Default, ToSchema)]
pub struct PerBandProperties {
    pub g: Option<BandProperties>,
    pub r: Option<BandProperties>,
    pub i: Option<BandProperties>,
    pub z: Option<BandProperties>,
    pub y: Option<BandProperties>,
    pub u: Option<BandProperties>,
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, AvroSchema)]
pub struct AllBandsProperties {
    pub peak_jd: f64,
    pub peak_mag: f32,
    pub peak_mag_err: f32,
    pub peak_band: Band,
    pub faintest_jd: f64,
    pub faintest_mag: f32,
    pub faintest_mag_err: f32,
    pub faintest_band: Band,
    pub first_jd: f64,
    pub last_jd: f64,
    // per-band g quantities (None if no g-band observations)
    pub days_since_peak_g: Option<f32>, // last_jd - peak_jd_g
    pub days_to_peak_g: Option<f32>,    // peak_jd_g - first_jd_g
    pub peakmag_g: Option<f32>,         // brightest (lowest) g-band mag
    pub maxmag_g: Option<f32>,          // faintest (highest) g-band mag
    // per-band r quantities (None if no r-band observations)
    pub days_since_peak_r: Option<f32>,
    pub days_to_peak_r: Option<f32>,
    pub peakmag_r: Option<f32>,
    pub maxmag_r: Option<f32>,
    // photometry counts
    pub n_photometry_total: f32,
    pub n_photometry_g: f32,
    pub n_photometry_r: f32,
}

/// Performs weighted least squares fit for y = a*x + b (centered for numerical stability)
/// Returns None if the fit cannot be performed (e.g., not enough data points)
/// or if the matrix is singular
fn weighted_least_squares_centered(
    x: &[f32],
    y: &[f32],
    sigma: &[f32],
) -> Option<BandRateProperties> {
    let n = x.len();

    if n < 2 || y.len() != n || sigma.len() != n {
        return None;
    }

    let mut sum_w = 0.0;
    let mut sum_wx = 0.0;
    let mut sum_wy = 0.0;

    for i in 0..n {
        if sigma[i] <= 0.0 || !sigma[i].is_finite() {
            return None;
        }
        let w = 1.0 / (sigma[i] * sigma[i]);
        if !w.is_finite() {
            return None;
        }

        sum_w += w;
        sum_wx += w * x[i];
        sum_wy += w * y[i];
    }

    let x_mean = sum_wx / sum_w;
    let y_mean = sum_wy / sum_w;

    let mut sxx = 0.0;
    let mut sxy = 0.0;

    for i in 0..n {
        let w = 1.0 / (sigma[i] * sigma[i]);
        let dx = x[i] - x_mean;
        let dy = y[i] - y_mean;

        sxx += w * dx * dx;
        sxy += w * dx * dy;
    }

    if sxx.abs() < 1e-10 {
        return None;
    }

    let a = sxy / sxx;
    let b = y_mean - a * x_mean;
    let a_err = (1.0 / sxx).sqrt();

    let mut chi2 = 0.0;
    for i in 0..n {
        let residual = y[i] - (a * x[i] + b);
        chi2 += (residual / sigma[i]).powi(2);
    }

    let reduced_chi2 = if n > 2 {
        chi2 / (n - 2) as f32
    } else {
        f32::NAN
    };

    Some(BandRateProperties {
        rate: a,
        rate_error: a_err,
        red_chi2: reduced_chi2,
        nb_data: n as i32,
        dt: x[n - 1] - x[0],
    })
}

/// Prepares photometry data by sorting and removing duplicates
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
pub fn analyze_photometry(
    sorted_photometry: &[PhotometryMag],
) -> (PerBandProperties, AllBandsProperties, bool) {
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
    let mut bands: std::collections::HashMap<Band, Vec<&PhotometryMag>> =
        std::collections::HashMap::new();
    for mag in sorted_photometry {
        bands
            .entry(mag.band.clone())
            .or_insert_with(Vec::new)
            .push(mag);
    }

    // Per-band g/r trackers for AllBandsProperties extra fields
    let mut g_peak_jd: Option<f64> = None;
    let mut g_first_jd: Option<f64> = None;
    let mut g_peakmag: Option<f32> = None;
    let mut g_maxmag: Option<f32> = None;
    let mut r_peak_jd: Option<f64> = None;
    let mut r_first_jd: Option<f64> = None;
    let mut r_peakmag: Option<f32> = None;
    let mut r_maxmag: Option<f32> = None;

    // let mut results = HashMap::new();
    let mut results: PerBandProperties = PerBandProperties {
        g: None,
        r: None,
        i: None,
        z: None,
        y: None,
        u: None,
    };
    for (band, mags) in &bands {
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

        let mut rising_properties = None;
        let mut fading_properties = None;

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

            rising_properties =
                weighted_least_squares_centered(&before_time, &before_mag, &before_mag_err);
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

            fading_properties =
                weighted_least_squares_centered(&after_time, &after_mag, &after_mag_err);
        }

        let dt = (mags.last().unwrap().time - mags.first().unwrap().time) as f32;
        let band_properties = BandProperties {
            peak_jd,
            peak_mag,
            peak_mag_err,
            dt,
            rising: rising_properties,
            fading: fading_properties,
        };
        match band {
            Band::G => {
                g_peak_jd = Some(peak_jd);
                g_first_jd = Some(mags.first().unwrap().time);
                g_peakmag = Some(peak_mag);
                g_maxmag = Some(faintest_mag);
                results.g = Some(band_properties);
            }
            Band::R => {
                r_peak_jd = Some(peak_jd);
                r_first_jd = Some(mags.first().unwrap().time);
                r_peakmag = Some(peak_mag);
                r_maxmag = Some(faintest_mag);
                results.r = Some(band_properties);
            }
            Band::I => results.i = Some(band_properties),
            Band::Z => results.z = Some(band_properties),
            Band::Y => results.y = Some(band_properties),
            Band::U => results.u = Some(band_properties),
        }
    }

    let all_bands_properties = AllBandsProperties {
        peak_jd: global_peak_jd,
        peak_mag: global_peak_mag,
        peak_mag_err: global_peak_mag_err,
        peak_band: global_peak_band,
        faintest_jd: global_faintest_jd,
        faintest_mag: global_faintest_mag,
        faintest_mag_err: global_faintest_mag_err,
        faintest_band: global_faintest_band,
        first_jd,
        last_jd,
        days_since_peak_g: g_peak_jd.map(|p| (last_jd - p) as f32),
        days_to_peak_g: g_peak_jd.zip(g_first_jd).map(|(p, f)| (p - f) as f32),
        peakmag_g: g_peakmag,
        maxmag_g: g_maxmag,
        days_since_peak_r: r_peak_jd.map(|p| (last_jd - p) as f32),
        days_to_peak_r: r_peak_jd.zip(r_first_jd).map(|(p, f)| (p - f) as f32),
        peakmag_r: r_peakmag,
        maxmag_r: r_maxmag,
        n_photometry_total: sorted_photometry.len() as f32,
        n_photometry_g: bands.get(&Band::G).map_or(0, |v| v.len()) as f32,
        n_photometry_r: bands.get(&Band::R).map_or(0, |v| v.len()) as f32,
    };

    (results, all_bands_properties, stationary)
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
    fn test_weighted_least_squares_centered() {
        // Test case 1: simple linear data (approximately y = 2x) with small errors
        // => expecting a good fit
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y = vec![2.0, 4.1, 6.0, 8.1, 10.2];
        let sigma = vec![0.1, 0.1, 0.1, 0.1, 0.1];

        let result = weighted_least_squares_centered(&x, &y, &sigma).unwrap();
        assert!((result.rate - 2.04).abs() < 1e-2);
        assert!((result.rate_error - 0.031623).abs() < 1e-6);
        assert!((result.red_chi2 - 0.400001).abs() < 1e-6);
        assert_eq!(result.nb_data, 5);
        assert!((result.dt - 4.0).abs() < 1e-5);

        // Test case 2: insufficient data points
        // => should return None
        let x = vec![1.0];
        let y = vec![2.0];
        let sigma = vec![0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma);
        assert!(result.is_none());

        // Test case 3: singular matrix (all x values are the same)
        // => should return None
        let x = vec![1.0, 1.0, 1.0];
        let y = vec![2.0, 2.1, 1.9];
        let sigma = vec![0.1, 0.1, 0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma);
        assert!(result.is_none());

        // Test case 4: mismatched input lengths
        // => should return None
        let x = vec![1.0, 2.0];
        let y = vec![2.0, 4.0, 6.0];
        let sigma = vec![0.1, 0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma);
        assert!(result.is_none());

        // Test case 5: zero or negative sigma values
        // => should return None
        let x = vec![1.0, 2.0, 3.0];
        let y = vec![2.0, 4.0, 6.0];
        let sigma = vec![0.1, 0.0, -0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma);
        assert!(result.is_none());

        // Test case 6: non-finite sigma values
        let x = vec![1.0, 2.0, 3.0];
        let y = vec![2.0, 4.0, 6.0];
        let sigma = vec![0.1, f32::NAN, 0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma);
        assert!(result.is_none());

        // Test case 7: non-finite weights due to very small sigma
        let x = vec![1.0, 2.0, 3.0];
        let y = vec![2.0, 4.0, 6.0];
        let sigma = vec![1e-40, 1e-40, 1e-40];
        let result = weighted_least_squares_centered(&x, &y, &sigma);
        assert!(result.is_none());

        // Test case 8: bad fit due to large scatter
        let x = vec![1.0, 2.0, 3.0, 4.0, 5.0];
        let y = vec![2.0, 8.0, 1.0, 7.0, 3.0];
        let sigma = vec![0.1, 0.1, 0.1, 0.1, 0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma).unwrap();
        assert!((result.red_chi2 - 1290.0).abs() < 1e-1);

        // Test case 9: exactly two data points
        let x = vec![1.0, 2.0];
        let y = vec![2.0, 4.0];
        let sigma = vec![0.1, 0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma).unwrap();
        assert!((result.rate - 2.0).abs() < 1e-6);
        assert!((result.rate_error - 0.141421).abs() < 1e-6);
        assert!(result.red_chi2.is_nan()); // for 2 data points, red_chi2 is NaN
        assert_eq!(result.nb_data, 2);
        assert!((result.dt - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_prepare_photometry() {
        let mut photometry = vec![
            PhotometryMag {
                time: 2459001.5,
                mag: 19.5,
                mag_err: 0.1,
                band: Band::R,
            }, // later point that should be sorted down
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            }, // earlier point that should be sorted up
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
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
            band: Band::R,
        }];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, false);
        let r_stats = results.r.unwrap();
        let r_peak_jd = r_stats.peak_jd;
        let r_peak_mag = r_stats.peak_mag;
        let r_peak_mag_err = r_stats.peak_mag_err;
        let r_dt = r_stats.dt;
        assert!((data[0].time - r_peak_jd).abs() < 1e-6);
        assert!((data[0].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[0].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert!((r_dt - 0.0).abs() < 1e-6);
        assert_eq!(r_stats.rising.is_some(), false);
        assert_eq!(r_stats.fading.is_some(), false);

        // the all band properties should also just match the one data point we have
        let peak_jd = all_bands_props.peak_jd;
        let peak_mag = all_bands_props.peak_mag;
        let peak_mag_err = all_bands_props.peak_mag_err;
        let peak_band = all_bands_props.peak_band;
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
                band: Band::R,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.0,
                mag_err: 0.1,
                band: Band::R,
            },
        ];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, true);
        let r_stats = results.r.unwrap();
        let r_peak_jd = r_stats.peak_jd;
        let r_peak_mag = r_stats.peak_mag;
        let r_peak_mag_err = r_stats.peak_mag_err;
        let r_dt = r_stats.dt;
        assert!((data[1].time - r_peak_jd).abs() < 1e-6);
        assert!((data[1].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[1].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert!((r_dt - 1.0).abs() < 1e-6);
        assert_eq!(r_stats.rising.is_some(), true);
        assert_eq!(r_stats.fading.is_none(), true);

        let rising_stats = r_stats.rising.clone().unwrap();
        let rising_rate = rising_stats.rate;
        let rising_red_chi2 = rising_stats.red_chi2;
        let rising_nb_data = rising_stats.nb_data;
        let rising_dt = rising_stats.dt;
        assert!((rising_rate + 1.0).abs() < 1e-6); // should be -1 mag/day
        assert!(rising_red_chi2.is_nan()); // for 2 data points, red_chi2 is NaN
        assert_eq!(rising_nb_data, 2);
        assert!((rising_dt - 1.0).abs() < 1e-6);

        // the all band properties should also just match the one data point we have
        let peak_jd = all_bands_props.peak_jd;
        let peak_mag = all_bands_props.peak_mag;
        let peak_mag_err = all_bands_props.peak_mag_err;
        let peak_band = all_bands_props.peak_band;
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
                band: Band::R,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            },
        ];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, true);
        let r_stats = results.r.unwrap();
        let r_peak_jd = r_stats.peak_jd;
        let r_peak_mag = r_stats.peak_mag;
        let r_peak_mag_err = r_stats.peak_mag_err;
        let r_dt = r_stats.dt;
        assert!((data[0].time - r_peak_jd).abs() < 1e-6);
        assert!((data[0].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[0].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert!((r_dt - 1.0).abs() < 1e-6);
        assert_eq!(r_stats.rising.is_none(), true);
        assert_eq!(r_stats.fading.is_some(), true);
        let fading_stats = r_stats.fading.clone().unwrap();
        let fading_rate = fading_stats.rate;
        let red_chi2 = fading_stats.red_chi2;
        let fading_nb_data = fading_stats.nb_data;
        let fading_dt = fading_stats.dt;
        assert!((fading_rate - 1.0).abs() < 1e-6); // should be 1 mag/day
        assert!(red_chi2.is_nan()); // for 2 data points, red_chi2 is NaN
        assert_eq!(fading_nb_data, 2);
        assert!((fading_dt - 1.0).abs() < 1e-6);
        // the all band properties should also just match the one data point we have
        let peak_jd = all_bands_props.peak_jd;
        let peak_mag = all_bands_props.peak_mag;
        let peak_mag_err = all_bands_props.peak_mag_err;
        let peak_band = all_bands_props.peak_band;
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
                band: Band::R,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.0,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459002.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            },
        ];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, true);
        let r_stats = results.r.unwrap();
        let r_peak_jd = r_stats.peak_jd;
        let r_peak_mag = r_stats.peak_mag;
        let r_peak_mag_err = r_stats.peak_mag_err;
        let r_dt = r_stats.dt;
        assert!((data[1].time - r_peak_jd).abs() < 1e-6);
        assert!((data[1].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[1].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert!((r_dt - 2.0).abs() < 1e-6);
        assert_eq!(r_stats.rising.is_some(), true);
        assert_eq!(r_stats.fading.is_some(), true);
        let rising_stats = r_stats.rising.clone().unwrap();
        let rising_rate = rising_stats.rate;
        let rising_red_chi2 = rising_stats.red_chi2;
        let rising_nb_data = rising_stats.nb_data;
        let rising_dt = rising_stats.dt;
        assert!((rising_rate + 1.0).abs() < 1e-6); // should be -1 mag/day
        assert!(rising_red_chi2.is_nan()); // for 2 data points, red_chi2 is NaN
        assert_eq!(rising_nb_data, 2);
        assert!((rising_dt - 1.0).abs() < 1e-6);

        let fading_stats = r_stats.fading.clone().unwrap();
        let fading_rate = fading_stats.rate;
        let fading_red_chi2 = fading_stats.red_chi2;
        let fading_nb_data = fading_stats.nb_data;
        let fading_dt = fading_stats.dt;
        assert!((fading_rate - 1.0).abs() < 1e-6); // should be 1 mag/day
        assert!(fading_red_chi2.is_nan()); // for 2 data points, red_chi2 is NaN
        assert_eq!(fading_nb_data, 2);
        assert!((fading_dt - 1.0).abs() < 1e-6);

        // the all band properties should also just match the one data point we have
        let peak_jd = all_bands_props.peak_jd;
        let peak_mag = all_bands_props.peak_mag;
        let peak_mag_err = all_bands_props.peak_mag_err;
        let peak_band = all_bands_props.peak_band;
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
                band: Band::R,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.0,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459002.5,
                mag: 18.0,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::G,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 21.0,
                mag_err: 0.1,
                band: Band::G,
            },
        ];
        prepare_photometry(&mut data);
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        // Verify results
        assert_eq!(stationary, true);
        // r band
        let r_stats = results.r.unwrap();
        let r_peak_jd = r_stats.peak_jd;
        let r_peak_mag = r_stats.peak_mag;
        let r_peak_mag_err = r_stats.peak_mag_err;
        let r_dt = r_stats.dt;
        // the original array was sorted and deduplicated,
        // so the r-band peak is now at index 2 (not 1)
        assert!((data[4].time - r_peak_jd).abs() < 1e-6);
        assert!((data[4].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[4].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert!((r_dt - 2.0).abs() < 1e-6);
        assert_eq!(r_stats.rising.is_some(), true);
        assert_eq!(r_stats.fading.is_none(), true);

        // check the rising stats in r band
        let rising_stats = r_stats.rising.clone().unwrap();
        let rising_rate = rising_stats.rate;
        let rising_red_chi2 = rising_stats.red_chi2;
        let rising_nb_data = rising_stats.nb_data;
        let rising_dt = rising_stats.dt;
        assert!((rising_rate + 1.0).abs() < 1e-6); // should be -1 mag/day
        assert!(rising_red_chi2.abs() < 1e-6); // perfect fit
        assert_eq!(rising_nb_data, 3);
        assert!((rising_dt - 2.0).abs() < 1e-6);

        // g band
        let g_stats = results.g.unwrap();
        let g_peak_jd = g_stats.peak_jd;
        let g_peak_mag = g_stats.peak_mag;
        let g_peak_mag_err = g_stats.peak_mag_err;
        let g_dt = g_stats.dt;
        // the original array was sorted and deduplicated,
        // so the g-band peak is now at index 3 (not 4)
        assert!((data[1].time - g_peak_jd).abs() < 1e-6);
        assert!((data[1].mag - g_peak_mag).abs() < 1e-6);
        assert!((data[1].mag_err - g_peak_mag_err).abs() < 1e-6);
        assert!((g_dt - 1.0).abs() < 1e-6);
        assert_eq!(g_stats.rising.is_some(), false);
        assert_eq!(g_stats.fading.is_some(), true);
        // check the fading stats in g band
        let fading_stats = g_stats.fading.clone().unwrap();
        let fading_rate = fading_stats.rate;
        let fading_red_chi2 = fading_stats.red_chi2;
        let fading_nb_data = fading_stats.nb_data;
        let fading_dt = fading_stats.dt;
        assert!((fading_rate - 1.0).abs() < 1e-6); // should be 1 mag/day
        assert!(fading_red_chi2.is_nan()); // for 2 data points, red_chi2 is NaN
        assert_eq!(fading_nb_data, 2);
        assert!((fading_dt - 1.0).abs() < 1e-6);

        // the all band properties should match the peak in r band
        let peak_jd = all_bands_props.peak_jd;
        let peak_mag = all_bands_props.peak_mag;
        let peak_mag_err = all_bands_props.peak_mag_err;
        let peak_band = all_bands_props.peak_band;
        assert!((data[4].time - peak_jd).abs() < 1e-6);
        assert!((data[4].mag - peak_mag).abs() < 1e-6);
        assert!((data[4].mag_err - peak_mag_err).abs() < 1e-6);
        assert_eq!(data[4].band, peak_band);

        // Edge case 1: duplicated points (same time and band, different mag)
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            },
            // duplicate of the first point
            PhotometryMag {
                time: 2459000.5,
                mag: 20.5,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.0,
                mag_err: 0.1,
                band: Band::R,
            },
        ];
        prepare_photometry(&mut data);
        let (results, _, stationary) = analyze_photometry(&data);
        // make sure that only 2 points were used (the duplicate should be removed)
        assert_eq!(stationary, true);
        let r_stats = results.r.unwrap();
        let r_peak_jd = r_stats.peak_jd;
        let r_peak_mag = r_stats.peak_mag;
        let r_peak_mag_err = r_stats.peak_mag_err;
        let r_dt = r_stats.dt;
        // the original array was sorted and deduplicated,
        // so the r-band peak is now at index 1 (not 2)
        assert!((data[1].time - r_peak_jd).abs() < 1e-6);
        assert!((data[1].mag - r_peak_mag).abs() < 1e-6);
        assert!((data[1].mag_err - r_peak_mag_err).abs() < 1e-6);
        assert!((r_dt - 1.0).abs() < 1e-6);
        assert_eq!(r_stats.rising.is_some(), true);
        assert_eq!(r_stats.fading.is_none(), true);
        let rising_stats = r_stats.rising.clone().unwrap();
        let rising_nb_data = rising_stats.nb_data;
        let rising_dt = rising_stats.dt;
        assert_eq!(rising_nb_data, 2);
        assert!((rising_dt - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_per_band_properties_both_bands() {
        // g: faint → bright → faint (peak at middle point)
        // r: bright → faint (monotonically fading, peak at first point)
        // last JD = 2459003.5 (the final r-band point)
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 20.5,
                mag_err: 0.1,
                band: Band::G,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 18.0,
                mag_err: 0.1,
                band: Band::G,
            }, // g peak
            PhotometryMag {
                time: 2459002.5,
                mag: 19.5,
                mag_err: 0.1,
                band: Band::G,
            },
            PhotometryMag {
                time: 2459001.0,
                mag: 17.5,
                mag_err: 0.1,
                band: Band::R,
            }, // r peak
            PhotometryMag {
                time: 2459003.5,
                mag: 21.0,
                mag_err: 0.1,
                band: Band::R,
            },
        ];
        prepare_photometry(&mut data);
        let (_, props, _) = analyze_photometry(&data);

        // last_jd = 2459003.5 (the final observation across all bands)
        let last_jd = 2459003.5_f64;

        // g-band: peak at JD 2459001.5, first g at JD 2459000.5
        assert!((props.peakmag_g.unwrap() - 18.0).abs() < 1e-5);
        assert!((props.maxmag_g.unwrap() - 20.5).abs() < 1e-5);
        assert!((props.days_to_peak_g.unwrap() - 1.0).abs() < 1e-4); // 2459001.5 - 2459000.5
        assert!((props.days_since_peak_g.unwrap() - (last_jd - 2459001.5) as f32).abs() < 1e-4);

        // r-band: peak at JD 2459001.0, first r at JD 2459001.0
        assert!((props.peakmag_r.unwrap() - 17.5).abs() < 1e-5);
        assert!((props.maxmag_r.unwrap() - 21.0).abs() < 1e-5);
        assert!((props.days_to_peak_r.unwrap() - 0.0).abs() < 1e-4); // peak == first r obs
        assert!((props.days_since_peak_r.unwrap() - (last_jd - 2459001.0) as f32).abs() < 1e-4);
    }

    #[test]
    fn test_per_band_properties_r_only() {
        // Only r-band observations — g-band fields must all be None.
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 20.0,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459002.5,
                mag: 18.5,
                mag_err: 0.1,
                band: Band::R,
            },
            PhotometryMag {
                time: 2459004.5,
                mag: 19.5,
                mag_err: 0.1,
                band: Band::R,
            },
        ];
        prepare_photometry(&mut data);
        let (_, props, _) = analyze_photometry(&data);

        assert!(props.peakmag_g.is_none());
        assert!(props.maxmag_g.is_none());
        assert!(props.days_to_peak_g.is_none());
        assert!(props.days_since_peak_g.is_none());

        // r-band: peak at JD 2459002.5 (mag 18.5), first r at JD 2459000.5
        assert!((props.peakmag_r.unwrap() - 18.5).abs() < 1e-5);
        assert!((props.maxmag_r.unwrap() - 20.0).abs() < 1e-5);
        assert!((props.days_to_peak_r.unwrap() - 2.0).abs() < 1e-4);
        assert!((props.days_since_peak_r.unwrap() - 2.0).abs() < 1e-4); // last_jd - peak = 4.5 - 2.5 = 2.0
    }

    #[test]
    fn test_per_band_properties_g_only() {
        // Only g-band observations — r-band fields must all be None.
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 21.0,
                mag_err: 0.1,
                band: Band::G,
            },
            PhotometryMag {
                time: 2459001.5,
                mag: 19.0,
                mag_err: 0.1,
                band: Band::G,
            }, // peak
        ];
        prepare_photometry(&mut data);
        let (_, props, _) = analyze_photometry(&data);

        assert!(props.peakmag_r.is_none());
        assert!(props.maxmag_r.is_none());
        assert!(props.days_to_peak_r.is_none());
        assert!(props.days_since_peak_r.is_none());

        assert!((props.peakmag_g.unwrap() - 19.0).abs() < 1e-5);
        assert!((props.maxmag_g.unwrap() - 21.0).abs() < 1e-5);
        assert!((props.days_to_peak_g.unwrap() - 1.0).abs() < 1e-4);
        assert!((props.days_since_peak_g.unwrap() - 0.0).abs() < 1e-4); // peak == last obs
    }

    #[test]
    fn test_per_band_properties_single_obs_per_band() {
        // Single observation in each band: days_to_peak = 0, days_since_peak = 0.
        let mut data = vec![
            PhotometryMag {
                time: 2459000.5,
                mag: 19.0,
                mag_err: 0.1,
                band: Band::G,
            },
            PhotometryMag {
                time: 2459000.5,
                mag: 18.5,
                mag_err: 0.1,
                band: Band::R,
            },
        ];
        prepare_photometry(&mut data);
        let (_, props, _) = analyze_photometry(&data);

        assert!((props.days_to_peak_g.unwrap() - 0.0).abs() < 1e-4);
        assert!((props.days_since_peak_g.unwrap() - 0.0).abs() < 1e-4);
        assert!((props.days_to_peak_r.unwrap() - 0.0).abs() < 1e-4);
        assert!((props.days_since_peak_r.unwrap() - 0.0).abs() < 1e-4);
        assert!((props.peakmag_g.unwrap() - 19.0).abs() < 1e-5);
        assert!((props.peakmag_r.unwrap() - 18.5).abs() < 1e-5);
        // single obs: peakmag == maxmag
        assert!((props.maxmag_g.unwrap() - 19.0).abs() < 1e-5);
        assert!((props.maxmag_r.unwrap() - 18.5).abs() < 1e-5);
    }
}
