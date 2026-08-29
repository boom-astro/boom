use apache_avro_derive::AvroSchema;
use apache_avro_macros::serdavro;
use mongodb::bson::doc;
use serde::{Deserialize, Serialize};
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
    // Near-infrared bands (e.g. WINTER: fid 0=Y, 1=J, 2=H, 3=K)
    #[serde(rename = "j")]
    J,
    #[serde(rename = "h")]
    H,
    #[serde(rename = "k")]
    K,
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
            Band::J => write!(f, "j"),
            Band::H => write!(f, "h"),
            Band::K => write!(f, "k"),
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

// TODO: avro serialization fail when we use skip_serializing_none,
// since the optional fields are not just None but simply missing
// (this needs to be fixed in the apache_avro-related crates)
// #[serde_as]
// #[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, AvroSchema, ToSchema)]
pub struct BandRateProperties {
    pub rate: f32,
    pub rate_error: f32,
    /// Chi-square of the linear fit. Always defined, including for a two-point
    /// fit, where the line passes through both points and it is exactly zero.
    pub chi2: f32,
    /// Degrees of freedom, `nb_data - 2`. Zero for a two-point fit, which is
    /// what makes `red_chi2` undefined there.
    pub dof: i32,
    /// Chi-square per degree of freedom, null when `dof` is zero.
    ///
    /// A two-point fit has nothing left over to test goodness of fit against, so
    /// there is no honest value to report. Null is not "a good fit" and not
    /// "a bad fit": a Mongo range cut such as `red_chi2 <= 2` matches *neither*
    /// null nor absent, so a filter written only on this field silently drops
    /// every two-point band. Pair the range cut with `{red_chi2: null}`, which
    /// matches both this and pre-existing documents, when sparse bands count.
    pub red_chi2: Option<f32>,
    pub nb_data: i32,
    pub dt: f32,
}

/// Window, in days before the object's latest detection, that `recent` covers.
///
/// The unbounded fit spans the whole light curve -- a median of ~1400 days in
/// production -- so its rate describes a line drawn across years rather than
/// what the object is doing now. Measured against real cadence, 7, 30 and 60 day
/// windows all yield a fittable band for about a tenth of alerts, so the exact
/// value matters far less than having a bound at all.
pub const RECENT_WINDOW_DAYS: f64 = 30.0;

/// Fit one monotonic segment, returning `None` when it cannot support a line.
///
/// Rejects a segment whose spread sits inside its own error bars: a fit through
/// noise produces a confident-looking rate that means nothing.
fn fit_segment(segment: &[&PhotometryMag]) -> Option<BandRateProperties> {
    if segment.len() < 2 {
        return None;
    }
    let t0 = segment[0].time;
    if segment.last()?.time - t0 <= 0.01 {
        return None;
    }

    let mut time = Vec::with_capacity(segment.len());
    let mut mag = Vec::with_capacity(segment.len());
    let mut mag_err = Vec::with_capacity(segment.len());
    let (mut min_mag, mut min_mag_err) = (f32::INFINITY, f32::INFINITY);
    let (mut max_mag, mut max_mag_err) = (f32::NEG_INFINITY, f32::NEG_INFINITY);

    for m in segment {
        time.push((m.time - t0) as f32);
        mag.push(m.mag);
        mag_err.push(m.mag_err);
        if m.mag < min_mag {
            min_mag = m.mag;
            min_mag_err = m.mag_err;
        }
        if m.mag > max_mag {
            max_mag = m.mag;
            max_mag_err = m.mag_err;
        }
    }

    if min_mag + min_mag_err >= max_mag - max_mag_err {
        return None;
    }
    weighted_least_squares_centered(&time, &mag, &mag_err)
}

/// Index of the brightest (numerically smallest magnitude) point.
fn brightest_index(mags: &[&PhotometryMag]) -> Option<usize> {
    (!mags.is_empty()).then(|| {
        mags.iter().enumerate().fold(
            0,
            |best, (i, m)| if m.mag < mags[best].mag { i } else { best },
        )
    })
}

/// Split a band's points at its brightest and fit each side.
fn fit_rising_and_fading(
    mags: &[&PhotometryMag],
) -> (Option<BandRateProperties>, Option<BandRateProperties>) {
    let Some(peak) = brightest_index(mags) else {
        return (None, None);
    };
    (fit_segment(&mags[0..=peak]), fit_segment(&mags[peak..]))
}

/// Fits over the last [`RECENT_WINDOW_DAYS`] only.
///
/// Present whenever the window holds any detection, so "no recent data" (null)
/// stays distinguishable from "recent data that cannot be fit" (present, with
/// `rising` and `fading` null). Most objects in the alert stream are sampled too
/// sparsely for a windowed fit; that is a fact about the cadence, and reporting
/// it as absent is the honest outcome rather than a defect.
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, AvroSchema, ToSchema)]
pub struct RecentProperties {
    /// Width of the window in days, so a consumer need not assume the constant.
    pub window_days: f32,
    /// Detections inside the window.
    pub nb_data: i32,
    pub peak_jd: f64,
    pub peak_mag: f32,
    pub rising: Option<BandRateProperties>,
    pub fading: Option<BandRateProperties>,
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
    /// Fits over the whole light curve. The window is unbounded, so on an object
    /// with years of history these describe a line across all of it rather than
    /// current behaviour -- see `recent` for that.
    pub rising: Option<BandRateProperties>,
    pub fading: Option<BandRateProperties>,
    /// The same fits restricted to the last [`RECENT_WINDOW_DAYS`], or null when
    /// the window holds no detection in this band.
    pub recent: Option<RecentProperties>,
}

/// Morphological indicators of cometary activity, computed per survey but expressed
/// on one scale so a filter reads identically on ZTF and LSST.
#[derive(
    Debug,
    Clone,
    Default,
    PartialEq,
    serde::Deserialize,
    serde::Serialize,
    AvroSchema,
    utoipa::ToSchema,
)]
#[serde(default)]
pub struct ActivityMetrics {
    /// Flux in the aperture beyond what a point source would put there, in
    /// magnitudes. Positive means extended. Cross-survey comparable because both
    /// sides reduce to a magnitude difference.
    ///
    /// Confounded by trailing: a fast mover smears during the exposure and looks
    /// extended. Use alongside `sso.sky_motion` rather than alone.
    ///
    /// Computed here only because the degenerate cases are batch-fatal in an
    /// aggregation: `$divide` by a zero flux and `$log10` of a negative one both
    /// abort the whole pipeline, and difference imaging produces such fluxes
    /// routinely. No threshold is applied — filters choose their own cut.
    pub aperture_excess: Option<f32>,
}

impl ActivityMetrics {
    /// ZTF: aperture and PSF magnitudes directly. Brighter aperture => positive.
    pub fn from_magnitudes(magpsf: Option<f32>, magap: Option<f32>) -> Self {
        let aperture_excess = match (magpsf, magap) {
            (Some(psf), Some(ap)) if psf.is_finite() && ap.is_finite() => Some(psf - ap),
            _ => None,
        };
        Self::from_excess(aperture_excess)
    }

    /// LSST: signed fluxes rather than magnitudes.
    ///
    /// Same sign rather than positive: a negative source has a well defined
    /// aperture excess. Mixed signs are rejected -- relating a positive residual to
    /// a negative one is not one.
    pub fn from_fluxes(psf_flux: Option<f32>, ap_flux: Option<f32>) -> Self {
        let aperture_excess = match (psf_flux, ap_flux) {
            (Some(psf), Some(ap)) if psf != 0.0 && ap != 0.0 && psf.signum() == ap.signum() => {
                Some(2.5 * (ap / psf).log10())
            }
            _ => None,
        };
        Self::from_excess(aperture_excess)
    }

    /// Single point where the value is admitted, so the finiteness check cannot
    /// be forgotten by a future constructor.
    ///
    /// Guarding the inputs is not sufficient on the flux path: a ratio of two
    /// perfectly finite fluxes overflows f32 once they differ by more than ~1e38,
    /// which difference imaging reaches with a near-zero PSF flux. A non-finite
    /// value stored here would serialize as a BSON double the filters then
    /// compare against, so it is dropped rather than passed on.
    fn from_excess(aperture_excess: Option<f32>) -> Self {
        ActivityMetrics {
            aperture_excess: aperture_excess.filter(|e| e.is_finite()),
        }
    }
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
    pub j: Option<BandProperties>,
    pub h: Option<BandProperties>,
    pub k: Option<BandProperties>,
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

    let dof = n.saturating_sub(2);
    let reduced_chi2 = (dof > 0).then(|| chi2 / dof as f32);

    Some(BandRateProperties {
        rate: a,
        rate_error: a_err,
        chi2,
        dof: dof as i32,
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
    // Defensive guard: an empty lightcurve has no peak/faintest point to
    // reference, and the code below unconditionally indexes `[0]`. Callers can
    // legitimately produce empty lightcurves (e.g. reprocessing historical
    // alerts whose photometry was entirely filtered out), so return neutral
    // defaults instead of panicking. Callers that need to treat "no photometry"
    // specially should check emptiness before calling.
    if sorted_photometry.is_empty() {
        return (
            PerBandProperties::default(),
            AllBandsProperties {
                peak_jd: 0.0,
                peak_mag: 0.0,
                peak_mag_err: 0.0,
                peak_band: Band::G,
                faintest_jd: 0.0,
                faintest_mag: 0.0,
                faintest_mag_err: 0.0,
                faintest_band: Band::G,
                first_jd: 0.0,
                last_jd: 0.0,
            },
            false,
        );
    }

    // The empty case returned early above, so the slice is non-empty here.
    let stationary = (sorted_photometry.last().unwrap().time - sorted_photometry[0].time) > 0.01;

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

    // let mut results = HashMap::new();
    let mut results: PerBandProperties = PerBandProperties {
        g: None,
        r: None,
        i: None,
        z: None,
        y: None,
        u: None,
        j: None,
        h: None,
        k: None,
    };
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

        let dt = (mags.last().unwrap().time - mags.first().unwrap().time) as f32;
        let (rising_properties, fading_properties) = fit_rising_and_fading(&mags);

        // Anchored to the object's newest detection in any band, so every band
        // covers the same interval and a colour comparison stays meaningful.
        let cutoff = last_jd - RECENT_WINDOW_DAYS;
        let window: Vec<&PhotometryMag> =
            mags.iter().copied().filter(|m| m.time >= cutoff).collect();
        let recent = brightest_index(&window).map(|peak| {
            let (rising, fading) = fit_rising_and_fading(&window);
            RecentProperties {
                window_days: RECENT_WINDOW_DAYS as f32,
                nb_data: window.len() as i32,
                peak_jd: window[peak].time,
                peak_mag: window[peak].mag,
                rising,
                fading,
            }
        });

        let band_properties = BandProperties {
            peak_jd,
            peak_mag,
            peak_mag_err,
            dt,
            rising: rising_properties,
            fading: fading_properties,
            recent,
        };
        match band {
            Band::G => results.g = Some(band_properties),
            Band::R => results.r = Some(band_properties),
            Band::I => results.i = Some(band_properties),
            Band::Z => results.z = Some(band_properties),
            Band::Y => results.y = Some(band_properties),
            Band::U => results.u = Some(band_properties),
            Band::J => results.j = Some(band_properties),
            Band::H => results.h = Some(band_properties),
            Band::K => results.k = Some(band_properties),
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
        assert!((result.red_chi2.unwrap() - 0.400001).abs() < 1e-6);
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
        assert!((result.red_chi2.unwrap() - 1290.0).abs() < 1e-1);

        // Test case 9: exactly two data points
        let x = vec![1.0, 2.0];
        let y = vec![2.0, 4.0];
        let sigma = vec![0.1, 0.1];
        let result = weighted_least_squares_centered(&x, &y, &sigma).unwrap();
        assert!((result.rate - 2.0).abs() < 1e-6);
        assert!((result.rate_error - 0.141421).abs() < 1e-6);
        // Two points define the line exactly: chi2 is 0 with no degrees of
        // freedom left, so there is no reduced chi2 to report.
        assert!(result.red_chi2.is_none());
        assert_eq!(result.dof, 0);
        assert!(result.chi2.abs() < 1e-9);
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
    fn test_analyze_photometry_empty_lightcurve() {
        // Regression: an empty lightcurve must not panic (it previously indexed
        // `[0]` unconditionally). It should return neutral defaults with no
        // per-band properties and stationary = false.
        let data: Vec<PhotometryMag> = Vec::new();
        let (results, all_bands_props, stationary) = analyze_photometry(&data);

        assert_eq!(stationary, false);
        assert!(results.g.is_none());
        assert!(results.r.is_none());
        assert!(results.i.is_none());
        assert!(results.z.is_none());
        assert!(results.y.is_none());
        assert!(results.u.is_none());
        assert!(results.j.is_none());
        assert!(results.h.is_none());
        assert!(results.k.is_none());
        assert_eq!(all_bands_props.peak_jd, 0.0);
        assert_eq!(all_bands_props.peak_mag, 0.0);
        assert_eq!(all_bands_props.first_jd, 0.0);
        assert_eq!(all_bands_props.last_jd, 0.0);
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
        assert!(rising_red_chi2.is_none()); // 2 points: exact line, no degrees of freedom
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
        assert!(red_chi2.is_none()); // 2 points: exact line, no degrees of freedom
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
        assert!(rising_red_chi2.is_none()); // 2 points: exact line, no degrees of freedom
        assert_eq!(rising_nb_data, 2);
        assert!((rising_dt - 1.0).abs() < 1e-6);

        let fading_stats = r_stats.fading.clone().unwrap();
        let fading_rate = fading_stats.rate;
        let fading_red_chi2 = fading_stats.red_chi2;
        let fading_nb_data = fading_stats.nb_data;
        let fading_dt = fading_stats.dt;
        assert!((fading_rate - 1.0).abs() < 1e-6); // should be 1 mag/day
        assert!(fading_red_chi2.is_none()); // 2 points: exact line, no degrees of freedom
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
        assert!(rising_red_chi2.unwrap().abs() < 1e-6); // perfect fit
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
        assert!(fading_red_chi2.is_none()); // 2 points: exact line, no degrees of freedom
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
}

#[cfg(test)]
mod activity_tests {
    use super::*;

    #[test]
    fn test_ztf_aperture_excess_positive_when_aperture_brighter() {
        // Brighter aperture => smaller magnitude => positive excess.
        let a = ActivityMetrics::from_magnitudes(Some(18.0), Some(17.5));
        assert_eq!(a.aperture_excess, Some(0.5));

        let point = ActivityMetrics::from_magnitudes(Some(18.0), Some(18.0));
        assert_eq!(point.aperture_excess, Some(0.0));
    }

    #[test]
    fn test_lsst_flux_ratio_matches_the_magnitude_scale() {
        // A 1.585x aperture/psf flux ratio is 0.5 mag, so both surveys agree.
        let a = ActivityMetrics::from_fluxes(Some(100.0), Some(158.489));
        let excess = a.aperture_excess.unwrap();
        assert!((excess - 0.5).abs() < 1e-3, "got {excess}");
    }

    // Difference imaging routinely yields non-positive fluxes; a log of those is
    // not a measurement.
    #[test]
    fn test_zero_or_missing_fluxes_yield_no_measurement() {
        for (psf, ap) in [
            (Some(0.0), Some(10.0)),
            (Some(10.0), Some(0.0)),
            (None, Some(10.0)),
        ] {
            let a = ActivityMetrics::from_fluxes(psf, ap);
            assert!(a.aperture_excess.is_none());
        }
    }

    // A negative source measures the same way as a positive one.
    #[test]
    fn test_same_sign_negative_fluxes_measure_the_same_excess() {
        let positive = ActivityMetrics::from_fluxes(Some(100.0), Some(150.0));
        let negative = ActivityMetrics::from_fluxes(Some(-100.0), Some(-150.0));
        assert_eq!(positive.aperture_excess, negative.aperture_excess);
        assert!(positive.aperture_excess.expect("measured") > 0.0);
    }

    // The magnitudes are built from |flux|, so they agree with the flux path on
    // same-sign input -- and would silently disagree on mixed-sign input.
    #[test]
    fn test_flux_and_magnitude_paths_agree_on_same_sign_input() {
        for (psf, ap) in [(100.0f32, 150.0f32), (-100.0, -150.0)] {
            let (magpsf, _) = flux2mag(psf.abs(), 1.0, LSST_ZP_AB_NJY);
            let (magap, _) = flux2mag(ap.abs(), 1.0, LSST_ZP_AB_NJY);
            let from_flux = ActivityMetrics::from_fluxes(Some(psf), Some(ap))
                .aperture_excess
                .expect("measured");
            let from_mag = ActivityMetrics::from_magnitudes(Some(magpsf), Some(magap))
                .aperture_excess
                .expect("measured");
            assert!(
                (from_flux - from_mag).abs() < 1e-4,
                "{from_flux} vs {from_mag}"
            );
        }
    }

    #[test]
    fn test_mixed_sign_fluxes_yield_no_measurement() {
        for (psf, ap) in [(100.0, -150.0), (-100.0, 150.0)] {
            assert!(
                ActivityMetrics::from_fluxes(Some(psf), Some(ap))
                    .aperture_excess
                    .is_none(),
                "from_fluxes({psf}, {ap})"
            );
        }
    }

    // Both fluxes can be finite and positive and still produce a non-finite
    // ratio, so the inputs alone are not enough to guard on.
    #[test]
    fn test_finite_fluxes_with_an_overflowing_ratio_yield_no_measurement() {
        let a = ActivityMetrics::from_fluxes(Some(f32::MIN_POSITIVE), Some(f32::MAX));
        assert!(a.aperture_excess.is_none(), "got {:?}", a.aperture_excess);
    }

    #[test]
    fn test_non_finite_inputs_yield_no_measurement() {
        for (psf, ap) in [
            (Some(f32::INFINITY), Some(10.0)),
            (Some(10.0), Some(f32::INFINITY)),
            (Some(f32::INFINITY), Some(f32::INFINITY)),
            (Some(f32::NAN), Some(10.0)),
        ] {
            assert!(
                ActivityMetrics::from_fluxes(psf, ap)
                    .aperture_excess
                    .is_none(),
                "from_fluxes({psf:?}, {ap:?})"
            );
            assert!(
                ActivityMetrics::from_magnitudes(psf, ap)
                    .aperture_excess
                    .is_none(),
                "from_magnitudes({psf:?}, {ap:?})"
            );
        }
    }

    #[test]
    fn test_missing_magnitudes_yield_no_measurement() {
        let a = ActivityMetrics::from_magnitudes(Some(18.0), None);
        assert!(a.aperture_excess.is_none());
    }
}

#[cfg(test)]
mod goodness_of_fit_tests {
    use super::*;

    fn fit(x: &[f32], y: &[f32], s: &[f32]) -> BandRateProperties {
        weighted_least_squares_centered(x, y, s).expect("fit")
    }

    // Two points define a line exactly. chi2 is 0 and there is nothing left to
    // reduce by, so red_chi2 has no honest value.
    #[test]
    fn test_two_point_fit_has_no_reduced_chi2_but_a_real_chi2() {
        let r = fit(&[0.0, 1.0], &[20.0, 19.0], &[0.1, 0.1]);
        assert_eq!(r.dof, 0);
        assert_eq!(r.nb_data, 2);
        assert!(r.chi2.abs() < 1e-9, "exact fit, got chi2 = {}", r.chi2);
        assert!(r.red_chi2.is_none());
    }

    // The whole point of carrying chi2 and dof: a filter that must not drop
    // sparse bands has something defined to cut on.
    #[test]
    fn test_chi2_is_defined_for_every_fit_including_two_point() {
        for n in 2..6 {
            let x: Vec<f32> = (0..n).map(|i| i as f32).collect();
            let y: Vec<f32> = (0..n).map(|i| 20.0 - 0.1 * i as f32).collect();
            let s = vec![0.1_f32; n];
            let r = fit(&x, &y, &s);
            assert!(r.chi2.is_finite(), "chi2 must be defined at n = {n}");
            assert_eq!(r.dof, (n as i32) - 2);
            assert_eq!(r.red_chi2.is_some(), n > 2);
        }
    }

    #[test]
    fn test_reduced_chi2_is_chi2_over_dof() {
        // Three points not on a line, so the fit leaves a residual.
        let r = fit(&[0.0, 1.0, 2.0], &[20.0, 19.0, 19.5], &[0.1, 0.1, 0.1]);
        assert_eq!(r.dof, 1);
        let expected = r.chi2 / r.dof as f32;
        assert!((r.red_chi2.unwrap() - expected).abs() < 1e-6);
    }

    // Documents the trap this change exists to make escapable: a range cut on
    // red_chi2 alone matches neither direction when the value is absent, so a
    // two-point band is silently rejected by an AND block.
    #[test]
    fn test_a_range_cut_on_reduced_chi2_alone_excludes_two_point_bands() {
        let sparse = fit(&[0.0, 1.0], &[20.0, 19.0], &[0.1, 0.1]);
        let clean = fit(&[0.0, 1.0, 2.0], &[20.0, 19.9, 19.8], &[0.1, 0.1, 0.1]);

        let passes_range_cut = |b: &BandRateProperties| b.red_chi2.is_some_and(|c| c <= 2.0);
        assert!(passes_range_cut(&clean));
        assert!(!passes_range_cut(&sparse), "this is the trap");

        // With chi2/dof the same intent is expressible without dropping it.
        let passes_with_dof = |b: &BandRateProperties| b.dof == 0 || b.chi2 <= 2.0 * b.dof as f32;
        assert!(passes_with_dof(&clean));
        assert!(passes_with_dof(&sparse));
    }
}

#[cfg(test)]
mod recent_window_tests {
    use super::*;

    fn point(time: f64, mag: f32, band: Band) -> PhotometryMag {
        PhotometryMag {
            time,
            mag,
            mag_err: 0.05,
            band,
        }
    }

    /// Years of flat history, then a genuine rise in the last few days. This is
    /// the case the unbounded fit gets wrong: it averages the real rise into a
    /// baseline stretching back years and reports a rate near zero.
    fn old_history_then_a_rise() -> Vec<PhotometryMag> {
        let mut lc = Vec::new();
        let mut t = 2_459_000.0;
        while t < 2_459_000.0 + 1200.0 {
            lc.push(point(t, 20.0, Band::G));
            t += 20.0;
        }
        let last = 2_459_000.0 + 1200.0;
        for (i, mag) in [19.5_f32, 19.0, 18.4, 17.9].iter().enumerate() {
            lc.push(point(last + 2.0 * i as f64, *mag, Band::G));
        }
        lc.sort_by(|a, b| a.time.partial_cmp(&b.time).unwrap());
        lc
    }

    #[test]
    fn test_recent_rate_is_steeper_than_the_whole_history_rate() {
        let (props, _, _) = analyze_photometry(&old_history_then_a_rise());
        let g = props.g.expect("g band");

        let whole = g.rising.expect("unbounded rising fit").rate;
        let recent = g
            .recent
            .as_ref()
            .expect("recent block")
            .rising
            .as_ref()
            .expect("recent rising fit")
            .rate;

        // Magnitudes fall as the object brightens, so a rise is a negative rate.
        assert!(
            recent < whole,
            "recent rate {recent} should be steeper than whole-history {whole}"
        );
        assert!(
            recent.abs() > 10.0 * whole.abs(),
            "the whole-history rate is diluted by the baseline: {whole} vs {recent}"
        );
    }

    #[test]
    fn test_recent_window_only_covers_the_configured_span() {
        let (props, _, _) = analyze_photometry(&old_history_then_a_rise());
        let recent = props.g.unwrap().recent.expect("recent block");
        assert_eq!(recent.window_days, RECENT_WINDOW_DAYS as f32);
        // 4 points in the burst plus the couple of baseline points inside 30 d.
        assert!(
            recent.nb_data >= 4 && recent.nb_data <= 8,
            "got {}",
            recent.nb_data
        );
    }

    /// Absence has to mean "no recent data", distinct from "recent data that
    /// cannot be fit" -- which is present with null fits.
    #[test]
    fn test_single_recent_point_reports_the_block_without_a_fit() {
        let lc = vec![
            point(2_459_000.0, 20.0, Band::G),
            point(2_459_400.0, 19.0, Band::G),
        ];
        let (props, _, _) = analyze_photometry(&lc);
        let recent = props
            .g
            .unwrap()
            .recent
            .expect("window holds the last point");
        assert_eq!(recent.nb_data, 1);
        assert!(recent.rising.is_none());
        assert!(recent.fading.is_none());
    }

    /// A band with nothing in the window gets no block at all, rather than a
    /// block full of nulls that reads as a failed fit.
    #[test]
    fn test_band_with_no_recent_detection_has_no_block() {
        let lc = vec![
            point(2_459_000.0, 20.0, Band::R),
            point(2_459_002.0, 19.5, Band::R),
            point(2_459_400.0, 19.0, Band::G),
        ];
        let (props, _, _) = analyze_photometry(&lc);
        assert!(
            props.r.unwrap().recent.is_none(),
            "r band last seen 400 d ago, well outside the window"
        );
        assert!(props.g.unwrap().recent.is_some());
    }

    /// The window is anchored to the newest detection in any band, so bands
    /// stay directly comparable.
    #[test]
    fn test_window_is_shared_across_bands() {
        let lc = vec![
            point(2_459_400.0, 19.0, Band::G),
            point(2_459_405.0, 18.5, Band::G),
            point(2_459_402.0, 19.2, Band::R),
            point(2_459_404.0, 18.9, Band::R),
        ];
        let (props, _, _) = analyze_photometry(&lc);
        let g = props.g.unwrap().recent.expect("g recent");
        let r = props.r.unwrap().recent.expect("r recent");
        assert_eq!(g.window_days, r.window_days);
        assert_eq!(g.nb_data, 2);
        assert_eq!(r.nb_data, 2);
    }
}

#[cfg(test)]
mod recent_avro_tests {
    use super::*;
    use crate::utils::derive_avro_schema::SerdavroWriter;
    use apache_avro::{AvroSchema, Writer};

    /// These reach Babamul through `append_serdavro`, which resolves every field
    /// the schema declares by name, so a nested optional that serializes as
    /// missing rather than null fails there while BSON stays happy. Cover both
    /// a populated and an absent `recent` block.
    #[test]
    fn test_recent_block_serializes_through_the_babamul_path() {
        let schema = PerBandProperties::get_schema();
        let band = weighted_least_squares_centered(
            &[0.0, 1.0, 2.0],
            &[20.0, 19.5, 19.0],
            &[0.05, 0.05, 0.05],
        );
        for (label, recent) in [
            ("recent absent", None),
            (
                "recent populated",
                Some(RecentProperties {
                    window_days: RECENT_WINDOW_DAYS as f32,
                    nb_data: 3,
                    peak_jd: 2_460_000.0,
                    peak_mag: 19.0,
                    rising: band.clone(),
                    fading: None,
                }),
            ),
        ] {
            let props = PerBandProperties {
                g: Some(BandProperties {
                    peak_jd: 2_460_000.0,
                    peak_mag: 19.0,
                    peak_mag_err: 0.05,
                    dt: 2.0,
                    rising: band.clone(),
                    fading: None,
                    recent,
                }),
                ..Default::default()
            };
            let mut writer = Writer::new(&schema, Vec::new());
            writer
                .append_serdavro(&props)
                .unwrap_or_else(|e| panic!("{label} failed to serialize to avro: {e}"));
        }
    }
}

#[cfg(test)]
mod rate_avro_tests {
    use super::*;
    use crate::utils::derive_avro_schema::SerdavroWriter;
    use apache_avro::{AvroSchema, Writer};

    fn fit(x: &[f32], y: &[f32], s: &[f32]) -> BandRateProperties {
        weighted_least_squares_centered(x, y, s).expect("fit")
    }

    /// `append_serdavro` resolves every field the schema declares by name, so a
    /// `None` that serializes as missing rather than null fails there while BSON
    /// stays happy. That is how `skip_serializing_none` on this struct broke the
    /// outgoing packet -- invisible to the plain unit tests, caught only by an
    /// integration test needing Mongo. Cover the undefined case directly.
    #[test]
    fn test_band_rate_properties_serialize_with_and_without_reduced_chi2() {
        let schema = PerBandProperties::get_schema();
        for (label, band) in [
            (
                "three points, red_chi2 defined",
                fit(&[0.0, 1.0, 2.0], &[20.0, 19.0, 19.5], &[0.1; 3]),
            ),
            (
                "two points, red_chi2 null",
                fit(&[0.0, 1.0], &[20.0, 19.0], &[0.1; 2]),
            ),
        ] {
            let props = PerBandProperties {
                g: Some(BandProperties {
                    peak_jd: 2_460_000.0,
                    peak_mag: 19.0,
                    peak_mag_err: 0.1,
                    dt: 1.0,
                    rising: Some(band.clone()),
                    fading: Some(band),
                    recent: None,
                }),
                ..Default::default()
            };
            let mut writer = Writer::new(&schema, Vec::new());
            writer
                .append_serdavro(&props)
                .unwrap_or_else(|e| panic!("{label} failed to serialize to avro: {e}"));
        }
    }
}
