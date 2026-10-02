//! FLARE light-curve classifier. Reads the alert photometry and crossmatches,
//! runs three ONNX boosters and writes the result under the alert's `flare`
//! field. Needs the `flare_classifier` feature and `config.flare.enabled`.
//! The ONNX files carry their input columns and calibration as metadata.

use std::collections::HashMap;
use std::sync::Mutex;

use applecider_flare::lightcurve::{Band as FlareBand, Detection};
use applecider_flare::model::{feature_map, vector_from_map, Head, Report};
use mongodb::bson::Document;
use ndarray::Array2;
use ort::{inputs, session::Session, value::TensorRef};
use serde::{Deserialize, Serialize};
use tracing::{info, trace, warn};

use super::{load_model, ModelError};
use crate::conf::FlareConfig;
use crate::utils::lightcurves::Band;

use crate::enrichment::ZtfAlertForEnrichment;

const TOP_MODEL: &str = "applecider_flare_top.onnx";
const BRANCH_MODEL: &str = "applecider_flare_slsn_branch.onnx";
const AD_SPACE_MODEL: &str = "applecider_flare_ad_space.onnx";

const CONTEXT_COLUMNS: [&str; 30] = [
    "host_sep",
    "host_r",
    "host_i",
    "host_gr",
    "host_ri",
    "host_iz",
    "host_ext",
    "n_ext_30",
    "near_sep",
    "near_ext",
    "M_pseudo",
    "gaia_sep",
    "parallax",
    "parallax_over_error",
    "pm",
    "pm_over_error",
    "gaia_g",
    "ruwe",
    "wise_sep",
    "w1",
    "w1w2",
    "w2w3",
    "pos_sep",
    "pos_g",
    "pos_r",
    "pos_i",
    "pos_gr",
    "pos_ri",
    "pos_star",
    "pos_ndet",
];

#[derive(thiserror::Error, Debug)]
pub enum FlareEnrichmentError {
    #[error("failed to load FLARE model {file}")]
    Load {
        file: String,
        #[source]
        source: ModelError,
    },
    #[error("FLARE model {file} has no `{key}` metadata")]
    Metadata { file: String, key: &'static str },
    #[error("invalid FLARE calibration in {file}: {source}")]
    Calibration {
        file: String,
        #[source]
        source: applecider_flare::FlareError,
    },
    #[error("error from ort")]
    Ort(#[from] ort::Error),
    #[error("json error")]
    Json(#[from] serde_json::Error),
}

/// FLARE output for one alert, stored under `flare` in the alert document.
#[derive(Debug, Clone, Serialize, Deserialize, utoipa::ToSchema)]
pub struct FlareClassification {
    pub p_sn_ia: f64,
    pub p_sn_cc: f64,
    pub p_slsn: f64,
    pub p_agn: f64,
    pub p_tde: f64,
    pub p_cv: f64,
    /// Most likely class after the SLSN branch threshold
    pub label: String,
    /// Conformal prediction set at `alpha`
    pub set: Vec<String>,
    pub alpha: f64,
    pub credibility: f64,
    pub confidence: f64,
    pub energy: f64,
    pub p_anomaly: f64,
    pub base_rate: f64,
    pub likelihood_ratio: f64,
    /// Fraction of known-class calibration objects at least this anomalous
    pub novelty_p: Option<f64>,
    /// True when the most likely class is not in its own prediction set
    pub argmax_excluded: bool,
    pub p_values: HashMap<String, f64>,
    pub energy_percentile: Option<f64>,
    pub coverage_measured: Option<f64>,
    pub coverage_n: Option<u64>,
    pub p_value_floor: Option<f64>,
    pub n_context_present: usize,
    pub model: String,
}

impl FlareClassification {
    fn from_report(r: Report, n_context_present: usize, model: &str) -> Self {
        FlareClassification {
            p_sn_ia: r.proba[0],
            p_sn_cc: r.proba[1],
            p_slsn: r.proba[2],
            p_agn: r.proba[3],
            p_tde: r.proba[4],
            p_cv: r.proba[5],
            label: r.label,
            set: r.set,
            alpha: r.alpha,
            credibility: r.credibility,
            confidence: r.confidence,
            energy: r.anomaly.energy,
            p_anomaly: r.anomaly.p_anomaly,
            base_rate: r.anomaly.base_rate,
            likelihood_ratio: r.anomaly.likelihood_ratio,
            novelty_p: r.anomaly.novelty_p,
            argmax_excluded: r.argmax_excluded,
            p_values: applecider_flare::model::SIX
                .iter()
                .zip(r.p_values.iter())
                .map(|(c, p)| (c.to_string(), *p))
                .collect(),
            energy_percentile: r.anomaly.energy_percentile,
            coverage_measured: r.coverage_measured,
            coverage_n: r.coverage_n,
            p_value_floor: r.p_value_floor,
            n_context_present,
            model: model.to_string(),
        }
    }
}

struct Booster {
    session: Mutex<Session>,
    feature_names: Vec<String>,
    file: String,
}

impl Booster {
    /// Loads the model and returns it with the custom metadata we care about.
    fn load(
        dir: &str,
        file: &str,
    ) -> Result<(Self, HashMap<&'static str, String>), FlareEnrichmentError> {
        let path = format!("{dir}/{file}");
        let session = load_model(&path).map_err(|source| FlareEnrichmentError::Load {
            file: file.into(),
            source,
        })?;
        let meta = session.metadata()?;
        let names = meta
            .custom("feature_names")
            .ok_or(FlareEnrichmentError::Metadata {
                file: file.into(),
                key: "feature_names",
            })?;
        let feature_names: Vec<String> = serde_json::from_str(&names)?;
        let mut metadata = HashMap::new();
        for key in [
            "model_card",
            "conformal",
            "anomaly_calibration",
            "flare_version",
        ] {
            if let Some(v) = meta.custom(key) {
                metadata.insert(key, v);
            }
        }
        drop(meta);
        Ok((
            Booster {
                session: Mutex::new(session),
                feature_names,
                file: file.into(),
            },
            metadata,
        ))
    }

    /// Missing columns are NaN, like in training.
    /// Also returns how many columns were present.
    fn raw(&self, f: &HashMap<String, f64>) -> Result<(Vec<f64>, usize), FlareEnrichmentError> {
        let x = vector_from_map(&self.feature_names, f);
        let n_present = x.iter().filter(|v| v.is_finite()).count();
        let arr = Array2::from_shape_vec((1, x.len()), x).expect("shape (1, n)");
        let mut session = self.session.lock().unwrap();
        let outputs = session.run(inputs! { "X" => TensorRef::from_array_view(&arr)? })?;
        let (_, raw) = outputs["raw"].try_extract_tensor::<f32>()?;
        Ok((raw.iter().map(|&v| v as f64).collect(), n_present))
    }
}

/// The three FLARE boosters and the calibration head. One per enrichment worker.
pub struct FlareEnricher {
    top: Booster,
    branch: Booster,
    ad_space: Booster,
    head: Head,
    base_rate: Option<f64>,
    model_name: String,
}

impl std::fmt::Debug for FlareEnricher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlareEnricher")
            .field("model", &self.model_name)
            .finish()
    }
}

fn required_metadata<'a>(
    meta: &'a HashMap<&'static str, String>,
    key: &'static str,
) -> Result<&'a String, FlareEnrichmentError> {
    meta.get(key).ok_or(FlareEnrichmentError::Metadata {
        file: TOP_MODEL.into(),
        key,
    })
}

impl FlareEnricher {
    /// Returns `None` when FLARE is disabled in the config.
    pub fn from_config(cfg: &FlareConfig) -> Result<Option<Self>, FlareEnrichmentError> {
        if !cfg.enabled {
            return Ok(None);
        }
        let (top, meta) = Booster::load(&cfg.model_dir, TOP_MODEL)?;
        let (branch, _) = Booster::load(&cfg.model_dir, BRANCH_MODEL)?;
        let (ad_space, _) = Booster::load(&cfg.model_dir, AD_SPACE_MODEL)?;
        let head = Head::from_metadata(
            required_metadata(&meta, "model_card")?,
            required_metadata(&meta, "conformal")?,
            required_metadata(&meta, "anomaly_calibration")?,
        )
        .map_err(|source| FlareEnrichmentError::Calibration {
            file: TOP_MODEL.into(),
            source,
        })?;
        let version = meta
            .get("flare_version")
            .cloned()
            .unwrap_or_else(|| "?".to_string());
        let model_name = format!("applecider_flare v{version}");
        info!(model_dir = %cfg.model_dir, model = %model_name, "loaded FLARE models");
        Ok(Some(FlareEnricher {
            top,
            branch,
            ad_space,
            head,
            base_rate: cfg.base_rate,
            model_name,
        }))
    }

    /// Classifies one alert. Returns `None` when the light curve does not pass
    /// the quality cut (fewer than 8 detections, or fewer than 2 in g or in r),
    /// which is the normal case for young alerts.
    pub fn classify(
        &self,
        alert: &ZtfAlertForEnrichment,
        peak_mag: Option<f64>,
    ) -> Option<FlareClassification> {
        let dets = detections(alert);
        let ctx = context(alert.cross_matches.as_ref(), peak_mag);
        let n_ctx = ctx.values().filter(|v| v.is_finite()).count();
        let features = match feature_map(&dets, &ctx) {
            Ok(f) => f,
            Err(applecider_flare::FlareError::QualityCut) => {
                trace!(
                    candid = alert.candid,
                    n = dets.len(),
                    "FLARE: below the quality cut"
                );
                return None;
            }
            Err(e) => {
                warn!(candid = alert.candid, "FLARE features failed: {e}");
                return None;
            }
        };
        match self.score(&features) {
            Ok(report) => Some(FlareClassification::from_report(
                report,
                n_ctx,
                &self.model_name,
            )),
            Err(e) => {
                warn!(candid = alert.candid, file = %self.top.file, "FLARE inference failed: {e}");
                None
            }
        }
    }

    fn score(&self, features: &HashMap<String, f64>) -> Result<Report, FlareEnrichmentError> {
        let (top, n_present) = self.top.raw(features)?;
        let (branch, _) = self.branch.raw(features)?;
        let (ad, _) = self.ad_space.raw(features)?;
        Ok(self
            .head
            .report(&top, branch[0], &ad, n_present, self.base_rate))
    }
}

fn flare_band(b: &Band) -> Option<FlareBand> {
    match b {
        Band::G => Some(FlareBand::G),
        Band::R => Some(FlareBand::R),
        Band::I => Some(FlareBand::I),
        _ => None,
    }
}

/// Alert detections up to the current candidate: the candidate itself plus the
/// `prv_candidates` that have a magnitude. Forced photometry is not used since
/// the model was trained on alert photometry only.
pub fn detections(alert: &ZtfAlertForEnrichment) -> Vec<Detection> {
    let cand = &alert.candidate.candidate;
    let mut out: Vec<Detection> = alert
        .prv_candidates
        .iter()
        .filter(|p| p.jd <= cand.jd)
        .filter_map(|p| {
            Some(Detection {
                mjd: p.jd - 2_400_000.5,
                band: flare_band(&p.band)?,
                mag: p.magpsf?,
                mag_err: p.sigmapsf?,
            })
        })
        .collect();
    if let Some(band) = FlareBand::from_fid(cand.fid as i64) {
        out.push(Detection {
            mjd: cand.jd - 2_400_000.5,
            band,
            mag: cand.magpsf as f64,
            mag_err: cand.sigmapsf as f64,
        });
    }
    out
}

/// NaN if missing or not a number.
fn get_f64(doc: &Document, key: &str) -> f64 {
    match doc.get(key) {
        Some(mongodb::bson::Bson::Double(v)) => *v,
        Some(mongodb::bson::Bson::Int32(v)) => *v as f64,
        Some(mongodb::bson::Bson::Int64(v)) => *v as f64,
        _ => f64::NAN,
    }
}

fn nearest<'a>(matches: Option<&'a Vec<Document>>) -> Option<&'a Document> {
    matches?
        .iter()
        .min_by(|a, b| get_f64(a, "distance_arcsec").total_cmp(&get_f64(b, "distance_arcsec")))
}

/// AB magnitude from Legacy Surveys nanomaggies
fn nmgy_to_mag(flux: f64) -> f64 {
    if flux > 0.0 {
        22.5 - 2.5 * flux.log10()
    } else {
        f64::NAN
    }
}

/// Luminosity distance in Mpc for a flat LCDM cosmology with H0 = 70 and Om = 0.3,
/// the same cosmology the FLARE training code uses.
pub fn lumdist_mpc(z: f64) -> f64 {
    if !(z > 0.0) {
        return f64::NAN;
    }
    let (h0, om) = (70.0, 0.3);
    let c = 299_792.458;
    let n = 2000;
    let dz = z / n as f64;
    let mut integral = 0.0;
    for k in 0..=n {
        let zz = k as f64 * dz;
        let e = (om * (1.0 + zz).powi(3) + (1.0 - om)).sqrt();
        let wgt = if k == 0 || k == n { 0.5 } else { 1.0 };
        integral += wgt / e;
    }
    (1.0 + z) * c / h0 * integral * dz
}

/// Legacy Surveys objects with a type other than PSF are extended. The photo-z
/// catalog has no `objtype`, in which case every match counts as a possible host.
fn is_extended(doc: &Document) -> bool {
    match doc.get_str("objtype") {
        Ok(t) => t.trim() != "PSF",
        Err(_) => true,
    }
}

/// Computes the 30 context columns from the alert's crossmatches (Gaia DR3,
/// AllWISE, Legacy Surveys DR10, PS1 DR2). Columns whose catalog is missing
/// are NaN. `host_ext` and `pos_ndet` are always NaN, BOOM does not have them.
pub fn context(
    xm: Option<&HashMap<String, Vec<Document>>>,
    peak_mag: Option<f64>,
) -> HashMap<String, f64> {
    let mut c: HashMap<String, f64> = CONTEXT_COLUMNS
        .into_iter()
        .map(|k| (k.to_string(), f64::NAN))
        .collect();
    let Some(xm) = xm else { return c };

    // Gaia DR3
    if let Some(g) = nearest(xm.get("Gaia_DR3")) {
        let (plx, plx_e) = (get_f64(g, "parallax"), get_f64(g, "parallax_error"));
        let pm = get_f64(g, "pmra").hypot(get_f64(g, "pmdec"));
        let pme = get_f64(g, "pmra_error").hypot(get_f64(g, "pmdec_error"));
        c.insert("gaia_sep".into(), get_f64(g, "distance_arcsec"));
        c.insert("parallax".into(), plx);
        c.insert(
            "parallax_over_error".into(),
            if plx_e > 0.0 { plx / plx_e } else { f64::NAN },
        );
        c.insert("pm".into(), pm);
        c.insert(
            "pm_over_error".into(),
            if pme > 0.0 { pm / pme } else { f64::NAN },
        );
        c.insert("gaia_g".into(), get_f64(g, "phot_g_mean_mag"));
        c.insert("ruwe".into(), get_f64(g, "ruwe"));
    }

    // AllWISE
    if let Some(w) = nearest(xm.get("AllWISE")) {
        let (w1, w2, w3) = (
            get_f64(w, "w1mpro"),
            get_f64(w, "w2mpro"),
            get_f64(w, "w3mpro"),
        );
        c.insert("wise_sep".into(), get_f64(w, "distance_arcsec"));
        c.insert("w1".into(), w1);
        c.insert("w1w2".into(), w1 - w2);
        c.insert("w2w3".into(), w2 - w3);
    }

    // Legacy Surveys DR10: host, photo-z and the WISE fallback
    let ls = xm
        .get("LSDR10")
        .or_else(|| xm.get("lsdr10"))
        .or_else(|| xm.get("LS_DR10_PHOTOZ"));
    if let Some(ls) = ls {
        if let Some(near) = nearest(Some(ls)) {
            c.insert("near_sep".into(), get_f64(near, "distance_arcsec"));
            c.insert("near_ext".into(), if is_extended(near) { 1.0 } else { 0.0 });
        }
        let mut hosts: Vec<&Document> = ls.iter().filter(|d| is_extended(d)).collect();
        hosts.sort_by(|a, b| {
            get_f64(a, "distance_arcsec").total_cmp(&get_f64(b, "distance_arcsec"))
        });
        c.insert("n_ext_30".into(), hosts.len() as f64);
        if let Some(h) = hosts.first() {
            let (mg, mr, mi, mz) = (
                nmgy_to_mag(get_f64(h, "flux_g")),
                nmgy_to_mag(get_f64(h, "flux_r")),
                nmgy_to_mag(get_f64(h, "flux_i")),
                nmgy_to_mag(get_f64(h, "flux_z")),
            );
            c.insert("host_sep".into(), get_f64(h, "distance_arcsec"));
            c.insert("host_r".into(), mr);
            c.insert("host_i".into(), mi);
            c.insert("host_gr".into(), mg - mr);
            c.insert("host_ri".into(), mr - mi);
            c.insert("host_iz".into(), mi - mz);
            // no AllWISE match: use the host's WISE forced photometry, AB to Vega
            if c["w1"].is_nan() {
                let (w1, w2, w3) = (
                    nmgy_to_mag(get_f64(h, "flux_w1")) - 2.699,
                    nmgy_to_mag(get_f64(h, "flux_w2")) - 3.339,
                    nmgy_to_mag(get_f64(h, "flux_w3")) - 5.174,
                );
                c.insert("wise_sep".into(), get_f64(h, "distance_arcsec"));
                c.insert("w1".into(), w1);
                c.insert("w1w2".into(), w1 - w2);
                c.insert("w2w3".into(), w2 - w3);
            }
            let z = {
                let zm = get_f64(h, "z_phot_median");
                if zm.is_finite() {
                    zm
                } else {
                    get_f64(h, "z_phot")
                }
            };
            if let Some(mpk) = peak_mag {
                let d = lumdist_mpc(z);
                if d.is_finite() {
                    c.insert("M_pseudo".into(), mpk - 5.0 * (d * 1e5).log10());
                }
            }
        }
    }

    // PS1 DR2
    if let Some(p) = nearest(xm.get("PS1_DR2")) {
        // PS1 uses -999 for missing values
        let clean = |m: f64| if m > -900.0 { m } else { f64::NAN };
        let (g, r, i) = (
            clean(get_f64(p, "gMeanPSFMag")),
            clean(get_f64(p, "rMeanPSFMag")),
            clean(get_f64(p, "iMeanPSFMag")),
        );
        c.insert("pos_sep".into(), get_f64(p, "distance_arcsec"));
        c.insert("pos_g".into(), g);
        c.insert("pos_r".into(), r);
        c.insert("pos_i".into(), i);
        c.insert("pos_gr".into(), g - r);
        c.insert("pos_ri".into(), r - i);
        let ps = get_f64(p, "strm_prob_star");
        c.insert(
            "pos_star".into(),
            if ps.is_finite() {
                (ps > 0.5) as i32 as f64
            } else {
                f64::NAN
            },
        );
    }
    c
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::doc;

    #[test]
    fn lumdist_matches_astropy() {
        // astropy FlatLambdaCDM(H0=70, Om0=0.3).luminosity_distance(0.1) = 460.3 Mpc
        assert!((lumdist_mpc(0.1) - 460.3).abs() < 1.0);
        assert!(lumdist_mpc(0.0).is_nan());
    }

    #[test]
    fn context_from_crossmatches() {
        let mut xm = HashMap::new();
        xm.insert(
            "Gaia_DR3".to_string(),
            vec![
                doc! {"distance_arcsec": 0.4, "parallax": 2.0, "parallax_error": 0.5,
                "pmra": 3.0, "pmdec": 4.0, "pmra_error": 1.0, "pmdec_error": 0.0,
                "phot_g_mean_mag": 17.5, "ruwe": 1.1},
            ],
        );
        xm.insert(
            "LSDR10".to_string(),
            vec![
                doc! {"distance_arcsec": 1.0, "objtype": "PSF", "flux_r": 100.0},
                doc! {"distance_arcsec": 5.0, "objtype": "DEV", "flux_g": 50.0, "flux_r": 100.0,
                "flux_i": 150.0, "flux_z": 200.0, "flux_w1": 30.0, "flux_w2": 20.0, "flux_w3": 10.0,
                "z_phot_median": 0.1},
            ],
        );
        xm.insert(
            "AllWISE".to_string(),
            vec![doc! {"distance_arcsec": 1.2, "w1mpro": 15.0, "w2mpro": 14.5, "w3mpro": 12.0}],
        );
        xm.insert(
            "PS1_DR2".to_string(),
            vec![
                doc! {"distance_arcsec": 0.3, "gMeanPSFMag": 20.0, "rMeanPSFMag": 19.5,
                "iMeanPSFMag": -999.0, "strm_prob_star": 0.9},
            ],
        );
        let c = context(Some(&xm), Some(18.0));
        assert_eq!(c["gaia_sep"], 0.4);
        assert_eq!(c["parallax_over_error"], 4.0);
        assert_eq!(c["pm"], 5.0);
        assert_eq!(c["pm_over_error"], 5.0);
        assert_eq!(c["near_sep"], 1.0);
        assert_eq!(c["near_ext"], 0.0);
        assert_eq!(c["n_ext_30"], 1.0);
        assert_eq!(c["host_sep"], 5.0);
        // 22.5 - 2.5 log10(100)
        assert!((c["host_r"] - 17.5).abs() < 1e-9);
        assert!((c["host_gr"] - 0.7526).abs() < 1e-3);
        assert!((c["M_pseudo"] - (18.0 - 5.0 * (460.3e5f64).log10())).abs() < 0.01);
        // AllWISE takes precedence over the LS forced photometry
        assert_eq!(c["wise_sep"], 1.2);
        assert!((c["w1w2"] - 0.5).abs() < 1e-12);
        assert!((c["w2w3"] - 2.5).abs() < 1e-12);
        assert_eq!(c["pos_star"], 1.0);
        assert!(c["pos_i"].is_nan() && c["pos_ri"].is_nan());
        assert!(c["host_ext"].is_nan() && c["pos_ndet"].is_nan());
        assert_eq!(c.len(), 30);
    }

    #[test]
    fn wise_falls_back_to_ls_forced_photometry() {
        let mut xm = HashMap::new();
        xm.insert(
            "lsdr10".to_string(),
            vec![
                doc! {"distance_arcsec": 2.0, "objtype": "REX", "flux_r": 100.0,
                "flux_w1": 100.0, "flux_w2": 100.0, "flux_w3": 100.0},
            ],
        );
        let c = context(Some(&xm), None);
        assert!((c["w1"] - (17.5 - 2.699)).abs() < 1e-9);
        assert!((c["w1w2"] - (3.339 - 2.699)).abs() < 1e-9);
        // no peak magnitude, so no M_pseudo
        assert!(c["M_pseudo"].is_nan());
    }

    #[test]
    fn empty_context_is_all_nan() {
        let c = context(None, None);
        assert_eq!(c.len(), 30);
        assert!(c.values().all(|v| v.is_nan()));
    }

    // Runs the shipped ONNX files, so it needs libonnxruntime (ORT_DYLIB_PATH on Linux)
    #[test]
    #[ignore = "needs libonnxruntime and data/models/applecider_flare"]
    fn onnx_models_load_and_score() {
        let cfg = FlareConfig {
            enabled: true,
            model_dir: "data/models/applecider_flare".into(),
            base_rate: None,
        };
        let e = FlareEnricher::from_config(&cfg).unwrap().unwrap();
        let mut f = HashMap::new();
        for n in &e.top.feature_names {
            f.insert(n.clone(), 0.5);
        }
        let (top, n) = e.top.raw(&f).unwrap();
        assert_eq!(top.len(), 5);
        assert_eq!(n, e.top.feature_names.len());
        let (branch, _) = e.branch.raw(&f).unwrap();
        let (ad, _) = e.ad_space.raw(&f).unwrap();
        let r = e.head.report(&top, branch[0], &ad, n, None);
        assert!((r.proba.iter().sum::<f64>() - 1.0).abs() < 1e-9);
    }
}
