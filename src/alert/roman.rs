//! Roman / RAPID alert ingestion.
//!
//! RAPID (the Roman Alerts Production and Investigation of Discoveries pipeline)
//! publishes difference-image alerts whose schema closely follows Rubin's:
//! a triggering `diaSource`, the object's previous `diaSource`s, optional forced
//! photometry, an object-level `diaObject` summary, and three FITS cutouts.
//!
//! Differences from LSST that matter here:
//!   - times are UTC MJD (not TAI), so `jd = mjd + 2400000.5`
//!   - the packets are Avro object container files with an embedded schema
//!     (like ZTF/DECam), not Confluent-framed payloads with a schema registry
//!   - aperture photometry, science/reference forced photometry and
//!     `diffimglimmag` are schema stubs that are currently always null, so the
//!     candidate is built from PSF photometry alone
//!   - the sign of the detection is carried by `isNegative` rather than by the
//!     sign of `psfFlux` (RAPID reports the absolute flux)
//!   - solar-system associations arrive as an `ssMatches` array of designations
//!     rather than as an `ssObjectId`

use std::collections::HashMap;

use crate::{
    alert::{
        base::{
            AlertError, AlertWorker, AlertWorkerError, LightcurveJdOnly, ProcessAlertStatus,
            SchemaCache,
        },
        lsst, ztf, TimeSeries,
    },
    conf::{self, AppConfig},
    utils::{
        cutouts::CutoutStorage,
        db::{mongify_vec, update_timeseries_op},
        enums::Survey,
        lightcurves::{flux2mag, fluxerr2diffmaglim, Band, ROMAN_ZP_AB_NJY, SNT},
        o11y::logging::as_error,
        spatial::{xmatch, Coordinates},
    },
};
use apache_avro_derive::AvroSchema;
use apache_avro_macros::serdavro;
use constcat::concat;
use flare::Time;
use mongodb::bson::{doc, Document};
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use tracing::{debug, error, instrument, warn};
use utoipa::ToSchema;

pub const STREAM_NAME: &str = "ROMAN";
/// Roman observes from Sun-Earth L2, so unlike ground-based surveys it is not
/// declination limited; the RAPID time-domain surveys just target specific
/// fields.
pub const ROMAN_DEC_RANGE: (f64, f64) = (-90.0, 90.0);
/// One WFI pixel (0.11 arcsec/pixel). The per-source centroid errors in the
/// packets are a few hundredths of a pixel, so a pixel is a conservative
/// cross-match radius against other surveys.
pub const ROMAN_POSITION_UNCERTAINTY: f64 = 0.11; // arcsec
pub const ALERT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts");
pub const ALERT_AUX_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_aux");

pub const ROMAN_ZTF_XMATCH_RADIUS: f64 =
    (ROMAN_POSITION_UNCERTAINTY.max(ztf::ZTF_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();
pub const ROMAN_LSST_XMATCH_RADIUS: f64 =
    (ROMAN_POSITION_UNCERTAINTY.max(lsst::LSST_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();

/// A single-epoch detection on a Roman difference image.
#[serde_as]
#[skip_serializing_none]
#[serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Default, ToSchema)]
#[serde(default)]
pub struct RomanDiaSource {
    /// Unique identifier for this source detection.
    #[serde(rename = "diaSourceId", alias = "candid")]
    pub candid: i64,
    /// RAPID-assigned exposure identifier (a pipeline database serial, not a
    /// Roman SOC identifier; see `observation_id`).
    #[serde(rename = "expId")]
    pub exp_id: i64,
    /// Detector (SCA) number.
    pub detector: i32,
    /// Associated diaObject identifier.
    #[serde(rename = "diaObjectId")]
    #[serde(deserialize_with = "deserialize_optional_id")]
    pub dia_object_id: Option<i64>,
    /// Effective mid-observation time (UTC scale) [MJD].
    #[serde(rename = "midpointMjd")]
    pub midpoint_mjd: f64,
    /// Exposure time [s].
    #[serde(rename = "exposureTime")]
    pub exposure_time: Option<f32>,
    /// Right ascension; ICRS [deg].
    pub ra: f64,
    /// Declination; ICRS [deg].
    pub dec: f64,
    /// x-pixel position on detector [pixels].
    pub x: f32,
    /// y-pixel position on detector [pixels].
    pub y: f32,
    /// Uncertainty in x [pixels].
    #[serde(rename = "xErr")]
    pub x_err: Option<f32>,
    /// Uncertainty in y [pixels].
    #[serde(rename = "yErr")]
    pub y_err: Option<f32>,
    /// Uncertainty in ra [deg].
    #[serde(rename = "raErr")]
    pub ra_err: Option<f32>,
    /// Uncertainty in dec [deg].
    #[serde(rename = "decErr")]
    pub dec_err: Option<f32>,
    /// Filter band this source was observed with.
    pub band: Option<Band>,
    /// Flux from PSF-fit on difference image [nJy]. RAPID reports the absolute
    /// value; the sign of the detection is in `is_negative`.
    #[serde(rename = "psfFlux")]
    pub psf_flux: Option<f32>,
    /// Uncertainty in psfFlux [nJy].
    #[serde(rename = "psfFluxErr")]
    pub psf_flux_err: Option<f32>,
    /// Signal-to-noise ratio (psfFlux / psfFluxErr).
    pub snr: Option<f32>,
    /// Source was detected as significantly negative.
    #[serde(rename = "isNegative")]
    pub is_negative: bool,
    /// Aperture flux on difference image (schema stub) [nJy].
    #[serde(rename = "apFlux")]
    pub ap_flux: Option<f32>,
    /// Uncertainty in apFlux (schema stub) [nJy].
    #[serde(rename = "apFluxErr")]
    pub ap_flux_err: Option<f32>,
    /// Forced PSF flux on science image (schema stub) [nJy].
    #[serde(rename = "scienceFlux")]
    pub science_flux: Option<f32>,
    /// Uncertainty in scienceFlux (schema stub) [nJy].
    #[serde(rename = "scienceFluxErr")]
    pub science_flux_err: Option<f32>,
    /// Forced PSF flux on reference image (schema stub) [nJy].
    #[serde(rename = "refFlux")]
    pub ref_flux: Option<f32>,
    /// Uncertainty in refFlux (schema stub) [nJy].
    #[serde(rename = "refFluxErr")]
    pub ref_flux_err: Option<f32>,
    /// Expected 5-sigma limiting magnitude of the difference image (schema
    /// stub) [mag].
    pub diffimglimmag: Option<f32>,
    /// PSF-fit quality metric.
    #[serde(rename = "psfQfit")]
    pub psf_qfit: Option<f32>,
    /// PSF-fit contamination metric.
    #[serde(rename = "psfCfit")]
    pub psf_cfit: Option<f32>,
    /// Reduced chi^2 of the PSF fit.
    #[serde(rename = "psfRChi2")]
    pub psf_rchi2: Option<f32>,
    /// Number of data points (pixels) used in the PSF fit.
    #[serde(rename = "psfNdata")]
    pub psf_ndata: Option<i32>,
    /// Source sharpness statistic.
    pub sharpness: Option<f32>,
    /// Source roundness statistic (first moment ratio).
    pub roundness1: Option<f32>,
    /// Source roundness statistic (second moment ratio).
    pub roundness2: Option<f32>,
    /// Peak pixel value in the difference image footprint.
    pub peak: Option<f32>,
    /// Measure of extendedness (schema stub).
    pub extendedness: Option<f32>,
    /// Reliability score from the real-bogus ML classifier.
    pub reliability: Option<f32>,
    /// Version of the reliability classifier.
    #[serde(rename = "reliabilityVersion")]
    pub reliability_version: Option<String>,
    /// Second moment along x.
    pub ixx: Option<f32>,
    /// Second moment along y.
    pub iyy: Option<f32>,
    /// Cross second moment.
    pub ixy: Option<f32>,
    /// Uncertainty in ixx.
    #[serde(rename = "ixxErr")]
    pub ixx_err: Option<f32>,
    /// Uncertainty in iyy.
    #[serde(rename = "iyyErr")]
    pub iyy_err: Option<f32>,
    /// Uncertainty in ixy.
    #[serde(rename = "ixyErr")]
    pub ixy_err: Option<f32>,
    /// Source elongation.
    pub elong: Option<f32>,
    /// Bitmask of PSF-fit failure flags.
    #[serde(rename = "psfFitFlags")]
    pub psf_fit_flags: i64,
    /// Saturated pixel in the source footprint.
    #[serde(rename = "pixelFlags_saturated")]
    pub pixel_flags_saturated: Option<bool>,
    /// Bad pixel in the source footprint.
    #[serde(rename = "pixelFlags_bad")]
    pub pixel_flags_bad: Option<bool>,
    /// Source footprint touches the edge of the usable region.
    #[serde(rename = "pixelFlags_edge")]
    pub pixel_flags_edge: Option<bool>,
    /// Cosmic ray in the source footprint.
    #[serde(rename = "pixelFlags_cr")]
    pub pixel_flags_cr: Option<bool>,
    /// Centroid measurement failed.
    pub centroid_flag: Option<bool>,
    /// Aperture flux measurement failed (schema stub).
    #[serde(rename = "apFlux_flag")]
    pub ap_flux_flag: Option<bool>,
    /// PSF flux measurement failed.
    #[serde(rename = "psfFlux_flag")]
    pub psf_flux_flag: Option<bool>,
    /// Science flux measurement failed (schema stub).
    #[serde(rename = "scienceFlux_flag")]
    pub science_flux_flag: Option<bool>,
    /// Reference flux measurement failed (schema stub).
    #[serde(rename = "refFlux_flag")]
    pub ref_flux_flag: Option<bool>,
    /// Source is a candidate solar-system object.
    #[serde(rename = "isSSCandidate")]
    pub is_ss_candidate: Option<bool>,
    /// RAPID field identifier.
    pub field: i32,
    /// HEALPix index at order 6.
    pub hp6: i32,
    /// HEALPix index at order 9.
    pub hp9: i32,
    /// RAPID processing identifier.
    pub pid: i64,
    /// Roman SOC observation identifier.
    pub observation_id: Option<String>,
    /// Roman observing program number.
    pub program: Option<i32>,
    /// Observing plan number.
    pub plan: Option<i32>,
    /// Pass number within the plan.
    pub pass: Option<i32>,
    /// Segment number within the pass.
    pub segment: Option<i32>,
    /// Observation number within the segment.
    pub observation: Option<i32>,
    /// Visit number within the observation.
    pub visit: Option<i32>,
    /// Exposure number within the visit.
    pub exposure: Option<i32>,
    /// Name of the RAPID survey that produced this source.
    pub survey: Option<String>,
}

impl RomanDiaSource {
    /// `objectId` used throughout the pipeline: the stringified diaObjectId.
    fn object_id(&self) -> Result<String, AlertError> {
        match self.dia_object_id {
            Some(id) => Ok(id.to_string()),
            None => Err(AlertError::MissingObjectId),
        }
    }

    /// PSF magnitude, its uncertainty, the limiting magnitude and the SNR,
    /// derived from the nJy difference-image PSF flux.
    fn psf_photometry(&self) -> Result<(f32, f32, f32, f32), AlertError> {
        let psf_flux = self.psf_flux.ok_or(AlertError::MissingFluxPSF)?.abs();
        let psf_flux_err = self.psf_flux_err.ok_or(AlertError::MissingFluxPSFError)?;

        let (magpsf, sigmapsf) = flux2mag(psf_flux, psf_flux_err, ROMAN_ZP_AB_NJY);
        // `diffimglimmag` is a schema stub today, so fall back to the flux error.
        let diffmaglim = self
            .diffimglimmag
            .unwrap_or_else(|| fluxerr2diffmaglim(psf_flux_err, ROMAN_ZP_AB_NJY));
        // RAPID already provides snr = psfFlux / psfFluxErr; recompute it when absent.
        let snr_psf = self.snr.unwrap_or(psf_flux / psf_flux_err);

        Ok((magpsf, sigmapsf, diffmaglim, snr_psf))
    }
}

/// A Roman detection with the magnitudes derived from its nJy PSF flux.
///
/// This covers both the triggering detection and the historical ones: only the
/// triggering detection's packet carries a `diaObject` summary, so
/// `jdstarthist`/`ndethist` are `None` on historical points. (Unlike the other
/// surveys there is no separate `PrvCandidate` type: one type keeps the stored
/// documents and the published filter schema exactly in step.)
#[serde_as]
#[skip_serializing_none]
#[serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, ToSchema)]
pub struct RomanCandidate {
    #[serde(flatten)]
    pub dia_source: RomanDiaSource,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub jd: f64,
    pub magpsf: f32,
    pub sigmapsf: f32,
    pub snr_psf: f32,
    /// Reduced chi^2 of the PSF fit, named to match the equivalent ZTF/LSST
    /// filtering field.
    pub chipsf: Option<f32>,
    pub diffmaglim: f32,
    pub isdiffpos: bool,
    pub jdstarthist: Option<f64>,
    pub ndethist: Option<i32>,
}

impl RomanCandidate {
    fn new(
        dia_source: RomanDiaSource,
        dia_object: Option<RomanDiaObject>,
    ) -> Result<Self, AlertError> {
        let object_id = dia_source.object_id()?;
        let (magpsf, sigmapsf, diffmaglim, snr_psf) = dia_source.psf_photometry()?;

        let (jdstarthist, ndethist) = match dia_object {
            Some(obj) => (
                obj.first_dia_source_mjd.map(mjd_to_jd),
                Some(obj.n_dia_sources),
            ),
            None => (None, None),
        };

        Ok(RomanCandidate {
            jd: mjd_to_jd(dia_source.midpoint_mjd),
            object_id,
            magpsf,
            sigmapsf,
            snr_psf,
            chipsf: dia_source.psf_rchi2,
            diffmaglim,
            isdiffpos: !dia_source.is_negative,
            jdstarthist,
            ndethist,
            dia_source,
        })
    }
}

impl TryFrom<RomanDiaSource> for RomanCandidate {
    type Error = AlertError;
    fn try_from(dia_source: RomanDiaSource) -> Result<Self, Self::Error> {
        RomanCandidate::new(dia_source, None)
    }
}

impl TimeSeries for RomanCandidate {
    fn time(&self) -> f64 {
        self.jd
    }
}

/// Object-level summary of a Roman DIA object.
///
/// The packet also carries per-band PSF flux summary statistics
/// (`<band>PsfFluxMean` and friends). Those are not modelled here: the object
/// record is only used to seed `jdstarthist`/`ndethist` on the candidate and is
/// not persisted, and the equivalent statistics are recomputed from the stored
/// lightcurve during enrichment.
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, ToSchema)]
#[serde(default)]
pub struct RomanDiaObject {
    /// Unique identifier for this object.
    #[serde(rename = "diaObjectId")]
    pub dia_object_id: i64,
    /// First measured right ascension of the object centroid; ICRS [deg].
    pub ra0: f64,
    /// First measured declination of the object centroid; ICRS [deg].
    pub dec0: f64,
    /// Mean right ascension of the object centroid; ICRS [deg].
    #[serde(rename = "meanRa")]
    pub mean_ra: Option<f64>,
    /// Mean declination of the object centroid; ICRS [deg].
    #[serde(rename = "meanDec")]
    pub mean_dec: Option<f64>,
    /// Uncertainty in ra [deg].
    #[serde(rename = "raErr")]
    pub ra_err: Option<f32>,
    /// Uncertainty in dec [deg].
    #[serde(rename = "decErr")]
    pub dec_err: Option<f32>,
    /// Total number of associated diaSources.
    #[serde(rename = "nDiaSources")]
    pub n_dia_sources: i32,
    /// MJD of the earliest associated diaSource (UTC scale).
    #[serde(rename = "firstDiaSourceMjd")]
    pub first_dia_source_mjd: Option<f64>,
    /// MJD of the latest associated diaSource (UTC scale).
    #[serde(rename = "lastDiaSourceMjd")]
    pub last_dia_source_mjd: Option<f64>,
    /// Start of the validity interval for this object summary (UTC scale).
    #[serde(rename = "validityStartMjd")]
    pub validity_start_mjd: f64,
    /// Number of times the object position fell on an observed image (stub).
    pub ncovhist: Option<i32>,
    /// MJD of the earliest exposure in the reference image (stub).
    #[serde(rename = "firstRefMjd")]
    pub first_ref_mjd: Option<f64>,
    /// MJD of the latest exposure in the reference image (stub).
    #[serde(rename = "lastRefMjd")]
    pub last_ref_mjd: Option<f64>,
}

impl Default for RomanDiaObject {
    fn default() -> Self {
        RomanDiaObject {
            dia_object_id: 0,
            ra0: 0.0,
            dec0: 0.0,
            mean_ra: None,
            mean_dec: None,
            ra_err: None,
            dec_err: None,
            n_dia_sources: 0,
            first_dia_source_mjd: None,
            last_dia_source_mjd: None,
            validity_start_mjd: 0.0,
            ncovhist: None,
            first_ref_mjd: None,
            last_ref_mjd: None,
        }
    }
}

/// Forced photometry at the object position on a difference image.
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Default, AvroSchema, ToSchema)]
#[serde(default)]
pub struct RomanDiaForcedSource {
    #[serde(rename = "diaForcedSourceId")]
    pub dia_forced_source_id: i64,
    #[serde(rename = "diaObjectId")]
    pub dia_object_id: i64,
    #[serde(rename = "expId")]
    pub exp_id: i64,
    pub detector: i32,
    pub ra: f64,
    pub dec: f64,
    pub band: Option<Band>,
    #[serde(rename = "psfFlux")]
    pub psf_flux: Option<f32>,
    #[serde(rename = "psfFluxErr")]
    pub psf_flux_err: Option<f32>,
    #[serde(rename = "scienceFlux")]
    pub science_flux: Option<f32>,
    #[serde(rename = "scienceFluxErr")]
    pub science_flux_err: Option<f32>,
    /// Effective mid-observation time (UTC scale) [MJD].
    #[serde(rename = "midpointMjd")]
    pub midpoint_mjd: f64,
    /// Time the forced photometry was produced (UTC scale) [MJD].
    #[serde(rename = "timeProcessedMjd")]
    pub time_processed_mjd: f64,
}

#[serde_as]
#[skip_serializing_none]
#[serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, ToSchema)]
pub struct RomanForcedPhot {
    #[serde(flatten)]
    pub dia_forced_source: RomanDiaForcedSource,
    pub jd: f64,
    pub magpsf: Option<f32>,
    pub sigmapsf: Option<f32>,
    pub diffmaglim: f32,
    pub isdiffpos: Option<bool>,
    pub snr_psf: Option<f32>,
}

impl TryFrom<RomanDiaForcedSource> for RomanForcedPhot {
    type Error = AlertError;
    fn try_from(dia_forced_source: RomanDiaForcedSource) -> Result<Self, Self::Error> {
        let psf_flux_err = dia_forced_source
            .psf_flux_err
            .ok_or(AlertError::MissingFluxPSFError)?;

        // Only points above the SNR threshold count as detections. Forced
        // photometry keeps the sign of the flux (there is no `isNegative` here).
        let (magpsf, sigmapsf, isdiffpos, snr_psf) = match dia_forced_source.psf_flux {
            Some(psf_flux) => {
                let psf_flux_abs = psf_flux.abs();
                let snr_psf = psf_flux_abs / psf_flux_err;
                if snr_psf > SNT {
                    let (magpsf, sigmapsf) = flux2mag(psf_flux_abs, psf_flux_err, ROMAN_ZP_AB_NJY);
                    (
                        Some(magpsf),
                        Some(sigmapsf),
                        Some(psf_flux > 0.0),
                        Some(snr_psf),
                    )
                } else {
                    (None, None, None, None)
                }
            }
            None => (None, None, None, None),
        };

        Ok(RomanForcedPhot {
            jd: mjd_to_jd(dia_forced_source.midpoint_mjd),
            magpsf,
            sigmapsf,
            diffmaglim: fluxerr2diffmaglim(psf_flux_err, ROMAN_ZP_AB_NJY),
            isdiffpos,
            snr_psf,
            dia_forced_source,
        })
    }
}

impl TimeSeries for RomanForcedPhot {
    fn time(&self) -> f64 {
        self.jd
    }
}

/// A known solar-system object matching the source position.
#[serde_as]
#[skip_serializing_none]
#[serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, ToSchema)]
pub struct RomanSsMatch {
    /// MPC designation of the matching object.
    pub designation: String,
    /// Predicted right ascension of the object; ICRS [deg].
    pub ra: f64,
    /// Predicted declination of the object; ICRS [deg].
    pub dec: f64,
    /// Separation between the source and the predicted position [arcsec].
    pub sep: f32,
    /// Position angle of the source relative to the predicted position [deg].
    pub pa: f32,
    /// Predicted V magnitude of the object.
    #[serde(rename = "predVMag")]
    pub pred_v_mag: Option<f32>,
}

/// RAPID Avro alert schema v00.02.
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct RomanRawAvroAlert {
    #[serde(rename(deserialize = "schemaVersion"))]
    pub schema_version: Option<String>,
    #[serde(rename(deserialize = "pipelineVersion"))]
    pub pipeline_version: Option<String>,
    #[serde(rename(deserialize = "diaSourceId"))]
    pub candid: i64,
    #[serde(rename(deserialize = "diaSource"))]
    pub dia_source: RomanDiaSource,
    #[serde(rename = "prvDiaSources")]
    #[serde(deserialize_with = "deserialize_prv_candidates")]
    pub prv_candidates: Option<Vec<RomanCandidate>>,
    #[serde(rename = "prvDiaForcedSources")]
    #[serde(deserialize_with = "deserialize_prv_forced_sources")]
    pub fp_hists: Option<Vec<RomanForcedPhot>>,
    #[serde(rename = "diaObject")]
    pub dia_object: Option<RomanDiaObject>,
    #[serde(rename = "ssMatches")]
    pub ss_matches: Option<Vec<RomanSsMatch>>,
    #[serde(rename = "cutoutDifference")]
    #[serde(deserialize_with = "deserialize_cutout")]
    pub cutout_difference: Vec<u8>,
    #[serde(rename = "cutoutScience")]
    #[serde(deserialize_with = "deserialize_cutout")]
    pub cutout_science: Vec<u8>,
    #[serde(rename = "cutoutReference")]
    #[serde(deserialize_with = "deserialize_cutout")]
    pub cutout_reference: Vec<u8>,
    pub observation_reason: Option<String>,
    pub target_name: Option<String>,
}

/// Roman times are UTC MJD, so the conversion to JD is a plain offset (no
/// TAI/UTC leap-second handling, unlike LSST).
fn mjd_to_jd(mjd: f64) -> f64 {
    mjd + 2400000.5
}

fn deserialize_optional_id<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    match <Option<i64> as Deserialize>::deserialize(deserializer)? {
        Some(0) | None => Ok(None),
        Some(id) => Ok(Some(id)),
    }
}

/// Cutouts are nullable in the schema, but an alert without them cannot be
/// served downstream, so treat a missing cutout as a decode failure.
fn deserialize_cutout<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let cutout: Option<Vec<u8>> = apache_avro::serde_avro_bytes_opt::deserialize(deserializer)?;
    match cutout {
        None => Err(serde::de::Error::custom("Missing cutout data")),
        Some(cutout) => Ok(cutout),
    }
}

fn deserialize_prv_candidates<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<RomanCandidate>>, D::Error>
where
    D: Deserializer<'de>,
{
    let dia_sources = <Option<Vec<RomanDiaSource>> as Deserialize>::deserialize(deserializer)?;
    match dia_sources {
        None => Ok(None),
        Some(dia_sources) => {
            let candidates = dia_sources
                .into_iter()
                .map(RomanCandidate::try_from)
                .collect::<Result<Vec<RomanCandidate>, AlertError>>()
                .map_err(serde::de::Error::custom)?;
            Ok(Some(candidates))
        }
    }
}

fn deserialize_prv_forced_sources<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<RomanForcedPhot>>, D::Error>
where
    D: Deserializer<'de>,
{
    let forced_sources =
        <Option<Vec<RomanDiaForcedSource>> as Deserialize>::deserialize(deserializer)?;
    match forced_sources {
        None => Ok(None),
        Some(forced_sources) => {
            let forced_phots = forced_sources
                .into_iter()
                .map(RomanForcedPhot::try_from)
                .collect::<Result<Vec<RomanForcedPhot>, AlertError>>()
                .map_err(serde::de::Error::custom)?;
            Ok(Some(forced_phots))
        }
    }
}

#[serdavro]
#[derive(Debug, Deserialize, Serialize, ToSchema, Default)]
pub struct RomanAliases {
    #[serde(rename = "ZTF")]
    pub ztf: Vec<String>,
    #[serde(rename = "LSST")]
    pub lsst: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct RomanObject {
    #[serde(rename = "_id")]
    pub object_id: String,
    pub prv_candidates: Vec<RomanCandidate>,
    pub fp_hists: Vec<RomanForcedPhot>,
    pub is_sso: bool,
    pub cross_matches: Option<HashMap<String, Vec<Document>>>,
    pub aliases: Option<RomanAliases>,
    pub coordinates: Coordinates,
    pub created_at: f64,
    pub updated_at: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct RomanAlert {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: RomanCandidate,
    /// Known solar-system objects matching this source position, if any.
    pub ss_matches: Vec<RomanSsMatch>,
    pub coordinates: Coordinates,
    pub created_at: f64,
    pub updated_at: f64,
}

#[derive(Deserialize, Serialize)]
struct AlertAuxForUpdate {
    #[serde(default)]
    pub prv_candidates: Vec<LightcurveJdOnly>,
    #[serde(default)]
    pub fp_hists: Vec<LightcurveJdOnly>,
    pub version: Option<i32>,
}

pub struct RomanAlertWorker {
    xmatch_configs: Vec<conf::CatalogXmatchConfig>,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<RomanAlert>,
    alert_aux_collection: mongodb::Collection<RomanObject>,
    alert_cutout_storage: CutoutStorage,
    alert_aux_collection_update: mongodb::Collection<AlertAuxForUpdate>,
    ztf_alert_aux_collection: mongodb::Collection<Document>,
    lsst_alert_aux_collection: mongodb::Collection<Document>,
    schema_cache: SchemaCache,
}

impl RomanAlertWorker {
    #[instrument(skip(self), err)]
    async fn get_survey_matches(&self, ra: f64, dec: f64) -> Result<RomanAliases, AlertError> {
        let ztf_matches = self
            .get_matches(
                ra,
                dec,
                ztf::ZTF_DEC_RANGE,
                ROMAN_ZTF_XMATCH_RADIUS,
                &self.ztf_alert_aux_collection,
            )
            .await?;

        let lsst_matches = self
            .get_matches(
                ra,
                dec,
                lsst::LSST_DEC_RANGE,
                ROMAN_LSST_XMATCH_RADIUS,
                &self.lsst_alert_aux_collection,
            )
            .await?;

        Ok(RomanAliases {
            ztf: ztf_matches,
            lsst: lsst_matches,
        })
    }

    async fn get_existing_aux(
        &self,
        object_id: &str,
    ) -> Result<Option<AlertAuxForUpdate>, AlertError> {
        let result = self
            .alert_aux_collection_update
            .find_one(doc! { "_id": object_id })
            .projection(doc! { "prv_candidates.jd": 1, "fp_hists.jd": 1, "version": 1 })
            .await
            .inspect_err(as_error!())?;
        Ok(result)
    }

    #[instrument(skip(self, prv_candidates, fp_hists, survey_matches), err)]
    async fn update_aux_fallback(
        &mut self,
        object_id: &str,
        prv_candidates: &Vec<RomanCandidate>,
        fp_hists: &Vec<RomanForcedPhot>,
        survey_matches: &Option<RomanAliases>,
        now: f64,
    ) -> Result<(), AlertError> {
        Self::db_only_aux_update(
            object_id,
            doc! {
                "prv_candidates": update_timeseries_op("prv_candidates", "jd", &mongify_vec(prv_candidates)),
                "fp_hists": update_timeseries_op("fp_hists", "jd", &mongify_vec(fp_hists)),
            },
            survey_matches,
            now,
            &self.alert_aux_collection,
        )
        .await
    }

    #[instrument(skip(self, prv_candidates, fp_hists, survey_matches, existing_alert_aux))]
    async fn update_aux_inner(
        &mut self,
        object_id: &str,
        prv_candidates: &Vec<RomanCandidate>,
        fp_hists: &Vec<RomanForcedPhot>,
        survey_matches: &Option<RomanAliases>,
        now: f64,
        existing_alert_aux: &AlertAuxForUpdate,
    ) -> Result<(), AlertError> {
        let current_version = existing_alert_aux.version;

        let prepared_prv_candidates = RomanCandidate::prepare_timeseries_update(
            prv_candidates,
            &existing_alert_aux.prv_candidates,
            "prv_candidates",
        )?;

        let prepared_fp_hists = RomanForcedPhot::prepare_timeseries_update(
            fp_hists,
            &existing_alert_aux.fp_hists,
            "fp_hists",
        )?;

        let mut push_updates = Document::new();
        Self::add_to_push_aux_update(&mut push_updates, "prv_candidates", prepared_prv_candidates);
        Self::add_to_push_aux_update(&mut push_updates, "fp_hists", prepared_fp_hists);

        Self::finalize_aux_update(
            object_id,
            push_updates,
            survey_matches,
            current_version,
            now,
            &self.alert_aux_collection,
        )
        .await
    }

    #[instrument(
        skip(self, prv_candidates, fp_hists, survey_matches, existing_alert_aux),
        err
    )]
    async fn update_aux(
        &mut self,
        object_id: &str,
        prv_candidates: &Vec<RomanCandidate>,
        fp_hists: &Vec<RomanForcedPhot>,
        survey_matches: &Option<RomanAliases>,
        now: f64,
        existing_alert_aux: &AlertAuxForUpdate,
    ) -> Result<(), AlertError> {
        match self
            .update_aux_inner(
                object_id,
                prv_candidates,
                fp_hists,
                survey_matches,
                now,
                existing_alert_aux,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(e) => {
                // if we get a concurrent modification error or an error preparing the lightcurves update,
                // we fallback to a full in-DB update, safe against concurrency and "self-healing", but less efficient
                match &e {
                    AlertError::ConcurrentAuxUpdate(_) => debug!(error = %e),
                    _ => error!(error = %e),
                }
                self.update_aux_fallback(object_id, prv_candidates, fp_hists, survey_matches, now)
                    .await
            }
        }
    }
}

#[async_trait::async_trait]
impl AlertWorker for RomanAlertWorker {
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<RomanAlertWorker, AlertWorkerError> {
        let config = AppConfig::from_path(config_path)?;

        let xmatch_configs = config
            .crossmatch
            .get(&Survey::Roman)
            .cloned()
            .unwrap_or_default();

        let db: mongodb::Database = config
            .build_db()
            .await
            .inspect_err(as_error!("failed to create mongo client"))?;

        let alert_collection = db.collection(&ALERT_COLLECTION);
        let alert_aux_collection = db.collection(&ALERT_AUX_COLLECTION);
        let alert_cutout_storage = config
            .build_cutout_storage(&Survey::Roman)
            .await
            .inspect_err(as_error!("failed to create cutout storage"))?;
        let alert_aux_collection_update = db.collection(&ALERT_AUX_COLLECTION);

        let ztf_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&ztf::ALERT_AUX_COLLECTION);

        let lsst_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&lsst::ALERT_AUX_COLLECTION);

        let worker = RomanAlertWorker {
            xmatch_configs,
            db,
            alert_collection,
            alert_aux_collection,
            alert_cutout_storage,
            alert_aux_collection_update,
            ztf_alert_aux_collection,
            lsst_alert_aux_collection,
            schema_cache: SchemaCache::default(),
        };
        Ok(worker)
    }

    fn survey() -> Survey {
        Survey::Roman
    }

    fn input_queue_name(&self) -> String {
        format!("{}_alerts_packets_queue", RomanAlertWorker::survey())
    }

    fn output_queue_name(&self) -> String {
        format!("{}_alerts_enrichment_queue", RomanAlertWorker::survey())
    }

    #[instrument(skip_all, err)]
    async fn process_alert(&mut self, avro_bytes: &[u8]) -> Result<ProcessAlertStatus, AlertError> {
        let now = Time::now().to_jd();
        let mut avro_alert: RomanRawAvroAlert = self
            .schema_cache
            .alert_from_avro_bytes(avro_bytes)
            .inspect_err(as_error!())?;

        let candidate = RomanCandidate::new(avro_alert.dia_source, avro_alert.dia_object)?;

        let candid = candidate.dia_source.candid;
        let object_id = candidate.object_id.clone();
        let ra = candidate.dia_source.ra;
        let dec = candidate.dia_source.dec;
        let ss_matches = avro_alert.ss_matches.take().unwrap_or_default();
        // Either a match against a known object, or a moving-object candidate
        // flagged by RAPID.
        let is_sso = !ss_matches.is_empty() || candidate.dia_source.is_ss_candidate == Some(true);

        let mut prv_candidates = avro_alert.prv_candidates.take().unwrap_or_default();
        let mut fp_hists = avro_alert.fp_hists.take().unwrap_or_default();

        // Add the current candidate as the last point in the prv_candidates, if it's not already there (based on jd)
        if !prv_candidates.iter().any(|pc| pc.jd == candidate.jd) {
            prv_candidates.push(candidate.clone());
        }

        // Sort and deduplicate time series data by jd
        RomanCandidate::sanitize_timeseries(&mut prv_candidates);
        RomanForcedPhot::sanitize_timeseries(&mut fp_hists);

        let alert = RomanAlert {
            candid,
            object_id: object_id.clone(),
            candidate,
            ss_matches,
            coordinates: Coordinates::new(ra, dec),
            created_at: now,
            updated_at: now,
        };

        let status = self
            .format_and_insert_alert(candid, &alert, &self.alert_collection)
            .await
            .inspect_err(as_error!())?;

        if let ProcessAlertStatus::Exists(_) = status {
            return Ok(status);
        }

        let survey_matches = Some(
            self.get_survey_matches(ra, dec)
                .await
                .inspect_err(as_error!())?,
        );

        let existing_alert_aux = self.get_existing_aux(&object_id).await?;

        if let Some(existing) = existing_alert_aux {
            self.update_aux(
                &object_id,
                &prv_candidates,
                &fp_hists,
                &survey_matches,
                now,
                &existing,
            )
            .await
            .inspect_err(as_error!())?;
        } else {
            let xmatches = xmatch(
                ra,
                dec,
                &object_id,
                &Survey::Roman,
                &self.xmatch_configs,
                &self.db,
            )
            .await?;
            let obj = RomanObject {
                object_id: object_id.clone(),
                prv_candidates,
                fp_hists,
                is_sso,
                cross_matches: Some(xmatches),
                aliases: survey_matches,
                coordinates: Coordinates::new(ra, dec),
                created_at: now,
                updated_at: now,
            };
            let result = self.insert_aux(&obj, &self.alert_aux_collection).await;
            if let Err(AlertError::AlertAuxExists) = result {
                // use the race-condition free fallback update
                warn!(
                    "Alert aux document for object_id {} already exists. Using fallback update.",
                    object_id
                );
                self.update_aux_fallback(
                    &object_id,
                    &obj.prv_candidates,
                    &obj.fp_hists,
                    &obj.aliases,
                    now,
                )
                .await
                .inspect_err(as_error!())?;
            } else {
                result.inspect_err(as_error!())?;
            }
        }

        let status = self
            .format_and_insert_cutouts(
                candid,
                &object_id,
                avro_alert.cutout_science,
                avro_alert.cutout_reference,
                avro_alert.cutout_difference,
                &self.alert_cutout_storage,
            )
            .await
            .inspect_err(as_error!())?;

        Ok(status)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::{
        enums::Survey,
        testing::{
            assert_update_aux_branches_and_fallback, drop_alert_from_collections,
            roman_alert_worker, AlertRandomizer, AuxBranchSnapshot, AuxUpdateBranchTestAdapter,
        },
    };

    struct PrvLightcurveGen {
        template: RomanCandidate,
        next_candid: i64,
    }

    impl PrvLightcurveGen {
        fn new(template: RomanCandidate, first_candid: i64) -> Self {
            Self {
                template,
                next_candid: first_candid,
            }
        }

        fn at_jd(&mut self, jd: f64) -> RomanCandidate {
            let mut candidate = self.template.clone();
            candidate.jd = jd;
            candidate.dia_source.midpoint_mjd = jd - 2400000.5;
            candidate.dia_source.candid = self.next_candid;
            self.next_candid += 1;
            candidate
        }
    }

    async fn seed_roman_alert(worker: &mut RomanAlertWorker) -> (i64, String, Vec<u8>) {
        let (candid, object_id, _ra, _dec, bytes_content) =
            AlertRandomizer::new_randomized(Survey::Roman).get().await;
        let status = worker.process_alert(&bytes_content).await.unwrap();
        assert_eq!(status, ProcessAlertStatus::Added(candid));
        (candid, object_id, bytes_content)
    }

    async fn load_aux(worker: &RomanAlertWorker, object_id: &str) -> AlertAuxForUpdate {
        worker.get_existing_aux(object_id).await.unwrap().unwrap()
    }

    async fn set_aux_fields(worker: &RomanAlertWorker, object_id: &str, set_doc: Document) {
        worker
            .alert_aux_collection
            .update_one(doc! { "_id": object_id }, doc! { "$set": set_doc })
            .await
            .unwrap();
    }

    async fn apply_update(
        worker: &mut RomanAlertWorker,
        object_id: &str,
        prv_candidates: Vec<RomanCandidate>,
        fp_hists: Vec<RomanForcedPhot>,
        survey_matches: &Option<RomanAliases>,
        existing_aux: &AlertAuxForUpdate,
    ) {
        worker
            .update_aux(
                object_id,
                &prv_candidates,
                &fp_hists,
                survey_matches,
                Time::now().to_jd(),
                existing_aux,
            )
            .await
            .unwrap();
    }

    struct RomanAuxBranchAdapter {
        lc_gen: PrvLightcurveGen,
    }

    #[async_trait::async_trait]
    impl AuxUpdateBranchTestAdapter for RomanAuxBranchAdapter {
        type Worker = RomanAlertWorker;
        type ExistingAux = AlertAuxForUpdate;
        type SurveyMatches = Option<RomanAliases>;
        type Updates = (Vec<RomanCandidate>, Vec<RomanForcedPhot>);

        async fn load_existing(&self, worker: &Self::Worker, object_id: &str) -> Self::ExistingAux {
            load_aux(worker, object_id).await
        }

        fn snapshot(&self, existing_aux: &Self::ExistingAux) -> AuxBranchSnapshot {
            AuxBranchSnapshot {
                series: vec![existing_aux.prv_candidates.clone()],
                version: existing_aux.version,
            }
        }

        fn survey_matches(&self) -> Self::SurveyMatches {
            Some(RomanAliases::default())
        }

        fn empty_updates(&self) -> Self::Updates {
            (vec![], vec![])
        }

        fn updates_at_jds(&mut self, jds: &[f64]) -> Self::Updates {
            assert_eq!(jds.len(), 1);
            (vec![self.lc_gen.at_jd(jds[0])], vec![])
        }

        async fn inject_corrupted_existing(&self, worker: &Self::Worker, object_id: &str) {
            set_aux_fields(
                worker,
                object_id,
                doc! {
                    "prv_candidates": vec![
                        doc! { "jd": 2.0 },
                        doc! { "jd": 1.0 },
                        doc! { "jd": 1.0 },
                    ],
                    "fp_hists": vec![
                        doc! { "jd": 3.0 },
                        doc! { "jd": 2.0 },
                        doc! { "jd": 2.0 },
                    ],
                },
            )
            .await;
        }

        fn expected_repaired_jds(&self) -> Vec<Vec<f64>> {
            vec![vec![1.0, 2.0], vec![2.0, 3.0]]
        }

        async fn inject_non_finite_existing(&self, worker: &Self::Worker, object_id: &str) {
            set_aux_fields(
                worker,
                object_id,
                doc! {
                    "prv_candidates": vec![
                        doc! { "jd": f64::NAN },
                        doc! { "jd": 1.0 },
                    ],
                },
            )
            .await;
        }

        fn expected_non_finite_repaired_jds(&self) -> Vec<Vec<f64>> {
            vec![vec![1.0], vec![2.0, 3.0]]
        }

        async fn apply_update(
            &self,
            worker: &mut Self::Worker,
            object_id: &str,
            updates: Self::Updates,
            survey_matches: &Self::SurveyMatches,
            existing_aux: &Self::ExistingAux,
        ) {
            let (prv_candidates, fp_hists) = updates;
            apply_update(
                worker,
                object_id,
                prv_candidates,
                fp_hists,
                survey_matches,
                existing_aux,
            )
            .await;
        }
    }

    /// Decoding needs no database, so this exercises the packet -> struct path on
    /// its own.
    #[tokio::test]
    async fn test_roman_alert_from_avro_bytes() {
        let mut schema_cache = SchemaCache::default();

        let (candid, object_id, ra, dec, bytes_content) =
            AlertRandomizer::new_randomized(Survey::Roman).get().await;
        let alert: Result<RomanRawAvroAlert, _> =
            schema_cache.alert_from_avro_bytes(&bytes_content);
        assert!(alert.is_ok(), "{:?}", alert.err());

        let alert = alert.unwrap();
        assert_eq!(alert.candid, candid);
        assert_eq!(alert.schema_version.as_deref(), Some("00.02"));

        // the wide filter is reported as W146 in the packets, and normalized to F146
        assert_eq!(alert.dia_source.band.clone().unwrap(), Band::F146);

        let prv_candidates = alert.prv_candidates.clone().unwrap();
        assert_eq!(prv_candidates.len(), 27);
        // prv sources carry the same objectId as the triggering source
        assert!(prv_candidates.iter().all(|pc| pc.object_id == object_id));

        let candidate = RomanCandidate::new(alert.dia_source, alert.dia_object).unwrap();
        assert_eq!(candidate.object_id, object_id);
        assert!((candidate.dia_source.ra - ra).abs() < 1e-6);
        assert!((candidate.dia_source.dec - dec).abs() < 1e-6);
        // UTC MJD 61679.30210648148 -> JD
        assert!((candidate.jd - 2461679.80210648).abs() < 1e-6);
        // nJy PSF flux against the AB nJy zeropoint
        assert!((candidate.magpsf - 24.825713).abs() < 1e-4);
        assert!((candidate.sigmapsf - 0.050215).abs() < 1e-5);
        assert!((candidate.snr_psf - 21.621889).abs() < 1e-5);
        // `diffimglimmag` is a stub, so diffmaglim comes from the flux error
        assert!((candidate.diffmaglim - 26.415522).abs() < 1e-4);
        // RAPID reports absolute flux, so the sign comes from isNegative
        assert_eq!(candidate.isdiffpos, true);
        // ndethist/jdstarthist come from the diaObject summary
        assert_eq!(candidate.ndethist, Some(28));
        assert!((candidate.jdstarthist.unwrap() - 2461679.80210648).abs() < 1e-6);
        // aperture/science/reference photometry are schema stubs for now
        assert!(candidate.dia_source.ap_flux.is_none());
        assert!(candidate.dia_source.science_flux.is_none());

        // forced photometry and solar-system matches are empty in this packet
        assert!(alert.fp_hists.is_none());
        assert!(alert.ss_matches.is_none());

        assert_eq!(alert.cutout_science.len(), 83520);
        assert_eq!(alert.cutout_reference.len(), 83520);
        assert_eq!(alert.cutout_difference.len(), 83520);
    }

    #[tokio::test]
    async fn test_update_aux_branches_and_fallback() {
        let mut worker = roman_alert_worker().await;

        let (candid, object_id, bytes_content) = seed_roman_alert(&mut worker).await;
        let parsed_alert: RomanRawAvroAlert = worker
            .schema_cache
            .alert_from_avro_bytes(&bytes_content)
            .unwrap();
        let base_prv = RomanCandidate::try_from(parsed_alert.dia_source.clone()).unwrap();
        let mut adapter = RomanAuxBranchAdapter {
            lc_gen: PrvLightcurveGen::new(base_prv, candid + 1),
        };

        assert_update_aux_branches_and_fallback(&mut worker, &object_id, &mut adapter).await;

        drop_alert_from_collections(candid, &Survey::Roman)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_process_alert_cutout_stored_and_retrievable() {
        let mut worker = roman_alert_worker().await;
        let (candid, _object_id, bytes_content) = seed_roman_alert(&mut worker).await;

        let parsed_alert: RomanRawAvroAlert = worker
            .schema_cache
            .alert_from_avro_bytes(&bytes_content)
            .unwrap();

        let stored = worker
            .alert_cutout_storage
            .retrieve_cutouts(candid, false)
            .await
            .expect("cutout should be retrievable after process_alert");

        assert_eq!(stored.candid, candid);
        assert_eq!(stored.cutout_science, parsed_alert.cutout_science);
        assert_eq!(stored.cutout_template, parsed_alert.cutout_reference);
        assert_eq!(stored.cutout_difference, parsed_alert.cutout_difference);

        drop_alert_from_collections(candid, &Survey::Roman)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_process_alert_cutout_deduplication() {
        let mut worker = roman_alert_worker().await;
        let (candid, _object_id, _ra, _dec, bytes_content) =
            AlertRandomizer::new_randomized(Survey::Roman).get().await;

        let first = worker.process_alert(&bytes_content).await.unwrap();
        assert_eq!(first, ProcessAlertStatus::Added(candid));

        let second = worker.process_alert(&bytes_content).await.unwrap();
        assert_eq!(second, ProcessAlertStatus::Exists(candid));

        drop_alert_from_collections(candid, &Survey::Roman)
            .await
            .unwrap();
    }
}
