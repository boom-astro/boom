use crate::{
    alert::{
        base::{
            AlertCutout, AlertError, AlertWorker, AlertWorkerError, ProcessAlertStatus,
            SchemaRegistry,
        },
        decam, ztf,
    },
    conf::{self, AppConfig, BoomConfigError},
    utils::{
        db::{mongify, update_timeseries_op},
        enums::Survey,
        lightcurves::{flux2mag, fluxerr2diffmaglim, Band, SNT, ZP_AB},
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
use std::collections::HashMap;
use tracing::instrument;

pub const STREAM_NAME: &str = "LSST";
pub const LSST_DEC_RANGE: (f64, f64) = (-90.0, 33.5);
pub const LSST_POSITION_UNCERTAINTY: f64 = 0.1; // arcsec
pub const ALERT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts");
pub const ALERT_AUX_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_aux");
pub const ALERT_CUTOUT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_cutouts");

pub const LSST_ZTF_XMATCH_RADIUS: f64 =
    (LSST_POSITION_UNCERTAINTY.max(ztf::ZTF_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();
pub const LSST_DECAM_XMATCH_RADIUS: f64 =
    (LSST_POSITION_UNCERTAINTY.max(decam::DECAM_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();

pub const LSST_SCHEMA_REGISTRY_URL: &str = "https://usdf-alert-schemas-dev.slac.stanford.edu";

const LSST_ZP_AB_NJY: f32 = ZP_AB + 22.5; // ZP + nJy to Jy conversion factor, as 2.5 * log10(1e9) = 22.5

#[serde_as]
#[skip_serializing_none]
#[serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Default, schemars::JsonSchema)]
#[serde(default)]
pub struct DiaSource {
    /// Unique identifier of this DiaSource.
    #[serde(rename = "candid")]
    #[serde(alias = "diaSourceId")]
    pub candid: i64,
    /// Id of the visit where this diaSource was measured.
    pub visit: i64,
    /// Id of the detector where this diaSource was measured. Datatype short instead of byte because of DB concerns about unsigned bytes.
    pub detector: i32,
    /// Id of the diaObject this source was associated with, if any. If not, it is set to NULL (each diaSource will be associated with either a diaObject or ssObject).
    #[serde(rename = "diaObjectId")]
    #[serde(deserialize_with = "deserialize_optional_id")]
    pub dia_object_id: Option<i64>,
    /// Id of the ssObject this source was associated with, if any. If not, it is set to NULL (each diaSource will be associated with either a diaObject or ssObject).
    #[serde(rename = "ssObjectId")]
    #[serde(deserialize_with = "deserialize_optional_id")]
    pub ss_object_id: Option<i64>,
    /// Id of the parent diaSource this diaSource has been deblended from, if any.
    #[serde(rename = "parentDiaSourceId")]
    pub parent_dia_source_id: Option<i64>,
    /// Effective mid-visit time for this diaSource, expressed as Modified Julian Date, International Atomic Time.
    #[serde(rename = "midpointMjdTai")]
    pub midpoint_mjd_tai: f64,
    /// Right ascension coordinate of the center of this diaSource.
    pub ra: f64,
    /// Uncertainty of ra.
    #[serde(rename = "raErr")]
    pub ra_err: Option<f32>,
    /// Declination coordinate of the center of this diaSource.
    pub dec: f64,
    /// Uncertainty of dec.
    #[serde(rename = "decErr")]
    pub dec_err: Option<f32>,
    /// General centroid algorithm failure flag; set if anything went wrong when fitting the centroid. Another centroid flag field should also be set to provide more information.
    pub centroid_flag: Option<bool>,
    /// Flux in a 12 pixel radius aperture on the difference image.
    #[serde(rename = "apFlux")]
    pub ap_flux: Option<f32>,
    /// Estimated uncertainty of apFlux.
    #[serde(rename = "apFluxErr")]
    pub ap_flux_err: Option<f32>,
    /// General aperture flux algorithm failure flag; set if anything went wrong when measuring aperture fluxes. Another apFlux flag field should also be set to provide more information.
    #[serde(rename = "apFlux_flag")]
    pub ap_flux_flag: Option<bool>,
    /// Aperture did not fit within measurement image.
    #[serde(rename = "apFlux_flag_apertureTruncated")]
    pub ap_flux_flag_aperture_truncated: Option<bool>,
    /// Flux for Point Source model. Note this actually measures the flux difference between the template and the visit image.
    #[serde(rename = "psfFlux")]
    pub psf_flux: Option<f32>,
    /// Uncertainty of psfFlux.
    #[serde(rename = "psfFluxErr")]
    pub psf_flux_err: Option<f32>,
    /// Chi^2 statistic of the point source model fit.
    #[serde(rename = "psfChi2")]
    pub psf_chi2: Option<f32>,
    /// The number of data points (pixels) used to fit the point source model.
    #[serde(rename = "psfNdata")]
    pub psf_ndata: Option<i32>,
    /// Failure to derive linear least-squares fit of psf model. Another psfFlux flag field should also be set to provide more information.
    #[serde(rename = "psfFlux_flag")]
    pub psf_flux_flag: Option<bool>,
    /// Object was too close to the edge of the image to use the full PSF model.
    #[serde(rename = "psfFlux_flag_edge")]
    pub psf_flux_flag_edge: Option<bool>,
    /// Not enough non-rejected pixels in data to attempt the fit.
    #[serde(rename = "psfFlux_flag_noGoodPixels")]
    pub psf_flux_flag_no_good_pixels: Option<bool>,
    /// Flux for a trailed source model. Note this actually measures the flux difference between the template and the visit image.
    #[serde(rename = "trailFlux")]
    pub trail_flux: Option<f32>,
    /// Uncertainty of trailFlux.
    #[serde(rename = "trailFluxErr")]
    pub trail_flux_err: Option<f32>,
    /// Right ascension coordinate of centroid for trailed source model.
    #[serde(rename = "trailRa")]
    pub trail_ra: Option<f64>,
    /// Uncertainty of trailRa.
    #[serde(rename = "trailRaErr")]
    pub trail_ra_err: Option<f32>,
    /// Declination coordinate of centroid for trailed source model.
    #[serde(rename = "trailDec")]
    pub trail_dec: Option<f64>,
    /// Uncertainty of trailDec.
    #[serde(rename = "trailDecErr")]
    pub trail_dec_err: Option<f32>,
    /// Maximum likelihood fit of trail length.
    #[serde(rename = "trailLength")]
    pub trail_length: Option<f32>,
    /// Uncertainty of trailLength.
    #[serde(rename = "trailLengthErr")]
    pub trail_length_err: Option<f32>,
    /// Maximum likelihood fit of the angle between the meridian through the centroid and the trail direction (bearing).
    #[serde(rename = "trailAngle")]
    pub trail_angle: Option<f32>,
    /// Uncertainty of trailAngle.
    #[serde(rename = "trailAngleErr")]
    pub trail_angle_err: Option<f32>,
    /// Chi^2 statistic of the trailed source model fit.
    #[serde(rename = "trailChi2")]
    pub trail_chi2: Option<f32>,
    /// The number of data points (pixels) used to fit the trailed source model.
    #[serde(rename = "trailNdata")]
    pub trail_ndata: Option<i32>,
    /// This flag is set if a trailed source extends onto or past edge pixels.
    pub trail_flag_edge: Option<bool>,
    /// Forced photometry flux for a point source model measured on the visit image centered at DiaSource position
    #[serde(rename = "scienceFlux")]
    pub science_flux: Option<f32>,
    /// Uncertainty of scienceFlux.
    #[serde(rename = "scienceFluxErr")]
    pub science_flux_err: Option<f32>,
    /// Forced PSF photometry on science image failed. Another forced_PsfFlux flag field should also be set to provide more information.
    #[serde(rename = "forced_PsfFlux_flag")]
    pub forced_psf_flux_flag: Option<bool>,
    /// Forced PSF flux on science image was too close to the edge of the image to use the full PSF model.
    #[serde(rename = "forced_PsfFlux_flag_edge")]
    pub forced_psf_flux_flag_edge: Option<bool>,
    /// Forced PSF flux not enough non-rejected pixels in data to attempt the fit.
    #[serde(rename = "forced_PsfFlux_flag_noGoodPixels")]
    pub forced_psf_flux_flag_no_good_pixels: Option<bool>,
    /// Forced photometry flux for a point source model measured on the template image centered at the DiaObject position.
    #[serde(rename = "templateFlux")]
    pub template_flux: Option<f32>,
    /// Uncertainty of templateFlux.
    #[serde(rename = "templateFluxErr")]
    pub template_flux_err: Option<f32>,
    /// General source shape algorithm failure flag; set if anything went wrong when measuring the shape. Another shape flag field should also be set to provide more information.
    pub shape_flag: Option<bool>,
    /// No pixels to measure shape.
    pub shape_flag_no_pixels: Option<bool>,
    /// Center not contained in footprint bounding box.
    pub shape_flag_not_contained: Option<bool>,
    /// This source is a parent source; we should only be measuring on deblended children in difference imaging.
    pub shape_flag_parent_source: Option<bool>,
    /// A measure of extendedness, computed by comparing an object's moment-based traced radius to the PSF moments. extendedness = 1 implies a high degree of confidence that the source is extended. extendedness = 0 implies a high degree of confidence that the source is point-like.
    pub extendedness: Option<f32>,
    /// A measure of reliability, computed using information from the source and image characterization, as well as the information on the Telescope and Camera system (e.g., ghost maps, defect maps, etc.).
    pub reliability: Option<f32>,
    /// Filter band this source was observed with.
    pub band: Option<Band>,
    /// Source well fit by a dipole.
    #[serde(rename = "isDipole")]
    pub is_dipole: Option<bool>,
    /// General pixel flags failure; set if anything went wrong when setting pixels flags from this footprint's mask. This implies that some pixelFlags for this source may be incorrectly set to False.
    #[serde(rename = "pixelFlags")]
    pub pixel_flags: Option<bool>,
    /// This flag is set if the source is part of a glint trail.
    pub glint_trail: Option<bool>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct LsstCandidate {
    #[serde(flatten)]
    pub dia_source: DiaSource,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub jd: f64,
    pub magpsf: f32,
    pub sigmapsf: f32,
    pub diffmaglim: f32,
    pub isdiffpos: bool,
    pub snr: f32,
    pub magap: f32,
    pub sigmagap: f32,
    pub is_sso: bool,
}

/// LsstCandidate for Avro serialization (without flatten)
#[serde_as]
#[skip_serializing_none]
#[serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct LsstCandidateAvro {
    pub dia_source: DiaSource,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub jd: f64,
    pub magpsf: f32,
    pub sigmapsf: f32,
    pub diffmaglim: f32,
    pub isdiffpos: bool,
    pub snr: f32,
    pub magap: f32,
    pub sigmagap: f32,
    pub is_sso: bool,
}

impl From<LsstCandidate> for LsstCandidateAvro {
    fn from(candidate: LsstCandidate) -> Self {
        LsstCandidateAvro {
            dia_source: candidate.dia_source,
            object_id: candidate.object_id,
            jd: candidate.jd,
            magpsf: candidate.magpsf,
            sigmapsf: candidate.sigmapsf,
            diffmaglim: candidate.diffmaglim,
            isdiffpos: candidate.isdiffpos,
            snr: candidate.snr,
            magap: candidate.magap,
            sigmagap: candidate.sigmagap,
            is_sso: candidate.is_sso,
        }
    }
}

impl TryFrom<DiaSource> for LsstCandidate {
    type Error = AlertError;
    fn try_from(dia_source: DiaSource) -> Result<Self, Self::Error> {
        let jd = dia_source.midpoint_mjd_tai + 2400000.5;
        let psf_flux = dia_source.psf_flux.ok_or(AlertError::MissingFluxPSF)?;
        let psf_flux_err = dia_source.psf_flux_err.ok_or(AlertError::MissingFluxPSF)?;

        let ap_flux = dia_source.ap_flux.ok_or(AlertError::MissingFluxAperture)?;
        let ap_flux_err = dia_source
            .ap_flux_err
            .ok_or(AlertError::MissingFluxAperture)?;

        // instead of converting all the nJy values to Jy, we just add 2.5 * log10(1e9) = 22.5
        // to the zeropoint

        let (magpsf, sigmapsf) = flux2mag(psf_flux.abs(), psf_flux_err, LSST_ZP_AB_NJY);
        let diffmaglim = fluxerr2diffmaglim(psf_flux_err, LSST_ZP_AB_NJY);

        let (magap, sigmagap) = flux2mag(ap_flux.abs(), ap_flux_err, LSST_ZP_AB_NJY);

        // if dia_object_id is defined, is_sso is false
        // if ss_object_id is defined, is_sso is true
        // if both are undefined or both are defined, we throw an error
        let (object_id, is_sso) = match (
            dia_source.dia_object_id.clone(),
            dia_source.ss_object_id.clone(),
        ) {
            (Some(dia_id), None) => (dia_id.to_string(), false),
            (None, Some(ss_id)) => (format!("sso{}", ss_id.to_string()), true),
            (None, None) => return Err(AlertError::MissingObjectId),
            (Some(dia_id), Some(ss_id)) => {
                return Err(AlertError::AmbiguousObjectId(dia_id, ss_id))
            }
        };

        Ok(LsstCandidate {
            dia_source,
            object_id,
            jd,
            magpsf,
            sigmapsf,
            diffmaglim,
            isdiffpos: psf_flux > 0.0,
            snr: psf_flux.abs() / psf_flux_err,
            magap,
            sigmagap,
            is_sso,
        })
    }
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct DiaObject {
    /// Unique identifier of this DiaObject.
    #[serde(rename = "diaObjectId")]
    pub dia_object_id: i64,
    /// Processing time when validity of this diaObject starts, expressed as Modified Julian Date, International Atomic Time.
    #[serde(rename = "validityStartMjdTai")]
    pub validity_start_mjd_tai: f64,
    /// Right ascension coordinate of the position of the object at time radecMjdTai.
    pub ra: f64,
    /// Uncertainty of ra.
    #[serde(rename = "raErr")]
    pub ra_err: Option<f32>,
    /// Declination coordinate of the position of the object at time radecMjdTai.
    pub dec: f64,
    /// Uncertainty of dec.
    #[serde(rename = "decErr")]
    pub dec_err: Option<f32>,
    /// Weighted mean point-source model magnitude for u filter.
    #[serde(rename = "u_psfFluxMean")]
    pub u_psf_flux_mean: Option<f32>,
    /// Standard error of u_psfFluxMean.
    #[serde(rename = "u_psfFluxMeanErr")]
    pub u_psf_flux_mean_err: Option<f32>,
    /// Chi^2 statistic for the scatter of u_psfFlux around u_psfFluxMean.
    #[serde(rename = "u_psfFluxChi2")]
    pub u_psf_flux_chi2: Option<f32>,
    /// The number of data points used to compute u_psfFluxChi2.
    #[serde(rename = "u_psfFluxNdata")]
    pub u_psf_flux_ndata: Option<i32>,
    /// Weighted mean forced photometry flux for u filter.
    #[serde(rename = "u_fpFluxMean")]
    pub u_fp_flux_mean: Option<f32>,
    /// Standard error of u_fpFluxMean.
    #[serde(rename = "u_fpFluxMeanErr")]
    pub u_fp_flux_mean_err: Option<f32>,
    /// Weighted mean point-source model magnitude for g filter.
    #[serde(rename = "g_psfFluxMean")]
    pub g_psf_flux_mean: Option<f32>,
    /// Standard error of g_psfFluxMean.
    #[serde(rename = "g_psfFluxMeanErr")]
    pub g_psf_flux_mean_err: Option<f32>,
    /// Chi^2 statistic for the scatter of g_psfFlux around g_psfFluxMean.
    #[serde(rename = "g_psfFluxChi2")]
    pub g_psf_flux_chi2: Option<f32>,
    /// The number of data points used to compute g_psfFluxChi2.
    #[serde(rename = "g_psfFluxNdata")]
    pub g_psf_flux_ndata: Option<i32>,
    /// Weighted mean forced photometry flux for g filter.
    #[serde(rename = "g_fpFluxMean")]
    pub g_fp_flux_mean: Option<f32>,
    /// Standard error of g_fpFluxMean.
    #[serde(rename = "g_fpFluxMeanErr")]
    pub g_fp_flux_mean_err: Option<f32>,
    /// Weighted mean point-source model magnitude for r filter.
    #[serde(rename = "r_psfFluxMean")]
    pub r_psf_flux_mean: Option<f32>,
    /// Standard error of r_psfFluxMean.
    #[serde(rename = "r_psfFluxMeanErr")]
    pub r_psf_flux_mean_err: Option<f32>,
    /// Chi^2 statistic for the scatter of r_psfFlux around r_psfFluxMean.
    #[serde(rename = "r_psfFluxChi2")]
    pub r_psf_flux_chi2: Option<f32>,
    /// The number of data points used to compute r_psfFluxChi2.
    #[serde(rename = "r_psfFluxNdata")]
    pub r_psf_flux_ndata: Option<i32>,
    /// Weighted mean forced photometry flux for r filter.
    #[serde(rename = "r_fpFluxMean")]
    pub r_fp_flux_mean: Option<f32>,
    /// Standard error of r_fpFluxMean.
    #[serde(rename = "r_fpFluxMeanErr")]
    pub r_fp_flux_mean_err: Option<f32>,
    /// Weighted mean point-source model magnitude for i filter.
    #[serde(rename = "i_psfFluxMean")]
    pub i_psf_flux_mean: Option<f32>,
    /// Standard error of i_psfFluxMean.
    #[serde(rename = "i_psfFluxMeanErr")]
    pub i_psf_flux_mean_err: Option<f32>,
    /// Chi^2 statistic for the scatter of i_psfFlux around i_psfFluxMean.
    #[serde(rename = "i_psfFluxChi2")]
    pub i_psf_flux_chi2: Option<f32>,
    /// The number of data points used to compute i_psfFluxChi2.
    #[serde(rename = "i_psfFluxNdata")]
    pub i_psf_flux_ndata: Option<i32>,
    /// Weighted mean forced photometry flux for i filter.
    #[serde(rename = "i_fpFluxMean")]
    pub i_fp_flux_mean: Option<f32>,
    /// Standard error of i_fpFluxMean.
    #[serde(rename = "i_fpFluxMeanErr")]
    pub i_fp_flux_mean_err: Option<f32>,
    /// Weighted mean point-source model magnitude for z filter.
    #[serde(rename = "z_psfFluxMean")]
    pub z_psf_flux_mean: Option<f32>,
    /// Standard error of z_psfFluxMean.
    #[serde(rename = "z_psfFluxMeanErr")]
    pub z_psf_flux_mean_err: Option<f32>,
    /// Chi^2 statistic for the scatter of z_psfFlux around z_psfFluxMean.
    #[serde(rename = "z_psfFluxChi2")]
    pub z_psf_flux_chi2: Option<f32>,
    /// The number of data points used to compute z_psfFluxChi2.
    #[serde(rename = "z_psfFluxNdata")]
    pub z_psf_flux_ndata: Option<i32>,
    /// Weighted mean forced photometry flux for z filter.
    #[serde(rename = "z_fpFluxMean")]
    pub z_fp_flux_mean: Option<f32>,
    /// Standard error of z_fpFluxMean.
    #[serde(rename = "z_fpFluxMeanErr")]
    pub z_fp_flux_mean_err: Option<f32>,
    /// Weighted mean point-source model magnitude for y filter.
    #[serde(rename = "y_psfFluxMean")]
    pub y_psf_flux_mean: Option<f32>,
    /// Standard error of y_psfFluxMean.
    #[serde(rename = "y_psfFluxMeanErr")]
    pub y_psf_flux_mean_err: Option<f32>,
    /// Chi^2 statistic for the scatter of y_psfFlux around y_psfFluxMean.
    #[serde(rename = "y_psfFluxChi2")]
    pub y_psf_flux_chi2: Option<f32>,
    /// The number of data points used to compute y_psfFluxChi2.
    #[serde(rename = "y_psfFluxNdata")]
    pub y_psf_flux_ndata: Option<i32>,
    /// Weighted mean forced photometry flux for y filter.
    #[serde(rename = "y_fpFluxMean")]
    pub y_fp_flux_mean: Option<f32>,
    /// Standard error of y_fpFluxMean.
    #[serde(rename = "y_fpFluxMeanErr")]
    pub y_fp_flux_mean_err: Option<f32>,
    /// Mean of the u band flux errors.
    #[serde(rename = "u_psfFluxErrMean")]
    pub u_psf_flux_err_mean: Option<f32>,
    /// Mean of the g band flux errors.
    #[serde(rename = "g_psfFluxErrMean")]
    pub g_psf_flux_err_mean: Option<f32>,
    /// Mean of the r band flux errors.
    #[serde(rename = "r_psfFluxErrMean")]
    pub r_psf_flux_err_mean: Option<f32>,
    /// Mean of the i band flux errors.
    #[serde(rename = "i_psfFluxErrMean")]
    pub i_psf_flux_err_mean: Option<f32>,
    /// Mean of the z band flux errors.
    #[serde(rename = "z_psfFluxErrMean")]
    pub z_psf_flux_err_mean: Option<f32>,
    /// Mean of the y band flux errors.
    #[serde(rename = "y_psfFluxErrMean")]
    pub y_psf_flux_err_mean: Option<f32>,
    /// Weighted mean forced photometry flux for u filter.
    #[serde(rename = "u_scienceFluxMean")]
    pub u_science_flux_mean: Option<f32>,
    /// Standard error of u_scienceFluxMean.
    #[serde(rename = "u_scienceFluxMeanErr")]
    pub u_science_flux_mean_err: Option<f32>,
    /// Weighted mean forced photometry flux for g filter.
    #[serde(rename = "g_scienceFluxMean")]
    pub g_science_flux_mean: Option<f32>,
    /// Standard error of g_scienceFluxMean.
    #[serde(rename = "g_scienceFluxMeanErr")]
    pub g_science_flux_mean_err: Option<f32>,
    /// Weighted mean forced photometry flux for r filter.
    #[serde(rename = "r_scienceFluxMean")]
    pub r_science_flux_mean: Option<f32>,
    /// Standard error of r_scienceFluxMean.
    #[serde(rename = "r_scienceFluxMeanErr")]
    pub r_science_flux_mean_err: Option<f32>,
    /// Weighted mean forced photometry flux for i filter.
    #[serde(rename = "i_scienceFluxMean")]
    pub i_science_flux_mean: Option<f32>,
    /// Standard error of i_scienceFluxMean.
    #[serde(rename = "i_scienceFluxMeanErr")]
    pub i_science_flux_mean_err: Option<f32>,
    /// Weighted mean forced photometry flux for z filter.
    #[serde(rename = "z_scienceFluxMean")]
    pub z_science_flux_mean: Option<f32>,
    /// Standard error of z_scienceFluxMean.
    #[serde(rename = "z_scienceFluxMeanErr")]
    pub z_science_flux_mean_err: Option<f32>,
    /// Weighted mean forced photometry flux for y filter.
    #[serde(rename = "y_scienceFluxMean")]
    pub y_science_flux_mean: Option<f32>,
    /// Standard error of y_scienceFluxMean.
    #[serde(rename = "y_scienceFluxMeanErr")]
    pub y_science_flux_mean_err: Option<f32>,
    /// Time of the first diaSource, expressed as Modified Julian Date, International Atomic Time.
    #[serde(rename = "firstDiaSourceMjdTai")]
    pub first_dia_source_mjd_tai: f64,
    /// Last time when non-forced DIASource was seen for this object.
    #[serde(rename = "lastDiaSourceMjdTai")]
    pub last_dia_source_mjd_tai: f64,
    /// Total number of DiaSources associated with this DiaObject.
    #[serde(rename = "ndethist")]
    #[serde(alias = "nDiaSources")]
    pub ndethist: i32,
}

#[serde_as]
#[skip_serializing_none]
#[derive(
    Debug, PartialEq, Clone, Deserialize, Serialize, Default, schemars::JsonSchema, AvroSchema,
)]
#[serde(default)]
pub struct DiaForcedSource {
    /// Unique id.
    #[serde(rename = "diaForcedSourceId")]
    pub dia_forced_source_id: i64,
    /// Id of the DiaObject that this DiaForcedSource was associated with.
    #[serde(rename = "diaObjectId")]
    pub object_id: i64,
    /// Right ascension coordinate of the position of the DiaObject at time radecMjdTai.
    pub ra: f64,
    /// Declination coordinate of the position of the DiaObject at time radecMjdTai.
    pub dec: f64,
    /// Id of the visit where this forcedSource was measured.
    pub visit: i64,
    /// Id of the detector where this forcedSource was measured. Datatype short instead of byte because of DB concerns about unsigned bytes.
    pub detector: i32,
    /// Point Source model flux.
    #[serde(rename = "psfFlux")]
    pub psf_flux: Option<f32>,
    /// Uncertainty of psfFlux.
    #[serde(rename = "psfFluxErr")]
    pub psf_flux_err: Option<f32>,
    /// Effective mid-visit time for this diaForcedSource, expressed as Modified Julian Date, International Atomic Time.
    #[serde(rename = "midpointMjdTai")]
    pub midpoint_mjd_tai: f64,
    /// Forced photometry flux for a point source model measured on the visit image centered at the DiaObject position.
    #[serde(rename = "scienceFlux")]
    pub science_flux: Option<f32>,
    /// Uncertainty of scienceFlux.
    #[serde(rename = "scienceFluxErr")]
    pub science_flux_err: Option<f32>,
    /// Filter band this source was observed with.
    pub band: Option<Band>,
}

#[serde_as]
#[skip_serializing_none]
#[serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, schemars::JsonSchema)]
pub struct LsstForcedPhot {
    #[serde(flatten)]
    pub dia_forced_source: DiaForcedSource,
    pub jd: f64,
    pub magpsf: Option<f32>,
    pub sigmapsf: Option<f32>,
    pub diffmaglim: f32,
    pub isdiffpos: Option<bool>,
    pub snr: Option<f32>,
}

impl TryFrom<DiaForcedSource> for LsstForcedPhot {
    type Error = AlertError;
    fn try_from(dia_forced_source: DiaForcedSource) -> Result<Self, Self::Error> {
        let jd = dia_forced_source.midpoint_mjd_tai + 2400000.5;
        let psf_flux_err = dia_forced_source
            .psf_flux_err
            .ok_or(AlertError::MissingFluxPSF)?;

        // for now, we only consider positive detections (flux positive) as detections
        // may revisit this later
        let (magpsf, sigmapsf, isdiffpos, snr) = match dia_forced_source.psf_flux {
            Some(psf_flux) => {
                let psf_flux_abs = psf_flux.abs();
                if (psf_flux_abs / psf_flux_err) > SNT {
                    let (magpsf, sigmapsf) = flux2mag(psf_flux_abs, psf_flux_err, LSST_ZP_AB_NJY);
                    (
                        Some(magpsf),
                        Some(sigmapsf),
                        Some(psf_flux > 0.0),
                        Some(psf_flux_abs / psf_flux_err),
                    )
                } else {
                    (None, None, None, None)
                }
            }
            _ => (None, None, None, None),
        };

        let diffmaglim = fluxerr2diffmaglim(psf_flux_err, LSST_ZP_AB_NJY);

        Ok(LsstForcedPhot {
            dia_forced_source,
            jd,
            magpsf,
            sigmapsf,
            diffmaglim,
            isdiffpos,
            snr,
        })
    }
}

/// Rubin Avro alert schema v7.3
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct LsstRawAvroAlert {
    #[serde(rename(deserialize = "diaSourceId"))]
    pub candid: i64,
    #[serde(rename(deserialize = "diaSource"))]
    #[serde(deserialize_with = "deserialize_candidate")]
    pub candidate: LsstCandidate,
    #[serde(rename = "prvDiaSources")]
    #[serde(deserialize_with = "deserialize_prv_candidates")]
    pub prv_candidates: Option<Vec<LsstCandidate>>,
    #[serde(rename = "prvDiaForcedSources")]
    #[serde(deserialize_with = "deserialize_prv_forced_sources")]
    pub fp_hists: Option<Vec<LsstForcedPhot>>,
    #[serde(rename = "diaObject")]
    pub dia_object: Option<DiaObject>,
    #[serde(rename = "cutoutDifference")]
    #[serde(deserialize_with = "deserialize_cutout")]
    pub cutout_difference: Vec<u8>,
    #[serde(rename = "cutoutScience")]
    #[serde(deserialize_with = "deserialize_cutout")]
    pub cutout_science: Vec<u8>,
    #[serde(rename = "cutoutTemplate")]
    #[serde(deserialize_with = "deserialize_cutout")]
    pub cutout_template: Vec<u8>,
}

fn deserialize_optional_id<'de, D>(deserializer: D) -> Result<Option<i64>, D::Error>
where
    D: Deserializer<'de>,
{
    match <Option<i64> as Deserialize>::deserialize(deserializer)? {
        Some(0) | None => Ok(None),
        Some(i) => Ok(Some(i)),
    }
}

pub fn deserialize_cutout<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let cutout: Option<Vec<u8>> = apache_avro::serde_avro_bytes_opt::deserialize(deserializer)?;
    // if cutout is None, return an error
    match cutout {
        None => Err(serde::de::Error::custom("Missing cutout data")),
        Some(cutout) => Ok(cutout),
    }
}

fn deserialize_candidate<'de, D>(deserializer: D) -> Result<LsstCandidate, D::Error>
where
    D: Deserializer<'de>,
{
    let dia_source = <DiaSource as Deserialize>::deserialize(deserializer)?;
    LsstCandidate::try_from(dia_source).map_err(serde::de::Error::custom)
}

fn deserialize_prv_candidates<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<LsstCandidate>>, D::Error>
where
    D: Deserializer<'de>,
{
    let dia_sources = <Vec<DiaSource> as Deserialize>::deserialize(deserializer)?;
    let candidates = dia_sources
        .into_iter()
        .map(LsstCandidate::try_from)
        .collect::<Result<Vec<LsstCandidate>, AlertError>>()
        .map_err(serde::de::Error::custom)?;
    Ok(Some(candidates))
}

fn deserialize_prv_forced_sources<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<LsstForcedPhot>>, D::Error>
where
    D: Deserializer<'de>,
{
    let dia_forced_sources = <Vec<DiaForcedSource> as Deserialize>::deserialize(deserializer)?;
    let forced_phots = dia_forced_sources
        .into_iter()
        .map(LsstForcedPhot::try_from)
        .collect::<Result<Vec<LsstForcedPhot>, AlertError>>()
        .map_err(serde::de::Error::custom)?;
    Ok(Some(forced_phots))
}

#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
pub struct LsstAliases {
    #[serde(rename = "ZTF")]
    pub ztf: Vec<String>,
    #[serde(rename = "DECAM")]
    pub decam: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct LsstObject {
    #[serde(rename = "_id")]
    pub object_id: String,
    pub prv_candidates: Vec<LsstCandidate>,
    pub fp_hists: Vec<LsstForcedPhot>,
    pub cross_matches: Option<HashMap<String, Vec<Document>>>,
    pub aliases: Option<LsstAliases>,
    pub coordinates: Coordinates,
    pub created_at: f64,
    pub updated_at: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, schemars::JsonSchema)]
pub struct LsstAlert {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: LsstCandidate,
    pub coordinates: Coordinates,
    pub created_at: f64,
    pub updated_at: f64,
}

pub struct LsstAlertWorker {
    stream_name: String,
    schema_registry: SchemaRegistry,
    xmatch_configs: Vec<conf::CatalogXmatchConfig>,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<LsstAlert>,
    alert_aux_collection: mongodb::Collection<LsstObject>,
    alert_cutout_collection: mongodb::Collection<AlertCutout>,
    ztf_alert_aux_collection: mongodb::Collection<Document>,
    decam_alert_aux_collection: mongodb::Collection<Document>,
}

impl LsstAlertWorker {
    #[instrument(skip(self), err)]
    async fn get_survey_matches(&self, ra: f64, dec: f64) -> Result<LsstAliases, AlertError> {
        let ztf_matches = self
            .get_matches(
                ra,
                dec,
                ztf::ZTF_DEC_RANGE,
                LSST_ZTF_XMATCH_RADIUS,
                &self.ztf_alert_aux_collection,
            )
            .await?;

        let decam_matches = self
            .get_matches(
                ra,
                dec,
                decam::DECAM_DEC_RANGE,
                LSST_DECAM_XMATCH_RADIUS,
                &self.decam_alert_aux_collection,
            )
            .await?;

        Ok(LsstAliases {
            ztf: ztf_matches,
            decam: decam_matches,
        })
    }
    #[instrument(skip(self, prv_candidates, fp_hists, survey_matches), err)]
    async fn update_aux(
        self: &mut Self,
        object_id: &str,
        prv_candidates: &Vec<LsstCandidate>,
        fp_hists: &Vec<LsstForcedPhot>,
        survey_matches: &Option<LsstAliases>,
        now: f64,
    ) -> Result<(), AlertError> {
        let update_pipeline = vec![doc! {
            "$set": {
                "prv_candidates": update_timeseries_op("prv_candidates", "jd", &prv_candidates.iter().map(|pc| mongify(pc)).collect::<Vec<Document>>()),
                "fp_hists": update_timeseries_op("fp_hists", "jd", &fp_hists.iter().map(|pc| mongify(pc)).collect::<Vec<Document>>()),
                "aliases": mongify(survey_matches),
                "updated_at": now,
            }
        }];
        self.alert_aux_collection
            .update_one(doc! { "_id": object_id }, update_pipeline)
            .await?;
        Ok(())
    }
}

#[async_trait::async_trait]
impl AlertWorker for LsstAlertWorker {
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<LsstAlertWorker, AlertWorkerError> {
        let config = AppConfig::from_path(config_path)?;

        let xmatch_configs = config
            .crossmatch
            .get(&Survey::Lsst)
            .cloned()
            .unwrap_or_default();

        let kafka_consumer_config = config
            .kafka
            .consumer
            .get(&Survey::Lsst)
            .cloned()
            .ok_or_else(|| {
                AlertWorkerError::from(BoomConfigError::MissingKeyError(
                    "kafka.consumer.lsst".to_string(),
                ))
            })?;

        let schema_registry_url = match kafka_consumer_config.schema_registry {
            Some(ref url) => url.as_ref(),
            None => LSST_SCHEMA_REGISTRY_URL,
        };

        let db: mongodb::Database = config
            .build_db()
            .await
            .inspect_err(as_error!("failed to create mongo client"))?;

        let alert_collection = db.collection(&ALERT_COLLECTION);
        let alert_aux_collection = db.collection(&ALERT_AUX_COLLECTION);
        let alert_cutout_collection = db.collection(&ALERT_CUTOUT_COLLECTION);

        let ztf_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&ztf::ALERT_AUX_COLLECTION);

        let decam_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&decam::ALERT_AUX_COLLECTION);

        let worker = LsstAlertWorker {
            stream_name: STREAM_NAME.to_string(),
            schema_registry: SchemaRegistry::new(schema_registry_url),
            xmatch_configs,
            db,
            alert_collection,
            alert_aux_collection,
            alert_cutout_collection,
            ztf_alert_aux_collection,
            decam_alert_aux_collection,
        };
        Ok(worker)
    }

    fn stream_name(&self) -> String {
        self.stream_name.clone()
    }

    fn input_queue_name(&self) -> String {
        format!("{}_alerts_packets_queue", self.stream_name)
    }

    fn output_queue_name(&self) -> String {
        format!("{}_alerts_enrichment_queue", self.stream_name)
    }

    #[instrument(skip_all, err)]
    async fn process_alert(
        self: &mut Self,
        avro_bytes: &[u8],
    ) -> Result<ProcessAlertStatus, AlertError> {
        let now = Time::now().to_jd();
        let mut avro_alert: LsstRawAvroAlert = self
            .schema_registry
            .alert_from_avro_bytes(avro_bytes)
            .await
            .inspect_err(as_error!())?;

        let candid = avro_alert.candid;
        let object_id = avro_alert.candidate.object_id.clone();
        let ra = avro_alert.candidate.dia_source.ra;
        let dec = avro_alert.candidate.dia_source.dec;

        let mut prv_candidates = avro_alert.prv_candidates.take().unwrap_or_default();
        let fp_hists = avro_alert.fp_hists.take().unwrap_or_default();

        let status = self
            .format_and_insert_cutouts(
                candid,
                avro_alert.cutout_science,
                avro_alert.cutout_template,
                avro_alert.cutout_difference,
                &self.alert_cutout_collection,
            )
            .await
            .inspect_err(as_error!())?;

        if let ProcessAlertStatus::Exists(_) = status {
            return Ok(status);
        }

        let alert_aux_exists = self
            .check_alert_aux_exists(&object_id, &self.alert_aux_collection)
            .await
            .inspect_err(as_error!())?;

        prv_candidates.push(avro_alert.candidate.clone());

        let survey_matches = Some(
            self.get_survey_matches(ra, dec)
                .await
                .inspect_err(as_error!())?,
        );

        if !alert_aux_exists {
            let xmatches = xmatch(ra, dec, &self.xmatch_configs, &self.db).await?;
            let obj = LsstObject {
                object_id: object_id.clone(),
                prv_candidates,
                fp_hists,
                cross_matches: Some(xmatches),
                aliases: survey_matches,
                coordinates: Coordinates::new(ra, dec),
                created_at: now,
                updated_at: now,
            };
            let result = self.insert_aux(&obj, &self.alert_aux_collection).await;
            if let Err(AlertError::AlertAuxExists) = result {
                self.update_aux(
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
        } else {
            self.update_aux(&object_id, &prv_candidates, &fp_hists, &survey_matches, now)
                .await
                .inspect_err(as_error!())?;
        }

        let alert = LsstAlert {
            candid,
            object_id: object_id.clone(),
            candidate: avro_alert.candidate,
            coordinates: Coordinates::new(ra, dec),
            created_at: now,
            updated_at: now,
        };

        let status = self
            .format_and_insert_alert(candid, &alert, &self.alert_collection)
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
        testing::{lsst_alert_worker, AlertRandomizer},
    };

    #[tokio::test]
    async fn test_lsst_alert_from_avro_bytes() {
        let mut alert_worker = lsst_alert_worker().await;

        let (candid, object_id, ra, dec, bytes_content) =
            AlertRandomizer::new_randomized(Survey::Lsst).get().await;
        let alert = alert_worker
            .schema_registry
            .alert_from_avro_bytes(&bytes_content)
            .await;
        assert!(alert.is_ok());

        // validate the alert
        let alert: LsstRawAvroAlert = alert.unwrap();
        assert_eq!(alert.candid, candid);
        assert_eq!(alert.candidate.object_id, object_id);
        assert!((alert.candidate.dia_source.ra - ra).abs() < 1e-6);
        assert!((alert.candidate.dia_source.dec - dec).abs() < 1e-6);
        assert!((alert.candidate.jd - 2460961.733092).abs() < 1e-6);
        assert!((alert.candidate.magpsf - 23.674994).abs() < 1e-6);
        assert!((alert.candidate.sigmapsf - 0.217043).abs() < 1e-6);
        assert!((alert.candidate.diffmaglim - 23.675514).abs() < 1e-5);
        assert!(alert.candidate.snr - 5.002406 < 1e-6);
        assert_eq!(alert.candidate.isdiffpos, false);
        assert_eq!(alert.candidate.dia_source.band.unwrap(), Band::R);
        // TODO: check prv_candidates and forced photometry once we have alerts
        //       where they aren't empty
        // TODO: check non detections once these are available in the schema
    }
}
