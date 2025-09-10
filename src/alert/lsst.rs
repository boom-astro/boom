use crate::{
    alert::{
        base::{
            Alert, AlertError, AlertWorker, AlertWorkerError, ProcessAlertStatus, SchemaRegistry,
        },
        decam, ztf,
    },
    conf,
    utils::{
        db::{mongify, update_timeseries_op},
        enums::Survey,
        lightcurves::{flux2mag, fluxerr2diffmaglim, SNT, ZP_AB},
        o11y::logging::as_error,
        spatial::xmatch,
    },
};

use constcat::concat;
use flare::Time;
use mongodb::bson::{doc, Document};
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use tracing::{instrument, warn};

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

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct DiaSource {
    /// Unique identifier of this DiaSource.
    #[serde(rename(deserialize = "diaSourceId", serialize = "candid"))]
    pub candid: i64,
    /// Id of the visit where this diaSource was measured.
    pub visit: i64,
    /// Id of the detector where this diaSource was measured. Datatype short instead of byte because of DB concerns about unsigned bytes.
    pub detector: i32,
    /// Id of the diaObject this source was associated with, if any. If not, it is set to NULL (each diaSource will be associated with either a diaObject or ssObject).
    #[serde(rename = "diaObjectId")]
    #[serde(deserialize_with = "deserialize_optional_id")]
    pub dia_object_id: Option<String>,
    /// Id of the ssObject this source was associated with, if any. If not, it is set to NULL (each diaSource will be associated with either a diaObject or ssObject).
    #[serde(rename = "ssObjectId")]
    #[serde(deserialize_with = "deserialize_optional_id")]
    pub ss_object_id: Option<String>,
    /// Id of the parent diaSource this diaSource has been deblended from, if any.
    #[serde(rename = "parentDiaSourceId")]
    pub parent_dia_source_id: Option<i64>,
    /// Effective mid-visit time for this diaSource, expressed as Modified Julian Date, International Atomic Time.
    #[serde(rename(deserialize = "midpointMjdTai", serialize = "jd"))]
    #[serde(deserialize_with = "deserialize_mjd")]
    pub jd: f64,
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
    pub band: Option<String>,
    /// Source well fit by a dipole.
    pub is_dipole: Option<bool>,
    /// General pixel flags failure; set if anything went wrong when setting pixels flags from this footprint's mask. This implies that some pixelFlags for this source may be incorrectly set to False.
    #[serde(rename = "pixelFlags")]
    pub pixel_flags: Option<bool>,
    /// This flag is set if the source is part of a glint trail.
    pub glint_trail: Option<bool>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct Candidate {
    #[serde(flatten)]
    pub dia_source: DiaSource,
    #[serde(rename(serialize = "objectId"))]
    pub object_id: String,
    pub magpsf: f32,
    pub sigmapsf: f32,
    pub diffmaglim: f32,
    pub isdiffpos: bool,
    pub snr: f32,
    pub magap: f32,
    pub sigmagap: f32,
    pub is_sso: bool,
}

impl TryFrom<DiaSource> for Candidate {
    type Error = AlertError;
    fn try_from(dia_source: DiaSource) -> Result<Self, Self::Error> {
        let psf_flux = dia_source.psf_flux.ok_or(AlertError::MissingFluxPSF)? * 1e-9;
        let psf_flux_err = dia_source.psf_flux_err.ok_or(AlertError::MissingFluxPSF)? * 1e-9;

        let ap_flux = dia_source.ap_flux.ok_or(AlertError::MissingFluxAperture)? * 1e-9;
        let ap_flux_err = dia_source
            .ap_flux_err
            .ok_or(AlertError::MissingFluxAperture)?
            * 1e-9;

        let (magpsf, sigmapsf) = flux2mag(psf_flux.abs(), psf_flux_err, ZP_AB);
        let diffmaglim = fluxerr2diffmaglim(psf_flux_err, ZP_AB);

        let (magap, sigmagap) = flux2mag(ap_flux.abs(), ap_flux_err, ZP_AB);

        // if dia_object_id is defined, is_sso is false
        // if ss_object_id is defined, is_sso is true
        // if both are undefined or both are defined, we throw an error
        let (object_id, is_sso) = match (
            dia_source.dia_object_id.clone(),
            dia_source.ss_object_id.clone(),
        ) {
            (Some(dia_id), None) => (dia_id, false),
            (None, Some(ss_id)) => (format!("sso{}", ss_id), true),
            (None, None) => return Err(AlertError::MissingObjectId),
            (Some(dia_id), Some(ss_id)) => {
                return Err(AlertError::AmbiguousObjectId(dia_id, ss_id))
            }
        };

        Ok(Candidate {
            dia_source,
            object_id,
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
    #[serde(rename(deserialize = "diaObjectId", serialize = "objectId"))]
    #[serde(deserialize_with = "deserialize_objid")]
    pub object_id: String,
    /// Processing time when validity of this diaObject starts, expressed as Modified Julian Date, International Atomic Time.
    #[serde(rename(deserialize = "validityStartMjdTai", serialize = "validity_start_jd"))]
    #[serde(deserialize_with = "deserialize_mjd")]
    pub validity_start_jd: f64,
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
    #[serde(rename(deserialize = "firstDiaSourceMjdTai", serialize = "jdstarthist"))]
    #[serde(deserialize_with = "deserialize_mjd")]
    pub jdstarthist: f64,
    /// Last time when non-forced DIASource was seen for this object.
    #[serde(rename(deserialize = "lastDiaSourceMjdTai", serialize = "jdendhist"))]
    #[serde(deserialize_with = "deserialize_mjd")]
    pub jdendhist: f64,
    /// Total number of DiaSources associated with this DiaObject.
    #[serde(rename(deserialize = "nDiaSources", serialize = "ndethist"))]
    pub ndethist: i32,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct DiaNondetectionLimit {
    #[serde(rename = "ccdVisitId")]
    pub ccd_visit_id: i64,
    #[serde(rename(deserialize = "midpointMjdTai", serialize = "jd"))]
    #[serde(deserialize_with = "deserialize_mjd")]
    pub jd: f64,
    pub band: String,
    #[serde(rename = "diaNoise")]
    pub dia_noise: f32,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct DiaForcedSource {
    /// Unique id.
    #[serde(rename = "diaForcedSourceId")]
    pub dia_forced_source_id: i64,
    /// Id of the DiaObject that this DiaForcedSource was associated with.
    #[serde(rename(deserialize = "diaObjectId", serialize = "objectId"))]
    #[serde(deserialize_with = "deserialize_objid")]
    pub object_id: String,
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
    #[serde(rename(deserialize = "midpointMjdTai", serialize = "jd"))]
    #[serde(deserialize_with = "deserialize_mjd")]
    pub jd: f64,
    /// Forced photometry flux for a point source model measured on the visit image centered at the DiaObject position.
    #[serde(rename = "scienceFlux")]
    pub science_flux: Option<f32>,
    /// Uncertainty of scienceFlux.
    #[serde(rename = "scienceFluxErr")]
    pub science_flux_err: Option<f32>,
    /// Filter band this source was observed with.
    pub band: Option<String>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct ForcedPhot {
    #[serde(flatten)]
    pub dia_forced_source: DiaForcedSource,
    pub magpsf: Option<f32>,
    pub sigmapsf: Option<f32>,
    pub diffmaglim: f32,
    pub isdiffpos: Option<bool>,
    pub snr: Option<f32>,
}

impl TryFrom<DiaForcedSource> for ForcedPhot {
    type Error = AlertError;
    fn try_from(dia_forced_source: DiaForcedSource) -> Result<Self, Self::Error> {
        let psf_flux_err = dia_forced_source
            .psf_flux_err
            .ok_or(AlertError::MissingFluxPSF)?
            * 1e-9;

        // for now, we only consider positive detections (flux positive) as detections
        // may revisit this later
        let (magpsf, sigmapsf, isdiffpos, snr) = match dia_forced_source.psf_flux {
            Some(psf_flux) => {
                let psf_flux = psf_flux * 1e-9;
                if (psf_flux / psf_flux_err) > SNT {
                    let isdiffpos = psf_flux > 0.0;
                    let (magpsf, sigmapsf) = flux2mag(psf_flux, psf_flux_err, ZP_AB);
                    (
                        Some(magpsf),
                        Some(sigmapsf),
                        Some(isdiffpos),
                        Some(psf_flux / psf_flux_err),
                    )
                } else {
                    (None, None, None, None)
                }
            }
            _ => (None, None, None, None),
        };

        let diffmaglim = fluxerr2diffmaglim(psf_flux_err, ZP_AB);

        Ok(ForcedPhot {
            dia_forced_source,
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
pub struct LsstAlert {
    #[serde(rename(deserialize = "diaSourceId"))]
    pub candid: i64,
    #[serde(rename(deserialize = "diaSource"))]
    #[serde(deserialize_with = "deserialize_candidate")]
    pub candidate: Candidate,
    #[serde(rename = "prvDiaSources")]
    #[serde(deserialize_with = "deserialize_prv_candidates")]
    pub prv_candidates: Option<Vec<Candidate>>,
    #[serde(rename = "prvDiaForcedSources")]
    #[serde(deserialize_with = "deserialize_prv_forced_sources")]
    pub fp_hists: Option<Vec<ForcedPhot>>,
    // NOTE: the prv_nondetections is missing in version 9 of the schema,
    // and will be reintroduced in a future version
    // #[serde(rename = "prvDiaNondetectionLimits")]
    // #[serde(deserialize_with = "deserialize_prv_nondetections")]
    // pub prv_nondetections: Option<Vec<NonDetection>>,
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

// Deserialize helper functions
fn deserialize_objid<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    // it's an i64 in the avro but we want to have it as a string
    let objid: i64 = <i64 as Deserialize>::deserialize(deserializer)?;
    Ok(objid.to_string())
}

fn deserialize_optional_id<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let objid: Option<i64> = <Option<i64> as Deserialize>::deserialize(deserializer)?;
    match objid {
        Some(0) | None => Ok(None),
        Some(i) => Ok(Some(i.to_string())),
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

fn deserialize_candidate<'de, D>(deserializer: D) -> Result<Candidate, D::Error>
where
    D: Deserializer<'de>,
{
    let dia_source = <DiaSource as Deserialize>::deserialize(deserializer)?;
    Candidate::try_from(dia_source).map_err(serde::de::Error::custom)
}

fn deserialize_prv_candidates<'de, D>(deserializer: D) -> Result<Option<Vec<Candidate>>, D::Error>
where
    D: Deserializer<'de>,
{
    let dia_sources = <Vec<DiaSource> as Deserialize>::deserialize(deserializer)?;
    let candidates = dia_sources
        .into_iter()
        .map(Candidate::try_from)
        .collect::<Result<Vec<Candidate>, AlertError>>()
        .map_err(serde::de::Error::custom)?;
    Ok(Some(candidates))
}

fn deserialize_prv_forced_sources<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<ForcedPhot>>, D::Error>
where
    D: Deserializer<'de>,
{
    let dia_forced_sources = <Vec<DiaForcedSource> as Deserialize>::deserialize(deserializer)?;
    let forced_phots = dia_forced_sources
        .into_iter()
        .map(ForcedPhot::try_from)
        .collect::<Result<Vec<ForcedPhot>, AlertError>>()
        .map_err(serde::de::Error::custom)?;
    Ok(Some(forced_phots))
}

fn deserialize_mjd<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    let mjd = <f64 as Deserialize>::deserialize(deserializer)?;
    Ok(mjd + 2400000.5)
}

impl Alert for LsstAlert {
    fn object_id(&self) -> String {
        self.candidate.object_id.clone()
    }
    fn ra(&self) -> f64 {
        self.candidate.dia_source.ra
    }
    fn dec(&self) -> f64 {
        self.candidate.dia_source.dec
    }
    fn candid(&self) -> i64 {
        self.candid
    }
}

pub struct LsstAlertWorker {
    stream_name: String,
    schema_registry: SchemaRegistry,
    xmatch_configs: Vec<conf::CatalogXmatchConfig>,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<Document>,
    alert_aux_collection: mongodb::Collection<Document>,
    alert_cutout_collection: mongodb::Collection<Document>,
    ztf_alert_aux_collection: mongodb::Collection<Document>,
    decam_alert_aux_collection: mongodb::Collection<Document>,
}

impl LsstAlertWorker {
    #[instrument(skip(self), err)]
    async fn get_survey_matches(&self, ra: f64, dec: f64) -> Result<Document, AlertError> {
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

        Ok(doc! {
            "ZTF": ztf_matches,
            "DECAM": decam_matches,
        })
    }

    #[instrument(skip(self, prv_candidates_doc, fp_hist_doc, xmatches,), err)]
    async fn insert_alert_aux(
        &self,
        object_id: String,
        ra: f64,
        dec: f64,
        prv_candidates_doc: &Vec<Document>,
        fp_hist_doc: &Vec<Document>,
        xmatches: Document,
        survey_matches: &Option<Document>,
        now: f64,
    ) -> Result<(), AlertError> {
        let alert_aux_doc = doc! {
            "_id": object_id,
            "prv_candidates": prv_candidates_doc,
            "fp_hists": fp_hist_doc,
            "cross_matches": xmatches,
            "aliases": survey_matches,
            "created_at": now,
            "updated_at": now,
            "coordinates": {
                "radec_geojson": {
                    "type": "Point",
                    "coordinates": [ra - 180.0, dec],
                },
            },
        };

        self.alert_aux_collection
            .insert_one(alert_aux_doc)
            .await
            .map_err(|e| match *e.kind {
                mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(
                    write_error,
                )) if write_error.code == 11000 => AlertError::AlertAuxExists,
                _ => e.into(),
            })?;
        Ok(())
    }

    #[instrument(skip(self), err)]
    async fn check_alert_aux_exists(&self, object_id: &str) -> Result<bool, AlertError> {
        let alert_aux_exists = self
            .alert_aux_collection
            .count_documents(doc! { "_id": object_id })
            .await?
            > 0;
        Ok(alert_aux_exists)
    }

    #[instrument(skip_all)]
    fn format_prv_candidates_and_fp_hist(
        &self,
        prv_candidates: Option<Vec<Candidate>>,
        candidate_doc: Document,
        fp_hist: Option<Vec<ForcedPhot>>,
    ) -> (Vec<Document>, Vec<Document>) {
        let mut prv_candidates_doc = prv_candidates
            .unwrap_or(vec![])
            .into_iter()
            .map(|x| mongify(&x))
            .collect::<Vec<_>>();
        prv_candidates_doc.push(candidate_doc);

        let fp_hist_doc = fp_hist
            .unwrap_or(vec![])
            .into_iter()
            .map(|x| mongify(&x))
            .collect::<Vec<_>>();

        (prv_candidates_doc, fp_hist_doc)
    }
}

#[async_trait::async_trait]
impl AlertWorker for LsstAlertWorker {
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<LsstAlertWorker, AlertWorkerError> {
        let config_file =
            conf::load_config(&config_path).inspect_err(as_error!("failed to load config"))?;

        let kafka_config = conf::build_kafka_config(&config_file, &Survey::Lsst)
            .inspect_err(as_error!("failed to build kafka config"))?;

        let schema_registry_url = match kafka_config.schema_registry {
            Some(ref url) => url.as_ref(),
            None => LSST_SCHEMA_REGISTRY_URL,
        };

        let xmatch_configs = conf::build_xmatch_configs(&config_file, STREAM_NAME)
            .inspect_err(as_error!("failed to load xmatch config"))?;

        let db: mongodb::Database = conf::build_db(&config_file)
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

    #[instrument(
        skip(
            self,
            ra,
            dec,
            prv_candidates_doc,
            _prv_nondetections_doc,
            fp_hist_doc,
            survey_matches
        ),
        err
    )]
    async fn insert_aux(
        self: &mut Self,
        object_id: &str,
        ra: f64,
        dec: f64,
        prv_candidates_doc: &Vec<Document>,
        _prv_nondetections_doc: &Vec<Document>,
        fp_hist_doc: &Vec<Document>,
        survey_matches: &Option<Document>,
        now: f64,
    ) -> Result<(), AlertError> {
        let xmatches = xmatch(ra, dec, &self.xmatch_configs, &self.db).await?;
        self.insert_alert_aux(
            object_id.into(),
            ra,
            dec,
            prv_candidates_doc,
            fp_hist_doc,
            xmatches,
            survey_matches,
            now,
        )
        .await?;
        Ok(())
    }

    #[instrument(
        skip(
            self,
            prv_candidates_doc,
            _prv_nondetections_doc,
            fp_hist_doc,
            survey_matches
        ),
        err
    )]
    async fn update_aux(
        self: &mut Self,
        object_id: &str,
        prv_candidates_doc: &Vec<Document>,
        _prv_nondetections_doc: &Vec<Document>,
        fp_hist_doc: &Vec<Document>,
        survey_matches: &Option<Document>,
        now: f64,
    ) -> Result<(), AlertError> {
        let update_pipeline = vec![doc! {
            "$set": {
                "prv_candidates": update_timeseries_op("prv_candidates", "jd", prv_candidates_doc),
                "fp_hists": update_timeseries_op("fp_hists", "jd", fp_hist_doc),
                "aliases": survey_matches,
                "updated_at": now,
            }
        }];
        self.alert_aux_collection
            .update_one(doc! { "_id": object_id }, update_pipeline)
            .await?;
        Ok(())
    }

    #[instrument(skip_all, err)]
    async fn process_alert(
        self: &mut Self,
        avro_bytes: &[u8],
    ) -> Result<ProcessAlertStatus, AlertError> {
        let now = Time::now().to_jd();
        let mut alert: LsstAlert = self
            .schema_registry
            .alert_from_avro_bytes(avro_bytes)
            .await
            .inspect_err(as_error!())?;

        let candid = alert.candid();
        let object_id = alert.object_id();
        let ra = alert.ra();
        let dec = alert.dec();

        let prv_candidates = alert.prv_candidates.take();
        let fp_hist = alert.fp_hists.take();

        let candidate_doc = mongify(&alert.candidate);

        let status = self
            .format_and_insert_alert(
                candid,
                &object_id,
                ra,
                dec,
                &candidate_doc,
                now,
                &self.alert_collection,
            )
            .await
            .inspect_err(as_error!())?;
        if let ProcessAlertStatus::Exists(_) = status {
            return Ok(status);
        }

        self.format_and_insert_cutouts(
            candid,
            alert.cutout_science,
            alert.cutout_template,
            alert.cutout_difference,
            &self.alert_cutout_collection,
        )
        .await
        .inspect_err(as_error!())?;
        let alert_aux_exists = self
            .check_alert_aux_exists(&object_id)
            .await
            .inspect_err(as_error!())?;

        let (prv_candidates_doc, fp_hist_doc) =
            self.format_prv_candidates_and_fp_hist(prv_candidates, candidate_doc, fp_hist);

        let survey_matches = Some(
            self.get_survey_matches(ra, dec)
                .await
                .inspect_err(as_error!())?,
        );

        if !alert_aux_exists {
            let result = self
                .insert_aux(
                    &object_id,
                    ra,
                    dec,
                    &prv_candidates_doc,
                    &Vec::new(),
                    &fp_hist_doc,
                    &survey_matches,
                    now,
                )
                .await;
            if let Err(AlertError::AlertAuxExists) = result {
                self.update_aux(
                    &object_id,
                    &prv_candidates_doc,
                    &Vec::new(),
                    &fp_hist_doc,
                    &survey_matches,
                    now,
                )
                .await
                .inspect_err(as_error!())?;
            } else {
                result.inspect_err(as_error!())?;
            }
        } else {
            self.update_aux(
                &object_id,
                &prv_candidates_doc,
                &Vec::new(),
                &fp_hist_doc,
                &survey_matches,
                now,
            )
            .await
            .inspect_err(as_error!())?;
        }

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
        let alert: LsstAlert = alert.unwrap();
        assert_eq!(alert.candid, candid);
        assert_eq!(alert.candidate.object_id, object_id);
        assert!((alert.candidate.dia_source.ra - ra).abs() < 1e-6);
        assert!((alert.candidate.dia_source.dec - dec).abs() < 1e-6);
        assert!((alert.candidate.dia_source.jd - 2460961.733092).abs() < 1e-6);
        assert!((alert.candidate.magpsf - 23.674994).abs() < 1e-6);
        assert!((alert.candidate.sigmapsf - 0.217043).abs() < 1e-6);
        assert!((alert.candidate.diffmaglim - 23.675514).abs() < 1e-5);
        assert!(alert.candidate.snr - 5.002406 < 1e-6);
        assert_eq!(alert.candidate.isdiffpos, false);
        assert_eq!(alert.candidate.dia_source.band.unwrap(), "r");
        // TODO: check prv_candidates and forced photometry once we have alerts
        //       where they aren't empty
        // TODO: check non detections once these are available in the schema
    }
}
