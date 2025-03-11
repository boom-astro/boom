use apache_avro::{from_avro_datum, from_value, Schema};
use flare::Time;
use mongodb::bson::doc;
use std::collections::HashMap;
use tracing::trace;

use crate::{
    alert::base::{AlertError, AlertWorker, AlertWorkerError, SchemaRegistryError},
    conf,
    utils::{
        db::{cutout2bsonbinary, get_coordinates, mongify},
        spatial::xmatch,
    },
};

const _MAGIC_BYTE: u8 = 0;
const _SCHEMA_REGISTRY_URL: &str = "https://usdf-alert-schemas-dev.slac.stanford.edu";

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct DiaSource {
    /// Unique identifier of this DiaSource.
    #[serde(rename(deserialize = "diaSourceId", serialize = "candid"))]
    pub candid: i64,
    /// Id of the visit where this diaSource was measured.
    pub visit: i64,
    /// Id of the detector where this diaSource was measured. Datatype short instead of byte because of DB concerns about unsigned bytes.
    pub detector: i32,
    /// Id of the diaObject this source was associated with, if any. If not, it is set to NULL (each diaSource will be associated with either a diaObject or ssObject).
    #[serde(rename(deserialize = "diaObjectId", serialize = "objectId"))]
    pub object_id: Option<i64>,
    /// Id of the ssObject this source was associated with, if any. If not, it is set to NULL (each diaSource will be associated with either a diaObject or ssObject).
    #[serde(rename = "ssObjectId")]
    pub ss_object_id: Option<i64>,
    /// Id of the parent diaSource this diaSource has been deblended from, if any.
    #[serde(rename = "parentDiaSourceId")]
    pub parent_dia_source_id: Option<i64>,
    /// Effective mid-visit time for this diaSource, expressed as Modified Julian Date, International Atomic Time.
    #[serde(rename(deserialize = "midpointMjdTai", serialize = "mjd"))]
    pub mjd: f64,
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
    /// Source was detected as significantly negative.
    pub is_negative: Option<bool>,
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
    /// The signal-to-noise ratio at which this source was detected in the difference image.
    pub snr: Option<f32>,
    /// Flux for Point Source model. Note this actually measures the flux difference between the template and the visit image.
    #[serde(rename = "psfFlux")]
    pub psf_flux: Option<f32>,
    /// Uncertainty of psfFlux.
    #[serde(rename = "psfFluxErr")]
    pub psf_flux_err: Option<f32>,
    /// Right ascension coordinate of centroid for point source model.
    #[serde(rename = "psfRa")]
    pub psf_ra: Option<f64>,
    /// Uncertainty of psfRa.
    #[serde(rename = "psfRaErr")]
    pub psf_ra_err: Option<f32>,
    /// Declination coordinate of centroid for point source model.
    #[serde(rename = "psfDec")]
    pub psf_dec: Option<f64>,
    /// Uncertainty of psfDec.
    #[serde(rename = "psfDecErr")]
    pub psf_dec_err: Option<f32>,
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
    /// Forced PSF photometry on science image failed. Another forced_PsfFlux flag field should also be set to provide more information.
    #[serde(rename = "forced_PsfFlux_flag")]
    pub forced_psf_flux_flag: Option<bool>,
    /// Forced PSF flux on science image was too close to the edge of the image to use the full PSF model.
    #[serde(rename = "forced_PsfFlux_flag_edge")]
    pub forced_psf_flux_flag_edge: Option<bool>,
    /// Forced PSF flux not enough non-rejected pixels in data to attempt the fit.
    #[serde(rename = "forced_PsfFlux_flag_noGoodPixels")]
    pub forced_psf_flux_flag_no_good_pixels: Option<bool>,
    /// Calibrated flux for Point Source model centered on radec but measured on the difference of snaps comprising this visit.
    #[serde(rename = "snapDiffFlux")]
    pub snap_diff_flux: Option<f32>,
    /// Estimated uncertainty of snapDiffFlux.
    #[serde(rename = "snapDiffFluxErr")]
    pub snap_diff_flux_err: Option<f32>,
    /// Estimated sky background at the position (centroid) of the object.
    #[serde(rename = "fpBkgd")]
    pub fp_bkgd: Option<f32>,
    /// Estimated uncertainty of fpBkgd.
    #[serde(rename = "fpBkgdErr")]
    pub fp_bkgd_err: Option<f32>,
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
    /// General pixel flags failure; set if anything went wrong when setting pixels flags from this footprint's mask. This implies that some pixelFlags for this source may be incorrectly set to False.
    #[serde(rename = "pixelFlags")]
    pub pixel_flags: Option<bool>,
    /// Bad pixel in the DiaSource footprint.
    #[serde(rename = "pixelFlags_bad")]
    pub pixel_flags_bad: Option<bool>,
    /// Cosmic ray in the DiaSource footprint.
    #[serde(rename = "pixelFlags_cr")]
    pub pixel_flags_cr: Option<bool>,
    /// Cosmic ray in the 3x3 region around the centroid.
    #[serde(rename = "pixelFlags_crCenter")]
    pub pixel_flags_cr_center: Option<bool>,
    /// Some of the source footprint is outside usable exposure region (masked EDGE or NO_DATA, or centroid off image).
    #[serde(rename = "pixelFlags_edge")]
    pub pixel_flags_edge: Option<bool>,
    /// Interpolated pixel in the DiaSource footprint.
    #[serde(rename = "pixelFlags_interpolated")]
    pub pixel_flags_interpolated: Option<bool>,
    /// Interpolated pixel in the 3x3 region around the centroid.
    #[serde(rename = "pixelFlags_interpolatedCenter")]
    pub pixel_flags_interpolated_center: Option<bool>,
    /// DiaSource center is off image.
    #[serde(rename = "pixelFlags_offimage")]
    pub pixel_flags_offimage: Option<bool>,
    /// Saturated pixel in the DiaSource footprint.
    #[serde(rename = "pixelFlags_saturated")]
    pub pixel_flags_saturated: Option<bool>,
    /// Saturated pixel in the 3x3 region around the centroid.
    #[serde(rename = "pixelFlags_saturatedCenter")]
    pub pixel_flags_saturated_center: Option<bool>,
    /// DiaSource's footprint includes suspect pixels.
    #[serde(rename = "pixelFlags_suspect")]
    pub pixel_flags_suspect: Option<bool>,
    /// Suspect pixel in the 3x3 region around the centroid.
    #[serde(rename = "pixelFlags_suspectCenter")]
    pub pixel_flags_suspect_center: Option<bool>,
    /// Streak in the DiaSource footprint.
    #[serde(rename = "pixelFlags_streak")]
    pub pixel_flags_streak: Option<bool>,
    /// Streak in the 3x3 region around the centroid.
    #[serde(rename = "pixelFlags_streakCenter")]
    pub pixel_flags_streak_center: Option<bool>,
    /// Injection in the DiaSource footprint.
    #[serde(rename = "pixelFlags_injected")]
    pub pixel_flags_injected: Option<bool>,
    /// Injection in the 3x3 region around the centroid.
    #[serde(rename = "pixelFlags_injectedCenter")]
    pub pixel_flags_injected_center: Option<bool>,
    /// Template injection in the DiaSource footprint.
    #[serde(rename = "pixelFlags_injected_template")]
    pub pixel_flags_injected_template: Option<bool>,
    /// Template injection in the 3x3 region around the centroid.
    #[serde(rename = "pixelFlags_injected_templateCenter")]
    pub pixel_flags_injected_template_center: Option<bool>,

    // these fields are computed later, from the psf_flux and science_flux (the latter is missing in the current LSST schema)
    pub magpsf: Option<f32>,
    pub sigmapsf: Option<f32>,
    pub diffmaglim: Option<f32>,
    pub isdiffpos: Option<bool>,
    // pub magdc: Option<f32>,
    // pub sigmadc: Option<f32>,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct DiaObject {
    /// Unique identifier of this DiaObject.
    #[serde(rename(deserialize = "diaObjectId", serialize = "objectId"))]
    pub object_id: i64,
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
    /// Time at which the object was at a position ra/dec, expressed as Modified Julian Date, International Atomic Time.
    #[serde(rename = "radecMjdTai")]
    pub radec_mjd_tai: Option<f64>,
    /// Proper motion in right ascension.
    #[serde(rename = "pmRa")]
    pub pm_ra: Option<f32>,
    /// Uncertainty of pmRa.
    #[serde(rename = "pmRaErr")]
    pub pm_ra_err: Option<f32>,
    /// Proper motion of declination.
    #[serde(rename = "pmDec")]
    pub pm_dec: Option<f32>,
    /// Uncertainty of pmDec.
    #[serde(rename = "pmDecErr")]
    pub pm_dec_err: Option<f32>,
    /// Parallax.
    pub parallax: Option<f32>,
    /// Uncertainty of parallax.
    #[serde(rename = "parallaxErr")]
    pub parallax_err: Option<f32>,
    /// Chi^2 static of the model fit.
    #[serde(rename = "pmParallaxChi2")]
    pub pm_parallax_chi2: Option<f32>,
    /// The number of data points used to fit the model.
    #[serde(rename = "pmParallaxNdata")]
    pub pm_parallax_ndata: Option<i32>,
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
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct DiaNondetectionLimit {
    #[serde(rename = "ccdVisitId")]
    pub ccd_visit_id: i64,
    #[serde(rename(deserialize = "midpointMjdTai", serialize = "mjd"))]
    pub mjd: f64,
    pub band: String,
    #[serde(rename = "diaNoise")]
    pub dia_noise: f32,

    // these fields are computed later, from the dia_noise
    pub diffmaglim: Option<f32>,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct DiaForcedSource {
    /// Unique id.
    #[serde(rename = "diaForcedSourceId")]
    pub dia_forced_source_id: i64,
    /// Id of the DiaObject that this DiaForcedSource was associated with.
    #[serde(rename(deserialize = "diaObjectId", serialize = "objectId"))]
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
    #[serde(rename(deserialize = "midpointMjdTai", serialize = "mjd"))]
    pub mjd: f64,
    /// Filter band this source was observed with.
    pub band: Option<String>,

    // these fields are computed later, from the psf_flux
    pub magpsf: Option<f32>,
    pub sigmapsf: Option<f32>,
    pub diffmaglim: Option<f32>,
    pub isdiffpos: Option<bool>,
    pub snr: Option<f32>,
}

/// Rubin Avro alert schema v7.3
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct LsstAlert {
    #[serde(rename(deserialize = "alertId"))]
    pub candid: i64,
    #[serde(rename(deserialize = "diaSource"))]
    pub candidate: DiaSource,
    #[serde(rename = "prvDiaSources")]
    pub prv_candidates: Option<Vec<DiaSource>>,
    #[serde(rename = "prvDiaForcedSources")]
    pub fp_hists: Option<Vec<DiaForcedSource>>,
    #[serde(rename = "prvDiaNondetectionLimits")]
    pub prv_nondetections: Option<Vec<DiaNondetectionLimit>>,
    #[serde(rename = "diaObject")]
    pub dia_object: Option<DiaObject>,
    #[serde(rename = "cutoutDifference")]
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    pub cutout_difference: Option<Vec<u8>>,
    #[serde(rename = "cutoutScience")]
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    pub cutout_science: Option<Vec<u8>>,
    #[serde(rename = "cutoutTemplate")]
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    pub cutout_template: Option<Vec<u8>>,
}

fn flux2mag(flux: f32, flux_err: f32) -> (f32, f32, f32) {
    // convert from nJy to Jy
    let flux = flux.abs() * 1e-9;
    let flux_err = flux_err * 1e-9;

    // convert from Jy to AB mag
    let mag = -2.5 * (flux).log10() + 8.9;
    let sigma = 1.0857362047581294 * (flux_err / flux);
    let diffmaglim = -2.5 * (5.0 * flux_err).log10() + 8.9;

    (mag, sigma, diffmaglim)
}

fn fluxerr2diffmaglim(flux_err: f32) -> f32 {
    // convert from nJy to Jy
    let flux_err = flux_err * 1e-9;

    // convert from Jy to AB mag
    -2.5 * (5.0 * flux_err).log10() + 8.9
}

impl DiaSource {
    pub fn add_mag_data(&mut self) -> Result<(), AlertError> {
        let (magpsf, sigmapsf, diffmaglim) = flux2mag(
            self.psf_flux.ok_or(AlertError::MissingFluxPSF)?.abs(),
            self.psf_flux_err.ok_or(AlertError::MissingFluxPSF)?,
        );
        self.magpsf = Some(magpsf);
        self.sigmapsf = Some(sigmapsf);
        self.diffmaglim = Some(diffmaglim);
        self.isdiffpos = Some(self.psf_flux.unwrap() > 0.0);
        self.snr = Some(self.psf_flux.unwrap() / self.psf_flux_err.unwrap());
        Ok(())
    }
}

impl DiaForcedSource {
    pub fn add_mag_data(&mut self) -> Result<(), AlertError> {
        let (magpsf, sigmapsf, diffmaglim) = flux2mag(
            self.psf_flux.ok_or(AlertError::MissingFluxPSF)?.abs(),
            self.psf_flux_err.ok_or(AlertError::MissingFluxPSF)?,
        );
        self.magpsf = Some(magpsf);
        self.sigmapsf = Some(sigmapsf);
        self.diffmaglim = Some(diffmaglim);
        self.isdiffpos = Some(self.psf_flux.unwrap() > 0.0);
        self.snr = Some(self.psf_flux.unwrap() / self.psf_flux_err.unwrap());
        Ok(())
    }
}

impl DiaNondetectionLimit {
    pub fn add_mag_data(&mut self) -> Result<(), AlertError> {
        self.diffmaglim = Some(fluxerr2diffmaglim(self.dia_noise));
        Ok(())
    }
}

pub struct LsstAlertWorker {
    stream_name: String,
    client: reqwest::Client,
    cache: HashMap<String, Schema>,
    xmatch_configs: Vec<conf::CatalogXmatchConfig>,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<mongodb::bson::Document>,
    alert_aux_collection: mongodb::Collection<mongodb::bson::Document>,
    alert_cutout_collection: mongodb::Collection<mongodb::bson::Document>,
}

impl LsstAlertWorker {
    async fn get_subjects(&self) -> Result<Vec<String>, SchemaRegistryError> {
        let response = self
            .client
            .get(&format!("{}/subjects", _SCHEMA_REGISTRY_URL))
            .send()
            .await
            .map_err(SchemaRegistryError::ConnectionError)?;

        let response = response
            .json::<Vec<String>>()
            .await
            .map_err(SchemaRegistryError::ParsingError)?;

        Ok(response)
    }

    async fn get_versions(&self, subject: &str) -> Result<Vec<u32>, SchemaRegistryError> {
        // first we check if the subject exists
        let subjects = self.get_subjects().await?;
        if !subjects.contains(&subject.to_string()) {
            return Err(SchemaRegistryError::InvalidSubject);
        }

        let response = self
            .client
            .get(&format!(
                "{}/subjects/{}/versions",
                _SCHEMA_REGISTRY_URL, subject
            ))
            .send()
            .await
            .map_err(SchemaRegistryError::ConnectionError)?;

        let response = response
            .json::<Vec<u32>>()
            .await
            .map_err(SchemaRegistryError::ParsingError)?;

        Ok(response)
    }

    async fn _get_schema_by_id(
        &self,
        subject: &str,
        version: u32,
    ) -> Result<Schema, SchemaRegistryError> {
        let versions = self.get_versions(subject).await?;
        if !versions.contains(&version) {
            return Err(SchemaRegistryError::InvalidVersion);
        }

        let response = self
            .client
            .get(&format!(
                "{}/subjects/{}/versions/{}",
                _SCHEMA_REGISTRY_URL, subject, version
            ))
            .send()
            .await
            .map_err(SchemaRegistryError::ConnectionError)?;

        let response = response
            .json::<serde_json::Value>()
            .await
            .map_err(SchemaRegistryError::ParsingError)?;

        let schema_str = response["schema"]
            .as_str()
            .ok_or(SchemaRegistryError::InvalidResponse)?;

        let schema = Schema::parse_str(schema_str).map_err(SchemaRegistryError::InvalidSchema)?;
        Ok(schema)
    }

    async fn get_schema(
        &mut self,
        subject: &str,
        version: u32,
    ) -> Result<&Schema, SchemaRegistryError> {
        let key = format!("{}:{}", subject, version);
        if !self.cache.contains_key(&key) {
            let schema = self._get_schema_by_id(subject, version).await?;
            self.cache.insert(key.clone(), schema);
        }
        Ok(self.cache.get(&key).unwrap())
    }

    pub async fn alert_from_avro_bytes(
        self: &mut Self,
        avro_bytes: &[u8],
    ) -> Result<LsstAlert, AlertError> {
        let magic = avro_bytes[0];
        if magic != _MAGIC_BYTE {
            return Err(AlertError::MagicBytesError);
        }
        let schema_id =
            u32::from_be_bytes([avro_bytes[1], avro_bytes[2], avro_bytes[3], avro_bytes[4]]);
        let schema = self.get_schema("alert-packet", schema_id).await?;

        let mut slice = &avro_bytes[5..];
        let value = from_avro_datum(&schema, &mut slice, None).map_err(AlertError::DecodeError)?;

        let alert: LsstAlert = from_value::<LsstAlert>(&value).map_err(AlertError::DecodeError)?;

        Ok(alert)
    }
}

#[async_trait::async_trait]
impl AlertWorker for LsstAlertWorker {
    async fn new(config_path: &str) -> Result<LsstAlertWorker, AlertWorkerError> {
        let stream_name = "LSST".to_string();

        let config_file = conf::load_config(&config_path)?;

        let xmatch_configs = conf::build_xmatch_configs(&config_file, "LSST")?;

        let db: mongodb::Database = conf::build_db(&config_file).await?;

        let alert_collection = db.collection(&format!("{}_alerts", stream_name));
        let alert_aux_collection = db.collection(&format!("{}_alerts_aux", stream_name));
        let alert_cutout_collection = db.collection(&format!("{}_alerts_cutouts", stream_name));

        let worker = LsstAlertWorker {
            stream_name: stream_name.clone(),
            client: reqwest::Client::new(),
            cache: HashMap::new(),
            xmatch_configs,
            db,
            alert_collection,
            alert_aux_collection,
            alert_cutout_collection,
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
        format!("{}_alerts_filter_queue", self.stream_name)
    }

    async fn process_alert(self: &mut Self, avro_bytes: &[u8]) -> Result<i64, AlertError> {
        let now = Time::now().to_jd();

        let mut alert = self.alert_from_avro_bytes(avro_bytes).await?;

        let start = std::time::Instant::now();

        let prv_candidates = alert.prv_candidates.take();
        let fp_hist = alert.fp_hists.take();
        let prv_nondetections = alert.prv_nondetections.take();

        let candid = alert.candid;
        let object_id = alert
            .candidate
            .object_id
            .ok_or(AlertError::MissingObjectId)?;
        let ra = alert.candidate.ra;
        let dec = alert.candidate.dec;

        alert.candidate.add_mag_data()?;

        let candidate_doc = mongify(&alert.candidate);

        let alert_doc = doc! {
            "_id": &candid,
            "objectId": &object_id,
            "candidate": &candidate_doc,
            "coordinates": get_coordinates(ra, dec),
            "created_at": now,
            "updated_at": now,
        };

        self.alert_collection
            .insert_one(alert_doc)
            .await
            .map_err(|e| match *e.kind {
                mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(
                    write_error,
                )) if write_error.code == 11000 => AlertError::AlertExists,
                _ => AlertError::InsertAlertError(e),
            })?;

        trace!("Formatting & Inserting alert: {:?}", start.elapsed());

        let start = std::time::Instant::now();

        let cutout_doc = doc! {
            "_id": &candid,
            "cutoutScience": cutout2bsonbinary(alert.cutout_science.ok_or(AlertError::MissingCutout)?),
            "cutoutTemplate": cutout2bsonbinary(alert.cutout_template.ok_or(AlertError::MissingCutout)?),
            "cutoutDifference": cutout2bsonbinary(alert.cutout_difference.ok_or(AlertError::MissingCutout)?),
        };

        self.alert_cutout_collection
            .insert_one(cutout_doc)
            .await
            .map_err(AlertError::InsertCutoutError)?;

        trace!("Formatting & Inserting cutout: {:?}", start.elapsed());

        let start = std::time::Instant::now();

        let alert_aux_exists = self
            .alert_aux_collection
            .count_documents(doc! { "_id": &object_id })
            .await
            .map_err(AlertError::FindObjectIdError)?
            > 0;

        trace!("Checking if alert_aux exists: {:?}", start.elapsed());

        let start = std::time::Instant::now();

        let mut prv_candidates_doc = prv_candidates
            .unwrap_or(vec![])
            .into_iter()
            .map(|mut x| {
                if let Err(e) = x.add_mag_data() {
                    return Err(e);
                }
                Ok(mongify(&x))
            })
            .collect::<Result<Vec<_>, _>>()?;
        prv_candidates_doc.push(candidate_doc);

        let fp_hist_doc = fp_hist
            .unwrap_or(vec![])
            .into_iter()
            .map(|mut x| {
                if let Err(e) = x.add_mag_data() {
                    return Err(e);
                }
                Ok(mongify(&x))
            })
            .collect::<Result<Vec<_>, _>>()?;

        let non_detections_doc = prv_nondetections
            .unwrap_or(vec![])
            .into_iter()
            .map(|mut x| {
                if let Err(e) = x.add_mag_data() {
                    return Err(e);
                }
                Ok(mongify(&x))
            })
            .collect::<Result<Vec<_>, _>>()?;

        trace!("Formatting prv_candidates & fp_hist: {:?}", start.elapsed());

        if !alert_aux_exists {
            let start = std::time::Instant::now();
            let alert_aux_doc = doc! {
                "_id": &object_id,
                "prv_candidates": prv_candidates_doc,
                "prv_nondetections": non_detections_doc,
                "fp_hists": fp_hist_doc,
                "cross_matches": xmatch(ra, dec, &self.xmatch_configs, &self.db).await,
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
                .map_err(AlertError::InsertAuxAlertError)?;

            trace!("Inserting alert_aux: {:?}", start.elapsed());
        } else {
            let start = std::time::Instant::now();
            let update_doc = doc! {
                "$addToSet": {
                    "prv_candidates": { "$each": prv_candidates_doc },
                    "prv_nondetections": { "$each": non_detections_doc },
                    "fp_hists": { "$each": fp_hist_doc }
                },
                "$set": {
                    "updated_at": now,
                }
            };

            self.alert_aux_collection
                .update_one(doc! { "_id": &object_id }, update_doc)
                .await
                .map_err(AlertError::UpdateAuxAlertError)?;

            trace!("Updating alert_aux: {:?}", start.elapsed());
        }

        Ok(candid)
    }
}
