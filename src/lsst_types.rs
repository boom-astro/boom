use apache_avro::from_value;
use apache_avro::{from_avro_datum, Schema};
use std::io::Read;
use tracing::error;

const _MAGIC_BYTE: u8 = 0;
const _SCHEMA_REGISTRY_ID: u32 = 703;

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct SsObject {
    /// Unique identifier.
    #[serde(rename = "ssObjectId")]
    pub ss_object_id: i64,
    /// The date the LSST first linked and submitted the discovery observations to the MPC. May be NULL if not an LSST discovery. The date format will follow general LSST conventions (MJD TAI, at the moment).
    #[serde(rename = "discoverySubmissionDate")]
    #[serde(default = "default_ssobject_discovery_submission_date")]
    pub discovery_submission_date: Option<f64>,
    /// The time of the first LSST observation of this object (could be precovered) as Modified Julian Date, International Atomic Time.
    #[serde(rename = "firstObservationDate")]
    #[serde(default = "default_ssobject_first_observation_date")]
    pub first_observation_date: Option<f64>,
    /// Arc of LSST observations.
    #[serde(default = "default_ssobject_arc")]
    pub arc: Option<f32>,
    /// Number of LSST observations of this object.
    #[serde(rename = "numObs")]
    #[serde(default = "default_ssobject_num_obs")]
    pub num_obs: Option<i32>,
    /// Minimum orbit intersection distance to Earth.
    #[serde(rename = "MOID")]
    #[serde(default = "default_ssobject_moid")]
    pub moid: Option<f32>,
    /// True anomaly of the MOID point.
    #[serde(rename = "MOIDTrueAnomaly")]
    #[serde(default = "default_ssobject_moid_true_anomaly")]
    pub moid_true_anomaly: Option<f32>,
    /// Ecliptic longitude of the MOID point.
    #[serde(rename = "MOIDEclipticLongitude")]
    #[serde(default = "default_ssobject_moid_ecliptic_longitude")]
    pub moid_ecliptic_longitude: Option<f32>,
    /// DeltaV at the MOID point.
    #[serde(rename = "MOIDDeltaV")]
    #[serde(default = "default_ssobject_moid_delta_v")]
    pub moid_delta_v: Option<f32>,
    /// Best fit absolute magnitude (u band).
    #[serde(rename = "u_H")]
    #[serde(default = "default_ssobject_u_h")]
    pub u_h: Option<f32>,
    /// Best fit G12 slope parameter (u band).
    #[serde(rename = "u_G12")]
    #[serde(default = "default_ssobject_u_g12")]
    pub u_g12: Option<f32>,
    /// Uncertainty of H (u band).
    #[serde(rename = "u_HErr")]
    #[serde(default = "default_ssobject_u_h_err")]
    pub u_h_err: Option<f32>,
    /// Uncertainty of G12 (u band).
    #[serde(rename = "u_G12Err")]
    #[serde(default = "default_ssobject_u_g12_err")]
    pub u_g12_err: Option<f32>,
    /// H-G12 covariance (u band).
    #[serde(rename = "u_H_u_G12_Cov")]
    #[serde(default = "default_ssobject_u_h_u_g12_cov")]
    pub u_h_u_g12_cov: Option<f32>,
    /// Chi^2 statistic of the phase curve fit (u band).
    #[serde(rename = "u_Chi2")]
    #[serde(default = "default_ssobject_u_chi2")]
    pub u_chi2: Option<f32>,
    /// The number of data points used to fit the phase curve (u band).
    #[serde(rename = "u_Ndata")]
    #[serde(default = "default_ssobject_u_ndata")]
    pub u_ndata: Option<i32>,
    /// Best fit absolute magnitude (g band).
    #[serde(rename = "g_H")]
    #[serde(default = "default_ssobject_g_h")]
    pub g_h: Option<f32>,
    /// Best fit G12 slope parameter (g band).
    #[serde(rename = "g_G12")]
    #[serde(default = "default_ssobject_g_g12")]
    pub g_g12: Option<f32>,
    /// Uncertainty of H (g band).
    #[serde(rename = "g_HErr")]
    #[serde(default = "default_ssobject_g_h_err")]
    pub g_h_err: Option<f32>,
    /// Uncertainty of G12 (g band).
    #[serde(rename = "g_G12Err")]
    #[serde(default = "default_ssobject_g_g12_err")]
    pub g_g12_err: Option<f32>,
    /// H-G12 covariance (g band).
    #[serde(rename = "g_H_g_G12_Cov")]
    #[serde(default = "default_ssobject_g_h_g_g12_cov")]
    pub g_h_g_g12_cov: Option<f32>,
    /// Chi^2 statistic of the phase curve fit (g band).
    #[serde(rename = "g_Chi2")]
    #[serde(default = "default_ssobject_g_chi2")]
    pub g_chi2: Option<f32>,
    /// The number of data points used to fit the phase curve (g band).
    #[serde(rename = "g_Ndata")]
    #[serde(default = "default_ssobject_g_ndata")]
    pub g_ndata: Option<i32>,
    /// Best fit absolute magnitude (r band).
    #[serde(rename = "r_H")]
    #[serde(default = "default_ssobject_r_h")]
    pub r_h: Option<f32>,
    /// Best fit G12 slope parameter (r band).
    #[serde(rename = "r_G12")]
    #[serde(default = "default_ssobject_r_g12")]
    pub r_g12: Option<f32>,
    /// Uncertainty of H (r band).
    #[serde(rename = "r_HErr")]
    #[serde(default = "default_ssobject_r_h_err")]
    pub r_h_err: Option<f32>,
    /// Uncertainty of G12 (r band).
    #[serde(rename = "r_G12Err")]
    #[serde(default = "default_ssobject_r_g12_err")]
    pub r_g12_err: Option<f32>,
    /// H-G12 covariance (r band).
    #[serde(rename = "r_H_r_G12_Cov")]
    #[serde(default = "default_ssobject_r_h_r_g12_cov")]
    pub r_h_r_g12_cov: Option<f32>,
    /// Chi^2 statistic of the phase curve fit (r band).
    #[serde(rename = "r_Chi2")]
    #[serde(default = "default_ssobject_r_chi2")]
    pub r_chi2: Option<f32>,
    /// The number of data points used to fit the phase curve (r band).
    #[serde(rename = "r_Ndata")]
    #[serde(default = "default_ssobject_r_ndata")]
    pub r_ndata: Option<i32>,
    /// Best fit absolute magnitude (i band).
    #[serde(rename = "i_H")]
    #[serde(default = "default_ssobject_i_h")]
    pub i_h: Option<f32>,
    /// Best fit G12 slope parameter (i band).
    #[serde(rename = "i_G12")]
    #[serde(default = "default_ssobject_i_g12")]
    pub i_g12: Option<f32>,
    /// Uncertainty of H (i band).
    #[serde(rename = "i_HErr")]
    #[serde(default = "default_ssobject_i_h_err")]
    pub i_h_err: Option<f32>,
    /// Uncertainty of G12 (i band).
    #[serde(rename = "i_G12Err")]
    #[serde(default = "default_ssobject_i_g12_err")]
    pub i_g12_err: Option<f32>,
    /// H-G12 covariance (i band).
    #[serde(rename = "i_H_i_G12_Cov")]
    #[serde(default = "default_ssobject_i_h_i_g12_cov")]
    pub i_h_i_g12_cov: Option<f32>,
    /// Chi^2 statistic of the phase curve fit (i band).
    #[serde(rename = "i_Chi2")]
    #[serde(default = "default_ssobject_i_chi2")]
    pub i_chi2: Option<f32>,
    /// The number of data points used to fit the phase curve (i band).
    #[serde(rename = "i_Ndata")]
    #[serde(default = "default_ssobject_i_ndata")]
    pub i_ndata: Option<i32>,
    /// Best fit absolute magnitude (z band).
    #[serde(rename = "z_H")]
    #[serde(default = "default_ssobject_z_h")]
    pub z_h: Option<f32>,
    /// Best fit G12 slope parameter (z band).
    #[serde(rename = "z_G12")]
    #[serde(default = "default_ssobject_z_g12")]
    pub z_g12: Option<f32>,
    /// Uncertainty of H (z band).
    #[serde(rename = "z_HErr")]
    #[serde(default = "default_ssobject_z_h_err")]
    pub z_h_err: Option<f32>,
    /// Uncertainty of G12 (z band).
    #[serde(rename = "z_G12Err")]
    #[serde(default = "default_ssobject_z_g12_err")]
    pub z_g12_err: Option<f32>,
    /// H-G12 covariance (z band).
    #[serde(rename = "z_H_z_G12_Cov")]
    #[serde(default = "default_ssobject_z_h_z_g12_cov")]
    pub z_h_z_g12_cov: Option<f32>,
    /// Chi^2 statistic of the phase curve fit (z band).
    #[serde(rename = "z_Chi2")]
    #[serde(default = "default_ssobject_z_chi2")]
    pub z_chi2: Option<f32>,
    /// The number of data points used to fit the phase curve (z band).
    #[serde(rename = "z_Ndata")]
    #[serde(default = "default_ssobject_z_ndata")]
    pub z_ndata: Option<i32>,
    /// Best fit absolute magnitude (y band).
    #[serde(rename = "y_H")]
    #[serde(default = "default_ssobject_y_h")]
    pub y_h: Option<f32>,
    /// Best fit G12 slope parameter (y band).
    #[serde(rename = "y_G12")]
    #[serde(default = "default_ssobject_y_g12")]
    pub y_g12: Option<f32>,
    /// Uncertainty of H (y band).
    #[serde(rename = "y_HErr")]
    #[serde(default = "default_ssobject_y_h_err")]
    pub y_h_err: Option<f32>,
    /// Uncertainty of G12 (y band).
    #[serde(rename = "y_G12Err")]
    #[serde(default = "default_ssobject_y_g12_err")]
    pub y_g12_err: Option<f32>,
    /// H-G12 covariance (y band).
    #[serde(rename = "y_H_y_G12_Cov")]
    #[serde(default = "default_ssobject_y_h_y_g12_cov")]
    pub y_h_y_g12_cov: Option<f32>,
    /// Chi^2 statistic of the phase curve fit (y band).
    #[serde(rename = "y_Chi2")]
    #[serde(default = "default_ssobject_y_chi2")]
    pub y_chi2: Option<f32>,
    /// The number of data points used to fit the phase curve (y band).
    #[serde(rename = "y_Ndata")]
    #[serde(default = "default_ssobject_y_ndata")]
    pub y_ndata: Option<i32>,
    /// median `extendedness` value from the DIASource.
    #[serde(rename = "medianExtendedness")]
    #[serde(default = "default_ssobject_median_extendedness")]
    pub median_extendedness: Option<f32>,
}

#[inline(always)]
fn default_ssobject_discovery_submission_date() -> Option<f64> {
    None
}

#[inline(always)]
fn default_ssobject_first_observation_date() -> Option<f64> {
    None
}

#[inline(always)]
fn default_ssobject_arc() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_num_obs() -> Option<i32> {
    None
}

#[inline(always)]
fn default_ssobject_moid() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_moid_true_anomaly() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_moid_ecliptic_longitude() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_moid_delta_v() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_u_h() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_u_g12() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_u_h_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_u_g12_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_u_h_u_g12_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_u_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_u_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_ssobject_g_h() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_g_g12() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_g_h_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_g_g12_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_g_h_g_g12_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_g_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_g_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_ssobject_r_h() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_r_g12() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_r_h_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_r_g12_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_r_h_r_g12_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_r_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_r_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_ssobject_i_h() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_i_g12() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_i_h_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_i_g12_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_i_h_i_g12_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_i_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_i_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_ssobject_z_h() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_z_g12() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_z_h_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_z_g12_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_z_h_z_g12_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_z_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_z_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_ssobject_y_h() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_y_g12() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_y_h_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_y_g12_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_y_h_y_g12_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_y_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_ssobject_y_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_ssobject_median_extendedness() -> Option<f32> {
    None
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct DiaSource {
    /// Unique identifier of this DiaSource.
    #[serde(rename = "diaSourceId")]
    pub dia_source_id: i64,
    /// Id of the visit where this diaSource was measured.
    pub visit: i64,
    /// Id of the detector where this diaSource was measured. Datatype short instead of byte because of DB concerns about unsigned bytes.
    pub detector: i32,
    /// Id of the diaObject this source was associated with, if any. If not, it is set to NULL (each diaSource will be associated with either a diaObject or ssObject).
    #[serde(rename = "diaObjectId")]
    #[serde(default = "default_diasource_dia_object_id")]
    pub dia_object_id: Option<i64>,
    /// Id of the ssObject this source was associated with, if any. If not, it is set to NULL (each diaSource will be associated with either a diaObject or ssObject).
    #[serde(rename = "ssObjectId")]
    #[serde(default = "default_diasource_ss_object_id")]
    pub ss_object_id: Option<i64>,
    /// Id of the parent diaSource this diaSource has been deblended from, if any.
    #[serde(rename = "parentDiaSourceId")]
    #[serde(default = "default_diasource_parent_dia_source_id")]
    pub parent_dia_source_id: Option<i64>,
    /// Effective mid-visit time for this diaSource, expressed as Modified Julian Date, International Atomic Time.
    #[serde(rename = "midpointMjdTai")]
    pub midpoint_mjd_tai: f64,
    /// Right ascension coordinate of the center of this diaSource.
    pub ra: f64,
    /// Uncertainty of ra.
    #[serde(rename = "raErr")]
    #[serde(default = "default_diasource_ra_err")]
    pub ra_err: Option<f32>,
    /// Declination coordinate of the center of this diaSource.
    pub dec: f64,
    /// Uncertainty of dec.
    #[serde(rename = "decErr")]
    #[serde(default = "default_diasource_dec_err")]
    pub dec_err: Option<f32>,
    /// Covariance between ra and dec.
    #[serde(rename = "ra_dec_Cov")]
    #[serde(default = "default_diasource_ra_dec_cov")]
    pub ra_dec_cov: Option<f32>,
    /// x position computed by a centroiding algorithm.
    pub x: f32,
    /// Uncertainty of x.
    #[serde(rename = "xErr")]
    #[serde(default = "default_diasource_x_err")]
    pub x_err: Option<f32>,
    /// y position computed by a centroiding algorithm.
    pub y: f32,
    /// Uncertainty of y.
    #[serde(rename = "yErr")]
    #[serde(default = "default_diasource_y_err")]
    pub y_err: Option<f32>,
    /// Covariance between x and y.
    #[serde(rename = "x_y_Cov")]
    #[serde(default = "default_diasource_x_y_cov")]
    pub x_y_cov: Option<f32>,
    /// General centroid algorithm failure flag; set if anything went wrong when fitting the centroid. Another centroid flag field should also be set to provide more information.
    #[serde(default = "default_diasource_centroid_flag")]
    pub centroid_flag: Option<bool>,
    /// Source was detected as significantly negative.
    #[serde(default = "default_diasource_is_negative")]
    pub is_negative: Option<bool>,
    /// Flux in a 12 pixel radius aperture on the difference image.
    #[serde(rename = "apFlux")]
    #[serde(default = "default_diasource_ap_flux")]
    pub ap_flux: Option<f32>,
    /// Estimated uncertainty of apFlux.
    #[serde(rename = "apFluxErr")]
    #[serde(default = "default_diasource_ap_flux_err")]
    pub ap_flux_err: Option<f32>,
    /// General aperture flux algorithm failure flag; set if anything went wrong when measuring aperture fluxes. Another apFlux flag field should also be set to provide more information.
    #[serde(rename = "apFlux_flag")]
    #[serde(default = "default_diasource_ap_flux_flag")]
    pub ap_flux_flag: Option<bool>,
    /// Aperture did not fit within measurement image.
    #[serde(rename = "apFlux_flag_apertureTruncated")]
    #[serde(default = "default_diasource_ap_flux_flag_aperture_truncated")]
    pub ap_flux_flag_aperture_truncated: Option<bool>,
    /// The signal-to-noise ratio at which this source was detected in the difference image.
    #[serde(default = "default_diasource_snr")]
    pub snr: Option<f32>,
    /// Flux for Point Source model. Note this actually measures the flux difference between the template and the visit image.
    #[serde(rename = "psfFlux")]
    #[serde(default = "default_diasource_psf_flux")]
    pub psf_flux: Option<f32>,
    /// Uncertainty of psfFlux.
    #[serde(rename = "psfFluxErr")]
    #[serde(default = "default_diasource_psf_flux_err")]
    pub psf_flux_err: Option<f32>,
    /// Right ascension coordinate of centroid for point source model.
    #[serde(rename = "psfRa")]
    #[serde(default = "default_diasource_psf_ra")]
    pub psf_ra: Option<f64>,
    /// Uncertainty of psfRa.
    #[serde(rename = "psfRaErr")]
    #[serde(default = "default_diasource_psf_ra_err")]
    pub psf_ra_err: Option<f32>,
    /// Declination coordinate of centroid for point source model.
    #[serde(rename = "psfDec")]
    #[serde(default = "default_diasource_psf_dec")]
    pub psf_dec: Option<f64>,
    /// Uncertainty of psfDec.
    #[serde(rename = "psfDecErr")]
    #[serde(default = "default_diasource_psf_dec_err")]
    pub psf_dec_err: Option<f32>,
    /// Covariance between psfFlux and psfRa.
    #[serde(rename = "psfFlux_psfRa_Cov")]
    #[serde(default = "default_diasource_psf_flux_psf_ra_cov")]
    pub psf_flux_psf_ra_cov: Option<f32>,
    /// Covariance between psfFlux and psfDec.
    #[serde(rename = "psfFlux_psfDec_Cov")]
    #[serde(default = "default_diasource_psf_flux_psf_dec_cov")]
    pub psf_flux_psf_dec_cov: Option<f32>,
    /// Covariance between psfRa and psfDec.
    #[serde(rename = "psfRa_psfDec_Cov")]
    #[serde(default = "default_diasource_psf_ra_psf_dec_cov")]
    pub psf_ra_psf_dec_cov: Option<f32>,
    /// Natural log likelihood of the observed data given the point source model.
    #[serde(rename = "psfLnL")]
    #[serde(default = "default_diasource_psf_ln_l")]
    pub psf_ln_l: Option<f32>,
    /// Chi^2 statistic of the point source model fit.
    #[serde(rename = "psfChi2")]
    #[serde(default = "default_diasource_psf_chi2")]
    pub psf_chi2: Option<f32>,
    /// The number of data points (pixels) used to fit the point source model.
    #[serde(rename = "psfNdata")]
    #[serde(default = "default_diasource_psf_ndata")]
    pub psf_ndata: Option<i32>,
    /// Failure to derive linear least-squares fit of psf model. Another psfFlux flag field should also be set to provide more information.
    #[serde(rename = "psfFlux_flag")]
    #[serde(default = "default_diasource_psf_flux_flag")]
    pub psf_flux_flag: Option<bool>,
    /// Object was too close to the edge of the image to use the full PSF model.
    #[serde(rename = "psfFlux_flag_edge")]
    #[serde(default = "default_diasource_psf_flux_flag_edge")]
    pub psf_flux_flag_edge: Option<bool>,
    /// Not enough non-rejected pixels in data to attempt the fit.
    #[serde(rename = "psfFlux_flag_noGoodPixels")]
    #[serde(default = "default_diasource_psf_flux_flag_no_good_pixels")]
    pub psf_flux_flag_no_good_pixels: Option<bool>,
    /// Flux for a trailed source model. Note this actually measures the flux difference between the template and the visit image.
    #[serde(rename = "trailFlux")]
    #[serde(default = "default_diasource_trail_flux")]
    pub trail_flux: Option<f32>,
    /// Uncertainty of trailFlux.
    #[serde(rename = "trailFluxErr")]
    #[serde(default = "default_diasource_trail_flux_err")]
    pub trail_flux_err: Option<f32>,
    /// Right ascension coordinate of centroid for trailed source model.
    #[serde(rename = "trailRa")]
    #[serde(default = "default_diasource_trail_ra")]
    pub trail_ra: Option<f64>,
    /// Uncertainty of trailRa.
    #[serde(rename = "trailRaErr")]
    #[serde(default = "default_diasource_trail_ra_err")]
    pub trail_ra_err: Option<f32>,
    /// Declination coordinate of centroid for trailed source model.
    #[serde(rename = "trailDec")]
    #[serde(default = "default_diasource_trail_dec")]
    pub trail_dec: Option<f64>,
    /// Uncertainty of trailDec.
    #[serde(rename = "trailDecErr")]
    #[serde(default = "default_diasource_trail_dec_err")]
    pub trail_dec_err: Option<f32>,
    /// Maximum likelihood fit of trail length.
    #[serde(rename = "trailLength")]
    #[serde(default = "default_diasource_trail_length")]
    pub trail_length: Option<f32>,
    /// Uncertainty of trailLength.
    #[serde(rename = "trailLengthErr")]
    #[serde(default = "default_diasource_trail_length_err")]
    pub trail_length_err: Option<f32>,
    /// Maximum likelihood fit of the angle between the meridian through the centroid and the trail direction (bearing).
    #[serde(rename = "trailAngle")]
    #[serde(default = "default_diasource_trail_angle")]
    pub trail_angle: Option<f32>,
    /// Uncertainty of trailAngle.
    #[serde(rename = "trailAngleErr")]
    #[serde(default = "default_diasource_trail_angle_err")]
    pub trail_angle_err: Option<f32>,
    /// Covariance of trailFlux and trailRa.
    #[serde(rename = "trailFlux_trailRa_Cov")]
    #[serde(default = "default_diasource_trail_flux_trail_ra_cov")]
    pub trail_flux_trail_ra_cov: Option<f32>,
    /// Covariance of trailFlux and trailDec.
    #[serde(rename = "trailFlux_trailDec_Cov")]
    #[serde(default = "default_diasource_trail_flux_trail_dec_cov")]
    pub trail_flux_trail_dec_cov: Option<f32>,
    /// Covariance of trailFlux and trailLength
    #[serde(rename = "trailFlux_trailLength_Cov")]
    #[serde(default = "default_diasource_trail_flux_trail_length_cov")]
    pub trail_flux_trail_length_cov: Option<f32>,
    /// Covariance of trailFlux and trailAngle
    #[serde(rename = "trailFlux_trailAngle_Cov")]
    #[serde(default = "default_diasource_trail_flux_trail_angle_cov")]
    pub trail_flux_trail_angle_cov: Option<f32>,
    /// Covariance of trailRa and trailDec.
    #[serde(rename = "trailRa_trailDec_Cov")]
    #[serde(default = "default_diasource_trail_ra_trail_dec_cov")]
    pub trail_ra_trail_dec_cov: Option<f32>,
    /// Covariance of trailRa and trailLength.
    #[serde(rename = "trailRa_trailLength_Cov")]
    #[serde(default = "default_diasource_trail_ra_trail_length_cov")]
    pub trail_ra_trail_length_cov: Option<f32>,
    /// Covariance of trailRa and trailAngle.
    #[serde(rename = "trailRa_trailAngle_Cov")]
    #[serde(default = "default_diasource_trail_ra_trail_angle_cov")]
    pub trail_ra_trail_angle_cov: Option<f32>,
    /// Covariance of trailDec and trailLength.
    #[serde(rename = "trailDec_trailLength_Cov")]
    #[serde(default = "default_diasource_trail_dec_trail_length_cov")]
    pub trail_dec_trail_length_cov: Option<f32>,
    /// Covariance of trailDec and trailAngle.
    #[serde(rename = "trailDec_trailAngle_Cov")]
    #[serde(default = "default_diasource_trail_dec_trail_angle_cov")]
    pub trail_dec_trail_angle_cov: Option<f32>,
    /// Covariance of trailLength and trailAngle
    #[serde(rename = "trailLength_trailAngle_Cov")]
    #[serde(default = "default_diasource_trail_length_trail_angle_cov")]
    pub trail_length_trail_angle_cov: Option<f32>,
    /// Natural log likelihood of the observed data given the trailed source model.
    #[serde(rename = "trailLnL")]
    #[serde(default = "default_diasource_trail_ln_l")]
    pub trail_ln_l: Option<f32>,
    /// Chi^2 statistic of the trailed source model fit.
    #[serde(rename = "trailChi2")]
    #[serde(default = "default_diasource_trail_chi2")]
    pub trail_chi2: Option<f32>,
    /// The number of data points (pixels) used to fit the trailed source model.
    #[serde(rename = "trailNdata")]
    #[serde(default = "default_diasource_trail_ndata")]
    pub trail_ndata: Option<i32>,
    /// This flag is set if a trailed source extends onto or past edge pixels.
    #[serde(default = "default_diasource_trail_flag_edge")]
    pub trail_flag_edge: Option<bool>,
    /// Maximum likelihood value for the mean absolute flux of the two lobes for a dipole model.
    #[serde(rename = "dipoleMeanFlux")]
    #[serde(default = "default_diasource_dipole_mean_flux")]
    pub dipole_mean_flux: Option<f32>,
    /// Uncertainty of dipoleMeanFlux.
    #[serde(rename = "dipoleMeanFluxErr")]
    #[serde(default = "default_diasource_dipole_mean_flux_err")]
    pub dipole_mean_flux_err: Option<f32>,
    /// Maximum likelihood value for the difference of absolute fluxes of the two lobes for a dipole model.
    #[serde(rename = "dipoleFluxDiff")]
    #[serde(default = "default_diasource_dipole_flux_diff")]
    pub dipole_flux_diff: Option<f32>,
    /// Uncertainty of dipoleFluxDiff.
    #[serde(rename = "dipoleFluxDiffErr")]
    #[serde(default = "default_diasource_dipole_flux_diff_err")]
    pub dipole_flux_diff_err: Option<f32>,
    /// Right ascension coordinate of centroid for dipole model.
    #[serde(rename = "dipoleRa")]
    #[serde(default = "default_diasource_dipole_ra")]
    pub dipole_ra: Option<f64>,
    /// Uncertainty of dipoleRa.
    #[serde(rename = "dipoleRaErr")]
    #[serde(default = "default_diasource_dipole_ra_err")]
    pub dipole_ra_err: Option<f32>,
    /// Declination coordinate of centroid for dipole model.
    #[serde(rename = "dipoleDec")]
    #[serde(default = "default_diasource_dipole_dec")]
    pub dipole_dec: Option<f64>,
    /// Uncertainty of dipoleDec.
    #[serde(rename = "dipoleDecErr")]
    #[serde(default = "default_diasource_dipole_dec_err")]
    pub dipole_dec_err: Option<f32>,
    /// Maximum likelihood value for the lobe separation in dipole model.
    #[serde(rename = "dipoleLength")]
    #[serde(default = "default_diasource_dipole_length")]
    pub dipole_length: Option<f32>,
    /// Uncertainty of dipoleLength.
    #[serde(rename = "dipoleLengthErr")]
    #[serde(default = "default_diasource_dipole_length_err")]
    pub dipole_length_err: Option<f32>,
    /// Maximum likelihood fit of the angle between the meridian through the centroid and the dipole direction (bearing, from negative to positive lobe).
    #[serde(rename = "dipoleAngle")]
    #[serde(default = "default_diasource_dipole_angle")]
    pub dipole_angle: Option<f32>,
    /// Uncertainty of dipoleAngle.
    #[serde(rename = "dipoleAngleErr")]
    #[serde(default = "default_diasource_dipole_angle_err")]
    pub dipole_angle_err: Option<f32>,
    /// Covariance of dipoleMeanFlux and dipoleFluxDiff.
    #[serde(rename = "dipoleMeanFlux_dipoleFluxDiff_Cov")]
    #[serde(default = "default_diasource_dipole_mean_flux_dipole_flux_diff_cov")]
    pub dipole_mean_flux_dipole_flux_diff_cov: Option<f32>,
    /// Covariance of dipoleMeanFlux and dipoleRa.
    #[serde(rename = "dipoleMeanFlux_dipoleRa_Cov")]
    #[serde(default = "default_diasource_dipole_mean_flux_dipole_ra_cov")]
    pub dipole_mean_flux_dipole_ra_cov: Option<f32>,
    /// Covariance of dipoleMeanFlux and dipoleDec.
    #[serde(rename = "dipoleMeanFlux_dipoleDec_Cov")]
    #[serde(default = "default_diasource_dipole_mean_flux_dipole_dec_cov")]
    pub dipole_mean_flux_dipole_dec_cov: Option<f32>,
    /// Covariance of dipoleMeanFlux and dipoleLength.
    #[serde(rename = "dipoleMeanFlux_dipoleLength_Cov")]
    #[serde(default = "default_diasource_dipole_mean_flux_dipole_length_cov")]
    pub dipole_mean_flux_dipole_length_cov: Option<f32>,
    /// Covariance of dipoleMeanFlux and dipoleAngle.
    #[serde(rename = "dipoleMeanFlux_dipoleAngle_Cov")]
    #[serde(default = "default_diasource_dipole_mean_flux_dipole_angle_cov")]
    pub dipole_mean_flux_dipole_angle_cov: Option<f32>,
    /// Covariance of dipoleFluxDiff and dipoleRa.
    #[serde(rename = "dipoleFluxDiff_dipoleRa_Cov")]
    #[serde(default = "default_diasource_dipole_flux_diff_dipole_ra_cov")]
    pub dipole_flux_diff_dipole_ra_cov: Option<f32>,
    /// Covariance of dipoleFluxDiff and dipoleDec.
    #[serde(rename = "dipoleFluxDiff_dipoleDec_Cov")]
    #[serde(default = "default_diasource_dipole_flux_diff_dipole_dec_cov")]
    pub dipole_flux_diff_dipole_dec_cov: Option<f32>,
    /// Covariance of dipoleFluxDiff and dipoleLength.
    #[serde(rename = "dipoleFluxDiff_dipoleLength_Cov")]
    #[serde(default = "default_diasource_dipole_flux_diff_dipole_length_cov")]
    pub dipole_flux_diff_dipole_length_cov: Option<f32>,
    /// Covariance of dipoleFluxDiff and dipoleAngle.
    #[serde(rename = "dipoleFluxDiff_dipoleAngle_Cov")]
    #[serde(default = "default_diasource_dipole_flux_diff_dipole_angle_cov")]
    pub dipole_flux_diff_dipole_angle_cov: Option<f32>,
    /// Covariance of dipoleRa and dipoleDec.
    #[serde(rename = "dipoleRa_dipoleDec_Cov")]
    #[serde(default = "default_diasource_dipole_ra_dipole_dec_cov")]
    pub dipole_ra_dipole_dec_cov: Option<f32>,
    /// Covariance of dipoleRa and dipoleLength.
    #[serde(rename = "dipoleRa_dipoleLength_Cov")]
    #[serde(default = "default_diasource_dipole_ra_dipole_length_cov")]
    pub dipole_ra_dipole_length_cov: Option<f32>,
    /// Covariance of dipoleRa and dipoleAngle.
    #[serde(rename = "dipoleRa_dipoleAngle_Cov")]
    #[serde(default = "default_diasource_dipole_ra_dipole_angle_cov")]
    pub dipole_ra_dipole_angle_cov: Option<f32>,
    /// Covariance of dipoleDec and dipoleLength.
    #[serde(rename = "dipoleDec_dipoleLength_Cov")]
    #[serde(default = "default_diasource_dipole_dec_dipole_length_cov")]
    pub dipole_dec_dipole_length_cov: Option<f32>,
    /// Covariance of dipoleDec and dipoleAngle.
    #[serde(rename = "dipoleDec_dipoleAngle_Cov")]
    #[serde(default = "default_diasource_dipole_dec_dipole_angle_cov")]
    pub dipole_dec_dipole_angle_cov: Option<f32>,
    /// Covariance of dipoleLength and dipoleAngle.
    #[serde(rename = "dipoleLength_dipoleAngle_Cov")]
    #[serde(default = "default_diasource_dipole_length_dipole_angle_cov")]
    pub dipole_length_dipole_angle_cov: Option<f32>,
    /// Natural log likelihood of the observed data given the dipole source model.
    #[serde(rename = "dipoleLnL")]
    #[serde(default = "default_diasource_dipole_ln_l")]
    pub dipole_ln_l: Option<f32>,
    /// Chi^2 statistic of the model fit.
    #[serde(rename = "dipoleChi2")]
    #[serde(default = "default_diasource_dipole_chi2")]
    pub dipole_chi2: Option<f32>,
    /// The number of data points (pixels) used to fit the model.
    #[serde(rename = "dipoleNdata")]
    #[serde(default = "default_diasource_dipole_ndata")]
    pub dipole_ndata: Option<i32>,
    /// Forced PSF photometry on science image failed. Another forced_PsfFlux flag field should also be set to provide more information.
    #[serde(rename = "forced_PsfFlux_flag")]
    #[serde(default = "default_diasource_forced_psf_flux_flag")]
    pub forced_psf_flux_flag: Option<bool>,
    /// Forced PSF flux on science image was too close to the edge of the image to use the full PSF model.
    #[serde(rename = "forced_PsfFlux_flag_edge")]
    #[serde(default = "default_diasource_forced_psf_flux_flag_edge")]
    pub forced_psf_flux_flag_edge: Option<bool>,
    /// Forced PSF flux not enough non-rejected pixels in data to attempt the fit.
    #[serde(rename = "forced_PsfFlux_flag_noGoodPixels")]
    #[serde(default = "default_diasource_forced_psf_flux_flag_no_good_pixels")]
    pub forced_psf_flux_flag_no_good_pixels: Option<bool>,
    /// Calibrated flux for Point Source model centered on radec but measured on the difference of snaps comprising this visit.
    #[serde(rename = "snapDiffFlux")]
    #[serde(default = "default_diasource_snap_diff_flux")]
    pub snap_diff_flux: Option<f32>,
    /// Estimated uncertainty of snapDiffFlux.
    #[serde(rename = "snapDiffFluxErr")]
    #[serde(default = "default_diasource_snap_diff_flux_err")]
    pub snap_diff_flux_err: Option<f32>,
    /// Estimated sky background at the position (centroid) of the object.
    #[serde(rename = "fpBkgd")]
    #[serde(default = "default_diasource_fp_bkgd")]
    pub fp_bkgd: Option<f32>,
    /// Estimated uncertainty of fpBkgd.
    #[serde(rename = "fpBkgdErr")]
    #[serde(default = "default_diasource_fp_bkgd_err")]
    pub fp_bkgd_err: Option<f32>,
    /// Adaptive second moment of the source intensity.
    #[serde(default = "default_diasource_ixx")]
    pub ixx: Option<f32>,
    /// Uncertainty of ixx.
    #[serde(rename = "ixxErr")]
    #[serde(default = "default_diasource_ixx_err")]
    pub ixx_err: Option<f32>,
    /// Adaptive second moment of the source intensity.
    #[serde(default = "default_diasource_iyy")]
    pub iyy: Option<f32>,
    /// Uncertainty of iyy.
    #[serde(rename = "iyyErr")]
    #[serde(default = "default_diasource_iyy_err")]
    pub iyy_err: Option<f32>,
    /// Adaptive second moment of the source intensity.
    #[serde(default = "default_diasource_ixy")]
    pub ixy: Option<f32>,
    /// Uncertainty of ixy.
    #[serde(rename = "ixyErr")]
    #[serde(default = "default_diasource_ixy_err")]
    pub ixy_err: Option<f32>,
    /// Covariance of ixx and iyy.
    #[serde(rename = "ixx_iyy_Cov")]
    #[serde(default = "default_diasource_ixx_iyy_cov")]
    pub ixx_iyy_cov: Option<f32>,
    /// Covariance of ixx and ixy.
    #[serde(rename = "ixx_ixy_Cov")]
    #[serde(default = "default_diasource_ixx_ixy_cov")]
    pub ixx_ixy_cov: Option<f32>,
    /// Covariance of iyy and ixy.
    #[serde(rename = "iyy_ixy_Cov")]
    #[serde(default = "default_diasource_iyy_ixy_cov")]
    pub iyy_ixy_cov: Option<f32>,
    /// Adaptive second moment for the PSF.
    #[serde(rename = "ixxPSF")]
    #[serde(default = "default_diasource_ixx_psf")]
    pub ixx_psf: Option<f32>,
    /// Adaptive second moment for the PSF.
    #[serde(rename = "iyyPSF")]
    #[serde(default = "default_diasource_iyy_psf")]
    pub iyy_psf: Option<f32>,
    /// Adaptive second moment for the PSF.
    #[serde(rename = "ixyPSF")]
    #[serde(default = "default_diasource_ixy_psf")]
    pub ixy_psf: Option<f32>,
    /// General source shape algorithm failure flag; set if anything went wrong when measuring the shape. Another shape flag field should also be set to provide more information.
    #[serde(default = "default_diasource_shape_flag")]
    pub shape_flag: Option<bool>,
    /// No pixels to measure shape.
    #[serde(default = "default_diasource_shape_flag_no_pixels")]
    pub shape_flag_no_pixels: Option<bool>,
    /// Center not contained in footprint bounding box.
    #[serde(default = "default_diasource_shape_flag_not_contained")]
    pub shape_flag_not_contained: Option<bool>,
    /// This source is a parent source; we should only be measuring on deblended children in difference imaging.
    #[serde(default = "default_diasource_shape_flag_parent_source")]
    pub shape_flag_parent_source: Option<bool>,
    /// A measure of extendedness, computed by comparing an object's moment-based traced radius to the PSF moments. extendedness = 1 implies a high degree of confidence that the source is extended. extendedness = 0 implies a high degree of confidence that the source is point-like.
    #[serde(default = "default_diasource_extendedness")]
    pub extendedness: Option<f32>,
    /// A measure of reliability, computed using information from the source and image characterization, as well as the information on the Telescope and Camera system (e.g., ghost maps, defect maps, etc.).
    #[serde(default = "default_diasource_reliability")]
    pub reliability: Option<f32>,
    /// Filter band this source was observed with.
    #[serde(default = "default_diasource_band")]
    pub band: Option<String>,
    /// Attempted to fit a dipole model to this source.
    #[serde(rename = "dipoleFitAttempted")]
    #[serde(default = "default_diasource_dipole_fit_attempted")]
    pub dipole_fit_attempted: Option<bool>,
    /// General pixel flags failure; set if anything went wrong when setting pixels flags from this footprint's mask. This implies that some pixelFlags for this source may be incorrectly set to False.
    #[serde(rename = "pixelFlags")]
    #[serde(default = "default_diasource_pixel_flags")]
    pub pixel_flags: Option<bool>,
    /// Bad pixel in the DiaSource footprint.
    #[serde(rename = "pixelFlags_bad")]
    #[serde(default = "default_diasource_pixel_flags_bad")]
    pub pixel_flags_bad: Option<bool>,
    /// Cosmic ray in the DiaSource footprint.
    #[serde(rename = "pixelFlags_cr")]
    #[serde(default = "default_diasource_pixel_flags_cr")]
    pub pixel_flags_cr: Option<bool>,
    /// Cosmic ray in the 3x3 region around the centroid.
    #[serde(rename = "pixelFlags_crCenter")]
    #[serde(default = "default_diasource_pixel_flags_cr_center")]
    pub pixel_flags_cr_center: Option<bool>,
    /// Some of the source footprint is outside usable exposure region (masked EDGE or NO_DATA, or centroid off image).
    #[serde(rename = "pixelFlags_edge")]
    #[serde(default = "default_diasource_pixel_flags_edge")]
    pub pixel_flags_edge: Option<bool>,
    /// Interpolated pixel in the DiaSource footprint.
    #[serde(rename = "pixelFlags_interpolated")]
    #[serde(default = "default_diasource_pixel_flags_interpolated")]
    pub pixel_flags_interpolated: Option<bool>,
    /// Interpolated pixel in the 3x3 region around the centroid.
    #[serde(rename = "pixelFlags_interpolatedCenter")]
    #[serde(default = "default_diasource_pixel_flags_interpolated_center")]
    pub pixel_flags_interpolated_center: Option<bool>,
    /// DiaSource center is off image.
    #[serde(rename = "pixelFlags_offimage")]
    #[serde(default = "default_diasource_pixel_flags_offimage")]
    pub pixel_flags_offimage: Option<bool>,
    /// Saturated pixel in the DiaSource footprint.
    #[serde(rename = "pixelFlags_saturated")]
    #[serde(default = "default_diasource_pixel_flags_saturated")]
    pub pixel_flags_saturated: Option<bool>,
    /// Saturated pixel in the 3x3 region around the centroid.
    #[serde(rename = "pixelFlags_saturatedCenter")]
    #[serde(default = "default_diasource_pixel_flags_saturated_center")]
    pub pixel_flags_saturated_center: Option<bool>,
    /// DiaSource's footprint includes suspect pixels.
    #[serde(rename = "pixelFlags_suspect")]
    #[serde(default = "default_diasource_pixel_flags_suspect")]
    pub pixel_flags_suspect: Option<bool>,
    /// Suspect pixel in the 3x3 region around the centroid.
    #[serde(rename = "pixelFlags_suspectCenter")]
    #[serde(default = "default_diasource_pixel_flags_suspect_center")]
    pub pixel_flags_suspect_center: Option<bool>,
    /// Streak in the DiaSource footprint.
    #[serde(rename = "pixelFlags_streak")]
    #[serde(default = "default_diasource_pixel_flags_streak")]
    pub pixel_flags_streak: Option<bool>,
    /// Streak in the 3x3 region around the centroid.
    #[serde(rename = "pixelFlags_streakCenter")]
    #[serde(default = "default_diasource_pixel_flags_streak_center")]
    pub pixel_flags_streak_center: Option<bool>,
    /// Injection in the DiaSource footprint.
    #[serde(rename = "pixelFlags_injected")]
    #[serde(default = "default_diasource_pixel_flags_injected")]
    pub pixel_flags_injected: Option<bool>,
    /// Injection in the 3x3 region around the centroid.
    #[serde(rename = "pixelFlags_injectedCenter")]
    #[serde(default = "default_diasource_pixel_flags_injected_center")]
    pub pixel_flags_injected_center: Option<bool>,
    /// Template injection in the DiaSource footprint.
    #[serde(rename = "pixelFlags_injected_template")]
    #[serde(default = "default_diasource_pixel_flags_injected_template")]
    pub pixel_flags_injected_template: Option<bool>,
    /// Template injection in the 3x3 region around the centroid.
    #[serde(rename = "pixelFlags_injected_templateCenter")]
    #[serde(default = "default_diasource_pixel_flags_injected_template_center")]
    pub pixel_flags_injected_template_center: Option<bool>,
}

#[inline(always)]
fn default_diasource_dia_object_id() -> Option<i64> {
    None
}

#[inline(always)]
fn default_diasource_ss_object_id() -> Option<i64> {
    None
}

#[inline(always)]
fn default_diasource_parent_dia_source_id() -> Option<i64> {
    None
}

#[inline(always)]
fn default_diasource_ra_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dec_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_ra_dec_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_x_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_y_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_x_y_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_centroid_flag() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_is_negative() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_ap_flux() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_ap_flux_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_ap_flux_flag() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_ap_flux_flag_aperture_truncated() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_snr() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_psf_flux() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_psf_flux_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_psf_ra() -> Option<f64> {
    None
}

#[inline(always)]
fn default_diasource_psf_ra_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_psf_dec() -> Option<f64> {
    None
}

#[inline(always)]
fn default_diasource_psf_dec_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_psf_flux_psf_ra_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_psf_flux_psf_dec_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_psf_ra_psf_dec_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_psf_ln_l() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_psf_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_psf_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_diasource_psf_flux_flag() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_psf_flux_flag_edge() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_psf_flux_flag_no_good_pixels() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_trail_flux() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_flux_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_ra() -> Option<f64> {
    None
}

#[inline(always)]
fn default_diasource_trail_ra_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_dec() -> Option<f64> {
    None
}

#[inline(always)]
fn default_diasource_trail_dec_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_length() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_length_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_angle() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_angle_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_flux_trail_ra_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_flux_trail_dec_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_flux_trail_length_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_flux_trail_angle_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_ra_trail_dec_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_ra_trail_length_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_ra_trail_angle_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_dec_trail_length_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_dec_trail_angle_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_length_trail_angle_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_ln_l() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_trail_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_diasource_trail_flag_edge() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_dipole_mean_flux() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_mean_flux_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_flux_diff() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_flux_diff_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_ra() -> Option<f64> {
    None
}

#[inline(always)]
fn default_diasource_dipole_ra_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_dec() -> Option<f64> {
    None
}

#[inline(always)]
fn default_diasource_dipole_dec_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_length() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_length_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_angle() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_angle_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_mean_flux_dipole_flux_diff_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_mean_flux_dipole_ra_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_mean_flux_dipole_dec_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_mean_flux_dipole_length_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_mean_flux_dipole_angle_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_flux_diff_dipole_ra_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_flux_diff_dipole_dec_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_flux_diff_dipole_length_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_flux_diff_dipole_angle_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_ra_dipole_dec_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_ra_dipole_length_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_ra_dipole_angle_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_dec_dipole_length_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_dec_dipole_angle_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_length_dipole_angle_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_ln_l() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_dipole_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_diasource_forced_psf_flux_flag() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_forced_psf_flux_flag_edge() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_forced_psf_flux_flag_no_good_pixels() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_snap_diff_flux() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_snap_diff_flux_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_fp_bkgd() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_fp_bkgd_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_ixx() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_ixx_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_iyy() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_iyy_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_ixy() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_ixy_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_ixx_iyy_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_ixx_ixy_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_iyy_ixy_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_ixx_psf() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_iyy_psf() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_ixy_psf() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_shape_flag() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_shape_flag_no_pixels() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_shape_flag_not_contained() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_shape_flag_parent_source() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_extendedness() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_reliability() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diasource_band() -> Option<String> {
    None
}

#[inline(always)]
fn default_diasource_dipole_fit_attempted() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_bad() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_cr() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_cr_center() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_edge() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_interpolated() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_interpolated_center() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_offimage() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_saturated() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_saturated_center() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_suspect() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_suspect_center() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_streak() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_streak_center() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_injected() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_injected_center() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_injected_template() -> Option<bool> {
    None
}

#[inline(always)]
fn default_diasource_pixel_flags_injected_template_center() -> Option<bool> {
    None
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct DiaObject {
    /// Unique identifier of this DiaObject.
    #[serde(rename = "diaObjectId")]
    pub dia_object_id: i64,
    /// Right ascension coordinate of the position of the object at time radecMjdTai.
    pub ra: f64,
    /// Uncertainty of ra.
    #[serde(rename = "raErr")]
    #[serde(default = "default_diaobject_ra_err")]
    pub ra_err: Option<f32>,
    /// Declination coordinate of the position of the object at time radecMjdTai.
    pub dec: f64,
    /// Uncertainty of dec.
    #[serde(rename = "decErr")]
    #[serde(default = "default_diaobject_dec_err")]
    pub dec_err: Option<f32>,
    /// Covariance between ra and dec.
    #[serde(rename = "ra_dec_Cov")]
    #[serde(default = "default_diaobject_ra_dec_cov")]
    pub ra_dec_cov: Option<f32>,
    /// Time at which the object was at a position ra/dec, expressed as Modified Julian Date, International Atomic Time.
    #[serde(rename = "radecMjdTai")]
    #[serde(default = "default_diaobject_radec_mjd_tai")]
    pub radec_mjd_tai: Option<f64>,
    /// Proper motion in right ascension.
    #[serde(rename = "pmRa")]
    #[serde(default = "default_diaobject_pm_ra")]
    pub pm_ra: Option<f32>,
    /// Uncertainty of pmRa.
    #[serde(rename = "pmRaErr")]
    #[serde(default = "default_diaobject_pm_ra_err")]
    pub pm_ra_err: Option<f32>,
    /// Proper motion of declination.
    #[serde(rename = "pmDec")]
    #[serde(default = "default_diaobject_pm_dec")]
    pub pm_dec: Option<f32>,
    /// Uncertainty of pmDec.
    #[serde(rename = "pmDecErr")]
    #[serde(default = "default_diaobject_pm_dec_err")]
    pub pm_dec_err: Option<f32>,
    /// Parallax.
    #[serde(default = "default_diaobject_parallax")]
    pub parallax: Option<f32>,
    /// Uncertainty of parallax.
    #[serde(rename = "parallaxErr")]
    #[serde(default = "default_diaobject_parallax_err")]
    pub parallax_err: Option<f32>,
    /// Covariance of pmRa and pmDec.
    #[serde(rename = "pmRa_pmDec_Cov")]
    #[serde(default = "default_diaobject_pm_ra_pm_dec_cov")]
    pub pm_ra_pm_dec_cov: Option<f32>,
    /// Covariance of pmRa and parallax.
    #[serde(rename = "pmRa_parallax_Cov")]
    #[serde(default = "default_diaobject_pm_ra_parallax_cov")]
    pub pm_ra_parallax_cov: Option<f32>,
    /// Covariance of pmDec and parallax.
    #[serde(rename = "pmDec_parallax_Cov")]
    #[serde(default = "default_diaobject_pm_dec_parallax_cov")]
    pub pm_dec_parallax_cov: Option<f32>,
    /// Natural log of the likelihood of the linear proper motion parallax fit.
    #[serde(rename = "pmParallaxLnL")]
    #[serde(default = "default_diaobject_pm_parallax_ln_l")]
    pub pm_parallax_ln_l: Option<f32>,
    /// Chi^2 static of the model fit.
    #[serde(rename = "pmParallaxChi2")]
    #[serde(default = "default_diaobject_pm_parallax_chi2")]
    pub pm_parallax_chi2: Option<f32>,
    /// The number of data points used to fit the model.
    #[serde(rename = "pmParallaxNdata")]
    #[serde(default = "default_diaobject_pm_parallax_ndata")]
    pub pm_parallax_ndata: Option<i32>,
    /// Weighted mean point-source model magnitude for u filter.
    #[serde(rename = "u_psfFluxMean")]
    #[serde(default = "default_diaobject_u_psf_flux_mean")]
    pub u_psf_flux_mean: Option<f32>,
    /// Standard error of u_psfFluxMean.
    #[serde(rename = "u_psfFluxMeanErr")]
    #[serde(default = "default_diaobject_u_psf_flux_mean_err")]
    pub u_psf_flux_mean_err: Option<f32>,
    /// Standard deviation of the distribution of u_psfFlux.
    #[serde(rename = "u_psfFluxSigma")]
    #[serde(default = "default_diaobject_u_psf_flux_sigma")]
    pub u_psf_flux_sigma: Option<f32>,
    /// Chi^2 statistic for the scatter of u_psfFlux around u_psfFluxMean.
    #[serde(rename = "u_psfFluxChi2")]
    #[serde(default = "default_diaobject_u_psf_flux_chi2")]
    pub u_psf_flux_chi2: Option<f32>,
    /// The number of data points used to compute u_psfFluxChi2.
    #[serde(rename = "u_psfFluxNdata")]
    #[serde(default = "default_diaobject_u_psf_flux_ndata")]
    pub u_psf_flux_ndata: Option<i32>,
    /// Weighted mean forced photometry flux for u filter.
    #[serde(rename = "u_fpFluxMean")]
    #[serde(default = "default_diaobject_u_fp_flux_mean")]
    pub u_fp_flux_mean: Option<f32>,
    /// Standard error of u_fpFluxMean.
    #[serde(rename = "u_fpFluxMeanErr")]
    #[serde(default = "default_diaobject_u_fp_flux_mean_err")]
    pub u_fp_flux_mean_err: Option<f32>,
    /// Standard deviation of the distribution of u_fpFlux.
    #[serde(rename = "u_fpFluxSigma")]
    #[serde(default = "default_diaobject_u_fp_flux_sigma")]
    pub u_fp_flux_sigma: Option<f32>,
    /// Weighted mean point-source model magnitude for g filter.
    #[serde(rename = "g_psfFluxMean")]
    #[serde(default = "default_diaobject_g_psf_flux_mean")]
    pub g_psf_flux_mean: Option<f32>,
    /// Standard error of g_psfFluxMean.
    #[serde(rename = "g_psfFluxMeanErr")]
    #[serde(default = "default_diaobject_g_psf_flux_mean_err")]
    pub g_psf_flux_mean_err: Option<f32>,
    /// Standard deviation of the distribution of g_psfFlux.
    #[serde(rename = "g_psfFluxSigma")]
    #[serde(default = "default_diaobject_g_psf_flux_sigma")]
    pub g_psf_flux_sigma: Option<f32>,
    /// Chi^2 statistic for the scatter of g_psfFlux around g_psfFluxMean.
    #[serde(rename = "g_psfFluxChi2")]
    #[serde(default = "default_diaobject_g_psf_flux_chi2")]
    pub g_psf_flux_chi2: Option<f32>,
    /// The number of data points used to compute g_psfFluxChi2.
    #[serde(rename = "g_psfFluxNdata")]
    #[serde(default = "default_diaobject_g_psf_flux_ndata")]
    pub g_psf_flux_ndata: Option<i32>,
    /// Weighted mean forced photometry flux for g filter.
    #[serde(rename = "g_fpFluxMean")]
    #[serde(default = "default_diaobject_g_fp_flux_mean")]
    pub g_fp_flux_mean: Option<f32>,
    /// Standard error of g_fpFluxMean.
    #[serde(rename = "g_fpFluxMeanErr")]
    #[serde(default = "default_diaobject_g_fp_flux_mean_err")]
    pub g_fp_flux_mean_err: Option<f32>,
    /// Standard deviation of the distribution of g_fpFlux.
    #[serde(rename = "g_fpFluxSigma")]
    #[serde(default = "default_diaobject_g_fp_flux_sigma")]
    pub g_fp_flux_sigma: Option<f32>,
    /// Weighted mean point-source model magnitude for r filter.
    #[serde(rename = "r_psfFluxMean")]
    #[serde(default = "default_diaobject_r_psf_flux_mean")]
    pub r_psf_flux_mean: Option<f32>,
    /// Standard error of r_psfFluxMean.
    #[serde(rename = "r_psfFluxMeanErr")]
    #[serde(default = "default_diaobject_r_psf_flux_mean_err")]
    pub r_psf_flux_mean_err: Option<f32>,
    /// Standard deviation of the distribution of r_psfFlux.
    #[serde(rename = "r_psfFluxSigma")]
    #[serde(default = "default_diaobject_r_psf_flux_sigma")]
    pub r_psf_flux_sigma: Option<f32>,
    /// Chi^2 statistic for the scatter of r_psfFlux around r_psfFluxMean.
    #[serde(rename = "r_psfFluxChi2")]
    #[serde(default = "default_diaobject_r_psf_flux_chi2")]
    pub r_psf_flux_chi2: Option<f32>,
    /// The number of data points used to compute r_psfFluxChi2.
    #[serde(rename = "r_psfFluxNdata")]
    #[serde(default = "default_diaobject_r_psf_flux_ndata")]
    pub r_psf_flux_ndata: Option<i32>,
    /// Weighted mean forced photometry flux for r filter.
    #[serde(rename = "r_fpFluxMean")]
    #[serde(default = "default_diaobject_r_fp_flux_mean")]
    pub r_fp_flux_mean: Option<f32>,
    /// Standard error of r_fpFluxMean.
    #[serde(rename = "r_fpFluxMeanErr")]
    #[serde(default = "default_diaobject_r_fp_flux_mean_err")]
    pub r_fp_flux_mean_err: Option<f32>,
    /// Standard deviation of the distribution of r_fpFlux.
    #[serde(rename = "r_fpFluxSigma")]
    #[serde(default = "default_diaobject_r_fp_flux_sigma")]
    pub r_fp_flux_sigma: Option<f32>,
    /// Weighted mean point-source model magnitude for i filter.
    #[serde(rename = "i_psfFluxMean")]
    #[serde(default = "default_diaobject_i_psf_flux_mean")]
    pub i_psf_flux_mean: Option<f32>,
    /// Standard error of i_psfFluxMean.
    #[serde(rename = "i_psfFluxMeanErr")]
    #[serde(default = "default_diaobject_i_psf_flux_mean_err")]
    pub i_psf_flux_mean_err: Option<f32>,
    /// Standard deviation of the distribution of i_psfFlux.
    #[serde(rename = "i_psfFluxSigma")]
    #[serde(default = "default_diaobject_i_psf_flux_sigma")]
    pub i_psf_flux_sigma: Option<f32>,
    /// Chi^2 statistic for the scatter of i_psfFlux around i_psfFluxMean.
    #[serde(rename = "i_psfFluxChi2")]
    #[serde(default = "default_diaobject_i_psf_flux_chi2")]
    pub i_psf_flux_chi2: Option<f32>,
    /// The number of data points used to compute i_psfFluxChi2.
    #[serde(rename = "i_psfFluxNdata")]
    #[serde(default = "default_diaobject_i_psf_flux_ndata")]
    pub i_psf_flux_ndata: Option<i32>,
    /// Weighted mean forced photometry flux for i filter.
    #[serde(rename = "i_fpFluxMean")]
    #[serde(default = "default_diaobject_i_fp_flux_mean")]
    pub i_fp_flux_mean: Option<f32>,
    /// Standard error of i_fpFluxMean.
    #[serde(rename = "i_fpFluxMeanErr")]
    #[serde(default = "default_diaobject_i_fp_flux_mean_err")]
    pub i_fp_flux_mean_err: Option<f32>,
    /// Standard deviation of the distribution of i_fpFlux.
    #[serde(rename = "i_fpFluxSigma")]
    #[serde(default = "default_diaobject_i_fp_flux_sigma")]
    pub i_fp_flux_sigma: Option<f32>,
    /// Weighted mean point-source model magnitude for z filter.
    #[serde(rename = "z_psfFluxMean")]
    #[serde(default = "default_diaobject_z_psf_flux_mean")]
    pub z_psf_flux_mean: Option<f32>,
    /// Standard error of z_psfFluxMean.
    #[serde(rename = "z_psfFluxMeanErr")]
    #[serde(default = "default_diaobject_z_psf_flux_mean_err")]
    pub z_psf_flux_mean_err: Option<f32>,
    /// Standard deviation of the distribution of z_psfFlux.
    #[serde(rename = "z_psfFluxSigma")]
    #[serde(default = "default_diaobject_z_psf_flux_sigma")]
    pub z_psf_flux_sigma: Option<f32>,
    /// Chi^2 statistic for the scatter of z_psfFlux around z_psfFluxMean.
    #[serde(rename = "z_psfFluxChi2")]
    #[serde(default = "default_diaobject_z_psf_flux_chi2")]
    pub z_psf_flux_chi2: Option<f32>,
    /// The number of data points used to compute z_psfFluxChi2.
    #[serde(rename = "z_psfFluxNdata")]
    #[serde(default = "default_diaobject_z_psf_flux_ndata")]
    pub z_psf_flux_ndata: Option<i32>,
    /// Weighted mean forced photometry flux for z filter.
    #[serde(rename = "z_fpFluxMean")]
    #[serde(default = "default_diaobject_z_fp_flux_mean")]
    pub z_fp_flux_mean: Option<f32>,
    /// Standard error of z_fpFluxMean.
    #[serde(rename = "z_fpFluxMeanErr")]
    #[serde(default = "default_diaobject_z_fp_flux_mean_err")]
    pub z_fp_flux_mean_err: Option<f32>,
    /// Standard deviation of the distribution of z_fpFlux.
    #[serde(rename = "z_fpFluxSigma")]
    #[serde(default = "default_diaobject_z_fp_flux_sigma")]
    pub z_fp_flux_sigma: Option<f32>,
    /// Weighted mean point-source model magnitude for y filter.
    #[serde(rename = "y_psfFluxMean")]
    #[serde(default = "default_diaobject_y_psf_flux_mean")]
    pub y_psf_flux_mean: Option<f32>,
    /// Standard error of y_psfFluxMean.
    #[serde(rename = "y_psfFluxMeanErr")]
    #[serde(default = "default_diaobject_y_psf_flux_mean_err")]
    pub y_psf_flux_mean_err: Option<f32>,
    /// Standard deviation of the distribution of y_psfFlux.
    #[serde(rename = "y_psfFluxSigma")]
    #[serde(default = "default_diaobject_y_psf_flux_sigma")]
    pub y_psf_flux_sigma: Option<f32>,
    /// Chi^2 statistic for the scatter of y_psfFlux around y_psfFluxMean.
    #[serde(rename = "y_psfFluxChi2")]
    #[serde(default = "default_diaobject_y_psf_flux_chi2")]
    pub y_psf_flux_chi2: Option<f32>,
    /// The number of data points used to compute y_psfFluxChi2.
    #[serde(rename = "y_psfFluxNdata")]
    #[serde(default = "default_diaobject_y_psf_flux_ndata")]
    pub y_psf_flux_ndata: Option<i32>,
    /// Weighted mean forced photometry flux for y filter.
    #[serde(rename = "y_fpFluxMean")]
    #[serde(default = "default_diaobject_y_fp_flux_mean")]
    pub y_fp_flux_mean: Option<f32>,
    /// Standard error of y_fpFluxMean.
    #[serde(rename = "y_fpFluxMeanErr")]
    #[serde(default = "default_diaobject_y_fp_flux_mean_err")]
    pub y_fp_flux_mean_err: Option<f32>,
    /// Standard deviation of the distribution of y_fpFlux.
    #[serde(rename = "y_fpFluxSigma")]
    #[serde(default = "default_diaobject_y_fp_flux_sigma")]
    pub y_fp_flux_sigma: Option<f32>,
    /// Id of the closest nearby object.
    #[serde(rename = "nearbyObj1")]
    #[serde(default = "default_diaobject_nearby_obj1")]
    pub nearby_obj1: Option<i64>,
    /// Distance to nearbyObj1.
    #[serde(rename = "nearbyObj1Dist")]
    #[serde(default = "default_diaobject_nearby_obj1_dist")]
    pub nearby_obj1_dist: Option<f32>,
    /// Natural log of the probability that the observed diaObject is the same as the nearbyObj1.
    #[serde(rename = "nearbyObj1LnP")]
    #[serde(default = "default_diaobject_nearby_obj1_ln_p")]
    pub nearby_obj1_ln_p: Option<f32>,
    /// Id of the second-closest nearby object.
    #[serde(rename = "nearbyObj2")]
    #[serde(default = "default_diaobject_nearby_obj2")]
    pub nearby_obj2: Option<i64>,
    /// Distance to nearbyObj2.
    #[serde(rename = "nearbyObj2Dist")]
    #[serde(default = "default_diaobject_nearby_obj2_dist")]
    pub nearby_obj2_dist: Option<f32>,
    /// Natural log of the probability that the observed diaObject is the same as the nearbyObj2.
    #[serde(rename = "nearbyObj2LnP")]
    #[serde(default = "default_diaobject_nearby_obj2_ln_p")]
    pub nearby_obj2_ln_p: Option<f32>,
    /// Id of the third-closest nearby object.
    #[serde(rename = "nearbyObj3")]
    #[serde(default = "default_diaobject_nearby_obj3")]
    pub nearby_obj3: Option<i64>,
    /// Distance to nearbyObj3.
    #[serde(rename = "nearbyObj3Dist")]
    #[serde(default = "default_diaobject_nearby_obj3_dist")]
    pub nearby_obj3_dist: Option<f32>,
    /// Natural log of the probability that the observed diaObject is the same as the nearbyObj3.
    #[serde(rename = "nearbyObj3LnP")]
    #[serde(default = "default_diaobject_nearby_obj3_ln_p")]
    pub nearby_obj3_ln_p: Option<f32>,
    /// Mean of the u band flux errors.
    #[serde(rename = "u_psfFluxErrMean")]
    #[serde(default = "default_diaobject_u_psf_flux_err_mean")]
    pub u_psf_flux_err_mean: Option<f32>,
    /// Mean of the g band flux errors.
    #[serde(rename = "g_psfFluxErrMean")]
    #[serde(default = "default_diaobject_g_psf_flux_err_mean")]
    pub g_psf_flux_err_mean: Option<f32>,
    /// Mean of the r band flux errors.
    #[serde(rename = "r_psfFluxErrMean")]
    #[serde(default = "default_diaobject_r_psf_flux_err_mean")]
    pub r_psf_flux_err_mean: Option<f32>,
    /// Mean of the i band flux errors.
    #[serde(rename = "i_psfFluxErrMean")]
    #[serde(default = "default_diaobject_i_psf_flux_err_mean")]
    pub i_psf_flux_err_mean: Option<f32>,
    /// Mean of the z band flux errors.
    #[serde(rename = "z_psfFluxErrMean")]
    #[serde(default = "default_diaobject_z_psf_flux_err_mean")]
    pub z_psf_flux_err_mean: Option<f32>,
    /// Mean of the y band flux errors.
    #[serde(rename = "y_psfFluxErrMean")]
    #[serde(default = "default_diaobject_y_psf_flux_err_mean")]
    pub y_psf_flux_err_mean: Option<f32>,
}

#[inline(always)]
fn default_diaobject_ra_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_dec_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_ra_dec_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_radec_mjd_tai() -> Option<f64> {
    None
}

#[inline(always)]
fn default_diaobject_pm_ra() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_pm_ra_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_pm_dec() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_pm_dec_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_parallax() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_parallax_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_pm_ra_pm_dec_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_pm_ra_parallax_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_pm_dec_parallax_cov() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_pm_parallax_ln_l() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_pm_parallax_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_pm_parallax_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_diaobject_u_psf_flux_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_u_psf_flux_mean_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_u_psf_flux_sigma() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_u_psf_flux_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_u_psf_flux_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_diaobject_u_fp_flux_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_u_fp_flux_mean_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_u_fp_flux_sigma() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_g_psf_flux_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_g_psf_flux_mean_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_g_psf_flux_sigma() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_g_psf_flux_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_g_psf_flux_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_diaobject_g_fp_flux_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_g_fp_flux_mean_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_g_fp_flux_sigma() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_r_psf_flux_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_r_psf_flux_mean_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_r_psf_flux_sigma() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_r_psf_flux_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_r_psf_flux_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_diaobject_r_fp_flux_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_r_fp_flux_mean_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_r_fp_flux_sigma() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_i_psf_flux_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_i_psf_flux_mean_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_i_psf_flux_sigma() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_i_psf_flux_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_i_psf_flux_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_diaobject_i_fp_flux_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_i_fp_flux_mean_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_i_fp_flux_sigma() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_z_psf_flux_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_z_psf_flux_mean_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_z_psf_flux_sigma() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_z_psf_flux_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_z_psf_flux_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_diaobject_z_fp_flux_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_z_fp_flux_mean_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_z_fp_flux_sigma() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_y_psf_flux_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_y_psf_flux_mean_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_y_psf_flux_sigma() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_y_psf_flux_chi2() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_y_psf_flux_ndata() -> Option<i32> {
    None
}

#[inline(always)]
fn default_diaobject_y_fp_flux_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_y_fp_flux_mean_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_y_fp_flux_sigma() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_nearby_obj1() -> Option<i64> {
    None
}

#[inline(always)]
fn default_diaobject_nearby_obj1_dist() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_nearby_obj1_ln_p() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_nearby_obj2() -> Option<i64> {
    None
}

#[inline(always)]
fn default_diaobject_nearby_obj2_dist() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_nearby_obj2_ln_p() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_nearby_obj3() -> Option<i64> {
    None
}

#[inline(always)]
fn default_diaobject_nearby_obj3_dist() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_nearby_obj3_ln_p() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_u_psf_flux_err_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_g_psf_flux_err_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_r_psf_flux_err_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_i_psf_flux_err_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_z_psf_flux_err_mean() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaobject_y_psf_flux_err_mean() -> Option<f32> {
    None
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct DiaNondetectionLimit {
    #[serde(rename = "ccdVisitId")]
    pub ccd_visit_id: i64,
    #[serde(rename = "midpointMjdTai")]
    pub midpoint_mjd_tai: f64,
    pub band: String,
    #[serde(rename = "diaNoise")]
    pub dia_noise: f32,
}

#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct DiaForcedSource {
    /// Unique id.
    #[serde(rename = "diaForcedSourceId")]
    pub dia_forced_source_id: i64,
    /// Id of the DiaObject that this DiaForcedSource was associated with.
    #[serde(rename = "diaObjectId")]
    pub dia_object_id: i64,
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
    #[serde(default = "default_diaforcedsource_psf_flux")]
    pub psf_flux: Option<f32>,
    /// Uncertainty of psfFlux.
    #[serde(rename = "psfFluxErr")]
    #[serde(default = "default_diaforcedsource_psf_flux_err")]
    pub psf_flux_err: Option<f32>,
    /// Effective mid-visit time for this diaForcedSource, expressed as Modified Julian Date, International Atomic Time.
    #[serde(rename = "midpointMjdTai")]
    pub midpoint_mjd_tai: f64,
    /// Filter band this source was observed with.
    #[serde(default = "default_diaforcedsource_band")]
    pub band: Option<String>,
}

#[inline(always)]
fn default_diaforcedsource_psf_flux() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaforcedsource_psf_flux_err() -> Option<f32> {
    None
}

#[inline(always)]
fn default_diaforcedsource_band() -> Option<String> {
    None
}

/// Rubin Avro alert schema v7.3
#[derive(Debug, PartialEq, Clone, serde::Deserialize, serde::Serialize)]
pub struct LsstAlert {
    /// unique alert identifer
    #[serde(rename = "alertId")]
    pub candid: i64,
    #[serde(rename = "diaSource")]
    pub candidate: DiaSource,
    #[serde(rename = "prvDiaSources")]
    #[serde(default = "default_alert_prv_dia_sources")]
    pub prv_candidates: Option<Vec<DiaSource>>,
    #[serde(rename = "prvDiaForcedSources")]
    #[serde(default = "default_alert_prv_dia_forced_sources")]
    pub fp_hists: Option<Vec<DiaForcedSource>>,
    #[serde(rename = "prvDiaNondetectionLimits")]
    #[serde(default = "default_alert_prv_dia_nondetection_limits")]
    pub prv_nondetections: Option<Vec<DiaNondetectionLimit>>,
    // #[serde(rename = "diaObject")]
    // #[serde(default = "default_alert_dia_object")]
    // pub dia_object: Option<DiaObject>,
    // #[serde(rename = "ssObject")]
    // #[serde(default = "default_alert_ss_object")]
    // pub ss_object: Option<SsObject>,
    #[serde(rename = "cutoutDifference")]
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    #[serde(default = "default_alert_cutout_difference")]
    pub cutout_difference: Option<Vec<u8>>,
    #[serde(rename = "cutoutScience")]
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    #[serde(default = "default_alert_cutout_science")]
    pub cutout_science: Option<Vec<u8>>,
    #[serde(rename = "cutoutTemplate")]
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    #[serde(default = "default_alert_cutout_template")]
    pub cutout_template: Option<Vec<u8>>,
}

#[inline(always)]
fn default_alert_prv_dia_sources() -> Option<Vec<DiaSource>> {
    None
}

#[inline(always)]
fn default_alert_prv_dia_forced_sources() -> Option<Vec<DiaForcedSource>> {
    None
}

#[inline(always)]
fn default_alert_prv_dia_nondetection_limits() -> Option<Vec<DiaNondetectionLimit>> {
    None
}

#[inline(always)]
fn default_alert_dia_object() -> Option<DiaObject> {
    None
}

#[inline(always)]
fn default_alert_ss_object() -> Option<SsObject> {
    None
}

#[inline(always)]
fn default_alert_cutout_difference() -> Option<Vec<u8>> {
    None
}

#[inline(always)]
fn default_alert_cutout_science() -> Option<Vec<u8>> {
    None
}

#[inline(always)]
fn default_alert_cutout_template() -> Option<Vec<u8>> {
    None
}

// create an Alert trait, that must implement alert_schema, from_avro_bytes
pub trait Alert: Sized {
    fn alert_schema() -> Option<Schema>;
    fn from_avro_bytes(
        avro_bytes: Vec<u8>,
        schema: &apache_avro::Schema,
    ) -> Result<Self, Box<dyn std::error::Error>>;
}

impl Alert for LsstAlert {
    fn alert_schema() -> Option<Schema> {
        let schema_str = std::fs::read_to_string("./schema/lsst/lsst.v7_3.alert.avsc").unwrap();
        let schema = Schema::parse_str(&schema_str).unwrap();
        Some(schema)
    }

    fn from_avro_bytes(
        avro_bytes: Vec<u8>,
        schema: &apache_avro::Schema,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut cursor = std::io::Cursor::new(avro_bytes);

        let mut buffer = [0; 5];
        cursor.read_exact(&mut buffer).unwrap();

        let magic = buffer[0];
        if magic != _MAGIC_BYTE {
            panic!("This is not a valid avro file");
        }
        let schema_id = u32::from_be_bytes([buffer[1], buffer[2], buffer[3], buffer[4]]);
        if schema_id != _SCHEMA_REGISTRY_ID {
            panic!(
                "Schema version not supported ({} instead of {})",
                schema_id, _SCHEMA_REGISTRY_ID
            );
        }

        let value = from_avro_datum(&schema, &mut cursor, None);
        match value {
            Ok(value) => {
                let alert: LsstAlert = from_value::<LsstAlert>(&value).unwrap();
                Ok(alert)
            }
            Err(e) => {
                error!("Error deserializing avro message: {}", e);
                Err(Box::new(e))
            }
        }
    }
}
