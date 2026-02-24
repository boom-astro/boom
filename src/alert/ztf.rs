use crate::{
    alert::{
        base::{AlertError, AlertWorker, AlertWorkerError, ProcessAlertStatus, SchemaCache},
        decam, lsst, AlertCutout, LightcurveJdOnly,
    },
    conf::{self, AppConfig},
    utils::{
        db::{mongify, update_timeseries_op},
        enums::Survey,
        lightcurves::{diffmaglim2fluxerr, flux2mag, mag2flux, Band, SNT, ZTF_ZP},
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
use std::collections::{HashMap, HashSet};
use tracing::{instrument, warn};
use utoipa::ToSchema;

pub const STREAM_NAME: &str = "ZTF";
pub const ZTF_DEC_RANGE: (f64, f64) = (-30.0, 90.0);
// Position uncertainty in arcsec (median FHWM from https://www.ztf.caltech.edu/ztf-camera.html)
pub const ZTF_POSITION_UNCERTAINTY: f64 = 2.;
pub const ALERT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts");
pub const ALERT_AUX_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_aux");
pub const ALERT_CUTOUT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_cutouts");

pub const ZTF_LSST_XMATCH_RADIUS: f64 =
    (ZTF_POSITION_UNCERTAINTY.max(lsst::LSST_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();
pub const ZTF_DECAM_XMATCH_RADIUS: f64 =
    (ZTF_POSITION_UNCERTAINTY.max(decam::DECAM_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();

fn fid2band(fid: i32) -> Result<Band, AlertError> {
    match fid {
        1 => Ok(Band::G),
        2 => Ok(Band::R),
        3 => Ok(Band::I),
        _ => Err(AlertError::UnknownFid(fid)),
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Deserialize, Serialize)]
pub struct Cutout {
    #[serde(rename = "fileName")]
    pub file_name: String,
    #[serde(rename = "stampData")]
    #[serde(with = "apache_avro::serde_avro_bytes")]
    pub stamp_data: Vec<u8>,
}

#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Default, AvroSchema, ToSchema)]
#[serde(default)]
pub struct PrvCandidate {
    pub jd: f64,
    pub fid: i32,
    pub pid: i64,
    pub diffmaglim: Option<f32>,
    pub programpi: Option<String>,
    pub programid: i32,
    pub candid: Option<i64>,
    #[serde(deserialize_with = "deserialize_isdiffpos_option")]
    pub isdiffpos: Option<bool>,
    pub nid: Option<i32>,
    pub rcid: Option<i32>,
    pub field: Option<i32>,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub magpsf: Option<f32>,
    pub sigmapsf: Option<f32>,
    pub chipsf: Option<f32>,
    pub magap: Option<f32>,
    pub sigmagap: Option<f32>,
    pub distnr: Option<f32>,
    pub magnr: Option<f32>,
    pub sigmagnr: Option<f32>,
    pub chinr: Option<f32>,
    pub sharpnr: Option<f32>,
    pub sky: Option<f32>,
    pub fwhm: Option<f32>,
    pub mindtoedge: Option<f32>,
    pub seeratio: Option<f32>,
    pub aimage: Option<f32>,
    pub bimage: Option<f32>,
    pub elong: Option<f32>,
    pub nneg: Option<i32>,
    pub nbad: Option<i32>,
    pub rb: Option<f32>,
    pub ssdistnr: Option<f32>,
    pub ssmagnr: Option<f32>,
    #[serde(deserialize_with = "deserialize_ssnamenr")]
    pub ssnamenr: Option<String>,
    pub ranr: Option<f64>,
    pub decnr: Option<f64>,
    pub scorr: Option<f64>,
    pub magzpsci: Option<f32>,
}

#[serde_as]
#[skip_serializing_none]
#[serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, ToSchema)]
pub struct ZtfPrvCandidate {
    #[serde(flatten)]
    pub prv_candidate: PrvCandidate,
    #[serde(rename = "psfFlux")]
    pub psf_flux: Option<f32>,
    #[serde(rename = "psfFluxErr")]
    pub psf_flux_err: Option<f32>,
    pub snr: Option<f32>,
    pub band: Band,
}

impl TryFrom<PrvCandidate> for ZtfPrvCandidate {
    type Error = AlertError;
    fn try_from(prv_candidate: PrvCandidate) -> Result<Self, Self::Error> {
        let magpsf = prv_candidate.magpsf;
        let sigmapsf = prv_candidate.sigmapsf;
        let isdiffpos = prv_candidate.isdiffpos;
        let diffmaglim = prv_candidate.diffmaglim;
        let band = fid2band(prv_candidate.fid)?;

        let (psf_flux, psf_flux_err, snr) = match (magpsf, sigmapsf, isdiffpos, diffmaglim) {
            (Some(mag), Some(sigmag), Some(isdiff), _) => {
                let (flux, flux_err) = mag2flux(mag, sigmag, ZTF_ZP);
                let snr = flux / flux_err;
                (
                    Some(if isdiff {
                        flux * 1e9_f32
                    } else {
                        -flux * 1e9_f32
                    }), // convert to nJy
                    Some(flux_err * 1e9_f32), // convert to nJy
                    Some(snr),
                )
            }
            (None, None, None, Some(diffmaglim)) => {
                let flux_err = diffmaglim2fluxerr(diffmaglim, ZTF_ZP) * 1e9_f32; // convert to nJy
                (None, Some(flux_err), None)
            }
            _ => {
                return Err(AlertError::MissingDiffmaglim);
            }
        };

        Ok(ZtfPrvCandidate {
            prv_candidate,
            psf_flux,
            psf_flux_err,
            snr,
            band,
        })
    }
}

pub fn deserialize_prv_candidates<'de, D>(
    deserializer: D,
) -> Result<Option<Vec<ZtfPrvCandidate>>, D::Error>
where
    D: Deserializer<'de>,
{
    let prv_candidates = <Option<Vec<PrvCandidate>> as Deserialize>::deserialize(deserializer)?;
    match prv_candidates {
        Some(prv_candidates) => {
            let ztf_prv_candidates = prv_candidates
                .into_iter()
                .filter_map(|pc| {
                    ZtfPrvCandidate::try_from(pc)
                        .map_err(|e| {
                            warn!("Failed to convert PrvCandidate to ZtfPrvCandidate: {}", e);
                        })
                        .ok()
                })
                .collect();
            Ok(Some(ztf_prv_candidates))
        }
        None => Ok(None),
    }
}

pub fn deserialize_prv_candidate<'de, D>(deserializer: D) -> Result<ZtfPrvCandidate, D::Error>
where
    D: Deserializer<'de>,
{
    let prv_candidate: PrvCandidate = PrvCandidate::deserialize(deserializer)?;
    ZtfPrvCandidate::try_from(prv_candidate).map_err(serde::de::Error::custom)
}

/// avro alert schema
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Default, AvroSchema, ToSchema)]
#[serde(default)]
pub struct FpHist {
    pub field: Option<i32>,
    pub rcid: Option<i32>,
    pub fid: i32,
    pub pid: i64,
    pub rfid: i64,
    pub magzpsci: Option<f32>,
    pub magzpsciunc: Option<f32>,
    pub magzpscirms: Option<f32>,
    pub exptime: Option<f32>,
    pub diffmaglim: Option<f32>,
    pub programid: i32,
    pub jd: f64,
    #[serde(deserialize_with = "deserialize_missing_flux")]
    pub forcediffimflux: Option<f32>,
    #[serde(deserialize_with = "deserialize_missing_flux")]
    pub forcediffimfluxunc: Option<f32>,
    pub procstatus: Option<String>,
    pub distnr: Option<f32>,
    pub magnr: Option<f32>,
    pub sigmagnr: Option<f32>,
    pub chinr: Option<f32>,
    pub sharpnr: Option<f32>,
}

// we want a custom deserializer for forcediffimflux, to avoid NaN values and -9999.0
fn deserialize_missing_flux<'de, D>(deserializer: D) -> Result<Option<f32>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: Option<f32> = Option::deserialize(deserializer)?;
    Ok(value.filter(|&x| x != -99999.0 && !x.is_nan()))
}

#[serde_as]
#[skip_serializing_none]
#[serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, ToSchema)]
pub struct ZtfForcedPhot {
    #[serde(flatten)]
    pub fp_hist: FpHist,
    pub magpsf: Option<f32>,
    pub sigmapsf: Option<f32>,
    #[serde(rename = "psfFlux")]
    pub psf_flux: Option<f32>,
    #[serde(rename = "psfFluxErr")]
    pub psf_flux_err: Option<f32>,
    pub isdiffpos: Option<bool>,
    pub snr: Option<f32>,
    pub band: Band,
}

impl TryFrom<FpHist> for ZtfForcedPhot {
    type Error = AlertError;
    fn try_from(fp_hist: FpHist) -> Result<Self, Self::Error> {
        let psf_flux_err = fp_hist
            .forcediffimfluxunc
            .ok_or(AlertError::MissingFluxPSF)?;

        let band = fid2band(fp_hist.fid)?;
        let magzpsci = fp_hist.magzpsci.ok_or(AlertError::MissingMagZPSci)?;
        let zp_scaling_factor = 10f64.powf((ZTF_ZP as f64 - magzpsci as f64) / 2.5);

        let (magpsf, sigmapsf, isdiffpos, snr, psf_flux) = match fp_hist.forcediffimflux {
            Some(psf_flux) => {
                let psf_flux_abs = psf_flux.abs();
                if (psf_flux_abs / psf_flux_err) > SNT {
                    let (magpsf, sigmapsf) = flux2mag(psf_flux_abs, psf_flux_err, magzpsci);
                    (
                        Some(magpsf),
                        Some(sigmapsf),
                        Some(psf_flux > 0.0),
                        Some(psf_flux_abs / psf_flux_err),
                        Some((psf_flux as f64 * 1e9 * zp_scaling_factor) as f32), // convert to nJy and a fixed ZTF_ZP
                    )
                } else {
                    (
                        None,
                        None,
                        None,
                        None,
                        Some((psf_flux as f64 * 1e9 * zp_scaling_factor) as f32),
                    ) // convert to nJy and a fixed ZTF_ZP
                }
            }
            _ => (None, None, None, None, None),
        };

        Ok(ZtfForcedPhot {
            fp_hist,
            magpsf,
            sigmapsf,
            psf_flux,
            psf_flux_err: Some((psf_flux_err as f64 * 1e9 * zp_scaling_factor) as f32), // convert to nJy and a fixed ZTF_ZP
            isdiffpos,
            snr,
            band,
        })
    }
}

/// avro alert schema
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, Default, AvroSchema, ToSchema)]
#[serde(default)]
pub struct Candidate {
    pub jd: f64,
    pub fid: i32,
    pub pid: i64,
    pub diffmaglim: Option<f32>,
    pub programpi: Option<String>,
    pub programid: i32,
    pub candid: i64,
    #[serde(deserialize_with = "deserialize_isdiffpos")]
    pub isdiffpos: bool,
    pub nid: Option<i32>,
    pub rcid: Option<i32>,
    pub field: Option<i32>,
    pub ra: f64,
    pub dec: f64,
    pub magpsf: f32,
    pub sigmapsf: f32,
    pub chipsf: Option<f32>,
    pub magap: Option<f32>,
    pub sigmagap: Option<f32>,
    pub distnr: Option<f32>,
    pub magnr: Option<f32>,
    pub sigmagnr: Option<f32>,
    pub chinr: Option<f32>,
    pub sharpnr: Option<f32>,
    pub sky: Option<f32>,
    pub fwhm: Option<f32>,
    pub classtar: Option<f32>,
    pub mindtoedge: Option<f32>,
    pub seeratio: Option<f32>,
    pub aimage: Option<f32>,
    pub bimage: Option<f32>,
    pub elong: Option<f32>,
    pub nneg: Option<i32>,
    pub nbad: Option<i32>,
    pub rb: Option<f32>,
    pub ssdistnr: Option<f32>,
    pub ssmagnr: Option<f32>,
    #[serde(deserialize_with = "deserialize_ssnamenr")]
    pub ssnamenr: Option<String>,
    pub ranr: f64,
    pub decnr: f64,
    pub sgmag1: Option<f32>,
    pub srmag1: Option<f32>,
    pub simag1: Option<f32>,
    pub szmag1: Option<f32>,
    pub sgscore1: Option<f32>,
    pub distpsnr1: Option<f32>,
    pub ndethist: i32,
    pub ncovhist: i32,
    pub jdstarthist: Option<f64>,
    pub scorr: Option<f64>,
    pub sgmag2: Option<f32>,
    pub srmag2: Option<f32>,
    pub simag2: Option<f32>,
    pub szmag2: Option<f32>,
    pub sgscore2: Option<f32>,
    pub distpsnr2: Option<f32>,
    pub sgmag3: Option<f32>,
    pub srmag3: Option<f32>,
    pub simag3: Option<f32>,
    pub szmag3: Option<f32>,
    pub sgscore3: Option<f32>,
    pub distpsnr3: Option<f32>,
    pub nmtchps: i32,
    pub dsnrms: Option<f32>,
    pub ssnrms: Option<f32>,
    pub dsdiff: Option<f32>,
    pub magzpsci: Option<f32>,
    pub magzpsciunc: Option<f32>,
    pub magzpscirms: Option<f32>,
    pub zpmed: Option<f32>,
    pub exptime: Option<f32>,
    pub drb: Option<f32>,

    pub clrcoeff: Option<f32>,
    pub clrcounc: Option<f32>,
    pub neargaia: Option<f32>,
    pub maggaia: Option<f32>,
    pub neargaiabright: Option<f32>,
    pub maggaiabright: Option<f32>,
}

fn deserialize_isdiffpos_option<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: Deserializer<'de>,
{
    let value: serde_json::Value = Deserialize::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(s) => {
            // if s is in t, T, true, True, "1"
            if s.eq_ignore_ascii_case("t")
                || s.eq_ignore_ascii_case("true")
                || s.eq_ignore_ascii_case("1")
            {
                Ok(Some(true))
            } else {
                Ok(Some(false))
            }
        }
        serde_json::Value::Number(n) => Ok(Some(
            n.as_i64().ok_or(serde::de::Error::custom(
                "Failed to convert isdiffpos to i64",
            ))? == 1,
        )),
        serde_json::Value::Bool(b) => Ok(Some(b)),
        _ => Ok(None),
    }
}

fn deserialize_isdiffpos<'de, D>(deserializer: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_isdiffpos_option(deserializer).map(|x| x.unwrap())
}

pub fn deserialize_fp_hists<'de, D>(deserializer: D) -> Result<Option<Vec<ZtfForcedPhot>>, D::Error>
where
    D: Deserializer<'de>,
{
    let fp_hists = <Vec<FpHist> as Deserialize>::deserialize(deserializer)?
        .into_iter()
        .filter_map(|fp| ZtfForcedPhot::try_from(fp).ok())
        .collect();

    Ok(Some(fp_hists))
}

fn deserialize_ssnamenr<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    // if the value is null, "null", "", return None
    let value: Option<String> = Deserialize::deserialize(deserializer)?;
    Ok(value.filter(|s| !s.is_empty() && !s.eq_ignore_ascii_case("null")))
}

#[serde_as]
#[skip_serializing_none]
#[serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize, ToSchema)]
pub struct ZtfCandidate {
    #[serde(flatten)]
    pub candidate: Candidate,
    #[serde(rename = "psfFlux")]
    pub psf_flux: f32,
    #[serde(rename = "psfFluxErr")]
    pub psf_flux_err: f32,
    pub snr: f32,
    pub band: Band,
}

impl TryFrom<Candidate> for ZtfCandidate {
    type Error = AlertError;
    fn try_from(candidate: Candidate) -> Result<Self, Self::Error> {
        // here we add the flux, flux_err, snr fields
        let magpsf = candidate.magpsf;
        let sigmapsf = candidate.sigmapsf;
        let isdiffpos = candidate.isdiffpos;
        let band = fid2band(candidate.fid)?;

        let (flux, flux_err) = mag2flux(magpsf, sigmapsf, ZTF_ZP);

        Ok(ZtfCandidate {
            candidate,
            psf_flux: if isdiffpos {
                flux * 1e9_f32
            } else {
                -flux * 1e9_f32
            }, // convert to nJy
            psf_flux_err: flux_err * 1e9_f32, // convert to nJy
            snr: flux / flux_err,
            band,
        })
    }
}

impl TryFrom<&ZtfCandidate> for ZtfPrvCandidate {
    type Error = AlertError;
    fn try_from(ztf_candidate: &ZtfCandidate) -> Result<Self, Self::Error> {
        Ok(ZtfPrvCandidate {
            prv_candidate: PrvCandidate {
                jd: ztf_candidate.candidate.jd,
                fid: ztf_candidate.candidate.fid,
                pid: ztf_candidate.candidate.pid,
                diffmaglim: ztf_candidate.candidate.diffmaglim,
                programpi: ztf_candidate.candidate.programpi.clone(),
                programid: ztf_candidate.candidate.programid,
                candid: Some(ztf_candidate.candidate.candid),
                isdiffpos: Some(ztf_candidate.candidate.isdiffpos),
                nid: ztf_candidate.candidate.nid,
                rcid: ztf_candidate.candidate.rcid,
                field: ztf_candidate.candidate.field,
                ra: Some(ztf_candidate.candidate.ra),
                dec: Some(ztf_candidate.candidate.dec),
                magpsf: Some(ztf_candidate.candidate.magpsf),
                sigmapsf: Some(ztf_candidate.candidate.sigmapsf),
                chipsf: ztf_candidate.candidate.chipsf,
                magap: ztf_candidate.candidate.magap,
                sigmagap: ztf_candidate.candidate.sigmagap,
                distnr: ztf_candidate.candidate.distnr,
                magnr: ztf_candidate.candidate.magnr,
                sigmagnr: ztf_candidate.candidate.sigmagnr,
                chinr: ztf_candidate.candidate.chinr,
                sharpnr: ztf_candidate.candidate.sharpnr,
                sky: ztf_candidate.candidate.sky,
                fwhm: ztf_candidate.candidate.fwhm,
                mindtoedge: ztf_candidate.candidate.mindtoedge,
                seeratio: ztf_candidate.candidate.seeratio,
                aimage: ztf_candidate.candidate.aimage,
                bimage: ztf_candidate.candidate.bimage,
                elong: ztf_candidate.candidate.elong,
                nneg: ztf_candidate.candidate.nneg,
                nbad: ztf_candidate.candidate.nbad,
                rb: ztf_candidate.candidate.rb,
                ssdistnr: ztf_candidate.candidate.ssdistnr,
                ssmagnr: ztf_candidate.candidate.ssmagnr,
                ssnamenr: ztf_candidate.candidate.ssnamenr.clone(),
                ranr: Some(ztf_candidate.candidate.ranr),
                decnr: Some(ztf_candidate.candidate.decnr),
                scorr: ztf_candidate.candidate.scorr,
                magzpsci: ztf_candidate.candidate.magzpsci,
            },
            psf_flux: Some(ztf_candidate.psf_flux),
            psf_flux_err: Some(ztf_candidate.psf_flux_err),
            snr: Some(ztf_candidate.snr),
            band: ztf_candidate.band.clone(),
        })
    }
}

fn deserialize_candidate<'de, D>(deserializer: D) -> Result<ZtfCandidate, D::Error>
where
    D: Deserializer<'de>,
{
    let candidate: Candidate = Candidate::deserialize(deserializer)?;
    ZtfCandidate::try_from(candidate).map_err(serde::de::Error::custom)
}

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct ZtfRawAvroAlert {
    pub schemavsn: String,
    pub publisher: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candid: i64,
    #[serde(deserialize_with = "deserialize_candidate")]
    pub candidate: ZtfCandidate,
    #[serde(deserialize_with = "deserialize_prv_candidates")]
    pub prv_candidates: Option<Vec<ZtfPrvCandidate>>,
    #[serde(deserialize_with = "deserialize_fp_hists")]
    pub fp_hists: Option<Vec<ZtfForcedPhot>>,
    #[serde(
        rename = "cutoutScience",
        deserialize_with = "deserialize_cutout_as_bytes"
    )]
    pub cutout_science: Vec<u8>,
    #[serde(
        rename = "cutoutTemplate",
        deserialize_with = "deserialize_cutout_as_bytes"
    )]
    pub cutout_template: Vec<u8>,
    #[serde(
        rename = "cutoutDifference",
        deserialize_with = "deserialize_cutout_as_bytes"
    )]
    pub cutout_difference: Vec<u8>,
}

fn deserialize_cutout_as_bytes<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let cutout: Option<Cutout> = Option::deserialize(deserializer)?;
    // if cutout is None, return an error
    match cutout {
        None => Err(serde::de::Error::custom("Missing cutout data")),
        Some(cutout) => Ok(cutout.stamp_data),
    }
}

#[derive(Debug, Deserialize, Serialize, AvroSchema, ToSchema, Default)]
pub struct ZtfAliases {
    #[serde(rename = "LSST")]
    pub lsst: Vec<String>,
    #[serde(rename = "DECAM")]
    pub decam: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ZtfObject {
    #[serde(rename = "_id")]
    pub object_id: String,
    pub prv_candidates: Vec<ZtfPrvCandidate>,
    #[serde(default)]
    pub prv_nondetections: Vec<ZtfPrvCandidate>,
    pub fp_hists: Vec<ZtfForcedPhot>,
    pub cross_matches: Option<HashMap<String, Vec<Document>>>,
    pub aliases: Option<ZtfAliases>,
    pub coordinates: Coordinates,
    pub created_at: f64,
    pub updated_at: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ZtfAlert {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: ZtfCandidate,
    pub coordinates: Coordinates,
    pub created_at: f64,
    pub updated_at: f64,
}

pub struct ZtfAlertWorker {
    stream_name: String,
    xmatch_configs: Vec<conf::CatalogXmatchConfig>,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<ZtfAlert>,
    alert_aux_collection: mongodb::Collection<ZtfObject>,
    alert_aux_collection_update: mongodb::Collection<AlertAuxForUpdate>,
    alert_cutout_collection: mongodb::Collection<AlertCutout>,
    schema_cache: SchemaCache,
    lsst_alert_aux_collection: mongodb::Collection<Document>,
    decam_alert_aux_collection: mongodb::Collection<Document>,
}

#[derive(Deserialize, Serialize)]
struct AlertAuxForUpdate {
    pub prv_candidates: Vec<LightcurveJdOnly>,
    pub prv_nondetections: Vec<LightcurveJdOnly>,
    pub fp_hists: Vec<LightcurveJdOnly>,
    pub version: Option<i32>,
}

impl ZtfAlertWorker {
    #[instrument(skip(self), err)]
    async fn get_survey_matches(&self, ra: f64, dec: f64) -> Result<ZtfAliases, AlertError> {
        let lsst_matches = self
            .get_matches(
                ra,
                dec,
                lsst::LSST_DEC_RANGE,
                ZTF_LSST_XMATCH_RADIUS,
                &self.lsst_alert_aux_collection,
            )
            .await?;

        let decam_matches = self
            .get_matches(
                ra,
                dec,
                decam::DECAM_DEC_RANGE,
                ZTF_DECAM_XMATCH_RADIUS,
                &self.decam_alert_aux_collection,
            )
            .await?;
        Ok(ZtfAliases {
            lsst: lsst_matches,
            decam: decam_matches,
        })
    }

    async fn get_existing_aux(
        &self,
        object_id: String,
    ) -> Result<Option<AlertAuxForUpdate>, AlertError> {
        let result = self
            .alert_aux_collection_update
            .find_one(doc! { "_id": &object_id })
            .projection(
                doc! { "prv_candidates.jd": 1, "prv_nondetections.jd": 1, "fp_hists.jd": 1, "version": 1 },
            )
            .await
            .inspect_err(as_error!())?;
        Ok(result)
    }

    #[instrument(skip(self, prv_candidates, existing_prv_candidates,), err)]
    async fn prepare_prv_candidates_update(
        self: &mut Self,
        prv_candidates: &Vec<ZtfPrvCandidate>,
        existing_prv_candidates: &Vec<LightcurveJdOnly>,
    ) -> Result<(Vec<Document>, bool), AlertError> {
        // if there is no new data, no update needed
        if prv_candidates.is_empty() {
            return Ok((vec![], false));
        }

        // if there is no existing data, we can just append without sorting
        if existing_prv_candidates.is_empty() {
            let new_prv_candidates_docs: Vec<Document> =
                prv_candidates.iter().map(|pc| mongify(pc)).collect();
            return Ok((new_prv_candidates_docs, false));
        }

        let min_new_jd_prv_candidate = prv_candidates
            .iter()
            .map(|pc| pc.prv_candidate.jd)
            .fold(f64::INFINITY, f64::min);
        let (existing_prv_candidate_jds, max_existing_jd_prv_candidate) = existing_prv_candidates
            .iter()
            .fold((HashSet::new(), None::<f64>), |(mut set, max_jd), pc| {
                set.insert(pc.jd.to_bits());
                let max_jd = match max_jd {
                    Some(jd) => Some(jd.max(pc.jd)),
                    None => Some(pc.jd),
                };
                (set, max_jd)
            });

        // if all new data is newer than existing data, we can just append without sorting
        if min_new_jd_prv_candidate > max_existing_jd_prv_candidate.unwrap() {
            let new_prv_candidates_docs: Vec<Document> =
                prv_candidates.iter().map(|pc| mongify(pc)).collect();
            return Ok((new_prv_candidates_docs, false));
        }

        // we filter out points from the "new" data that are already in the existing data (deduplication)
        // and at the same time find the minimum jd of the new post-deduplication data, to check if we can skip sorting
        let (new_prv_candidates_docs, min_new_jd_prv_candidate) =
            prv_candidates
                .iter()
                .fold((vec![], f64::INFINITY), |(mut docs, min_jd), pc| {
                    let min_jd = min_jd.min(pc.prv_candidate.jd);
                    if !existing_prv_candidate_jds.contains(&pc.prv_candidate.jd.to_bits()) {
                        docs.push(mongify(pc));
                    }
                    (docs, min_jd)
                });

        // if all the deduplicated new data is newer than existing data, we can just append without sorting
        if min_new_jd_prv_candidate > max_existing_jd_prv_candidate.unwrap() {
            return Ok((new_prv_candidates_docs, false));
        }

        // else, we need to do the full update with sorting
        Ok((new_prv_candidates_docs, true))
    }

    async fn prepare_fp_hists_update(
        self: &mut Self,
        fp_hists: &Vec<ZtfForcedPhot>,
        existing_fp_hists: &Vec<LightcurveJdOnly>,
    ) -> Result<(Vec<Document>, bool), AlertError> {
        // if there is no new data, no update needed
        if fp_hists.is_empty() {
            return Ok((vec![], false));
        }

        // if there is no existing data, we can just append without sorting
        if existing_fp_hists.is_empty() {
            let new_fp_hists_docs: Vec<Document> = fp_hists.iter().map(|pc| mongify(pc)).collect();
            return Ok((new_fp_hists_docs, false));
        }

        let min_new_jd_fp_hist = fp_hists
            .iter()
            .map(|pc| pc.fp_hist.jd)
            .fold(f64::INFINITY, f64::min);
        let (existing_fp_hist_jds, max_existing_jd_fp_hist) = existing_fp_hists.iter().fold(
            (HashSet::new(), None::<f64>),
            |(mut set, max_jd), pc| {
                set.insert(pc.jd.to_bits());
                let max_jd = match max_jd {
                    Some(jd) => Some(jd.max(pc.jd)),
                    None => Some(pc.jd),
                };
                (set, max_jd)
            },
        );

        // if all new data is newer than existing data, we can just append without sorting
        if min_new_jd_fp_hist > max_existing_jd_fp_hist.unwrap() {
            let new_fp_hists_docs: Vec<Document> = fp_hists.iter().map(|pc| mongify(pc)).collect();
            return Ok((new_fp_hists_docs, false));
        }

        // we filter out points from the "new" data that are already in the existing data (deduplication)
        // and at the same time find the minimum jd of the new post-deduplication data, to check if we can skip sorting
        let (new_fp_hists_docs, min_new_jd_fp_hist) =
            fp_hists
                .iter()
                .fold((vec![], f64::INFINITY), |(mut docs, min_jd), pc| {
                    let min_jd = min_jd.min(pc.fp_hist.jd);
                    if !existing_fp_hist_jds.contains(&pc.fp_hist.jd.to_bits()) {
                        docs.push(mongify(pc));
                    }
                    (docs, min_jd)
                });

        // if all the deduplicated new data is newer than existing data, we can just append without sorting
        if min_new_jd_fp_hist > max_existing_jd_fp_hist.unwrap() {
            return Ok((new_fp_hists_docs, false));
        }

        // else, we need to do the full update with sorting
        Ok((new_fp_hists_docs, true))
    }

    #[instrument(
        skip(self, prv_candidates, prv_nondetections, fp_hists, survey_matches),
        err
    )]
    async fn update_aux_fallback(
        self: &mut Self,
        object_id: &str,
        prv_candidates: &Vec<ZtfPrvCandidate>,
        prv_nondetections: &Vec<ZtfPrvCandidate>,
        fp_hists: &Vec<ZtfForcedPhot>,
        survey_matches: &Option<ZtfAliases>,
        now: f64,
    ) -> Result<(), AlertError> {
        let update_pipeline = vec![doc! {
            "$set": {
                "prv_candidates": update_timeseries_op("prv_candidates", "jd", &prv_candidates.iter().map(|pc| mongify(pc)).collect::<Vec<Document>>()),
                "prv_nondetections": update_timeseries_op("prv_nondetections", "jd", &prv_nondetections.iter().map(|pc| mongify(pc)).collect::<Vec<Document>>()),
                "fp_hists": update_timeseries_op("fp_hists", "jd", &fp_hists.iter().map(|pc| mongify(pc)).collect::<Vec<Document>>()),
                "aliases": mongify(survey_matches),
                "updated_at": now,
                // we still want to increment the version even in the fallback,
                // to prevent concurrency issues between a fallback update and a normal update from another thread
                "version": doc! { "$add": [ { "$ifNull": [ "$version", 0 ] }, 1 ] },
            }
        }];
        self.alert_aux_collection
            .update_one(doc! { "_id": object_id }, update_pipeline)
            .await?;
        Ok(())
    }

    #[instrument(
        skip(
            self,
            prv_candidates,
            prv_nondetections,
            fp_hists,
            survey_matches,
            existing_alert_aux
        ),
        err
    )]
    async fn update_aux(
        self: &mut Self,
        object_id: &str,
        prv_candidates: &Vec<ZtfPrvCandidate>,
        prv_nondetections: &Vec<ZtfPrvCandidate>,
        fp_hists: &Vec<ZtfForcedPhot>,
        survey_matches: &Option<ZtfAliases>,
        now: f64,
        existing_alert_aux: &AlertAuxForUpdate,
    ) -> Result<(), AlertError> {
        let current_version = existing_alert_aux.version;
        let (new_prv_candidates_docs, need_sort_prv_candidates) = self
            .prepare_prv_candidates_update(prv_candidates, &existing_alert_aux.prv_candidates)
            .await?;
        let (new_prv_nondetections_docs, need_sort_prv_nondetections) = self
            .prepare_prv_candidates_update(prv_nondetections, &existing_alert_aux.prv_nondetections)
            .await?;
        let (new_fp_hists_docs, need_sort_fp_hists) = self
            .prepare_fp_hists_update(fp_hists, &existing_alert_aux.fp_hists)
            .await?;

        let mut push_updates = Document::new();
        if !new_prv_candidates_docs.is_empty() {
            if need_sort_prv_candidates {
                push_updates.insert(
                    "prv_candidates",
                    doc! { "$each": new_prv_candidates_docs, "$sort": { "jd": 1 } },
                );
            } else {
                push_updates.insert("prv_candidates", doc! { "$each": new_prv_candidates_docs });
            }
        }
        if !new_prv_nondetections_docs.is_empty() {
            if need_sort_prv_nondetections {
                push_updates.insert(
                    "prv_nondetections",
                    doc! { "$each": new_prv_nondetections_docs, "$sort": { "jd": 1 } },
                );
            } else {
                push_updates.insert(
                    "prv_nondetections",
                    doc! { "$each": new_prv_nondetections_docs },
                );
            }
        }
        if !new_fp_hists_docs.is_empty() {
            if need_sort_fp_hists {
                push_updates.insert(
                    "fp_hists",
                    doc! { "$each": new_fp_hists_docs, "$sort": { "jd": 1 } },
                );
            } else {
                push_updates.insert("fp_hists", doc! { "$each": new_fp_hists_docs });
            }
        }

        let update_doc = if push_updates.is_empty() {
            doc! {
                "$set": {
                    "aliases": mongify(survey_matches),
                    "updated_at": now,
                    "version": current_version.unwrap_or(0) + 1,
                }
            }
        } else {
            doc! {
                "$push": push_updates,
                "$set": {
                    "aliases": mongify(survey_matches),
                    "updated_at": now,
                    "version": current_version.unwrap_or(0) + 1,
                }
            }
        };
        let find_doc = if let Some(version) = current_version {
            doc! { "_id": object_id, "version": version }
        } else {
            doc! { "_id": object_id, "version": { "$exists": false } }
        };
        let update_result = self
            .alert_aux_collection
            .update_one(find_doc, update_doc)
            .await?;
        if update_result.matched_count == 0 {
            warn!(
                "Concurrent modification detected for object_id {}. Using DB-only update.",
                object_id
            );
            return self
                .update_aux_fallback(
                    object_id,
                    prv_candidates,
                    prv_nondetections,
                    fp_hists,
                    survey_matches,
                    now,
                )
                .await;
        }
        Ok(())
    }

    #[instrument(skip_all)]
    fn format_prv_candidates(
        &self,
        prv_candidates: Vec<ZtfPrvCandidate>,
        candidate: &ZtfCandidate,
    ) -> (Vec<ZtfPrvCandidate>, Vec<ZtfPrvCandidate>) {
        // we split the prv_candidates into detections and non-detections
        let (mut new_prv_candidates, prv_nondetections): (
            Vec<ZtfPrvCandidate>,
            Vec<ZtfPrvCandidate>,
        ) = prv_candidates
            .into_iter()
            .partition(|p| p.prv_candidate.magpsf.is_some());
        // use the from candidate to create a PrvCandidate and add to new_prv_candidates
        new_prv_candidates.push(ZtfPrvCandidate::try_from(candidate).unwrap());

        (new_prv_candidates, prv_nondetections)
    }
}

#[async_trait::async_trait]
impl AlertWorker for ZtfAlertWorker {
    #[instrument(err)]
    async fn new(config_path: &str) -> Result<ZtfAlertWorker, AlertWorkerError> {
        let config = AppConfig::from_path(config_path)?;

        let xmatch_configs = config
            .crossmatch
            .get(&Survey::Ztf)
            .cloned()
            .unwrap_or_default();

        let db: mongodb::Database = config
            .build_db()
            .await
            .inspect_err(as_error!("failed to create mongo client"))?;

        let alert_collection = db.collection(&ALERT_COLLECTION);
        let alert_aux_collection = db.collection(&ALERT_AUX_COLLECTION);
        let alert_aux_collection_update = db.collection(&ALERT_AUX_COLLECTION);
        let alert_cutout_collection = db.collection(&ALERT_CUTOUT_COLLECTION);

        let lsst_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&lsst::ALERT_AUX_COLLECTION);

        let decam_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&decam::ALERT_AUX_COLLECTION);

        let worker = ZtfAlertWorker {
            stream_name: STREAM_NAME.to_string(),
            xmatch_configs,
            db,
            alert_collection,
            alert_aux_collection,
            alert_cutout_collection,
            schema_cache: SchemaCache::default(),
            lsst_alert_aux_collection,
            decam_alert_aux_collection,
            alert_aux_collection_update,
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
        let mut avro_alert: ZtfRawAvroAlert = self
            .schema_cache
            .alert_from_avro_bytes(avro_bytes)
            .inspect_err(as_error!())?;

        let candid = avro_alert.candid;
        let object_id = avro_alert.object_id;
        let ra = avro_alert.candidate.candidate.ra;
        let dec = avro_alert.candidate.candidate.dec;

        let prv_candidates = match avro_alert.prv_candidates.take() {
            Some(candidates) => candidates,
            None => Vec::new(),
        };
        let mut fp_hists = match avro_alert.fp_hists.take() {
            Some(hists) => hists,
            None => Vec::new(),
        };

        let candidate: ZtfCandidate = avro_alert.candidate;

        // add the cutouts, skip processing if the cutouts already exist
        let cutout_status = self
            .format_and_insert_cutouts(
                candid,
                avro_alert.cutout_science,
                avro_alert.cutout_template,
                avro_alert.cutout_difference,
                &self.alert_cutout_collection,
            )
            .await
            .inspect_err(as_error!())?;

        if let ProcessAlertStatus::Exists(_) = cutout_status {
            return Ok(cutout_status);
        }

        let existing_alert_aux = self.get_existing_aux(object_id.clone()).await?;

        let (mut prv_candidates, mut prv_nondetections) =
            self.format_prv_candidates(prv_candidates, &candidate);

        // let's make sure all 3 arrays are sorted by jd ascending
        prv_candidates.sort_by(|a, b| {
            a.prv_candidate
                .jd
                .partial_cmp(&b.prv_candidate.jd)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        prv_nondetections.sort_by(|a, b| {
            a.prv_candidate
                .jd
                .partial_cmp(&b.prv_candidate.jd)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        fp_hists.sort_by(|a, b| {
            a.fp_hist
                .jd
                .partial_cmp(&b.fp_hist.jd)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let survey_matches = Some(
            self.get_survey_matches(ra, dec)
                .await
                .inspect_err(as_error!())?,
        );

        if existing_alert_aux.is_none() {
            let xmatches = xmatch(ra, dec, &self.xmatch_configs, &self.db).await?;
            let obj = ZtfObject {
                object_id: object_id.clone(),
                prv_candidates,
                prv_nondetections,
                fp_hists,
                cross_matches: Some(xmatches),
                aliases: survey_matches,
                coordinates: Coordinates::new(ra, dec),
                created_at: now,
                updated_at: now,
            };
            let result = self.insert_aux(&obj, &self.alert_aux_collection).await;
            if let Err(AlertError::AlertAuxExists) = result {
                warn!("Alert aux document for object_id {} already exists. This can happen when multiple alerts with the same object_id are processed concurrently. Attempting to update the existing document.", object_id);
                let existing_alert_aux: Option<AlertAuxForUpdate> =
                    self.get_existing_aux(object_id.clone()).await?;
                self.update_aux(
                    &object_id,
                    &obj.prv_candidates,
                    &obj.prv_nondetections,
                    &obj.fp_hists,
                    &obj.aliases,
                    now,
                    &existing_alert_aux.unwrap(),
                )
                .await
                .inspect_err(as_error!())?;
            } else {
                result.inspect_err(as_error!())?;
            }
        } else {
            self.update_aux(
                &object_id,
                &prv_candidates,
                &prv_nondetections,
                &fp_hists,
                &survey_matches,
                now,
                &existing_alert_aux.unwrap(),
            )
            .await
            .inspect_err(as_error!())?;
        }

        let alert = ZtfAlert {
            candid,
            object_id: object_id.clone(),
            candidate,
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
        testing::{ztf_alert_worker, AlertRandomizer},
    };

    #[tokio::test]
    async fn test_ztf_alert_from_avro_bytes() {
        let mut alert_worker = ztf_alert_worker().await;

        let (candid, object_id, ra, dec, bytes_content) =
            AlertRandomizer::new_randomized(Survey::Ztf).get().await;
        let avro_alert = alert_worker
            .schema_cache
            .alert_from_avro_bytes(&bytes_content);
        assert!(avro_alert.is_ok());

        // validate the alert
        let avro_alert: ZtfRawAvroAlert = avro_alert.unwrap();
        assert_eq!(avro_alert.schemavsn, "4.02");
        assert_eq!(avro_alert.publisher, "ZTF (www.ztf.caltech.edu)");
        assert_eq!(avro_alert.object_id, object_id);
        assert_eq!(avro_alert.candid, candid);
        assert_eq!(avro_alert.candidate.candidate.ra, ra);
        assert_eq!(avro_alert.candidate.candidate.dec, dec);

        // validate the prv_candidates
        let prv_candidates = avro_alert.clone().prv_candidates;
        assert!(!prv_candidates.is_none());

        let prv_candidates = prv_candidates.unwrap();
        assert_eq!(prv_candidates.len(), 10);

        let non_detection = prv_candidates.get(0).unwrap();
        assert_eq!(non_detection.prv_candidate.magpsf.is_none(), true);
        assert_eq!(non_detection.prv_candidate.diffmaglim.is_some(), true);

        let detection = prv_candidates.get(1).unwrap();
        assert_eq!(detection.prv_candidate.magpsf.is_some(), true);
        assert_eq!(detection.prv_candidate.sigmapsf.is_some(), true);
        assert_eq!(detection.prv_candidate.diffmaglim.is_some(), true);
        assert_eq!(detection.prv_candidate.isdiffpos.is_some(), true);

        // let's also verify that we can recover the original magpsf and sigmapsf from the flux and flux_err for the detection
        let (magpsf, sigmapsf) = flux2mag(
            detection.psf_flux.unwrap() / 1e9_f32,     // convert back to Jy
            detection.psf_flux_err.unwrap() / 1e9_f32, // convert back to Jy
            ZTF_ZP,
        );
        assert!((magpsf - detection.prv_candidate.magpsf.unwrap()).abs() < 1e-6);
        assert!((sigmapsf - detection.prv_candidate.sigmapsf.unwrap()).abs() < 1e-6);

        // validate the fp_hists
        let fp_hists = avro_alert.clone().fp_hists;
        assert!(fp_hists.is_some());

        let fp_hists = fp_hists.unwrap();
        assert_eq!(fp_hists.len(), 10);

        // at the moment, negative fluxes should yield detections,
        // but with isdiffpos = false
        let fp_negative_det = fp_hists.get(0).unwrap();
        assert!((fp_negative_det.magpsf.unwrap() - 15.949999).abs() < 1e-6);
        assert!((fp_negative_det.sigmapsf.unwrap() - 0.002316).abs() < 1e-6);
        assert!((fp_negative_det.fp_hist.diffmaglim.unwrap() - 20.4005).abs() < 1e-6);
        assert_eq!(fp_negative_det.isdiffpos.unwrap(), false);
        assert!((fp_negative_det.snr.unwrap() - 468.75623).abs() < 1e-6);
        assert!((fp_negative_det.fp_hist.jd - 2460447.920278).abs() < 1e-6);
        assert_eq!(fp_negative_det.band, Band::G);
        // let's verify that the psf_flux is negative AND that we can recover the original magpsf and sigmapsf from the flux and flux_err
        assert!(fp_negative_det.psf_flux.unwrap() < 0.0);
        let (magpsf, sigmapsf) = flux2mag(
            -fp_negative_det.psf_flux.unwrap() / 1e9_f32, // convert back to Jy
            fp_negative_det.psf_flux_err.unwrap() / 1e9_f32, // convert back to Jy
            ZTF_ZP,
        );
        assert!((magpsf - 15.949999).abs() < 1e-6);
        assert!((sigmapsf - 0.002316).abs() < 1e-6);
        // let's also verify that forcediffimflux(unc) converts to psfFlux(Err) correctly
        let zp_scaling_factor =
            10f64.powf((ZTF_ZP as f64 - fp_negative_det.fp_hist.magzpsci.unwrap() as f64) / 2.5);
        let expected_flux = (fp_negative_det.fp_hist.forcediffimflux.unwrap() as f64
            * 1e9
            * zp_scaling_factor) as f32;
        let expected_flux_err = (fp_negative_det.fp_hist.forcediffimfluxunc.unwrap() as f64
            * 1e9
            * zp_scaling_factor) as f32;
        assert!(
            (expected_flux - fp_negative_det.psf_flux.unwrap()).abs() < 1e-6,
            "Expected flux: {}, PSF flux: {}",
            expected_flux,
            fp_negative_det.psf_flux.unwrap()
        );
        assert!(
            (expected_flux_err - fp_negative_det.psf_flux_err.unwrap()).abs() < 1e-6,
            "Expected flux err: {}, PSF flux err: {}",
            expected_flux_err,
            fp_negative_det.psf_flux_err.unwrap()
        );

        let fp_positive_det = fp_hists.get(9).unwrap();
        assert!((fp_positive_det.magpsf.unwrap() - 20.801506).abs() < 1e-6);
        assert!((fp_positive_det.sigmapsf.unwrap() - 0.3616859).abs() < 1e-6);
        assert!((fp_positive_det.fp_hist.diffmaglim.unwrap() - 19.7873).abs() < 1e-6);
        assert_eq!(fp_positive_det.isdiffpos.is_some(), true);
        assert!((fp_positive_det.snr.unwrap() - 3.0018756).abs() < 1e-6);
        assert!((fp_positive_det.fp_hist.jd - 2460420.9637616).abs() < 1e-6);
        assert_eq!(fp_positive_det.band, Band::G);
        // let's verify that the psf_flux is positive AND that we can recover the original magpsf and sigmapsf from the flux and flux_err
        assert!(fp_positive_det.psf_flux.unwrap() > 0.0);
        let (magpsf, sigmapsf) = flux2mag(
            fp_positive_det.psf_flux.unwrap() / 1e9_f32, // convert back to Jy
            fp_positive_det.psf_flux_err.unwrap() / 1e9_f32, // convert back to Jy
            ZTF_ZP,
        );
        assert!((magpsf - 20.801506).abs() < 1e-6);
        assert!((sigmapsf - 0.3616859).abs() < 1e-6);
        let zp_scaling_factor =
            10f64.powf((ZTF_ZP as f64 - fp_positive_det.fp_hist.magzpsci.unwrap() as f64) / 2.5);
        let expected_flux = (fp_positive_det.fp_hist.forcediffimflux.unwrap() as f64
            * 1e9
            * zp_scaling_factor) as f32;
        let expected_flux_err = (fp_positive_det.fp_hist.forcediffimfluxunc.unwrap() as f64
            * 1e9
            * zp_scaling_factor) as f32;
        assert!(
            (expected_flux - fp_positive_det.psf_flux.unwrap()).abs() < 1e-6,
            "Expected flux: {}, PSF flux: {}",
            expected_flux,
            fp_positive_det.psf_flux.unwrap()
        );
        assert!(
            (expected_flux_err - fp_positive_det.psf_flux_err.unwrap()).abs() < 1e-6,
            "Expected flux err: {}, PSF flux err: {}",
            expected_flux_err,
            fp_positive_det.psf_flux_err.unwrap()
        );

        // validate the cutouts
        assert_eq!(avro_alert.cutout_science.len(), 13107);
        assert_eq!(avro_alert.cutout_template.len(), 12410);
        assert_eq!(avro_alert.cutout_difference.len(), 14878);
    }
}
