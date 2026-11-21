use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

use crate::{
    alert::{
        base::{AlertError, AlertWorker, AlertWorkerError, ProcessAlertStatus, SchemaCache},
        lsst, ztf, TimeSeries,
    },
    conf::{self, AppConfig},
    utils::{
        cutouts::CutoutStorage,
        db::{mongify, mongify_vec},
        enums::Survey,
        host::{self, HostGalaxyConfig},
        o11y::logging::as_error,
        spatial::{xmatch, Coordinates},
        worker::{WorkerQueues, WorkerType},
    },
};
use apache_avro::{from_avro_datum, from_value, to_avro_datum, to_value, Schema};
use constcat::concat;
use flare::Time;
use mongodb::bson::{doc, Bson, Document};
use serde::{Deserialize, Serialize};
use serde_with::skip_serializing_none;
use tracing::instrument;

pub const STREAM_NAME: &str = "ARGUS";
pub const ARGUS_DEC_RANGE: (f64, f64) = (-60.0, 90.0);
pub const ARGUS_POSITION_UNCERTAINTY: f64 = 2.0;
pub const ALERT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts");
pub const ALERT_AUX_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_aux");
pub const ARGUS_RESULTS_TOPIC: &str = concat!(STREAM_NAME, "_alerts_results");

pub const ARGUS_ZTF_XMATCH_RADIUS: f64 =
    (ARGUS_POSITION_UNCERTAINTY.max(ztf::ZTF_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();
pub const ARGUS_LSST_XMATCH_RADIUS: f64 =
    (ARGUS_POSITION_UNCERTAINTY.max(lsst::LSST_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();

pub const ARGUS_SCHEMA_ID: u32 = 3;

const ALERT_SCHEMA: &str = include_str!("../../schemas/argus/3.0/argus.alert.avsc");
const CANDIDATE_SCHEMA: &str = include_str!("../../schemas/argus/3.0/argus.alert.candidate.avsc");
const EVENT_SCHEMA: &str =
    include_str!("../../schemas/argus/3.0/argus.alert.eventAssociation.avsc");
const SSO_SCHEMA: &str = include_str!("../../schemas/argus/3.0/argus.alert.ssoAssociation.avsc");

pub static ARGUS_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    let composed = inline_named_types(ALERT_SCHEMA, &[CANDIDATE_SCHEMA, EVENT_SCHEMA, SSO_SCHEMA]);
    Schema::parse_str(&composed).expect("the bundled Argus schema must parse")
});

fn inline_named_types(root: &str, named: &[&str]) -> String {
    let mut pending: HashMap<String, serde_json::Value> = named
        .iter()
        .map(|json| {
            let value: serde_json::Value =
                serde_json::from_str(json).expect("the bundled Argus schema must be JSON");
            let fullname = format!(
                "{}.{}",
                value["namespace"].as_str().unwrap_or_default(),
                value["name"].as_str().unwrap_or_default()
            );
            (fullname, value)
        })
        .collect();
    let mut root: serde_json::Value =
        serde_json::from_str(root).expect("the bundled Argus schema must be JSON");
    inline_into(&mut root, &mut pending);
    root.to_string()
}

fn inline_into(value: &mut serde_json::Value, pending: &mut HashMap<String, serde_json::Value>) {
    match value {
        serde_json::Value::String(name) => {
            if let Some(mut definition) = pending.remove(name.as_str()) {
                inline_into(&mut definition, pending);
                *value = definition;
            }
        }
        serde_json::Value::Array(items) => {
            for item in items {
                inline_into(item, pending);
            }
        }
        serde_json::Value::Object(map) => {
            for key in ["type", "items", "values", "fields"] {
                if let Some(child) = map.get_mut(key) {
                    inline_into(child, pending);
                }
            }
        }
        _ => {}
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArgusCandidate {
    pub alert_id: String,
    pub obs_start_time: String,
    pub obs_end_time: String,
    pub obs_weighted_midpoint: String,
    pub integration_time: f32,
    pub coadd: bool,
    pub coadd_spacing: Option<i64>,
    pub filter: i32,
    pub epoch_id: String,
    pub camera_ids: Vec<i32>,
    pub ota_ids: Vec<i32>,
    pub filter_ids: Vec<i32>,
    pub focuser_ids: Vec<i32>,
    pub subarray_ids: Vec<i32>,
    pub timestamp: i64,
    pub withdrawal_time: Option<String>,
    pub ra: f64,
    pub dec: f64,
    pub ra_err: Option<f32>,
    pub dec_err: Option<f32>,
    pub ra_dec_covariance: Option<f32>,
    pub x: f32,
    pub y: f32,
    pub x_err: Option<f32>,
    pub y_err: Option<f32>,
    pub flags: i32,
    pub tanseg_id: i64,
    pub minipix_idx: Option<i32>,
    pub healpix_id: Option<i64>,
    pub healpix_order: Option<i32>,
    pub num_reference_sources: Option<i32>,
    #[serde(rename = "residualRMS")]
    pub residual_rms: Option<f32>,
    pub psf_flux_sci: Option<f32>,
    pub psf_magnitude_sci: Option<f32>,
    pub psf_flux_dif: Option<f32>,
    pub psf_magnitude_dif: Option<f32>,
    pub psf_flux_sci_err: Option<f32>,
    pub psf_magnitude_sci_err: Option<f32>,
    pub psf_flux_dif_err: Option<f32>,
    pub psf_magnitude_dif_err: Option<f32>,
    pub psf_chi2: Option<f32>,
    pub aper_flux_sci: Option<f32>,
    pub aper_magnitude_sci: Option<f32>,
    pub aper_flux_dif: Option<f32>,
    pub aper_magnitude_dif: Option<f32>,
    pub aper_flux_sci_err: Option<f32>,
    pub aper_magnitude_sci_err: Option<f32>,
    pub aper_flux_dif_err: Option<f32>,
    pub aper_magnitude_dif_err: Option<f32>,
    pub psf_snr: Option<f32>,
    pub aper_snr: Option<f32>,
    pub is_flux_positive: bool,
    pub psf_lim_mag: Option<f32>,
    pub aper_lim_mag: Option<f32>,
    pub sky_background: Option<f32>,
    pub psf_zeropoint: Option<f32>,
    pub aper_zeropoint: Option<f32>,
    pub psf_zp_err: Option<f32>,
    pub aper_zp_err: Option<f32>,
    pub placeholder_photometric_calibration: Option<String>,
    pub star_galaxy_placeholder: Option<f32>,
    pub fwhm: Option<f32>,
    pub ellipticity: Option<f32>,
    pub n_pixels: Option<i32>,
    pub peak: Option<f32>,
    pub psf_fwhm: Option<f32>,
    pub psf_beta: Option<f32>,
    pub psf_over_ap: Option<f32>,
    pub artefact_placeholder: Option<String>,
    pub real_bogus_score: Option<f32>,
    pub rb_model_version: String,
    pub real_bogus_confidence: Option<f32>,
    pub placeholder_catalog_context: Option<String>,
    pub reference_id: i64,
    pub reference_start: f64,
    pub reference_end: f64,
    pub reference_depth: f32,
}

impl ArgusCandidate {
    pub fn jd(&self) -> Result<f64, AlertError> {
        iso_to_jd(&self.obs_weighted_midpoint)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArgusEventAssociation {
    pub event_id: String,
    pub event_ra: f64,
    pub event_ra_err: Option<f32>,
    pub event_dec: f64,
    pub event_dec_err: Option<f32>,
    pub summary_start: Option<String>,
    pub first_detection: Option<i64>,
    pub most_recent_detection_at_cadence: Option<i64>,
    pub num_detection_at_this_cadence: Option<i64>,
    pub num_detection_base_cadence: Option<i64>,
    pub beta_psf_flux_mean: Option<f32>,
    pub beta_psf_flux_min: Option<f32>,
    pub beta_psf_flux_max: Option<f32>,
    pub beta_psf_flux_slope_before_max: Option<f32>,
    pub beta_psf_flux_slope_after_max: Option<f32>,
    pub beta_ap_flux_mean: Option<f32>,
    pub beta_ap_flux_min: Option<f32>,
    pub beta_ap_flux_max: Option<f32>,
    pub beta_ap_flux_slope_before_max: Option<f32>,
    pub beta_ap_flux_slope_after_max: Option<f32>,
    pub rho_psf_flux_mean: Option<f32>,
    pub rho_psf_flux_min: Option<f32>,
    pub rho_psf_flux_max: Option<f32>,
    pub rho_psf_flux_slope_before_max: Option<f32>,
    pub rho_psf_flux_slope_after_max: Option<f32>,
    pub rho_ap_flux_mean: Option<f32>,
    pub rho_ap_flux_min: Option<f32>,
    pub rho_ap_flux_max: Option<f32>,
    pub rho_ap_flux_slope_before_max: Option<f32>,
    pub rho_ap_flux_slope_after_max: Option<f32>,
    pub second_cadence_history: Option<Vec<ArgusCandidate>>,
    pub minute_cadence_history: Option<Vec<ArgusCandidate>>,
    pub base_coadd_history: Option<Vec<ArgusCandidate>>,
    pub hour_coadd_history: Option<Vec<ArgusCandidate>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArgusSsoHistoryEntry {
    pub epoch: i64,
    pub eph_offset_ra: f32,
    pub eph_offset_dec: f32,
    pub reduced_mag: f32,
    pub candidate: ArgusCandidate,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArgusSsoAssociation {
    pub sso_id: String,
    pub spk_id: i64,
    pub sso_name: Option<String>,
    pub orb_source: Option<String>,
    pub eph_ra: Option<f64>,
    pub eph_dec: Option<f64>,
    pub eph_vmag: Option<f32>,
    pub eph_offset_ra: Option<f32>,
    pub eph_offset_dec: Option<f32>,
    pub eph_offset_along_track: Option<f32>,
    pub eph_offset_cross_track: Option<f32>,
    pub eph_rate_ra: Option<f32>,
    pub eph_rate_dec: Option<f32>,
    pub eph_trail_length: Option<f32>,
    pub eph_trail_angle: Option<f32>,
    pub topo_dist: Option<f32>,
    pub helio_dist: Option<f32>,
    pub ecl_lambda: Option<f32>,
    pub ecl_beta: Option<f32>,
    pub phase_angle: Option<f32>,
    pub reduced_mag: Option<f32>,
    pub reduced_mag_mean: Option<f32>,
    pub reduced_mag_min: Option<f32>,
    pub reduced_mag_max: Option<f32>,
    pub reduced_mag_rms: Option<f32>,
    pub first_detection: Option<i64>,
    pub most_recent_detection_at_cadence: Option<i64>,
    pub num_detection_at_this_cadence: Option<i64>,
    pub num_detection_base_cadence: Option<i64>,
    pub sso_history: Option<Vec<ArgusSsoHistoryEntry>>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArgusRawAvroAlert {
    pub schema_version: String,
    pub alert_origin: String,
    pub alert_cadence: i32,
    pub alert_id: String,
    pub candidate: ArgusCandidate,
    pub event_association: Option<ArgusEventAssociation>,
    pub sso_association: Option<ArgusSsoAssociation>,
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    pub cutout_science: Option<Vec<u8>>,
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    pub cutout_template: Option<Vec<u8>>,
    #[serde(with = "apache_avro::serde_avro_bytes_opt")]
    pub cutout_difference: Option<Vec<u8>>,
}

impl ArgusRawAvroAlert {
    pub fn lane(&self) -> ArgusLane {
        ArgusLane::route(self.alert_cadence, self.sso_association.is_some())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ArgusLane {
    #[serde(rename = "1s")]
    Second,
    #[serde(rename = "1m")]
    Minute,
    #[serde(rename = "15m")]
    Base,
    #[serde(rename = "1h")]
    Hour,
    #[serde(rename = "1d")]
    Day,
    #[serde(rename = "sso")]
    Sso,
}

impl ArgusLane {
    pub const ALL: [ArgusLane; 6] = [
        ArgusLane::Second,
        ArgusLane::Minute,
        ArgusLane::Base,
        ArgusLane::Hour,
        ArgusLane::Day,
        ArgusLane::Sso,
    ];

    pub fn slug(&self) -> &'static str {
        match self {
            ArgusLane::Second => "1s",
            ArgusLane::Minute => "1m",
            ArgusLane::Base => "15m",
            ArgusLane::Hour => "1h",
            ArgusLane::Day => "1d",
            ArgusLane::Sso => "sso",
        }
    }

    pub fn from_slug(slug: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|lane| lane.slug() == slug)
    }

    pub fn route(cadence_secs: i32, is_sso: bool) -> Self {
        if is_sso {
            return ArgusLane::Sso;
        }
        match cadence_secs {
            ..30 => ArgusLane::Second,
            30..450 => ArgusLane::Minute,
            450..1800 => ArgusLane::Base,
            1800..43_200 => ArgusLane::Hour,
            _ => ArgusLane::Day,
        }
    }

    pub fn cadence_secs(&self) -> Option<i32> {
        match self {
            ArgusLane::Second => Some(1),
            ArgusLane::Minute => Some(60),
            ArgusLane::Base => Some(900),
            ArgusLane::Hour => Some(3600),
            ArgusLane::Day => Some(86_400),
            ArgusLane::Sso => None,
        }
    }

    pub fn alert_queue(&self) -> String {
        format!("{}_{}", Survey::Argus.alert_input_queue(), self.slug())
    }

    pub fn enrichment_queue(&self) -> String {
        format!("{}_alerts_enrichment_queue_{}", STREAM_NAME, self.slug())
    }

    pub fn filter_queue(&self) -> String {
        format!("{}_alerts_filter_queue_{}", STREAM_NAME, self.slug())
    }

    pub fn worker_queues(&self, worker_type: WorkerType) -> WorkerQueues {
        let (input, output) = match worker_type {
            WorkerType::Alert => (self.alert_queue(), Some(self.enrichment_queue())),
            WorkerType::Enrichment => (self.enrichment_queue(), Some(self.filter_queue())),
            WorkerType::Filter => (self.filter_queue(), None),
        };
        WorkerQueues {
            input: Some(input),
            output,
        }
    }

    pub fn topic(&self) -> String {
        format!("argus_alerts_{}", self.slug())
    }
}

pub fn argus_candidate_fields() -> HashSet<String> {
    let mut fields = HashSet::from(["jd".to_string()]);
    if let Schema::Record(root) = &*ARGUS_SCHEMA {
        if let Some(Schema::Record(candidate)) = root
            .fields
            .iter()
            .find(|field| field.name == "candidate")
            .map(|field| &field.schema)
        {
            fields.extend(candidate.fields.iter().map(|field| field.name.clone()));
        }
    }
    fields
}

impl std::fmt::Display for ArgusLane {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.slug())
    }
}

// alertId is HEALPix-derived: every detection at one position shares it.
fn argus_candid(alert: &ArgusRawAvroAlert) -> i64 {
    let key = format!(
        "{}|{}|{}|{}",
        alert.alert_id,
        alert.alert_cadence,
        alert.candidate.obs_weighted_midpoint,
        alert.candidate.filter
    );
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in key.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash & i64::MAX as u64) as i64
}

fn iso_to_jd(iso: &str) -> Result<f64, AlertError> {
    let utc = chrono::DateTime::parse_from_rfc3339(iso)
        .map(|dt| dt.with_timezone(&chrono::Utc))
        .or_else(|_| {
            chrono::NaiveDateTime::parse_from_str(iso, "%Y-%m-%dT%H:%M:%S%.f")
                .map(|dt| dt.and_utc())
        })
        .map_err(|_| AlertError::DecodeError(format!("invalid ISO time {}", iso)))?;
    Ok(Time::from_utc(utc).to_jd())
}

pub fn encode_argus_alert(alert: &ArgusRawAvroAlert) -> Result<Vec<u8>, apache_avro::Error> {
    let value = to_value(alert)?;
    let datum = to_avro_datum(&ARGUS_SCHEMA, value)?;
    let mut packet = Vec::with_capacity(datum.len() + 5);
    packet.push(0);
    packet.extend_from_slice(&ARGUS_SCHEMA_ID.to_be_bytes());
    packet.extend_from_slice(&datum);
    Ok(packet)
}

fn decode_argus_datum(datum: &[u8]) -> Result<ArgusRawAvroAlert, AlertError> {
    let value = from_avro_datum(&ARGUS_SCHEMA, &mut &datum[..], None)?;
    Ok(from_value::<ArgusRawAvroAlert>(&value)?)
}

pub fn decode_argus_packet(packet: &[u8]) -> Result<ArgusRawAvroAlert, AlertError> {
    match packet {
        [0, _, _, _, _, datum @ ..] => decode_argus_datum(datum),
        datum => decode_argus_datum(datum),
    }
}

fn strip_nulls(document: Document) -> Document {
    document
        .into_iter()
        .filter_map(|(key, value)| match value {
            Bson::Null => None,
            Bson::Document(inner) => Some((key, Bson::Document(strip_nulls(inner)))),
            Bson::Array(items) => Some((
                key,
                Bson::Array(
                    items
                        .into_iter()
                        .map(|item| match item {
                            Bson::Document(inner) => Bson::Document(strip_nulls(inner)),
                            other => other,
                        })
                        .collect(),
                ),
            )),
            other => Some((key, other)),
        })
        .collect()
}

#[apache_avro_macros::serdavro]
#[skip_serializing_none]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ArgusPhotometry {
    pub jd: f64,
    pub cadence: i32,
    #[serde(rename = "alertId")]
    pub alert_id: String,
    pub filter: i32,
    pub coadd: bool,
    pub ra: f64,
    pub dec: f64,
    #[serde(rename = "psfFluxDif")]
    pub psf_flux_dif: Option<f32>,
    #[serde(rename = "psfFluxDifErr")]
    pub psf_flux_dif_err: Option<f32>,
    #[serde(rename = "psfMagnitudeDif")]
    pub psf_magnitude_dif: Option<f32>,
    #[serde(rename = "psfMagnitudeDifErr")]
    pub psf_magnitude_dif_err: Option<f32>,
    #[serde(rename = "psfSnr")]
    pub psf_snr: Option<f32>,
    #[serde(rename = "psfLimMag")]
    pub psf_lim_mag: Option<f32>,
    #[serde(rename = "isFluxPositive")]
    pub is_flux_positive: bool,
    #[serde(rename = "realBogusScore")]
    pub real_bogus_score: Option<f32>,
    #[serde(rename = "reducedMag")]
    pub reduced_mag: Option<f32>,
}

impl ArgusPhotometry {
    fn from_candidate(candidate: &ArgusCandidate, cadence: i32) -> Result<Self, AlertError> {
        Ok(ArgusPhotometry {
            jd: candidate.jd()?,
            cadence,
            alert_id: candidate.alert_id.clone(),
            filter: candidate.filter,
            coadd: candidate.coadd,
            ra: candidate.ra,
            dec: candidate.dec,
            psf_flux_dif: candidate.psf_flux_dif,
            psf_flux_dif_err: candidate.psf_flux_dif_err,
            psf_magnitude_dif: candidate.psf_magnitude_dif,
            psf_magnitude_dif_err: candidate.psf_magnitude_dif_err,
            psf_snr: candidate.psf_snr,
            psf_lim_mag: candidate.psf_lim_mag,
            is_flux_positive: candidate.is_flux_positive,
            real_bogus_score: candidate.real_bogus_score,
            reduced_mag: None,
        })
    }
}

impl TimeSeries for ArgusPhotometry {
    fn time(&self) -> f64 {
        self.jd
    }

    fn sanitize_timeseries(timeseries: &mut Vec<Self>) {
        timeseries.retain(|point| point.jd.is_finite());
        timeseries.sort_by(|a, b| {
            a.jd.total_cmp(&b.jd)
                .then(a.filter.cmp(&b.filter))
                .then(a.cadence.cmp(&b.cadence))
        });
        timeseries.dedup_by(|a, b| a.key() == b.key());
    }
}

impl ArgusPhotometry {
    fn key(&self) -> (u64, i32, i32) {
        (self.jd.to_bits(), self.filter, self.cadence)
    }
}

#[apache_avro_macros::serdavro]
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ArgusAliases {
    #[serde(rename = "ZTF")]
    pub ztf: Vec<String>,
    #[serde(rename = "LSST")]
    pub lsst: Vec<String>,
}

struct Association {
    kind: &'static str,
    object_id: String,
    coordinates: (f64, f64),
    summary_field: &'static str,
    summary: Document,
    photometry: Vec<ArgusPhotometry>,
}

fn split_association(alert: &mut ArgusRawAvroAlert) -> Result<Association, AlertError> {
    let cadence = alert.alert_cadence;
    let mut photometry = vec![ArgusPhotometry::from_candidate(&alert.candidate, cadence)?];
    match (alert.event_association.take(), alert.sso_association.take()) {
        (Some(mut event), None) => {
            for (history, history_cadence) in [
                (event.second_cadence_history.take(), 1),
                (event.minute_cadence_history.take(), 60),
                (event.base_coadd_history.take(), 900),
                (event.hour_coadd_history.take(), 3600),
            ] {
                for candidate in history.unwrap_or_default() {
                    photometry.push(ArgusPhotometry::from_candidate(
                        &candidate,
                        history_cadence,
                    )?);
                }
            }
            Ok(Association {
                kind: "event",
                object_id: event.event_id.clone(),
                coordinates: (event.event_ra, event.event_dec),
                summary_field: "eventAssociation",
                summary: strip_nulls(mongify(&event)),
                photometry,
            })
        }
        (None, Some(mut sso)) => {
            photometry[0].reduced_mag = sso.reduced_mag;
            for entry in sso.sso_history.take().unwrap_or_default() {
                let mut point = ArgusPhotometry::from_candidate(&entry.candidate, cadence)?;
                point.reduced_mag = Some(entry.reduced_mag);
                photometry.push(point);
            }
            Ok(Association {
                kind: "sso",
                object_id: sso.sso_id.clone(),
                coordinates: (alert.candidate.ra, alert.candidate.dec),
                summary_field: "ssoAssociation",
                summary: strip_nulls(mongify(&sso)),
                photometry,
            })
        }
        (None, None) => Err(AlertError::MissingObjectId),
        (Some(_), Some(_)) => Err(AlertError::DecodeError(format!(
            "alert {} is associated with both an event and an SSO",
            alert.alert_id
        ))),
    }
}

pub struct ArgusAlertWorker {
    xmatch_configs: Vec<conf::CatalogXmatchConfig>,
    host_galaxy_config: HostGalaxyConfig,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<Document>,
    alert_aux_collection: mongodb::Collection<Document>,
    alert_cutout_storage: CutoutStorage,
    ztf_alert_aux_collection: mongodb::Collection<Document>,
    lsst_alert_aux_collection: mongodb::Collection<Document>,
    schema_cache: SchemaCache,
}

impl ArgusAlertWorker {
    #[instrument(skip_all, err)]
    pub fn decode(&mut self, avro_bytes: &[u8]) -> Result<ArgusRawAvroAlert, AlertError> {
        match avro_bytes {
            [b'O', b'b', b'j', 1, ..] => self.schema_cache.alert_from_avro_bytes(avro_bytes),
            packet => decode_argus_packet(packet),
        }
    }

    #[instrument(skip(self), err)]
    async fn get_survey_matches(&self, ra: f64, dec: f64) -> Result<ArgusAliases, AlertError> {
        let ztf = self
            .get_matches(
                ra,
                dec,
                ztf::ZTF_DEC_RANGE,
                ARGUS_ZTF_XMATCH_RADIUS,
                &self.ztf_alert_aux_collection,
            )
            .await?;
        let lsst = self
            .get_matches(
                ra,
                dec,
                lsst::LSST_DEC_RANGE,
                ARGUS_LSST_XMATCH_RADIUS,
                &self.lsst_alert_aux_collection,
            )
            .await?;
        Ok(ArgusAliases { ztf, lsst })
    }

    async fn update_aux(
        &self,
        association: &Association,
        survey_matches: &Option<ArgusAliases>,
        now: f64,
    ) -> Result<(), AlertError> {
        let object_id = association.object_id.as_str();
        let mut set = doc! {
            "aliases": mongify(survey_matches),
            "updated_at": now,
        };
        set.insert(association.summary_field, association.summary.clone());
        let mut pushed = false;
        for point in &association.photometry {
            let absent = doc! {
                "_id": object_id,
                "prv_candidates": { "$not": { "$elemMatch": {
                    "jd": point.jd,
                    "filter": point.filter,
                    "cadence": point.cadence,
                } } },
            };
            let mut newest = absent.clone();
            newest.insert(
                "$expr",
                doc! { "$lte": [{ "$last": "$prv_candidates.jd" }, point.jd] },
            );
            let appended = self
                .alert_aux_collection
                .update_one(
                    newest,
                    doc! {
                        "$push": { "prv_candidates": mongify(point) },
                        "$set": set.clone(),
                        "$inc": { "version": 1 },
                    },
                )
                .await?;
            if appended.modified_count > 0 {
                pushed = true;
                continue;
            }
            let inserted = self
                .alert_aux_collection
                .update_one(
                    absent,
                    doc! {
                        "$push": { "prv_candidates": { "$each": [mongify(point)], "$sort": { "jd": 1 } } },
                        "$set": set.clone(),
                        "$inc": { "version": 1 },
                    },
                )
                .await?;
            pushed |= inserted.modified_count > 0;
        }
        if !pushed {
            let result = self
                .alert_aux_collection
                .update_one(doc! { "_id": object_id }, doc! { "$set": set })
                .await?;
            if result.matched_count == 0 {
                return Err(AlertError::AlertAuxFallbackUpdateFailed(
                    object_id.to_string(),
                ));
            }
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl AlertWorker for ArgusAlertWorker {
    async fn new(config_path: &str) -> Result<ArgusAlertWorker, AlertWorkerError> {
        let config = AppConfig::from_path(config_path)?;

        let xmatch_configs = config
            .crossmatch
            .get(&Survey::Argus)
            .cloned()
            .unwrap_or_default();

        let db: mongodb::Database = config
            .build_db()
            .await
            .inspect_err(as_error!("failed to create mongo client"))?;

        crate::catalogs::warn_on_empty_crossmatch_catalogs(&db, &xmatch_configs).await;

        let alert_cutout_storage = config
            .build_cutout_storage(&Survey::Argus)
            .await
            .inspect_err(as_error!("failed to create cutout storage"))?;

        Ok(ArgusAlertWorker {
            xmatch_configs,
            host_galaxy_config: config.host_galaxy.clone(),
            alert_collection: db.collection(ALERT_COLLECTION),
            alert_aux_collection: db.collection(ALERT_AUX_COLLECTION),
            alert_cutout_storage,
            ztf_alert_aux_collection: db.collection(ztf::ALERT_AUX_COLLECTION),
            lsst_alert_aux_collection: db.collection(lsst::ALERT_AUX_COLLECTION),
            schema_cache: SchemaCache::default(),
            db,
        })
    }

    fn survey() -> Survey {
        Survey::Argus
    }

    fn output_queue_name(&self) -> String {
        format!("{}_alerts_enrichment_queue", ArgusAlertWorker::survey())
    }

    async fn process_alert(&mut self, avro_bytes: &[u8]) -> Result<ProcessAlertStatus, AlertError> {
        let now = Time::now().to_jd();
        let mut avro_alert = self.decode(avro_bytes).inspect_err(as_error!())?;

        let candid = argus_candid(&avro_alert);
        let lane = avro_alert.lane();
        let mut association = split_association(&mut avro_alert).inspect_err(as_error!())?;
        ArgusPhotometry::sanitize_timeseries(&mut association.photometry);

        let ra = avro_alert.candidate.ra;
        let dec = avro_alert.candidate.dec;
        let mut candidate = strip_nulls(mongify(&avro_alert.candidate));
        candidate.insert("jd", avro_alert.candidate.jd()?);

        let alert = doc! {
            "_id": candid,
            "alertId": &avro_alert.alert_id,
            "objectId": &association.object_id,
            "kind": association.kind,
            "lane": lane.slug(),
            "schemaVersion": &avro_alert.schema_version,
            "alertOrigin": &avro_alert.alert_origin,
            "alertCadence": avro_alert.alert_cadence,
            "candidate": candidate,
            association.summary_field: association.summary.clone(),
            "coordinates": mongify(&Coordinates::new(ra, dec)),
            "created_at": now,
            "updated_at": now,
        };

        if self
            .check_alert_exists(candid, &self.alert_collection)
            .await
            .inspect_err(as_error!())?
        {
            return Ok(ProcessAlertStatus::Exists(candid));
        }

        let (object_ra, object_dec) = association.coordinates;
        let survey_matches = match association.kind {
            "event" => Some(
                self.get_survey_matches(object_ra, object_dec)
                    .await
                    .inspect_err(as_error!())?,
            ),
            _ => Some(ArgusAliases::default()),
        };

        match self
            .check_alert_aux_exists(&association.object_id, &self.alert_aux_collection)
            .await
            .inspect_err(as_error!())?
        {
            true => {
                self.update_aux(&association, &survey_matches, now)
                    .await
                    .inspect_err(as_error!())?;
            }
            false => {
                let cross_matches = match association.kind {
                    "event" => {
                        xmatch(
                            object_ra,
                            object_dec,
                            &association.object_id,
                            &Survey::Argus,
                            &self.xmatch_configs,
                            &self.db,
                        )
                        .await?
                    }
                    _ => HashMap::new(),
                };
                let host_galaxy = host::associate_from_xmatches(
                    object_ra,
                    object_dec,
                    &cross_matches,
                    &self.host_galaxy_config,
                );
                let mut aux = doc! {
                    "_id": &association.object_id,
                    "kind": association.kind,
                    "prv_candidates": mongify_vec(&association.photometry),
                    "cross_matches": mongify(&cross_matches),
                    "aliases": mongify(&survey_matches),
                    "coordinates": mongify(&Coordinates::new(object_ra, object_dec)),
                    "created_at": now,
                    "updated_at": now,
                };
                aux.insert(association.summary_field, association.summary.clone());
                if let Some(host_galaxy) = host_galaxy {
                    aux.insert("host_galaxy", mongify(&host_galaxy));
                }
                match self.insert_aux(&aux, &self.alert_aux_collection).await {
                    Err(AlertError::AlertAuxExists) => {
                        self.update_aux(&association, &survey_matches, now)
                            .await
                            .inspect_err(as_error!())?;
                    }
                    result => result.inspect_err(as_error!())?,
                }
            }
        }

        if let (Some(science), Some(template), Some(difference)) = (
            avro_alert.cutout_science,
            avro_alert.cutout_template,
            avro_alert.cutout_difference,
        ) {
            self.format_and_insert_cutouts(
                candid,
                &association.object_id,
                science,
                template,
                difference,
                &self.alert_cutout_storage,
            )
            .await
            .inspect_err(as_error!())?;
        }

        self.format_and_insert_alert(candid, &alert, &self.alert_collection)
            .await
            .inspect_err(as_error!())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alert::argus_mock::{ArgusMock, MockSettings};
    use std::sync::Arc;

    #[test]
    fn mock_packets_round_trip_through_the_bundled_schema() {
        let mut mock = ArgusMock::new(MockSettings::default(), Arc::default());
        for lane in ArgusLane::ALL {
            let alert = mock.next_alert(lane, 1_791_000_000_000);
            let packet = encode_argus_alert(&alert).unwrap();
            assert_eq!(packet[0], 0);
            let decoded = decode_argus_packet(&packet).unwrap();
            assert_eq!(decoded, alert);
            assert_eq!(decoded.lane(), lane);
        }
    }
}
