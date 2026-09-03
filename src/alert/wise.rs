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
        lightcurves::Band,
        o11y::logging::as_error,
        spatial::{xmatch, Coordinates},
    },
};
use constcat::concat;
use flare::Time;
use mongodb::bson::{doc, Document};
use serde::{Deserialize, Deserializer, Serialize};
use serde_with::{serde_as, skip_serializing_none};
use tracing::{debug, error, instrument, warn};

pub const STREAM_NAME: &str = "WISE";
// NEOWISE scans the full sky; no dec restriction.
pub const WISE_DEC_RANGE: (f64, f64) = (-90.0, 90.0);
// WISE PSF positional accuracy is ~1" for bright sources.
pub const WISE_POSITION_UNCERTAINTY: f64 = 1.0;
pub const ALERT_COLLECTION: &str = concat!(STREAM_NAME, "_alerts");
pub const ALERT_AUX_COLLECTION: &str = concat!(STREAM_NAME, "_alerts_aux");

pub const WISE_ZTF_XMATCH_RADIUS: f64 =
    (WISE_POSITION_UNCERTAINTY.max(ztf::ZTF_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();
pub const WISE_LSST_XMATCH_RADIUS: f64 =
    (WISE_POSITION_UNCERTAINTY.max(lsst::LSST_POSITION_UNCERTAINTY) / 3600.0_f64).to_radians();

/// WTP `bandid` -> Band: 1 = W1 (3.4um), 2 = W2 (4.6um).
fn bandid_to_band(bandid: Option<i32>) -> Band {
    match bandid {
        Some(2) => Band::W2,
        _ => Band::W1,
    }
}

/// SNR from a PSF magnitude uncertainty: sigma_mag ~= 1.0857 / snr.
fn mag_snr(sigmapsf: f32) -> Option<f64> {
    if sigmapsf > 0.0 {
        Some(1.0857 / sigmapsf as f64)
    } else {
        None
    }
}

/// Raw science `candidate` record from the WTP Avro packet (useful subset).
#[serde_as]
#[skip_serializing_none]
#[apache_avro_macros::serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct Candidate {
    pub mjd: f64,
    pub candid: i64,
    /// "t"/"1" => positive (sci-ref) subtraction; "f"/"0" => negative.
    pub isdiffpos: String,
    pub bandid: Option<i32>,
    pub field: Option<i32>,
    pub ra: f64,
    pub dec: f64,
    pub magpsf: f32,
    pub sigmapsf: f32,
    pub fluxpsf: f32,
    pub sigmafluxpsf: f32,
    pub diffmaglim: Option<f32>,
    pub fwhm: Option<f32>,
    pub chipsf: Option<f32>,
    pub ndethist: i32,
    pub scorr: Option<f64>,
    /// Deep-learning real-bogus score.
    pub drb: Option<f32>,
    pub drbversion: String,
}

/// Processed science candidate: adds `jd` (from `mjd`), `band` (from `bandid`)
/// and a computed detection `snr`, so WISE is filterable like the other surveys.
#[serde_as]
#[skip_serializing_none]
#[apache_avro_macros::serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct WiseCandidate {
    #[serde(flatten)]
    pub candidate: Candidate,
    pub jd: f64,
    pub band: Band,
    pub snr: Option<f64>,
}

impl TryFrom<Candidate> for WiseCandidate {
    type Error = AlertError;

    fn try_from(candidate: Candidate) -> Result<Self, Self::Error> {
        let snr = if candidate.sigmafluxpsf > 0.0 {
            Some((candidate.fluxpsf / candidate.sigmafluxpsf) as f64)
        } else {
            None
        };
        Ok(WiseCandidate {
            jd: candidate.mjd + 2400000.5,
            band: bandid_to_band(candidate.bandid),
            snr,
            candidate,
        })
    }
}

impl WiseCandidate {
    fn to_light_point(&self) -> WiseLightPoint {
        WiseLightPoint {
            jd: self.jd,
            magpsf: self.candidate.magpsf,
            sigmapsf: self.candidate.sigmapsf,
            band: self.band.clone(),
            snr: mag_snr(self.candidate.sigmapsf),
            ra: Some(self.candidate.ra),
            dec: Some(self.candidate.dec),
            isdiffpos: Some(self.candidate.isdiffpos.clone()),
            drb: self.candidate.drb,
        }
    }
}

impl TimeSeries for WiseCandidate {
    fn time(&self) -> f64 {
        self.jd
    }
}

/// Raw `prv_candidate` record (nearly everything is nullable upstream).
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct PrvCandidate {
    pub mjd: f64,
    pub bandid: Option<i32>,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub magpsf: Option<f32>,
    pub sigmapsf: Option<f32>,
    pub isdiffpos: Option<String>,
    pub drb: Option<f32>,
}

impl PrvCandidate {
    /// Drop history points with no PSF magnitude (nothing photometric to keep).
    fn into_light_point(self) -> Option<WiseLightPoint> {
        let magpsf = self.magpsf?;
        let sigmapsf = self.sigmapsf.unwrap_or(0.0);
        Some(WiseLightPoint {
            jd: self.mjd + 2400000.5,
            magpsf,
            sigmapsf,
            band: bandid_to_band(self.bandid),
            snr: mag_snr(sigmapsf),
            ra: self.ra,
            dec: self.dec,
            isdiffpos: self.isdiffpos,
            drb: self.drb,
        })
    }
}

/// A stored detection-history point (the science candidate plus any
/// `prv_candidates`), reduced to a common photometric shape.
#[serde_as]
#[skip_serializing_none]
#[apache_avro_macros::serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct WiseLightPoint {
    pub jd: f64,
    pub magpsf: f32,
    pub sigmapsf: f32,
    pub band: Band,
    pub snr: Option<f64>,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub isdiffpos: Option<String>,
    pub drb: Option<f32>,
}

impl TimeSeries for WiseLightPoint {
    fn time(&self) -> f64 {
        self.jd
    }
}

/// Raw `fp_record` (forced photometry) from the WTP packet.
#[serde_as]
#[skip_serializing_none]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct FpRecord {
    pub mjd: f64,
    pub bandid: Option<i32>,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub forcediffimflux: f32,
    pub forcediffimfluxunc: f32,
    pub forcediffmagpsf: f32,
    pub forcediffsigmapsf: f32,
}

/// Stored forced-photometry point. Carries `magpsf`/`sigmapsf` (aliased from the
/// packet's `forcediffmagpsf`/`forcediffsigmapsf`) so it round-trips and feeds
/// the shared mag-based enrichment, plus the raw difference flux and a `snr`.
#[serde_as]
#[skip_serializing_none]
#[apache_avro_macros::serdavro]
#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct WiseForcedPhot {
    pub jd: f64,
    pub magpsf: f32,
    pub sigmapsf: f32,
    pub band: Band,
    pub forcediffimflux: f32,
    pub forcediffimfluxunc: f32,
    pub snr: Option<f64>,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
}

impl TryFrom<FpRecord> for WiseForcedPhot {
    type Error = AlertError;

    fn try_from(fp: FpRecord) -> Result<Self, Self::Error> {
        let snr = if fp.forcediffimfluxunc > 0.0 {
            Some((fp.forcediffimflux / fp.forcediffimfluxunc) as f64)
        } else {
            None
        };
        Ok(WiseForcedPhot {
            jd: fp.mjd + 2400000.5,
            magpsf: fp.forcediffmagpsf,
            sigmapsf: fp.forcediffsigmapsf,
            band: bandid_to_band(fp.bandid),
            forcediffimflux: fp.forcediffimflux,
            forcediffimfluxunc: fp.forcediffimfluxunc,
            snr,
            ra: fp.ra,
            dec: fp.dec,
        })
    }
}

impl TimeSeries for WiseForcedPhot {
    fn time(&self) -> f64 {
        self.jd
    }
}

fn deserialize_candidate<'de, D>(deserializer: D) -> Result<WiseCandidate, D::Error>
where
    D: Deserializer<'de>,
{
    let candidate = <Candidate as Deserialize>::deserialize(deserializer)?;
    WiseCandidate::try_from(candidate).map_err(serde::de::Error::custom)
}

fn deserialize_prv_candidates<'de, D>(deserializer: D) -> Result<Vec<PrvCandidate>, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(<Option<Vec<PrvCandidate>> as Deserialize>::deserialize(deserializer)?.unwrap_or_default())
}

fn deserialize_fp_records<'de, D>(deserializer: D) -> Result<Vec<WiseForcedPhot>, D::Error>
where
    D: Deserializer<'de>,
{
    let records =
        <Option<Vec<FpRecord>> as Deserialize>::deserialize(deserializer)?.unwrap_or_default();
    records
        .into_iter()
        .map(WiseForcedPhot::try_from)
        .collect::<Result<Vec<WiseForcedPhot>, _>>()
        .map_err(serde::de::Error::custom)
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct WiseRawAvroAlert {
    pub publisher: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candid: i64,
    #[serde(deserialize_with = "deserialize_candidate")]
    pub candidate: WiseCandidate,
    #[serde(default, deserialize_with = "deserialize_prv_candidates")]
    pub prv_candidates: Vec<PrvCandidate>,
    #[serde(
        rename = "fp_records",
        default,
        deserialize_with = "deserialize_fp_records"
    )]
    pub fp_hists: Vec<WiseForcedPhot>,
    #[serde(rename = "cutoutScience")]
    #[serde(with = "apache_avro::serde_avro_bytes")]
    pub cutout_science: Vec<u8>,
    #[serde(rename = "cutoutTemplate")]
    #[serde(with = "apache_avro::serde_avro_bytes")]
    pub cutout_template: Vec<u8>,
    #[serde(rename = "cutoutDifference")]
    #[serde(with = "apache_avro::serde_avro_bytes")]
    pub cutout_difference: Vec<u8>,
}

#[apache_avro_macros::serdavro]
#[derive(Debug, Deserialize, Serialize)]
pub struct WiseAliases {
    #[serde(rename = "ZTF")]
    pub ztf: Vec<String>,
    #[serde(rename = "LSST")]
    pub lsst: Vec<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct WiseObject {
    #[serde(rename = "_id")]
    pub object_id: String,
    pub prv_candidates: Vec<WiseLightPoint>,
    pub fp_hists: Vec<WiseForcedPhot>,
    pub cross_matches: Option<HashMap<String, Vec<Document>>>,
    pub aliases: Option<WiseAliases>,
    pub coordinates: Coordinates,
    pub created_at: f64,
    pub updated_at: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct WiseAlert {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: WiseCandidate,
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

pub struct WiseAlertWorker {
    xmatch_configs: Vec<conf::CatalogXmatchConfig>,
    db: mongodb::Database,
    alert_collection: mongodb::Collection<WiseAlert>,
    alert_aux_collection: mongodb::Collection<WiseObject>,
    alert_cutout_storage: CutoutStorage,
    alert_aux_collection_update: mongodb::Collection<AlertAuxForUpdate>,
    ztf_alert_aux_collection: mongodb::Collection<Document>,
    lsst_alert_aux_collection: mongodb::Collection<Document>,
    schema_cache: SchemaCache,
}

impl WiseAlertWorker {
    #[instrument(skip(self), err)]
    async fn get_survey_matches(&self, ra: f64, dec: f64) -> Result<WiseAliases, AlertError> {
        let ztf_matches = self
            .get_matches(
                ra,
                dec,
                ztf::ZTF_DEC_RANGE,
                WISE_ZTF_XMATCH_RADIUS,
                &self.ztf_alert_aux_collection,
            )
            .await?;

        let lsst_matches = self
            .get_matches(
                ra,
                dec,
                lsst::LSST_DEC_RANGE,
                WISE_LSST_XMATCH_RADIUS,
                &self.lsst_alert_aux_collection,
            )
            .await?;
        Ok(WiseAliases {
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
        prv_candidates: &Vec<WiseLightPoint>,
        fp_hists: &Vec<WiseForcedPhot>,
        survey_matches: &Option<WiseAliases>,
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
        prv_candidates: &Vec<WiseLightPoint>,
        fp_hists: &Vec<WiseForcedPhot>,
        survey_matches: &Option<WiseAliases>,
        now: f64,
        existing_alert_aux: &AlertAuxForUpdate,
    ) -> Result<(), AlertError> {
        let current_version = existing_alert_aux.version;

        let prepared_prv_candidates = WiseLightPoint::prepare_timeseries_update(
            prv_candidates,
            &existing_alert_aux.prv_candidates,
            "prv_candidates",
        )?;

        let prepared_fp_hists = WiseForcedPhot::prepare_timeseries_update(
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
            Document::new(),
        )
        .await
    }

    async fn update_aux(
        &mut self,
        object_id: &str,
        prv_candidates: &Vec<WiseLightPoint>,
        fp_hists: &Vec<WiseForcedPhot>,
        survey_matches: &Option<WiseAliases>,
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
impl AlertWorker for WiseAlertWorker {
    async fn new(config_path: &str) -> Result<WiseAlertWorker, AlertWorkerError> {
        let config = AppConfig::from_path(config_path)?;

        let xmatch_configs = config
            .crossmatch
            .get(&Survey::Wise)
            .cloned()
            .unwrap_or_default();

        let db: mongodb::Database = config
            .build_db()
            .await
            .inspect_err(as_error!("failed to create mongo client"))?;

        let alert_collection = db.collection(&ALERT_COLLECTION);
        let alert_aux_collection = db.collection(&ALERT_AUX_COLLECTION);
        let alert_cutout_storage = config
            .build_cutout_storage(&Survey::Wise)
            .await
            .inspect_err(as_error!("failed to create cutout storage"))?;
        let alert_aux_collection_update = db.collection(&ALERT_AUX_COLLECTION);

        let ztf_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&ztf::ALERT_AUX_COLLECTION);

        let lsst_alert_aux_collection: mongodb::Collection<Document> =
            db.collection(&lsst::ALERT_AUX_COLLECTION);

        let worker = WiseAlertWorker {
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
        Survey::Wise
    }

    fn input_queue_name(&self) -> String {
        format!("{}_alerts_packets_queue", WiseAlertWorker::survey())
    }

    fn output_queue_name(&self) -> String {
        format!("{}_alerts_enrichment_queue", WiseAlertWorker::survey())
    }

    async fn process_alert(&mut self, avro_bytes: &[u8]) -> Result<ProcessAlertStatus, AlertError> {
        let now = Time::now().to_jd();
        let avro_alert: WiseRawAvroAlert = self
            .schema_cache
            .alert_from_avro_bytes(avro_bytes)
            .inspect_err(as_error!())?;

        let candid = avro_alert.candid;
        let object_id = avro_alert.object_id;
        let ra = avro_alert.candidate.candidate.ra;
        let dec = avro_alert.candidate.candidate.dec;

        // Detection history = the science candidate plus any prv_candidates that
        // carry a magnitude; forced photometry goes to fp_hists.
        let mut prv_candidates = vec![avro_alert.candidate.to_light_point()];
        prv_candidates.extend(
            avro_alert
                .prv_candidates
                .into_iter()
                .filter_map(PrvCandidate::into_light_point),
        );
        let mut fp_hists = avro_alert.fp_hists;

        WiseLightPoint::sanitize_timeseries(&mut prv_candidates);
        WiseForcedPhot::sanitize_timeseries(&mut fp_hists);

        let alert = WiseAlert {
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
                &Survey::Wise,
                &self.xmatch_configs,
                &self.db,
            )
            .await?;
            let obj = WiseObject {
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
                avro_alert.cutout_template,
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
    use crate::utils::{enums::Survey, testing::AlertRandomizer};

    #[tokio::test]
    async fn test_wise_alert_from_avro_bytes() {
        let mut schema_cache = SchemaCache::default();

        let (candid, object_id, ra, dec, bytes_content) =
            AlertRandomizer::new_randomized(Survey::Wise).get().await;
        let alert: WiseRawAvroAlert = schema_cache.alert_from_avro_bytes(&bytes_content).unwrap();

        assert_eq!(alert.publisher, "wtp");
        assert_eq!(alert.object_id, object_id);
        assert_eq!(alert.candid, candid);
        assert_eq!(alert.candidate.candidate.ra, ra);
        assert_eq!(alert.candidate.candidate.dec, dec);
        // W1/W2 mapped from bandid
        assert!(matches!(alert.candidate.band, Band::W1 | Band::W2));
        // forced photometry present, cutout triplet non-empty
        assert!(!alert.fp_hists.is_empty());
        assert!(!alert.cutout_science.is_empty());
        assert!(!alert.cutout_template.is_empty());
        assert!(!alert.cutout_difference.is_empty());
    }
}
