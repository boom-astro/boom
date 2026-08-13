use crate::alert::{RomanCandidate, RomanSsMatch};
use crate::conf::AppConfig;
use crate::enrichment::{fetch_alerts, EnrichmentWorker, EnrichmentWorkerError};
use crate::utils::db::mongify;
use crate::utils::enums::Survey;
use crate::utils::lightcurves::{
    analyze_photometry, prepare_photometry, Band, PerBandProperties, PhotometryMag,
};
use apache_avro_derive::AvroSchema;
use apache_avro_macros::serdavro;
use mongodb::bson::{doc, Document};
use mongodb::options::{UpdateOneModel, WriteModel};
use tracing::{instrument, warn};

/// A single Roman difference-image photometry point, as stored in the aux
/// collection (both `prv_candidates` and `fp_hists` share this shape).
#[serdavro]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RomanPhotometry {
    pub jd: f64,
    pub magpsf: Option<f32>,
    pub sigmapsf: Option<f32>,
    pub diffmaglim: f32,
    #[serde(rename = "psfFlux")]
    pub flux: Option<f64>, // in nJy
    #[serde(rename = "psfFluxErr")]
    pub flux_err: f64, // in nJy
    /// `band` is nullable in the RAPID schema, so it can be absent on a stored
    /// point. Such a point can't be placed on a lightcurve, so it is dropped
    /// rather than failing the whole batch's deserialization.
    #[serde(default)]
    pub band: Option<Band>,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub snr_psf: Option<f64>,
}

impl RomanPhotometry {
    pub fn to_photometry_mag(&self, min_snr: Option<f64>) -> Option<PhotometryMag> {
        match (self.snr_psf, self.magpsf, self.sigmapsf, self.band.clone()) {
            (Some(snr), Some(mag), Some(sig), Some(band)) => match min_snr {
                Some(thresh) if snr.abs() < thresh => None,
                _ => Some(PhotometryMag {
                    time: self.jd,
                    mag,
                    mag_err: sig,
                    band,
                }),
            },
            _ => None,
        }
    }
}

pub fn create_roman_alert_pipeline() -> Vec<Document> {
    vec![
        doc! {
            "$match": {
                "_id": {"$in": []}
            }
        },
        doc! {
            "$lookup": {
                "from": "ROMAN_alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        doc! {
            "$unwind": {
                "path": "$aux",
                "preserveNullAndEmptyArrays": false
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "candidate": 1,
                "ss_matches": 1,
                "prv_candidates": "$aux.prv_candidates",
                "fp_hists": "$aux.fp_hists",
                "cross_matches": "$aux.cross_matches",
            }
        },
    ]
}

/// Roman alert structure used to deserialize alerts from the database, used by
/// the enrichment worker to compute features (and, later, ML scores).
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct RomanAlertForEnrichment {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: RomanCandidate,
    #[serde(default)]
    pub ss_matches: Vec<RomanSsMatch>,
    #[serde(default)]
    pub prv_candidates: Vec<RomanPhotometry>,
    #[serde(default)]
    pub fp_hists: Vec<RomanPhotometry>,
    pub cross_matches: Option<std::collections::HashMap<String, Vec<serde_json::Value>>>,
}

/// Roman alert properties computed during enrichment and inserted back into the
/// alert document.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize, AvroSchema, utoipa::ToSchema)]
pub struct RomanAlertProperties {
    /// The source is associated with a known (or candidate) solar-system object.
    pub rock: bool,
    pub stationary: bool,
    pub photstats: PerBandProperties,
}

pub struct RomanEnrichmentWorker {
    input_queue: String,
    output_queue: String,
    client: mongodb::Client,
    alert_collection: mongodb::Collection<Document>,
    alert_pipeline: Vec<Document>,
}

#[async_trait::async_trait]
impl EnrichmentWorker for RomanEnrichmentWorker {
    #[instrument(err)]
    async fn new(
        config_path: &str,
        _shared_models: Option<std::sync::Arc<crate::enrichment::models::SharedModels>>,
    ) -> Result<Self, EnrichmentWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db: mongodb::Database = config.build_db().await?;
        let client = db.client().clone();
        let alert_collection = db.collection("ROMAN_alerts");

        let input_queue = "ROMAN_alerts_enrichment_queue".to_string();
        let output_queue = "ROMAN_alerts_filter_queue".to_string();

        Ok(RomanEnrichmentWorker {
            input_queue,
            output_queue,
            client,
            alert_collection,
            alert_pipeline: create_roman_alert_pipeline(),
        })
    }

    fn survey() -> Survey {
        Survey::Roman
    }

    fn disable_babamul(&mut self) {}

    fn input_queue_name(&self) -> String {
        self.input_queue.clone()
    }

    fn output_queue_name(&self) -> String {
        self.output_queue.clone()
    }

    #[instrument(skip_all, err)]
    async fn process_alerts(
        &mut self,
        candids: &[i64],
    ) -> Result<Vec<String>, EnrichmentWorkerError> {
        let alerts: Vec<RomanAlertForEnrichment> =
            fetch_alerts(&candids, &self.alert_pipeline, &self.alert_collection).await?;

        if alerts.len() != candids.len() {
            warn!(
                "only {} alerts fetched from {} candids",
                alerts.len(),
                candids.len()
            );
        }

        if alerts.is_empty() {
            return Ok(vec![]);
        }

        let now = flare::Time::now().to_jd();

        // we keep it very simple for now, let's run on 1 alert at a time
        // we will move to batch processing later
        let mut updates = Vec::new();
        let mut processed_alerts = Vec::new();
        for alert in alerts {
            let candid = alert.candid;

            let properties = self.get_alert_properties(&alert).await?;

            let update_alert_document = doc! {
                "$set": {
                    "properties": mongify(&properties),
                    "updated_at": now,
                }
            };

            let update = WriteModel::UpdateOne(
                UpdateOneModel::builder()
                    .namespace(self.alert_collection.namespace())
                    .filter(doc! {"_id": candid})
                    .update(update_alert_document)
                    .build(),
            );

            updates.push(update);
            processed_alerts.push(format!("{}", candid));
        }

        let _ = self.client.bulk_write(updates).await?.modified_count;

        Ok(processed_alerts)
    }
}

impl RomanEnrichmentWorker {
    pub async fn get_alert_properties(
        &self,
        alert: &RomanAlertForEnrichment,
    ) -> Result<RomanAlertProperties, EnrichmentWorkerError> {
        let is_rock = !alert.ss_matches.is_empty()
            || alert.candidate.dia_source.is_ss_candidate == Some(true);

        let prv_candidates: Vec<PhotometryMag> = alert
            .prv_candidates
            .iter()
            .filter(|p| p.jd <= alert.candidate.jd)
            .filter_map(|p| p.to_photometry_mag(None))
            .collect();
        let fp_hists: Vec<PhotometryMag> = alert
            .fp_hists
            .iter()
            .filter(|p| p.jd <= alert.candidate.jd)
            .filter_map(|p| p.to_photometry_mag(Some(3.0)))
            .collect();

        // lightcurve is prv_candidates + fp_hists, no need for parse_photometry here
        let mut lightcurve = [prv_candidates, fp_hists].concat();

        prepare_photometry(&mut lightcurve);
        let (photstats, _, stationary) = analyze_photometry(&lightcurve);

        Ok(RomanAlertProperties {
            rock: is_rock,
            stationary,
            photstats,
        })
    }
}
