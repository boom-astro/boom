use mongodb::bson::{doc, Document};
use std::collections::HashMap;
use tracing::{info, instrument, warn};

use crate::alert::DecamCandidate;
use crate::conf::AppConfig;
use crate::enrichment::fetch_alerts;
use crate::filter::{
    build_loaded_filters, run_filter, uses_field_in_filter, validate_filter_pipeline, Alert,
    Classification, Filter, FilterError, FilterResults, FilterWorker, FilterWorkerError,
    LoadedFilter, Origin, Photometry, SurveyMatches,
};
use crate::utils::cutouts::CutoutStorage;
use crate::utils::db::{fetch_timeseries_op, get_array_dict_element};
use crate::utils::enums::Survey;
use crate::utils::lightcurves::Band;

/// A single DECam difference-image forced-photometry point, as fetched from the
/// DECam aux collection to build the outgoing alert packet.
///
/// `prv_candidates` points carry `ra`/`dec`; `fp_hists` points do not (those
/// fields are simply absent from the aux document and deserialize to `None`).
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DecamPhotometry {
    pub jd: f64,
    pub forcediffimflux: f64,
    pub forcediffimfluxunc: f64,
    pub band: Band,
    #[serde(default)]
    pub ra: Option<f64>,
    #[serde(default)]
    pub dec: Option<f64>,
}

impl DecamPhotometry {
    /// DECam difference fluxes are stored on the uJy scale (AB zeropoint 23.9),
    /// while the downstream photometry packet uses nJy (AB zeropoint 31.4) like
    /// the other surveys -- a fixed factor of 1000 (7.5 mag).
    fn to_flux(&self) -> (Option<f64>, f64) {
        (
            Some(self.forcediffimflux * 1000.0),
            self.forcediffimfluxunc * 1000.0,
        )
    }
}

/// DECam alert as fetched from the database to build the outgoing alert packet.
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct DecamAlertForFilter {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: DecamCandidate,
    #[serde(default)]
    pub prv_candidates: Vec<DecamPhotometry>,
    #[serde(default)]
    pub fp_hists: Vec<DecamPhotometry>,
}

/// Pipeline used to fetch full DECam alert data (candidate + lightcurve) to
/// build the alert packets emitted after filtering.
fn create_decam_filter_alert_pipeline() -> Vec<Document> {
    vec![
        doc! {
            "$match": {
                "_id": {"$in": []}
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "candidate": 1,
            }
        },
        doc! {
            "$lookup": {
                "from": "DECAM_alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "prv_candidates": fetch_timeseries_op(
                    "aux.prv_candidates",
                    "candidate.jd",
                    1000,
                    None
                ),
                "fp_hists": fetch_timeseries_op(
                    "aux.fp_hists",
                    "candidate.jd",
                    1000,
                    None
                ),
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "prv_candidates.jd": 1,
                "prv_candidates.forcediffimflux": 1,
                "prv_candidates.forcediffimfluxunc": 1,
                "prv_candidates.band": 1,
                "prv_candidates.ra": 1,
                "prv_candidates.dec": 1,
                "fp_hists.jd": 1,
                "fp_hists.forcediffimflux": 1,
                "fp_hists.forcediffimfluxunc": 1,
                "fp_hists.band": 1,
            }
        },
    ]
}

/// Builds DECam Alert packets from the provided filter results.
#[instrument(skip_all, err)]
pub async fn build_decam_alerts(
    alerts_with_filter_results: &HashMap<i64, Vec<FilterResults>>,
    alert_pipeline: &Vec<Document>,
    alert_collection: &mongodb::Collection<Document>,
    alert_cutout_storage: &CutoutStorage,
) -> Result<Vec<Alert>, FilterWorkerError> {
    let candids: Vec<i64> = alerts_with_filter_results.keys().cloned().collect();
    if candids.is_empty() {
        return Ok(Vec::new());
    }

    let alerts: Vec<DecamAlertForFilter> =
        fetch_alerts(&candids, &alert_pipeline, alert_collection)
            .await
            .map_err(|e| FilterWorkerError::FetchAlertsError(e.to_string()))?;

    if alerts.len() != candids.len() {
        let nb_total = candids.len();
        let mut missing_candids: Vec<&i64> = candids
            .iter()
            .filter(|c| !alerts.iter().any(|a| a.candid == **c))
            .collect();
        missing_candids.sort();
        warn!(
            "Only fetched {} alerts from {} candids. Missing candids: {:?}",
            alerts.len(),
            nb_total,
            missing_candids
        );
    }

    let mut candid_to_cutouts = alert_cutout_storage
        .retrieve_multiple_cutouts(&candids, false)
        .await?;

    if candid_to_cutouts.len() != alerts.len() {
        let mut missing_cutouts_candids: Vec<&i64> = alerts
            .iter()
            .filter(|a| !candid_to_cutouts.contains_key(&a.candid))
            .map(|a| &a.candid)
            .collect();
        missing_cutouts_candids.sort();
        warn!(
            "Only fetched cutouts for {} alerts from {} candids. Missing cutouts for candids: {:?}",
            candid_to_cutouts.len(),
            alerts.len(),
            missing_cutouts_candids
        );
        return Err(FilterWorkerError::MissingCutoutsBatch(
            missing_cutouts_candids.len(),
        ));
    }

    let mut alerts_output = Vec::new();
    for alert in alerts {
        let candid = alert.candid;

        let mut classifications = Vec::new();
        if let Some(reliability) = alert.candidate.candidate.reliability {
            classifications.push(Classification {
                classifier: "reliability".to_string(),
                score: reliability as f32,
                distance_arcsec: None,
            });
        }

        let mut photometry = Vec::new();
        for doc in alert.prv_candidates.iter() {
            let (flux, flux_err) = doc.to_flux();
            photometry.push(Photometry {
                jd: doc.jd,
                flux,
                flux_err,
                band: format!("decam{}", doc.band),
                origin: Origin::Alert,
                programid: 1, // DECam has a single public stream
                survey: Survey::Decam,
                ra: doc.ra,
                dec: doc.dec,
            });
        }
        for doc in alert.fp_hists.iter() {
            let (flux, flux_err) = doc.to_flux();
            photometry.push(Photometry {
                jd: doc.jd,
                flux,
                flux_err,
                band: format!("decam{}", doc.band),
                origin: Origin::ForcedPhot,
                programid: 1,
                survey: Survey::Decam,
                ra: doc.ra,
                dec: doc.dec,
            });
        }
        photometry.sort_by(|a, b| a.jd.partial_cmp(&b.jd).unwrap());

        let cutouts = candid_to_cutouts
            .remove(&candid)
            .ok_or_else(|| FilterWorkerError::MissingCutouts(candid))?;

        let alert = Alert {
            candid: alert.candid,
            object_id: alert.object_id,
            jd: alert.candidate.jd,
            ra: alert.candidate.candidate.ra,
            dec: alert.candidate.candidate.dec,
            filters: alerts_with_filter_results
                .get(&candid)
                .cloned()
                .unwrap_or_else(Vec::new),
            classifications,
            photometry,
            cutout_science: cutouts.cutout_science,
            cutout_template: cutouts.cutout_template,
            cutout_difference: cutouts.cutout_difference,
            survey: Survey::Decam,
            survey_matches: SurveyMatches {
                ztf: None,
                lsst: None,
            },
        };

        alerts_output.push(alert);
    }

    Ok(alerts_output)
}

/// Builds a MongoDB aggregation pipeline for DECam filter execution.
///
/// Augments the user filter pipeline with DECam aux lookups (prv_candidates,
/// fp_hists, cross_matches, aliases) based on which fields the filter
/// references.
pub async fn build_decam_filter_pipeline(
    filter_pipeline: &Vec<serde_json::Value>,
    _permissions: &HashMap<Survey, Vec<i32>>,
) -> Result<Vec<Document>, FilterError> {
    validate_filter_pipeline(&filter_pipeline)?;

    let use_prv_candidates_index = uses_field_in_filter(filter_pipeline, "prv_candidates");
    let use_fp_hists_index = uses_field_in_filter(filter_pipeline, "fp_hists");
    let use_cross_matches_index = uses_field_in_filter(filter_pipeline, "cross_matches");
    let use_aliases_index = uses_field_in_filter(filter_pipeline, "aliases");

    let mut aux_add_fields = doc! {
        "aux": mongodb::bson::Bson::Null,
    };

    if use_prv_candidates_index.is_some() {
        aux_add_fields.insert(
            "prv_candidates".to_string(),
            fetch_timeseries_op("aux.prv_candidates", "candidate.jd", 1000, None),
        );
    }
    if use_fp_hists_index.is_some() {
        aux_add_fields.insert(
            "fp_hists".to_string(),
            fetch_timeseries_op("aux.fp_hists", "candidate.jd", 1000, None),
        );
    }
    if use_cross_matches_index.is_some() {
        aux_add_fields.insert(
            "cross_matches".to_string(),
            get_array_dict_element("aux.cross_matches"),
        );
    }
    if use_aliases_index.is_some() {
        aux_add_fields.insert("aliases".to_string(), get_array_dict_element("aux.aliases"));
    }

    let insert_aux_pipeline = use_prv_candidates_index.is_some()
        || use_fp_hists_index.is_some()
        || use_cross_matches_index.is_some()
        || use_aliases_index.is_some();

    let mut insert_aux_index = usize::MAX;
    if let Some(index) = use_prv_candidates_index {
        insert_aux_index = insert_aux_index.min(index);
    }
    if let Some(index) = use_fp_hists_index {
        insert_aux_index = insert_aux_index.min(index);
    }
    if let Some(index) = use_cross_matches_index {
        insert_aux_index = insert_aux_index.min(index);
    }
    if let Some(index) = use_aliases_index {
        insert_aux_index = insert_aux_index.min(index);
    }

    if insert_aux_index == usize::MAX && insert_aux_pipeline {
        return Err(FilterError::InvalidFilterPipeline(
            "could not determine where to insert aux pipeline".to_string(),
        ));
    }

    let mut pipeline = vec![
        doc! {
            "$match": doc! {
                "_id": doc! {
                    "$in": [] // candids will be inserted here
                }
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "properties": 1,
                "coordinates": 1,
            }
        },
    ];

    let mut insert_aux_pipeline = insert_aux_pipeline;
    for i in 0..filter_pipeline.len() {
        let x = mongodb::bson::to_document(&filter_pipeline[i])?;

        if insert_aux_pipeline && i == insert_aux_index {
            pipeline.push(doc! {
                "$lookup": doc! {
                    "from": "DECAM_alerts_aux",
                    "localField": "objectId",
                    "foreignField": "_id",
                    "as": "aux"
                }
            });
            pipeline.push(doc! {
                "$addFields": &aux_add_fields
            });
            insert_aux_pipeline = false; // only insert once
        }

        pipeline.push(x);
    }
    Ok(pipeline)
}

pub struct DecamFilterWorker {
    alert_pipeline: Vec<Document>,
    alert_collection: mongodb::Collection<Document>,
    alert_cutout_storage: CutoutStorage,
    filter_collection: mongodb::Collection<Filter>,
    input_queue: String,
    output_topic: String,
    filter_ids: Option<Vec<String>>,
    filters: Vec<LoadedFilter>,
}

#[async_trait::async_trait]
impl FilterWorker for DecamFilterWorker {
    #[instrument(err)]
    async fn new(
        config_path: &str,
        filter_ids: Option<Vec<String>>,
    ) -> Result<Self, FilterWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db: mongodb::Database = config.build_db().await?;
        let alert_collection = db.collection("DECAM_alerts");
        let filter_collection = db.collection("filters");
        let alert_cutout_storage = config.build_cutout_storage(&Survey::Decam).await?;

        let input_queue = "DECAM_alerts_filter_queue".to_string();
        let output_topic = "DECAM_alerts_results".to_string();

        let filters = build_loaded_filters(&filter_ids, &Survey::Decam, &filter_collection).await?;

        Ok(DecamFilterWorker {
            alert_pipeline: create_decam_filter_alert_pipeline(),
            alert_collection,
            alert_cutout_storage,
            filter_collection,
            input_queue,
            output_topic,
            filter_ids,
            filters,
        })
    }

    async fn refresh_filters(&mut self) -> Result<(), FilterWorkerError> {
        info!("refreshing DECAM filters from database");
        self.filters =
            build_loaded_filters(&self.filter_ids, &Survey::Decam, &self.filter_collection).await?;
        info!(
            "refreshed DECAM filters from database; now tracking {} filters",
            self.filters.len()
        );
        Ok(())
    }

    fn survey() -> Survey {
        Survey::Decam
    }

    fn input_queue_name(&self) -> String {
        self.input_queue.clone()
    }

    fn output_topic_name(&self) -> String {
        self.output_topic.clone()
    }

    fn has_filters(&self) -> bool {
        !self.filters.is_empty()
    }

    #[instrument(skip_all, err)]
    async fn process_alerts(&mut self, alerts: &[String]) -> Result<Vec<Alert>, FilterWorkerError> {
        let mut alerts_output = Vec::new();

        // DECam has a single public stream (no programids), so the queue holds
        // bare candids.
        let candids: Vec<i64> = alerts.iter().map(|alert| alert.parse().unwrap()).collect();

        let mut results_map: HashMap<i64, Vec<FilterResults>> = HashMap::new();
        for filter in &self.filters {
            let out_documents = run_filter(
                &candids,
                &filter.id,
                filter.pipeline.clone(),
                &self.alert_collection,
            )
            .await?;

            if out_documents.is_empty() {
                continue;
            } else {
                info!(
                    "{} alerts passed decam filter {}",
                    out_documents.len(),
                    filter.id,
                );
            }

            let now_ts = chrono::Utc::now().timestamp_millis() as f64;

            for doc in out_documents {
                let candid = doc.get_i64("_id")?;
                let annotations =
                    serde_json::to_string(doc.get_document("annotations").unwrap_or(&doc! {}))?;
                let filter_result = FilterResults {
                    filter_id: filter.id.clone(),
                    filter_name: filter.name.clone(),
                    passed_at: now_ts,
                    annotations,
                };
                let entry = results_map.entry(candid).or_insert(Vec::new());
                entry.push(filter_result);
            }
        }

        let alerts = build_decam_alerts(
            &results_map,
            &self.alert_pipeline,
            &self.alert_collection,
            &self.alert_cutout_storage,
        )
        .await?;
        alerts_output.extend(alerts);

        self.alert_cutout_storage.evict_from_cache(&candids).await;

        Ok(alerts_output)
    }
}
