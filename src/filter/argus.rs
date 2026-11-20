use mongodb::bson::{doc, Bson, Document};
use std::collections::HashMap;
use tracing::{debug, info, instrument, warn};

use crate::alert::ARGUS_RESULTS_TOPIC;
use crate::conf::AppConfig;
use crate::enrichment::fetch_alerts;
use crate::filter::{
    build_loaded_filters, build_lsst_aux_data, build_ztf_aux_data,
    insert_lsst_aux_pipeline_if_needed, insert_ztf_aux_pipeline_if_needed, record_filter_result,
    run_filter, uses_field_in_filter, validate_filter_pipeline, watchlist_projections, Alert,
    AlertHostGalaxy, Classification, Filter, FilterError, FilterResults, FilterWorker,
    FilterWorkerError, LoadedFilter, Origin, Photometry, SurveyMatches,
};
use crate::utils::cutouts::CutoutStorage;
use crate::utils::db::{fetch_timeseries_op, get_array_dict_element};
use crate::utils::enums::Survey;
use crate::utils::host::HostGalaxyAssociation;

const AB_NJY_ZEROPOINT: f64 = 31.4;

#[derive(Debug, Clone, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArgusOutputPoint {
    pub jd: f64,
    pub filter: i32,
    pub psf_magnitude_dif: Option<f32>,
    pub psf_magnitude_dif_err: Option<f32>,
    pub is_flux_positive: bool,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
}

impl ArgusOutputPoint {
    fn to_photometry(&self) -> Option<Photometry> {
        let mag = self.psf_magnitude_dif.filter(|m| m.is_finite())? as f64;
        let mag_err = self.psf_magnitude_dif_err.filter(|e| e.is_finite())? as f64;
        let flux = 10f64.powf((AB_NJY_ZEROPOINT - mag) / 2.5);
        let band = match self.filter {
            1 => "argusbeta",
            2 => "argusrho",
            _ => "argus",
        };
        Some(Photometry {
            jd: self.jd,
            flux: Some(if self.is_flux_positive { flux } else { -flux }),
            flux_err: flux * mag_err / 1.0857,
            band: band.to_string(),
            origin: Origin::Alert,
            programid: 1,
            survey: Survey::Argus,
            ra: self.ra,
            dec: self.dec,
        })
    }
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ArgusOutputCandidate {
    pub jd: f64,
    pub ra: f64,
    pub dec: f64,
    pub real_bogus_score: Option<f32>,
}

#[derive(Debug, serde::Deserialize)]
pub struct ArgusAlertForFilter {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: ArgusOutputCandidate,
    #[serde(default)]
    pub prv_candidates: Vec<ArgusOutputPoint>,
    #[serde(default)]
    pub host_galaxy: Option<HostGalaxyAssociation>,
}

fn create_argus_filter_alert_pipeline() -> Vec<Document> {
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
                "from": "ARGUS_alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "candidate": 1,
                "prv_candidates": fetch_timeseries_op(
                    "aux.prv_candidates",
                    "candidate.jd",
                    1000,
                    None
                ),
                "host_galaxy": {"$arrayElemAt": ["$aux.host_galaxy", 0]},
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "candidate.jd": 1,
                "candidate.ra": 1,
                "candidate.dec": 1,
                "candidate.realBogusScore": 1,
                "prv_candidates.jd": 1,
                "prv_candidates.filter": 1,
                "prv_candidates.psfMagnitudeDif": 1,
                "prv_candidates.psfMagnitudeDifErr": 1,
                "prv_candidates.isFluxPositive": 1,
                "prv_candidates.ra": 1,
                "prv_candidates.dec": 1,
                "host_galaxy": 1,
            }
        },
    ]
}

#[instrument(skip_all, err)]
pub async fn build_argus_alerts(
    alerts_with_filter_results: &HashMap<i64, Vec<FilterResults>>,
    alert_pipeline: &Vec<Document>,
    alert_collection: &mongodb::Collection<Document>,
    alert_cutout_storage: &CutoutStorage,
) -> Result<Vec<Alert>, FilterWorkerError> {
    let candids: Vec<i64> = alerts_with_filter_results.keys().cloned().collect();
    if candids.is_empty() {
        return Ok(Vec::new());
    }

    let alerts: Vec<ArgusAlertForFilter> = fetch_alerts(&candids, alert_pipeline, alert_collection)
        .await
        .map_err(|e| FilterWorkerError::FetchAlertsError(e.to_string()))?;
    if alerts.len() != candids.len() {
        warn!(
            "only fetched {} ARGUS alerts from {} candids",
            alerts.len(),
            candids.len()
        );
    }

    let mut candid_to_cutouts = alert_cutout_storage
        .retrieve_multiple_cutouts(&candids, false)
        .await?;

    let mut alerts_output = Vec::with_capacity(alerts.len());
    for alert in alerts {
        let candid = alert.candid;
        let classifications = alert
            .candidate
            .real_bogus_score
            .map(|score| Classification {
                classifier: "realBogus".to_string(),
                score,
                distance_arcsec: None,
            })
            .into_iter()
            .collect();
        let mut photometry: Vec<Photometry> = alert
            .prv_candidates
            .iter()
            .filter_map(ArgusOutputPoint::to_photometry)
            .collect();
        photometry.sort_by(|a, b| a.jd.total_cmp(&b.jd));
        let (cutout_science, cutout_template, cutout_difference) =
            match candid_to_cutouts.remove(&candid) {
                Some(cutouts) => (
                    cutouts.cutout_science,
                    cutouts.cutout_template,
                    cutouts.cutout_difference,
                ),
                None => (Vec::new(), Vec::new(), Vec::new()),
            };

        alerts_output.push(Alert {
            candid,
            object_id: alert.object_id,
            jd: alert.candidate.jd,
            ra: alert.candidate.ra,
            dec: alert.candidate.dec,
            survey: Survey::Argus,
            filters: alerts_with_filter_results
                .get(&candid)
                .cloned()
                .unwrap_or_default(),
            classifications,
            photometry,
            cutout_science,
            cutout_template,
            cutout_difference,
            survey_matches: SurveyMatches {
                ztf: None,
                lsst: None,
                decam: None,
            },
            host_galaxy: alert
                .host_galaxy
                .as_ref()
                .and_then(AlertHostGalaxy::from_association),
        });
    }

    Ok(alerts_output)
}

pub async fn build_argus_filter_pipeline(
    filter_pipeline: &Vec<serde_json::Value>,
    permissions: &HashMap<Survey, Vec<i32>>,
) -> Result<Vec<Document>, FilterError> {
    validate_filter_pipeline(filter_pipeline)?;

    let use_prv_candidates_index = uses_field_in_filter(filter_pipeline, "prv_candidates");
    let use_cross_matches_index = uses_field_in_filter(filter_pipeline, "cross_matches");
    let use_host_galaxy_index = uses_field_in_filter(filter_pipeline, "host_galaxy");
    let use_aliases_index = uses_field_in_filter(filter_pipeline, "aliases");

    let (use_aliases_index, mut ztf_insert_aux_pipeline, ztf_aux_add_fields) =
        build_ztf_aux_data(use_aliases_index, filter_pipeline, permissions);
    let (use_aliases_index, mut lsst_insert_aux_pipeline, lsst_aux_add_fields) =
        build_lsst_aux_data(use_aliases_index, filter_pipeline);

    let mut aux_add_fields = doc! { "aux": Bson::Null };
    if use_prv_candidates_index.is_some() {
        aux_add_fields.insert(
            "prv_candidates",
            fetch_timeseries_op("aux.prv_candidates", "candidate.jd", 1000, None),
        );
    }
    if use_cross_matches_index.is_some() {
        aux_add_fields.insert("cross_matches", get_array_dict_element("aux.cross_matches"));
    }
    if use_host_galaxy_index.is_some() {
        aux_add_fields.insert("host_galaxy", get_array_dict_element("aux.host_galaxy"));
    }
    if use_aliases_index.is_some() {
        aux_add_fields.insert("aliases", get_array_dict_element("aux.aliases"));
    }

    let insert_aux_index = [
        use_prv_candidates_index,
        use_cross_matches_index,
        use_host_galaxy_index,
        use_aliases_index,
    ]
    .into_iter()
    .flatten()
    .min();

    let mut pipeline = vec![
        doc! {
            "$match": {
                "_id": {"$in": []}
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "kind": 1,
                "lane": 1,
                "alertCadence": 1,
                "candidate": 1,
                "properties": 1,
                "coordinates": 1,
                "eventAssociation": 1,
                "ssoAssociation": 1,
            }
        },
    ];

    for (index, stage) in filter_pipeline.iter().enumerate() {
        if Some(index) == insert_aux_index {
            pipeline.push(doc! {
                "$lookup": {
                    "from": "ARGUS_alerts_aux",
                    "localField": "objectId",
                    "foreignField": "_id",
                    "as": "aux"
                }
            });
            pipeline.push(doc! { "$addFields": &aux_add_fields });
            insert_ztf_aux_pipeline_if_needed(
                &mut pipeline,
                &mut ztf_insert_aux_pipeline,
                &ztf_aux_add_fields,
            );
            insert_lsst_aux_pipeline_if_needed(
                &mut pipeline,
                &mut lsst_insert_aux_pipeline,
                &lsst_aux_add_fields,
            );
        }
        pipeline.push(mongodb::bson::to_document(stage)?);
    }
    Ok(pipeline)
}

pub struct ArgusFilterWorker {
    alert_pipeline: Vec<Document>,
    alert_collection: mongodb::Collection<Document>,
    alert_cutout_storage: CutoutStorage,
    filter_collection: mongodb::Collection<Filter>,
    filter_ids: Option<Vec<String>>,
    filters: Vec<LoadedFilter>,
    watchlist_projections: HashMap<String, Document>,
}

#[async_trait::async_trait]
impl FilterWorker for ArgusFilterWorker {
    #[instrument(err)]
    async fn new(
        config_path: &str,
        filter_ids: Option<Vec<String>>,
    ) -> Result<Self, FilterWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db: mongodb::Database = config.build_db().await?;
        let alert_collection = db.collection("ARGUS_alerts");
        let filter_collection = db.collection("filters");
        let alert_cutout_storage = config.build_cutout_storage(&Survey::Argus).await?;
        let watchlist_projections = watchlist_projections(&config, &Survey::Argus);
        let filters = build_loaded_filters(
            &filter_ids,
            &Survey::Argus,
            &filter_collection,
            &watchlist_projections,
        )
        .await?;

        Ok(ArgusFilterWorker {
            alert_pipeline: create_argus_filter_alert_pipeline(),
            alert_collection,
            alert_cutout_storage,
            filter_collection,
            filter_ids,
            filters,
            watchlist_projections,
        })
    }

    async fn refresh_filters(&mut self) -> Result<(), FilterWorkerError> {
        self.filters = build_loaded_filters(
            &self.filter_ids,
            &Survey::Argus,
            &self.filter_collection,
            &self.watchlist_projections,
        )
        .await?;
        info!(
            "refreshed ARGUS filters; now tracking {}",
            self.filters.len()
        );
        Ok(())
    }

    fn survey() -> Survey {
        Survey::Argus
    }

    fn input_queue_name(&self) -> String {
        "ARGUS_alerts_filter_queue".to_string()
    }

    fn output_topic_name(&self) -> String {
        ARGUS_RESULTS_TOPIC.to_string()
    }

    fn has_filters(&self) -> bool {
        !self.filters.is_empty()
    }

    #[instrument(skip_all, err)]
    async fn process_alerts(&mut self, alerts: &[String]) -> Result<Vec<Alert>, FilterWorkerError> {
        let candids: Vec<i64> = alerts
            .iter()
            .filter_map(|alert| alert.parse().ok())
            .collect();

        let mut results_map: HashMap<i64, Vec<FilterResults>> = HashMap::new();
        for filter in &self.filters {
            let out_documents = run_filter(
                &candids,
                &filter.id,
                filter.pipeline.clone(),
                &self.alert_collection,
            )
            .await?;

            record_filter_result(&Survey::Argus, filter, out_documents.len(), candids.len());
            debug!(
                "{}/{} ARGUS alerts passed filter {}",
                out_documents.len(),
                candids.len(),
                filter.id,
            );
            if out_documents.is_empty() {
                continue;
            }

            let now_ts = chrono::Utc::now().timestamp_millis() as f64;
            for doc in out_documents {
                let candid = doc.get_i64("_id")?;
                let annotations =
                    serde_json::to_string(doc.get_document("annotations").unwrap_or(&doc! {}))?;
                results_map.entry(candid).or_default().push(FilterResults {
                    filter_id: filter.id.clone(),
                    filter_name: filter.name.clone(),
                    passed_at: now_ts,
                    annotations,
                });
            }
        }

        build_argus_alerts(
            &results_map,
            &self.alert_pipeline,
            &self.alert_collection,
            &self.alert_cutout_storage,
        )
        .await
    }
}
