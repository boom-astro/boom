use mongodb::bson::{doc, Document};
use std::collections::HashMap;
use tracing::{info, instrument, warn};

use crate::alert::ZtfCandidate;
use crate::conf::AppConfig;
use crate::enrichment::{
    create_ztf_alert_pipeline, deserialize_ztf_alert_lightcurve, deserialize_ztf_forced_lightcurve,
    fetch_alert_cutouts, fetch_alerts, ZtfAlertClassifications, ZtfPhotometry, ZtfSurveyMatches,
};
use crate::filter::{
    build_loaded_filters, build_lsst_aux_data, insert_lsst_aux_pipeline_if_needed,
    parse_programid_candid_tuple, run_filter, update_aliases_index_multiple, uses_field_in_filter,
    validate_filter_pipeline, Alert, Classification, FilterError, FilterResults, FilterWorker,
    FilterWorkerError, LoadedFilter, Origin, Photometry, SurveyMatch, SurveyMatches,
};
use crate::utils::db::{fetch_timeseries_op, get_array_dict_element};
use crate::utils::{enums::Survey, o11y::logging::as_error};

/// For a filter running on another survey (e.g., LSST), determine if we need to
/// fetch ZTF auxiliary data (prv_candidates, fp_hists) based on the fields
/// used in the filter pipeline.
///
/// # Arguments
/// * `use_aliases_index` - Current index of the aliases lookup stage, if any.
/// * `filter_pipeline` - The user-defined filter pipeline stages.
///
/// Returns
/// * `Option<usize>` - Updated index of the aliases lookup stage, if any.
/// * `bool` - Whether to insert the ZTF auxiliary data lookup pipeline.
/// * `Document` - The fields to add to the alert documents.
pub fn build_ztf_aux_data(
    use_aliases_index: Option<usize>,
    filter_pipeline: &Vec<serde_json::Value>,
    permissions: &HashMap<Survey, Vec<i32>>,
) -> (Option<usize>, bool, Document) {
    let use_ztf_prv_candidates_index = uses_field_in_filter(filter_pipeline, "ZTF.prv_candidates");
    let use_ztf_fp_hists_index = uses_field_in_filter(filter_pipeline, "ZTF.fp_hists");

    let mut ztf_aux_add_fields = doc! {
        "ztf_aux": mongodb::bson::Bson::Null,
    };

    static DEFAULT_ZTF_PERMS: &[i32] = &[1];
    let ztf_permissions: &[i32] = permissions
        .get(&Survey::Ztf)
        .map(|v| &v[..])
        .unwrap_or(DEFAULT_ZTF_PERMS);

    let permissions_check = Some(vec![doc! {
        "$in": [
            "$$x.programid",
            &ztf_permissions
        ]
    }]);
    if use_ztf_prv_candidates_index.is_some() {
        ztf_aux_add_fields.insert(
            "ZTF.prv_candidates".to_string(),
            fetch_timeseries_op(
                "ztf_aux.prv_candidates",
                "candidate.jd",
                365,
                permissions_check.clone(),
            ),
        );
    }
    if use_ztf_fp_hists_index.is_some() {
        ztf_aux_add_fields.insert(
            "ZTF.fp_hists".to_string(),
            fetch_timeseries_op(
                "ztf_aux.fp_hists",
                "candidate.jd",
                365,
                permissions_check.clone(),
            ),
        );
    }

    let mut ztf_insert_aux_index = usize::MAX;
    if let Some(index) = use_ztf_prv_candidates_index {
        ztf_insert_aux_index = ztf_insert_aux_index.min(index);
    }
    if let Some(index) = use_ztf_fp_hists_index {
        ztf_insert_aux_index = ztf_insert_aux_index.min(index);
    }
    let ztf_insert_aux_pipeline = ztf_insert_aux_index != usize::MAX;

    let updated_use_aliases_index = update_aliases_index_multiple(
        use_aliases_index,
        vec![use_ztf_prv_candidates_index, use_ztf_fp_hists_index],
    );

    (
        updated_use_aliases_index,
        ztf_insert_aux_pipeline,
        ztf_aux_add_fields,
    )
}

/// Inserts the ZTF auxiliary data lookup pipeline into the provided pipeline
/// if needed.
///
/// # Arguments
/// * `pipeline` - The MongoDB aggregation pipeline to modify.
/// * `ztf_insert_aux_pipeline` - Whether to insert the ZTF auxiliary data lookup pipeline.
/// * `ztf_aux_add_fields` - The fields to add to the alert documents.
///
/// Returns
/// * `()` - The function modifies the pipeline in place.
pub fn insert_ztf_aux_pipeline_if_needed(
    pipeline: &mut Vec<Document>,
    ztf_insert_aux_pipeline: &mut bool,
    ztf_aux_add_fields: &Document,
) {
    if *ztf_insert_aux_pipeline {
        pipeline.push(doc! {
            "$lookup": doc! {
                "from": "ZTF_alerts_aux",
                "localField": "aliases.ZTF.0",
                "foreignField": "_id",
                "as": "ztf_aux"
            }
        });
        pipeline.push(doc! {
            "$addFields": ztf_aux_add_fields
        });
        *ztf_insert_aux_pipeline = false; // only insert once
    }
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ZtfAlertEnriched {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: ZtfCandidate,
    pub classifications: Option<ZtfAlertClassifications>,
    #[serde(deserialize_with = "deserialize_ztf_alert_lightcurve")]
    pub prv_candidates: Vec<ZtfPhotometry>,
    #[serde(deserialize_with = "deserialize_ztf_alert_lightcurve")]
    pub prv_nondetections: Vec<ZtfPhotometry>,
    #[serde(deserialize_with = "deserialize_ztf_forced_lightcurve")]
    pub fp_hists: Vec<ZtfPhotometry>,
    pub survey_matches: Option<ZtfSurveyMatches>,
}

/// Builds ZTF Alert objects from the provided filter results and alert collection.
///
/// # Arguments
/// * `alerts_with_filter_results` - A mapping of alert candids to their corresponding filter results.
/// * `alert_pipeline` - The MongoDB aggregation pipeline to fetch alert data, which should be pre-populated with the necessary lookups for auxiliary data.
/// * `alert_collection` - The MongoDB collection containing ZTF alert documents.
/// * `alert_cutout_collection` - The MongoDB collection containing ZTF alert cutout documents.

///
/// # Returns
/// * `Result<Vec<Alert>, FilterWorkerError>` - A vector of constructed Alert objects or a FilterWorkerError.
#[instrument(skip_all, err)]
pub async fn build_ztf_alerts(
    alerts_with_filter_results: &HashMap<i64, Vec<FilterResults>>,
    alert_pipeline: &Vec<Document>,
    alert_collection: &mongodb::Collection<Document>,
    alert_cutout_collection: &mongodb::Collection<Document>,
) -> Result<Vec<Alert>, FilterWorkerError> {
    let candids: Vec<i64> = alerts_with_filter_results.keys().cloned().collect();
    if candids.is_empty() {
        return Ok(Vec::new());
    }

    let alerts: Vec<ZtfAlertEnriched> = fetch_alerts(&candids, &alert_pipeline, alert_collection)
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

    let mut candid_to_cutouts = fetch_alert_cutouts(&candids, &alert_cutout_collection)
        .await
        .map_err(|e| FilterWorkerError::FetchCutoutsError(e.to_string()))?;

    if candid_to_cutouts.len() != alerts.len() {
        return Err(FilterWorkerError::MissingCutoutsBatch(
            alerts.len() - candid_to_cutouts.len(),
        ));
    }

    let mut alerts_output = Vec::new();
    for alert in alerts {
        let candid = alert.candid;
        let cutouts = candid_to_cutouts
            .remove(&candid)
            .ok_or_else(|| FilterWorkerError::MissingCutouts(candid))?;

        let mut classifications = Vec::new();
        if let Some(rb) = alert.candidate.candidate.rb {
            classifications.push(Classification {
                classifier: "rb".to_string(),
                score: rb,
                distance_arcsec: None,
            });
        }
        if let Some(drb) = alert.candidate.candidate.drb {
            classifications.push(Classification {
                classifier: "drb".to_string(),
                score: drb,
                distance_arcsec: None,
            });
        }
        if let (Some(sgscore), Some(distpsnr1)) = (
            alert.candidate.candidate.sgscore1,
            alert.candidate.candidate.distpsnr1,
        ) {
            classifications.push(Classification {
                classifier: "sgscore1".to_string(),
                score: sgscore,
                distance_arcsec: Some(distpsnr1),
            });
        }

        if let Some(alert_classifications) = alert.classifications {
            // ACAI (h,n,o,v,b)
            classifications.push(Classification {
                classifier: "acai_h".to_string(),
                score: alert_classifications.acai_h,
                distance_arcsec: None,
            });
            classifications.push(Classification {
                classifier: "acai_n".to_string(),
                score: alert_classifications.acai_n,
                distance_arcsec: None,
            });
            classifications.push(Classification {
                classifier: "acai_o".to_string(),
                score: alert_classifications.acai_o,
                distance_arcsec: None,
            });
            classifications.push(Classification {
                classifier: "acai_v".to_string(),
                score: alert_classifications.acai_v,
                distance_arcsec: None,
            });
            classifications.push(Classification {
                classifier: "acai_b".to_string(),
                score: alert_classifications.acai_b,
                distance_arcsec: None,
            });
            // BTSbot
            classifications.push(Classification {
                classifier: "btsbot".to_string(),
                score: alert_classifications.btsbot,
                distance_arcsec: None,
            });
        }

        // TODO, get classifications from the alert document

        let mut photometry = Vec::new();
        for doc in alert.prv_candidates.iter() {
            photometry.push(Photometry {
                jd: doc.jd,
                flux: doc.flux,
                flux_err: doc.flux_err,
                band: format!("ztf{}", doc.band),
                origin: Origin::Alert,
                programid: doc.programid,
                survey: Survey::Ztf,
                ra: doc.ra,
                dec: doc.dec,
            });
        }

        for doc in alert.prv_nondetections.iter() {
            photometry.push(Photometry {
                jd: doc.jd,
                flux: None, // for non-detections, flux is None
                flux_err: doc.flux_err,
                band: format!("ztf{}", doc.band),
                origin: Origin::Alert,
                programid: doc.programid,
                survey: Survey::Ztf,
                ra: None,
                dec: None,
            });
        }

        for doc in alert.fp_hists.iter() {
            photometry.push(Photometry {
                jd: doc.jd,
                flux: doc.flux,
                flux_err: doc.flux_err,
                band: format!("ztf{}", doc.band),
                origin: Origin::ForcedPhot,
                programid: doc.programid,
                survey: Survey::Ztf,
                ra: None,
                dec: None,
            });
        }

        photometry.sort_by(|a, b| a.jd.partial_cmp(&b.jd).unwrap());

        let mut survey_matches = SurveyMatches {
            ztf: None,
            lsst: None,
        };
        if let Some(lsst_match) = alert.survey_matches.as_ref().and_then(|m| m.lsst.as_ref()) {
            let mut lsst_photometry = Vec::new();
            for doc in lsst_match.prv_candidates.iter() {
                lsst_photometry.push(Photometry {
                    jd: doc.jd,
                    flux: doc.flux,
                    flux_err: doc.flux_err,
                    band: format!("lsst{}", doc.band),
                    origin: Origin::Alert,
                    programid: 1,
                    survey: Survey::Lsst,
                    ra: doc.ra,
                    dec: doc.dec,
                });
            }
            for doc in lsst_match.fp_hists.iter() {
                lsst_photometry.push(Photometry {
                    jd: doc.jd,
                    flux: doc.flux,
                    flux_err: doc.flux_err,
                    band: format!("lsst{}", doc.band),
                    origin: Origin::ForcedPhot,
                    programid: 1,
                    survey: Survey::Lsst,
                    ra: None,
                    dec: None,
                });
            }

            lsst_photometry.sort_by(|a, b| a.jd.partial_cmp(&b.jd).unwrap());

            survey_matches.lsst = Some(SurveyMatch {
                object_id: lsst_match.object_id.clone(),
                ra: lsst_match.ra,
                dec: lsst_match.dec,
                photometry: lsst_photometry,
            });
        }

        let alert = Alert {
            candid: alert.candid,
            object_id: alert.object_id,
            jd: alert.candidate.candidate.jd,
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
            survey: Survey::Ztf,
            survey_matches,
        };

        alerts_output.push(alert);
    }

    Ok(alerts_output)
}

/// Builds a MongoDB aggregation pipeline for ZTF filter execution.
///
/// This function validates the provided filter pipeline and augments it with necessary
/// auxiliary data lookups (prv_candidates, fp_hists, cross_matches, aliases) based on
/// which fields are referenced in the filter. The resulting pipeline starts with a match stage
/// to filter by candids, and should be populated with the actual candids before execution.
///
/// # Arguments
/// * `filter_pipeline` - The user-defined filter pipeline stages
///
/// # Returns
/// * `Result<Vec<Document>, FilterError>` - A complete MongoDB aggregation pipeline ready for execution, or a `FilterError` if validation fails.
pub async fn build_ztf_filter_pipeline(
    filter_pipeline: &Vec<serde_json::Value>,
    permissions: &HashMap<Survey, Vec<i32>>,
) -> Result<Vec<Document>, FilterError> {
    // validate filter
    validate_filter_pipeline(&filter_pipeline)?;

    let use_prv_candidates_index = uses_field_in_filter(filter_pipeline, "prv_candidates");
    let use_prv_nondetections_index = uses_field_in_filter(filter_pipeline, "prv_nondetections");
    let use_fp_hists_index = uses_field_in_filter(filter_pipeline, "fp_hists");
    let use_cross_matches_index = uses_field_in_filter(filter_pipeline, "cross_matches");
    let use_aliases_index = uses_field_in_filter(filter_pipeline, "aliases");

    // LSST data products
    let (use_aliases_index, mut lsst_insert_aux_pipeline, lsst_aux_add_fields) =
        build_lsst_aux_data(use_aliases_index, filter_pipeline);

    let mut aux_add_fields = doc! {
        "aux": mongodb::bson::Bson::Null,
    };

    let ztf_permissions = match permissions.get(&Survey::Ztf) {
        Some(perms) => perms,
        None => {
            return Err(FilterError::InvalidFilterPipeline(
                "No ZTF permissions found for the filter".to_string(),
            ))
        }
    };

    if use_prv_candidates_index.is_some() {
        // insert it in aux addFields stage
        aux_add_fields.insert(
            "prv_candidates".to_string(),
            fetch_timeseries_op(
                "aux.prv_candidates",
                "candidate.jd",
                365,
                Some(vec![doc! {
                    "$in": [
                        "$$x.programid",
                        &ztf_permissions
                    ]
                }]),
            ),
        );
    }
    if use_prv_nondetections_index.is_some() {
        aux_add_fields.insert(
            "prv_nondetections".to_string(),
            fetch_timeseries_op(
                "aux.prv_nondetections",
                "candidate.jd",
                365,
                Some(vec![doc! {
                    "$in": [
                        "$$x.programid",
                        &ztf_permissions
                    ]
                }]),
            ),
        );
    }
    if use_fp_hists_index.is_some() {
        aux_add_fields.insert(
            "fp_hists".to_string(),
            fetch_timeseries_op(
                "aux.fp_hists",
                "candidate.jd",
                365,
                Some(vec![doc! {
                    "$in": [
                        "$$x.programid",
                        &ztf_permissions
                    ]
                }]),
            ),
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

    let mut insert_aux_pipeline = use_prv_candidates_index.is_some()
        || use_prv_nondetections_index.is_some()
        || use_cross_matches_index.is_some()
        || use_fp_hists_index.is_some()
        || use_aliases_index.is_some();

    let mut insert_aux_index = usize::MAX;
    if let Some(index) = use_prv_candidates_index {
        insert_aux_index = insert_aux_index.min(index);
    }
    if let Some(index) = use_prv_nondetections_index {
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

    // some sanity checks
    if insert_aux_index == usize::MAX && insert_aux_pipeline {
        return Err(FilterError::InvalidFilterPipeline(
            "could not determine where to insert aux pipeline".to_string(),
        ));
    }

    // filter prefix (with permissions)
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
                "classifications": 1,
                "properties": 1,
                "coordinates": 1,
            }
        },
    ];

    // now we loop over the base_pipeline and insert stages from the filter_pipeline
    // and when i = insert_index, we insert the aux_pipeline before the stage
    for i in 0..filter_pipeline.len() {
        let x = mongodb::bson::to_document(&filter_pipeline[i])?;

        if insert_aux_pipeline && i == insert_aux_index {
            pipeline.push(doc! {
                "$lookup": doc! {
                    "from": "ZTF_alerts_aux",
                    "localField": "objectId",
                    "foreignField": "_id",
                    "as": "aux"
                }
            });
            pipeline.push(doc! {
                "$addFields": &aux_add_fields
            });
            insert_aux_pipeline = false; // only insert once

            insert_lsst_aux_pipeline_if_needed(
                &mut pipeline,
                &mut lsst_insert_aux_pipeline,
                &lsst_aux_add_fields,
            );
        }

        // push the current stage
        pipeline.push(x);
    }

    Ok(pipeline)
}

pub struct ZtfFilterWorker {
    alert_pipeline: Vec<Document>,
    alert_collection: mongodb::Collection<Document>,
    alert_cutout_collection: mongodb::Collection<Document>,
    input_queue: String,
    output_topic: String,
    filters: Vec<LoadedFilter>,
    filters_by_permission: HashMap<i32, Vec<String>>,
}

#[async_trait::async_trait]
impl FilterWorker for ZtfFilterWorker {
    #[instrument(err)]
    async fn new(
        config_path: &str,
        filter_ids: Option<Vec<String>>,
    ) -> Result<Self, FilterWorkerError> {
        let config = AppConfig::from_path(config_path)?;
        let db: mongodb::Database = config.build_db().await?;
        let alert_collection = db.collection("ZTF_alerts");
        let alert_cutout_collection = db.collection("ZTF_alerts_cutouts");
        let filter_collection = db.collection("filters");

        let input_queue = "ZTF_alerts_filter_queue".to_string();
        let output_topic = "ZTF_alerts_results".to_string();

        let filters = build_loaded_filters(&filter_ids, &Survey::Ztf, &filter_collection).await?;

        // Create a hashmap of filters per programid (permissions)
        let mut filters_by_permission: HashMap<i32, Vec<String>> = HashMap::new();
        for filter in &filters {
            let ztf_permissions = match filter.permissions.get(&Survey::Ztf) {
                Some(perms) => perms,
                None => {
                    warn!(
                        "Filter {} running on ZTF alerts has no ZTF permissions set, skipping",
                        filter.id
                    );
                    continue;
                }
            };
            for permission in ztf_permissions {
                let entry = filters_by_permission
                    .entry(*permission)
                    .or_insert(Vec::new());
                entry.push(filter.id.clone());
            }
        }

        Ok(ZtfFilterWorker {
            alert_pipeline: create_ztf_alert_pipeline(true),
            alert_collection,
            alert_cutout_collection,
            input_queue,
            output_topic,
            filters,
            filters_by_permission,
        })
    }

    fn survey() -> Survey {
        Survey::Ztf
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

        // retrieve alerts to process and group by programid
        let mut alerts_by_programid: HashMap<i32, Vec<i64>> = HashMap::new();
        for tuple_str in alerts {
            if let Some(tuple) = parse_programid_candid_tuple(&tuple_str) {
                let entry = alerts_by_programid.entry(tuple.0).or_insert(Vec::new());
                entry.push(tuple.1);
            } else {
                warn!("Failed to parse tuple from string: {}", tuple_str);
            }
        }

        // For each programid, get the filters that have that programid in their
        // permissions and run the filters
        for (programid, candids) in alerts_by_programid {
            let mut results_map: HashMap<i64, Vec<FilterResults>> = HashMap::new();

            let filter_ids_with_perms = self
                .filters_by_permission
                .get(&programid)
                .ok_or(FilterWorkerError::GetFilterByQueueError)?;

            for filter in &self.filters {
                // If the filter ID is not in the list of filter IDs for this
                // programid, skip it
                if !filter_ids_with_perms.contains(&filter.id) {
                    continue;
                }

                let out_documents = run_filter(
                    &candids,
                    &filter.id,
                    filter.pipeline.clone(),
                    &self.alert_collection,
                )
                .await?;

                info!(
                    "{}/{} ZTF alerts with programid {} passed filter {}",
                    out_documents.len(),
                    candids.len(),
                    programid,
                    filter.id,
                );

                // If we have output documents, we need to process them
                // and create filter results for each document (which contain annotations)
                // however, if the array is empty, there's nothing to do
                if out_documents.is_empty() {
                    continue;
                }

                let now_ts = chrono::Utc::now().timestamp_millis() as f64;

                for doc in out_documents {
                    let candid = doc
                        .get_i64("_id")
                        .inspect_err(as_error!("Failed to get candid from document"))?;
                    // might want to have the annotations as an optional field instead of empty
                    let annotations =
                        serde_json::to_string(doc.get_document("annotations").unwrap_or(&doc! {}))
                            .inspect_err(as_error!("Failed to serialize annotations"))?;
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

            let alerts = build_ztf_alerts(
                &results_map,
                &self.alert_pipeline,
                &self.alert_collection,
                &self.alert_cutout_collection,
            )
            .await?;
            alerts_output.extend(alerts);
        }

        Ok(alerts_output)
    }
}
