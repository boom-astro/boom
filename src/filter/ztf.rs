use futures::stream::StreamExt;
use mongodb::bson::{doc, Document};
use std::collections::HashMap;
use tracing::{info, instrument, warn};

use crate::conf::AppConfig;
use crate::filter::{
    build_loaded_filters, build_lsst_aux_data, insert_lsst_aux_pipeline_if_needed,
    parse_programid_candid_tuple, run_filter, update_aliases_index_multiple, uses_field_in_filter,
    validate_filter_pipeline, Alert, Classification, FilterError, FilterResults, FilterWorker,
    FilterWorkerError, LoadedFilter, Origin, Photometry,
};
use crate::utils::db::{fetch_timeseries_op, get_array_dict_element, get_array_element};
use crate::utils::{enums::Survey, lightcurves::ZTF_ZP, o11y::logging::as_error};

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

/// Builds ZTF Alert objects from the provided filter results and alert collection.
///
/// # Arguments
/// * `alerts_with_filter_results` - A mapping of alert candids to their corresponding filter results.
/// * `alert_collection` - The MongoDB collection containing ZTF alert documents.
///
/// # Returns
/// * `Result<Vec<Alert>, FilterWorkerError>` - A vector of constructed Alert objects or a FilterWorkerError.
#[instrument(skip_all, err)]
pub async fn build_ztf_alerts(
    alerts_with_filter_results: &HashMap<i64, Vec<FilterResults>>,
    alert_collection: &mongodb::Collection<Document>,
) -> Result<Vec<Alert>, FilterWorkerError> {
    let candids: Vec<i64> = alerts_with_filter_results.keys().cloned().collect();
    let pipeline = vec![
        doc! {
            "$match": {
                "_id": { "$in": &candids }
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "jd": "$candidate.jd",
                "ra": "$candidate.ra",
                "dec": "$candidate.dec",
                "rb": "$candidate.rb",
                "drb": "$candidate.drb",
                "classifications": 1,
            }
        },
        doc! {
            "$lookup": {
                "from": "ZTF_alerts_aux",
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        doc! {
            "$lookup": {
                "from": "ZTF_alerts_cutouts",
                "localField": "_id",
                "foreignField": "_id",
                "as": "cutouts"
            }
        },
        doc! {
            "$project": {
                "objectId": 1,
                "jd": 1,
                "ra": 1,
                "dec": 1,
                "prv_candidates": get_array_element("aux.prv_candidates"),
                "prv_nondetections": get_array_element("aux.prv_nondetections"),
                "fp_hists": get_array_element("aux.fp_hists"),
                "cutoutScience": get_array_element("cutouts.cutoutScience"),
                "cutoutTemplate": get_array_element("cutouts.cutoutTemplate"),
                "cutoutDifference": get_array_element("cutouts.cutoutDifference"),
                "classifications": 1,
            }
        },
    ];

    // Execute the aggregation pipeline
    let mut cursor = alert_collection.aggregate(pipeline).await?;

    let mut alerts_output = Vec::new();
    while let Some(alert_document) = cursor.next().await {
        let alert_document = alert_document?;
        let candid = alert_document.get_i64("_id")?;
        let object_id = alert_document.get_str("objectId")?.to_string();
        let jd = alert_document.get_f64("jd")?;
        let ra = alert_document.get_f64("ra")?;
        let dec = alert_document.get_f64("dec")?;
        let cutout_science = alert_document.get_binary_generic("cutoutScience")?.to_vec();
        let cutout_template = alert_document
            .get_binary_generic("cutoutTemplate")?
            .to_vec();
        let cutout_difference = alert_document
            .get_binary_generic("cutoutDifference")?
            .to_vec();

        // let's create the array of photometry (non-forced phot only for now)
        let mut photometry = Vec::new();
        for doc in alert_document.get_array("prv_candidates")?.iter() {
            let doc = match doc.as_document() {
                Some(doc) => doc,
                None => continue, // skip if not a document
            };
            let jd = doc.get_f64("jd")?;
            let flux = doc.get_f64("psfFlux")?; // in nJy
            let flux_err = doc.get_f64("psfFluxErr")?; // in nJy
            let band = doc.get_str("band")?.to_string();
            let programid = doc.get_i32("programid")?;
            let ra = doc.get_f64("ra").ok(); // optional, might not be present
            let dec = doc.get_f64("dec").ok(); // optional, might not be present

            photometry.push(Photometry {
                jd,
                flux: Some(flux),
                flux_err,
                band: format!("ztf{}", band),
                origin: Origin::Alert,
                programid,
                survey: Survey::Ztf,
                ra,
                dec,
            });
        }

        // next we do the non detections
        for doc in alert_document.get_array("prv_nondetections")?.iter() {
            let doc = match doc.as_document() {
                Some(doc) => doc,
                None => continue, // skip if not a document
            };
            let jd = doc.get_f64("jd")?;
            let flux_err = doc.get_f64("psfFluxErr")?;
            let band = doc.get_str("band")?.to_string();
            let programid = doc.get_i32("programid")?;

            photometry.push(Photometry {
                jd,
                flux: None, // for non-detections, flux is None
                flux_err,
                band: format!("ztf{}", band),
                origin: Origin::Alert,
                programid,
                survey: Survey::Ztf,
                ra: None,
                dec: None,
            });
        }

        for doc in alert_document.get_array("fp_hists")?.iter() {
            let doc = match doc.as_document() {
                Some(doc) => doc,
                None => continue, // skip if not a document
            };

            // we only want forced photometry with procstatus == "0"
            if doc.get_str("procstatus")? != "0" {
                continue;
            }
            let jd = doc.get_f64("jd")?;
            let magzpsci = doc.get_f64("magzpsci")?;
            // TODO: read from psfFlux once that is moved to a fixed ZP in the database
            //       (instead of doing the conversion here in code)
            let flux = doc.get_f64("forcediffimflux").ok();
            let flux_err = doc.get_f64("forcediffimfluxunc")?;
            let band = doc.get_str("band")?.to_string();
            let programid = doc.get_i32("programid")?;

            // TODO: remove this conversion once we read flux and flux_err from the database with a fixed ZP
            let zp_scaling_factor = 10f64.powf((ZTF_ZP as f64 - magzpsci) / 2.5);
            let flux = if flux != Some(-99999.0) && flux.map_or(false, |f| !f.is_nan()) {
                flux.map(|f| f * 1e9_f64 * zp_scaling_factor) // convert to a fixed ZP and nJy
            } else {
                None
            };
            let flux_err = if flux_err != -99999.0 && !flux_err.is_nan() {
                flux_err * 1e9_f64 * zp_scaling_factor // convert to a fixed ZP and nJy
            } else {
                return Err(FilterWorkerError::MissingFluxPSF);
            };

            photometry.push(Photometry {
                jd,
                flux,
                flux_err,
                band: format!("ztf{}", band),
                origin: Origin::ForcedPhot,
                programid,
                survey: Survey::Ztf,
                ra: None,
                dec: None,
            });
        }

        // sort the photometry by jd ascending
        photometry.sort_by(|a, b| a.jd.partial_cmp(&b.jd).unwrap());

        // last but not least, we need to get the classifications
        let mut classifications = Vec::new();
        // classifications in the alert is a document with classifier names as keys and the scores as values
        // we need to convert it to a vec of Classification structs
        if let Some(classifications_doc) = alert_document.get_document("classifications").ok() {
            for (key, value) in classifications_doc.iter() {
                if let Some(score) = value.as_f64() {
                    classifications.push(Classification {
                        classifier: key.to_string(),
                        score,
                    });
                }
            }
        }

        // add the rb and drb to the classifications if present
        if let Some(rb) = alert_document.get_f64("rb").ok() {
            classifications.push(Classification {
                classifier: "rb".to_string(),
                score: rb,
            });
        }
        if let Some(drb) = alert_document.get_f64("drb").ok() {
            classifications.push(Classification {
                classifier: "drb".to_string(),
                score: drb,
            });
        }

        let alert = Alert {
            candid,
            object_id,
            jd,
            ra,
            dec,
            filters: alerts_with_filter_results
                .get(&candid)
                .cloned()
                .unwrap_or_else(Vec::new),
            classifications,
            photometry,
            cutout_science,
            cutout_template,
            cutout_difference,
            survey: Survey::Ztf,
        };

        alerts_output.push(alert);
    }

    if candids.len() != alerts_output.len() {
        return Err(FilterWorkerError::AlertNotFound);
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
    alert_collection: mongodb::Collection<Document>,
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
            alert_collection,
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

            let alerts = build_ztf_alerts(&results_map, &self.alert_collection).await?;
            alerts_output.extend(alerts);
        }

        Ok(alerts_output)
    }
}
