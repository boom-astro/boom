use flare::phot::limmag_to_fluxerr;
use futures::stream::StreamExt;
use mongodb::bson::{doc, Document};
use std::collections::HashMap;
use tracing::{info, instrument, warn};

use crate::filter::base::{get_active_filter_pipeline, LoadedFilter};
use crate::filter::{
    get_filter, parse_programid_candid_tuple, run_filter, uses_field_in_filter,
    validate_filter_pipeline, Alert, Classification, Filter, FilterError, FilterResults,
    FilterWorker, FilterWorkerError, Origin, Photometry,
};
use crate::utils::db::{fetch_timeseries_op, get_array_element};
use crate::utils::{enums::Survey, o11y::logging::as_error};

const ZTF_ZP: f64 = 23.9;

#[instrument(skip_all, err)]
pub async fn build_ztf_alerts(
    alerts_with_filter_results: &HashMap<i64, Vec<FilterResults>>,
    alert_collection: &mongodb::Collection<mongodb::bson::Document>,
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

        // let's create the array of photometry (non forced phot only for now)
        let mut photometry = Vec::new();
        for doc in alert_document.get_array("prv_candidates")?.iter() {
            let doc = match doc.as_document() {
                Some(doc) => doc,
                None => continue, // skip if not a document
            };
            let jd = doc.get_f64("jd")?;
            let flux = doc.get_f64("psfFlux").ok(); // optional, might not be present
            let flux_err = doc.get_f64("psfFluxErr")?;
            let band = doc.get_str("band")?.to_string();
            let programid = doc.get_i32("programid")?;
            let ra = doc.get_f64("ra").ok(); // optional, might not be present
            let dec = doc.get_f64("dec").ok(); // optional, might not be present

            photometry.push(Photometry {
                jd,
                flux,
                flux_err,
                band: format!("ztf{}", band),
                zero_point: ZTF_ZP,
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
                zero_point: ZTF_ZP,
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
            let flux = match doc.get_f64("forcediffimflux") {
                Ok(flux) => Some(flux),
                Err(_) => None,
            };
            let flux_err = match doc.get_f64("forcediffimfluxunc") {
                Ok(flux_err) => flux_err,
                Err(_) => {
                    let diffmaglim = doc.get_f64("diffmaglim")?;
                    limmag_to_fluxerr(diffmaglim, magzpsci, 5.0)
                }
            };
            let band = doc.get_str("band")?.to_string();
            let programid = doc.get_i32("programid")?;

            photometry.push(Photometry {
                jd,
                flux,
                flux_err,
                band: format!("ztf{}", band),
                zero_point: magzpsci,
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

pub async fn build_ztf_filter_pipeline(
    filter_pipeline: &Vec<serde_json::Value>,
    permissions: &Vec<i32>,
) -> Result<Vec<Document>, FilterError> {
    // validate filter
    validate_filter_pipeline(&filter_pipeline)?;

    let use_prv_candidates_index = uses_field_in_filter(filter_pipeline, "prv_candidates");
    let use_prv_nondetections_index = uses_field_in_filter(filter_pipeline, "prv_nondetections");
    let use_fp_hists_index = uses_field_in_filter(filter_pipeline, "fp_hists");
    let use_cross_matches_index = uses_field_in_filter(filter_pipeline, "cross_matches");
    let use_aliases_index = uses_field_in_filter(filter_pipeline, "aliases");

    let mut aux_add_fields = doc! {};

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
                        &permissions
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
                        &permissions
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
                        &permissions
                    ]
                }]),
            ),
        );
    }
    if use_cross_matches_index.is_some() {
        aux_add_fields.insert(
            "cross_matches".to_string(),
            get_array_element("aux.cross_matches"),
        );
    }
    if use_aliases_index.is_some() {
        aux_add_fields.insert("aliases".to_string(), get_array_element("aux.aliases"));
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
                    "from": format!("ZTF_alerts_aux"),
                    "localField": "objectId",
                    "foreignField": "_id",
                    "as": "aux"
                }
            });
            pipeline.push(doc! {
                "$addFields": &aux_add_fields
            });
            pipeline.push(doc! {
                "$unset": "aux"
            });
            insert_aux_pipeline = false; // only insert once
        }

        // push the current stage
        pipeline.push(x);
    }

    Ok(pipeline)
}

pub async fn build_ztf_loaded_filter(
    filter_id: &str,
    filter_collection: &mongodb::Collection<Filter>,
) -> Result<LoadedFilter, FilterError> {
    let filter = get_filter(filter_id, &Survey::Ztf, filter_collection).await?;

    if filter.permissions.is_empty() {
        return Err(FilterError::InvalidFilterPermissions);
    }

    let pipeline = get_active_filter_pipeline(&filter)?;

    let pipeline = build_ztf_filter_pipeline(&pipeline, &filter.permissions).await?;

    let loaded = LoadedFilter {
        id: filter.id.clone(),
        pipeline: pipeline,
        permissions: filter.permissions,
    };
    Ok(loaded)
}

pub struct ZtfFilterWorker {
    alert_collection: mongodb::Collection<mongodb::bson::Document>,
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
        let config_file = crate::conf::load_raw_config(&config_path)?;
        let db: mongodb::Database = crate::conf::build_db(&config_file).await?;
        let alert_collection = db.collection("ZTF_alerts");
        let filter_collection = db.collection("filters");

        let input_queue = "ZTF_alerts_filter_queue".to_string();
        let output_topic = "ZTF_alerts_results".to_string();

        let all_filter_ids: Vec<String> = filter_collection
            .distinct("_id", doc! {"active": true, "survey": "ZTF"})
            .await?
            .into_iter()
            .map(|x| {
                x.as_str()
                    .map(|s| s.to_string())
                    .ok_or(FilterError::InvalidFilterId)
            })
            .collect::<Result<Vec<String>, FilterError>>()?;

        let filter_ids = match &filter_ids {
            Some(ids) => {
                // verify that they all exist in all_filter_ids
                for id in ids {
                    if !all_filter_ids.contains(id) {
                        return Err(FilterWorkerError::FilterNotFound);
                    }
                }
                ids.clone()
            }
            None => all_filter_ids.clone(),
        };

        let mut filters: Vec<LoadedFilter> = Vec::new();
        for filter_id in filter_ids {
            filters.push(build_ztf_loaded_filter(&filter_id, &filter_collection).await?);
        }

        // Create a hashmap of filters per programid (permissions)
        // basically we'll have the 4 programid (from 0 to 3) as keys
        // and the ids of the filters that have that programid in their
        // permissions as values
        let mut filters_by_permission: HashMap<i32, Vec<String>> = HashMap::new();
        for filter in &filters {
            for permission in &filter.permissions {
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
                    candids.clone(),
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
