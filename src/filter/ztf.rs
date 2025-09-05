use flare::phot::{limmag_to_fluxerr, mag_to_flux};
use futures::stream::StreamExt;
use mongodb::bson::{doc, Document};
use std::collections::HashMap;
use tracing::{info, instrument, warn};

use crate::filter::{
    get_filter_object, parse_programid_candid_tuple, run_filter, uses_field_in_filter,
    validate_filter_pipeline, Alert, Classification, Filter, FilterError, FilterResults,
    FilterWorker, FilterWorkerError, Origin, Photometry,
};
use crate::utils::db::{fetch_timeseries_op, get_array_element};
use crate::utils::{enums::Survey, o11y::logging::as_error};

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
            let mag = doc.get_f64("magpsf")?;
            let mag_err = doc.get_f64("sigmapsf")?;
            let isdiffpos = doc.get_bool("isdiffpos")?;
            let band = doc.get_str("band")?.to_string();
            let programid = doc.get_i32("programid")?;
            let zero_point = 23.9;
            let ra = doc.get_f64("ra").ok(); // optional, might not be present
            let dec = doc.get_f64("dec").ok(); // optional, might not be present

            let (flux, flux_err) = mag_to_flux(mag, mag_err, zero_point);
            photometry.push(Photometry {
                jd,
                flux: match isdiffpos {
                    true => Some(flux),
                    false => Some(-1.0 * flux),
                },
                flux_err,
                band: format!("ztf{}", band),
                zero_point,
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
            let mag_limit = doc.get_f64("diffmaglim")?;
            let band = doc.get_str("band")?.to_string();
            let programid = doc.get_i32("programid")?;
            let zero_point = 23.9;

            let flux_err = limmag_to_fluxerr(mag_limit, zero_point, 5.0);

            photometry.push(Photometry {
                jd,
                flux: None, // for non-detections, flux is None
                flux_err,
                band: format!("ztf{}", band),
                zero_point,
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
            let jd = doc.get_f64("jd")?;
            let flux = match doc.get_f64("forcediffimflux") {
                Ok(flux) => Some(flux),
                Err(_) => None,
            };
            let magzpsci = doc.get_f64("magzpsci")?;
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

#[derive(Debug)]
pub struct ZtfFilter {
    pub id: String,
    pub pipeline: Vec<Document>,
    pub permissions: Vec<i32>,
}

#[async_trait::async_trait]
impl Filter for ZtfFilter {
    #[instrument(skip(filter_collection), err)]
    async fn build(
        filter_id: &str,
        filter_collection: &mongodb::Collection<mongodb::bson::Document>,
    ) -> Result<Self, FilterError> {
        // get filter object
        let filter_obj = get_filter_object(filter_id, "ZTF_alerts", filter_collection).await?;

        // get permissions
        let permissions = match filter_obj.get("permissions") {
            Some(permissions) => {
                let permissions_array = match permissions.as_array() {
                    Some(permissions_array) => permissions_array,
                    None => return Err(FilterError::InvalidFilterPermissions),
                };
                permissions_array
                    .iter()
                    .map(|x| x.as_i32().ok_or(FilterError::InvalidFilterPermissions))
                    .filter_map(Result::ok)
                    .collect::<Vec<i32>>()
            }
            None => vec![],
        };

        if permissions.is_empty() {
            return Err(FilterError::InvalidFilterPermissions);
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

        let mut aux_add_fields = doc! {};

        // get filter pipeline as str and convert to Vec<Bson>
        let filter_pipeline = filter_obj
            .get("pipeline")
            .ok_or(FilterError::FilterPipelineError)?
            .as_str()
            .ok_or(FilterError::FilterPipelineError)?;

        let filter_pipeline = serde_json::from_str::<serde_json::Value>(filter_pipeline)?;
        let filter_pipeline = filter_pipeline
            .as_array()
            .ok_or(FilterError::InvalidFilterPipeline)?;

        // validate filter
        validate_filter_pipeline(&filter_pipeline)?;

        let use_prv_candidates_index = uses_field_in_filter(filter_pipeline, "prv_candidates");
        let use_prv_nondetections_index =
            uses_field_in_filter(filter_pipeline, "prv_nondetections");
        let use_fp_hists_index = uses_field_in_filter(filter_pipeline, "fp_hists");
        let use_cross_matches_index = uses_field_in_filter(filter_pipeline, "cross_matches");
        let mut use_aliases_index = uses_field_in_filter(filter_pipeline, "aliases");

        // check if other survey's data is used, in which case we do want to bring in aliases
        let use_lsst_prv_candidates_index =
            uses_field_in_filter(filter_pipeline, "LSST.prv_candidates");
        let use_lsst_fp_hists_index = uses_field_in_filter(filter_pipeline, "LSST.fp_hists");

        if use_lsst_prv_candidates_index.is_some() {
            if use_aliases_index.is_none() {
                use_aliases_index = use_lsst_prv_candidates_index;
            } else {
                use_aliases_index = Some(
                    use_aliases_index
                        .unwrap()
                        .min(use_lsst_prv_candidates_index.unwrap()),
                );
            }
        }
        if use_lsst_fp_hists_index.is_some() {
            if use_aliases_index.is_none() {
                use_aliases_index = use_lsst_fp_hists_index;
            } else {
                use_aliases_index = Some(
                    use_aliases_index
                        .unwrap()
                        .min(use_lsst_fp_hists_index.unwrap()),
                );
            }
        }

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
        let mut insert_aux_pipeline = insert_aux_index != usize::MAX;

        // same for LSST
        let mut lsst_aux_add_fields = doc! {};
        if use_lsst_prv_candidates_index.is_some() {
            lsst_aux_add_fields.insert(
                "LSST.prv_candidates".to_string(),
                fetch_timeseries_op("lsst_aux.prv_candidates", "candidate.jd", 365, None),
            );
        }
        if use_lsst_fp_hists_index.is_some() {
            lsst_aux_add_fields.insert(
                "LSST.fp_hists".to_string(),
                fetch_timeseries_op("lsst_aux.fp_hists", "candidate.jd", 365, None),
            );
        }

        let mut lsst_insert_aux_index = usize::MAX;
        if let Some(index) = use_lsst_prv_candidates_index {
            lsst_insert_aux_index = lsst_insert_aux_index.min(index);
        }
        if let Some(index) = use_lsst_fp_hists_index {
            lsst_insert_aux_index = lsst_insert_aux_index.min(index);
        }
        let mut lsst_insert_aux_pipeline = lsst_insert_aux_index != usize::MAX;

        // now we loop over the base_pipeline and insert stages
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

            if lsst_insert_aux_pipeline {
                pipeline.push(doc! {
                    "$lookup": doc! {
                        "from": format!("LSST_alerts_aux"),
                        "localField": "aliases.LSST.0",
                        "foreignField": "_id",
                        "as": "lsst_aux"
                    }
                });
                pipeline.push(doc! {
                    "$addFields": &lsst_aux_add_fields
                });
                pipeline.push(doc! {
                    "$unset": "lsst_aux"
                });
                lsst_insert_aux_pipeline = false; // only insert once
            }

            // push the current stage
            pipeline.push(x);
        }

        let filter = ZtfFilter {
            id: filter_id.to_string(),
            pipeline: pipeline,
            permissions: permissions,
        };

        Ok(filter)
    }
}

pub struct ZtfFilterWorker {
    alert_collection: mongodb::Collection<mongodb::bson::Document>,
    input_queue: String,
    output_topic: String,
    filters: Vec<ZtfFilter>,
    filters_by_permission: HashMap<i32, Vec<String>>,
}

#[async_trait::async_trait]
impl FilterWorker for ZtfFilterWorker {
    #[instrument(err)]
    async fn new(
        config_path: &str,
        filter_ids: Option<Vec<String>>,
    ) -> Result<Self, FilterWorkerError> {
        let config_file = crate::conf::load_config(&config_path)?;
        let db: mongodb::Database = crate::conf::build_db(&config_file).await?;
        let alert_collection = db.collection("ZTF_alerts");
        let filter_collection = db.collection("filters");

        let input_queue = "ZTF_alerts_filter_queue".to_string();
        let output_topic = "ZTF_alerts_results".to_string();

        let all_filter_ids: Vec<String> = filter_collection
            .distinct("_id", doc! {"active": true, "catalog": "ZTF_alerts"})
            .await?
            .into_iter()
            .map(|x| {
                x.as_str()
                    .map(|s| s.to_string())
                    .ok_or(FilterError::InvalidFilterId)
            })
            .collect::<Result<Vec<String>, FilterError>>()?;

        let mut filters: Vec<ZtfFilter> = Vec::new();
        if let Some(filter_ids) = filter_ids {
            // if filter_ids is provided, we only build those filters
            for filter_id in filter_ids {
                if !all_filter_ids.contains(&filter_id) {
                    return Err(FilterWorkerError::FilterNotFound);
                }
                filters.push(ZtfFilter::build(&filter_id, &filter_collection).await?);
            }
        } else {
            for filter_id in all_filter_ids {
                filters.push(ZtfFilter::build(&filter_id, &filter_collection).await?);
            }
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
