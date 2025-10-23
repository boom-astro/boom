use flare::phot::{limmag_to_fluxerr, mag_to_flux};
use futures::stream::StreamExt;
use mongodb::bson::{doc, Document};
use std::collections::HashMap;
use tracing::{info, instrument, warn};

use crate::filter::lsst::build_lsst_alert;
use crate::filter::{
    get_filter_object, parse_programid_candid_tuple, run_filter, uses_field_in_filter,
    validate_filter_pipeline, Alert, Classification, Filter, FilterError, FilterResults,
    FilterWorker, FilterWorkerError, Origin, Photometry,
};
use crate::utils::db::{fetch_timeseries_op, get_array_element};
use crate::utils::{enums::Survey, o11y::logging::as_error};

#[instrument(skip_all, err)]
pub async fn get_ztf_alerts(
    alerts_with_filter_results: &HashMap<i64, Vec<FilterResults>>,
    alert_collection: &mongodb::Collection<mongodb::bson::Document>,
    alert_cutout_collection: &mongodb::Collection<mongodb::bson::Document>,
    lsst_alert_collection: &mongodb::Collection<mongodb::bson::Document>,
    lsst_alert_cutout_collection: &mongodb::Collection<mongodb::bson::Document>,
    include_survey_matches: bool,
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
                "sgscore1": "$candidate.sgscore1",
                "sgscore2": "$candidate.sgscore2",
                "sgscore3": "$candidate.sgscore3",
                "distpsnr1": "$candidate.distpsnr1",
                "distpsnr2": "$candidate.distpsnr2",
                "distpsnr3": "$candidate.distpsnr3",
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
            "$project": {
                "objectId": 1,
                "jd": 1,
                "ra": 1,
                "dec": 1,
                "prv_candidates": get_array_element("aux.prv_candidates"),
                "prv_nondetections": get_array_element("aux.prv_nondetections"),
                "fp_hists": get_array_element("aux.fp_hists"),
                // "cutoutScience": get_array_element("cutouts.cutoutScience"),
                // "cutoutTemplate": get_array_element("cutouts.cutoutTemplate"),
                // "cutoutDifference": get_array_element("cutouts.cutoutDifference"),
                "aliases": get_array_element("aux.aliases"),
                "classifications": 1,
            }
        },
    ];

    // Execute the aggregation pipeline
    let mut cursor = alert_collection.aggregate(pipeline).await?;

    let mut alerts_output = Vec::new();
    let mut candid_to_idx = std::collections::HashMap::new();
    let mut lsst_aliases_to_idx: HashMap<String, Vec<usize>> = HashMap::new();
    while let Some(alert_document) = cursor.next().await {
        info!("Processing ZTF alert document");
        let alert_document = alert_document?;
        let candid = alert_document.get_i64("_id")?;
        let object_id = alert_document.get_str("objectId")?.to_string();
        let jd = alert_document.get_f64("jd")?;
        let ra = alert_document.get_f64("ra")?;
        let dec = alert_document.get_f64("dec")?;

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
            let flux_err = match doc.get_f64("forcediffimfluxunc") {
                Ok(flux_err) => flux_err,
                Err(_) => {
                    let diffmaglim = doc.get_f64("diffmaglim")?;
                    limmag_to_fluxerr(diffmaglim, 23.9, 5.0)
                }
            };
            let band = doc.get_str("band")?.to_string();
            let programid = doc.get_i32("programid")?;
            let zero_point = 23.9;

            photometry.push(Photometry {
                jd,
                flux,
                flux_err,
                band: format!("ztf{}", band),
                zero_point,
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
                        separation: None,
                    });
                }
            }
        }

        // add the rb and drb to the classifications if present
        if let Some(rb) = alert_document.get_f64("rb").ok() {
            classifications.push(Classification {
                classifier: "rb".to_string(),
                score: rb,
                separation: None,
            });
        }
        if let Some(drb) = alert_document.get_f64("drb").ok() {
            classifications.push(Classification {
                classifier: "drb".to_string(),
                score: drb,
                separation: None,
            });
        }

        // if we have both sgscore1 and distpsnr1, we add a classification for the nearest PS1 star
        if let (Some(sgscore1), Some(distpsnr1)) = (
            alert_document.get_f64("sgscore1").ok(),
            alert_document.get_f64("distpsnr1").ok(),
        ) {
            classifications.push(Classification {
                classifier: "ps1_sg".to_string(),
                score: sgscore1,
                separation: Some(distpsnr1), // in arcseconds
            });
        }
        // if we have both sgscore2 and distpsnr2, we add a classification for the 2d nearest PS1 star
        if let (Some(sgscore2), Some(distpsnr2)) = (
            alert_document.get_f64("sgscore2").ok(),
            alert_document.get_f64("distpsnr2").ok(),
        ) {
            classifications.push(Classification {
                classifier: "ps1_sg".to_string(),
                score: sgscore2,
                separation: Some(distpsnr2), // in arcseconds
            });
        }
        // if we have both sgscore3 and distpsnr3, we add a classification for the 3rd nearest PS1 star
        if let (Some(sgscore3), Some(distpsnr3)) = (
            alert_document.get_f64("sgscore3").ok(),
            alert_document.get_f64("distpsnr3").ok(),
        ) {
            classifications.push(Classification {
                classifier: "ps1_sg".to_string(),
                score: sgscore3,
                separation: Some(distpsnr3), // in arcseconds
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
            cutout_science: Vec::<u8>::new(),    //cutout_science,
            cutout_template: Vec::<u8>::new(),   //cutout_template,
            cutout_difference: Vec::<u8>::new(), //cutout_difference,
            survey: Survey::Ztf,
            survey_matches: None,
        };

        info!(
            "Built ZTF alert candid={} object_id={}",
            candid, &alert.object_id
        );

        alerts_output.push(alert);
        candid_to_idx.insert(candid, alerts_output.len() - 1);

        // if we have aliases for LSST, we store them for later
        // aliases is a dict with survey names as keys and array of strings as values
        if include_survey_matches {
            if let Some(aliases_doc) = alert_document.get_document("aliases").ok() {
                // we only care about the first alias for now
                if let Some(lsst_aliases) = aliases_doc.get_array("LSST").ok() {
                    println!(
                        "ZTF alert candid={} has LSST aliases: {:?}",
                        candid, lsst_aliases
                    );
                    for alias in lsst_aliases {
                        if let Some(alias_str) = alias.as_str() {
                            lsst_aliases_to_idx
                                .entry(alias_str.to_string())
                                .or_insert(Vec::new())
                                .push(alerts_output.len() - 1);
                            break;
                        }
                    }
                }
            }
        }
    }

    if candids.len() != alerts_output.len() {
        return Err(FilterWorkerError::AlertNotFound);
    }

    let mut cutout_cursor = alert_cutout_collection
        .find(doc! {
            "_id": {"$in": candids}
        })
        .await?;
    while let Some(result) = cutout_cursor.next().await {
        match result {
            Ok(cutout_doc) => {
                let candid = cutout_doc.get_i64("_id")?;
                if let Some(idx) = candid_to_idx.get(&candid) {
                    alerts_output[*idx].cutout_science =
                        cutout_doc.get_binary_generic("cutoutScience")?.to_vec();
                    alerts_output[*idx].cutout_template =
                        cutout_doc.get_binary_generic("cutoutTemplate")?.to_vec();
                    alerts_output[*idx].cutout_difference =
                        cutout_doc.get_binary_generic("cutoutDifference")?.to_vec();
                }
            }
            Err(e) => {
                warn!("Error fetching cutout document: {}", e);
                continue;
            }
        }
    }

    if include_survey_matches {
        // print the lsst_aliases_to_idx map
        println!(
            "ZTF alerts LSST aliases to indices map: {:?}",
            lsst_aliases_to_idx
        );
        let lsst_object_ids: Vec<String> = lsst_aliases_to_idx.keys().cloned().collect();
        // we care about getting the most recent alert for each object id
        println!(
            "ZTF alerts querying LSST alerts with objectIDs: {:?}",
            lsst_object_ids
        );
        let pipeline = vec![
            doc! {
                "$match": {
                    "objectId": { "$in": &lsst_object_ids }
                }
            },
            doc! {
                "$sort": {
                    "jd": -1
                }
            },
            doc! {
                "$group": {
                    "_id": "$objectId",
                    "candid": { "$first": "$_id" },
                    "jd": { "$first": "$candidate.jd" },
                    "ra": { "$first": "$candidate.ra" },
                    "dec": { "$first": "$candidate.dec" },
                    "classifications": { "$first": "$classifications" },
                }
            },
            doc! {
                "$lookup": {
                    "from": "LSST_alerts_aux",
                    "localField": "_id",
                    "foreignField": "_id",
                    "as": "aux"
                }
            },
            doc! {
                "$project": {
                    "_id": "$candid",
                    "objectId": "$_id",
                    "jd": 1,
                    "ra": 1,
                    "dec": 1,
                    "classifications": 1,
                    "prv_candidates": get_array_element("aux.prv_candidates"),
                    "fp_hists": get_array_element("aux.fp_hists"),
                }
            },
        ];

        let mut cursor = lsst_alert_collection.aggregate(pipeline).await?;

        let mut lsst_alerts = Vec::new();
        let mut lsst_candid_to_idx = HashMap::new();
        let mut lsst_objectid_to_idx = HashMap::new();
        while let Some(alert_document) = cursor.next().await {
            info!("Processing LSST alert document for ZTF aliases");
            let alert_document = alert_document?;
            let alert =
                build_lsst_alert(alert_document, &HashMap::<i64, Vec<FilterResults>>::new())?;
            let candid = alert.candid;
            let object_id = alert.object_id.clone();
            info!(
                "Found matching LSST alert for ZTF aliases: {:?}",
                alert.object_id
            );
            lsst_alerts.push(alert);
            let idx = lsst_alerts.len() - 1;
            lsst_candid_to_idx.insert(candid, idx);
            lsst_objectid_to_idx.insert(object_id, idx);
        }

        // grab the lsst cutouts and add them to the lsst alerts
        let mut lsst_cutout_cursor = lsst_alert_cutout_collection
            .find(doc! {
                "_id": {"$in": lsst_candid_to_idx.keys().cloned().collect::<Vec<i64>>()}
            })
            .await?;
        while let Some(result) = lsst_cutout_cursor.next().await {
            match result {
                Ok(cutout_doc) => {
                    let candid = cutout_doc.get_i64("_id")?;
                    if let Some(idx) = lsst_candid_to_idx.get(&candid) {
                        lsst_alerts[*idx].cutout_science =
                            cutout_doc.get_binary_generic("cutoutScience")?.to_vec();
                        lsst_alerts[*idx].cutout_template =
                            cutout_doc.get_binary_generic("cutoutTemplate")?.to_vec();
                        lsst_alerts[*idx].cutout_difference =
                            cutout_doc.get_binary_generic("cutoutDifference")?.to_vec();
                    }
                }
                Err(e) => {
                    warn!("Error fetching LSST cutout document: {}", e);
                    continue;
                }
            }
        }

        // now we can assign the lsst alerts to the ztf alerts
        for (lsst_object_id, idxs) in lsst_aliases_to_idx.iter() {
            if let Some(lsst_idx) = lsst_objectid_to_idx.get(lsst_object_id) {
                let lsst_alert = lsst_alerts.get(*lsst_idx).unwrap().clone();
                for &ztf_idx in idxs {
                    alerts_output[ztf_idx]
                        .survey_matches
                        .get_or_insert(Vec::new())
                        .push(lsst_alert.clone());
                }
            }
        }
    }

    Ok(alerts_output)
}

// let's make a function that takes:
// - an array of alerts
// -

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
        let use_aliases_index = uses_field_in_filter(filter_pipeline, "aliases");

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
            return Err(FilterError::InvalidFilterPipeline);
        }

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
    alert_cutout_collection: mongodb::Collection<mongodb::bson::Document>,
    input_queue: String,
    output_topic: String,
    filters: Vec<ZtfFilter>,
    filters_by_permission: HashMap<i32, Vec<String>>,
    lsst_alert_collection: mongodb::Collection<mongodb::bson::Document>,
    lsst_alert_cutout_collection: mongodb::Collection<mongodb::bson::Document>,
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
        let alert_cutout_collection = db.collection("ZTF_alerts_cutouts");
        let filter_collection = db.collection("filters");

        let lsst_alert_collection = db.collection("LSST_alerts");
        let lsst_alert_cutout_collection = db.collection("LSST_alerts_cutouts");

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
            alert_cutout_collection,
            input_queue,
            output_topic,
            filters,
            filters_by_permission,
            lsst_alert_collection,
            lsst_alert_cutout_collection,
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

            let alerts = get_ztf_alerts(
                &results_map,
                &self.alert_collection,
                &self.alert_cutout_collection,
                &self.lsst_alert_collection,
                &self.lsst_alert_cutout_collection,
                true,
            )
            .await?;
            alerts_output.extend(alerts);
        }

        Ok(alerts_output)
    }
}
