use flare::phot::{limmag_to_fluxerr, mag_to_flux};
use futures::stream::StreamExt;
use mongodb::bson::{doc, Document};
use std::collections::HashMap;
use tracing::{info, instrument, warn};

use crate::filter::{
    get_filter_object, parse_programid_candid_tuple, run_filter, Alert, Classification, Filter,
    FilterError, FilterResults, FilterWorker, FilterWorkerError, Origin, Photometry,
};
use crate::utils::enums::Survey;

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
                "$lookup": doc! {
                    "from": format!("ZTF_alerts_aux"),
                    "localField": "objectId",
                    "foreignField": "_id",
                    "as": "aux"
                }
            },
            doc! {
                "$project": doc! {
                    "objectId": 1,
                    "candidate": 1,
                    "classifications": 1,
                    "coordinates": 1,
                    "cross_matches": doc! {
                        "$arrayElemAt": [
                            "$aux.cross_matches",
                            0
                        ]
                    },
                    "aliases": doc! {
                        "$arrayElemAt": [
                            "$aux.aliases",
                            0
                        ]
                    },
                    "prv_candidates": doc! {
                        "$filter": doc! {
                            "input": doc! {
                                "$arrayElemAt": [
                                    "$aux.prv_candidates",
                                    0
                                ]
                            },
                            "as": "x",
                            "cond": doc! {
                                "$and": [
                                    {
                                        "$in": [
                                            "$$x.programid",
                                            &permissions
                                        ]
                                    },
                                    { // maximum 1 year of past data
                                        "$lt": [
                                            {
                                                "$subtract": [
                                                    "$candidate.jd",
                                                    "$$x.jd"
                                                ]
                                            },
                                            365
                                        ]
                                    },
                                    { // only datapoints up to (and including) current alert
                                        "$lte": [
                                            "$$x.jd",
                                            "$candidate.jd"
                                        ]
                                    }

                                ]
                            }
                        }
                    },
                }
            },
        ];

        // get filter pipeline as str and convert to Vec<Bson>
        let filter_pipeline = filter_obj
            .get("pipeline")
            .ok_or(FilterError::FilterNotFound)?
            .as_str()
            .ok_or(FilterError::FilterNotFound)?;

        let filter_pipeline = serde_json::from_str::<serde_json::Value>(filter_pipeline)?;
        let filter_pipeline = filter_pipeline
            .as_array()
            .ok_or(FilterError::InvalidFilterPipeline)?;

        // append stages to prefix
        for stage in filter_pipeline {
            let x = mongodb::bson::to_document(stage)?;
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
    async fn new(config_path: &str) -> Result<Self, FilterWorkerError> {
        let config_file = crate::conf::load_config(&config_path)?;
        let db: mongodb::Database = crate::conf::build_db(&config_file).await?;
        let alert_collection = db.collection("ZTF_alerts");
        let filter_collection = db.collection("filters");

        let input_queue = "ZTF_alerts_filter_queue".to_string();
        let output_topic = "ZTF_alerts_results".to_string();

        // Get a list of active filter IDs for ZTF alerts
        let filter_ids = filter_collection
            .distinct("id", doc! {"active": true, "catalog": "ZTF_alerts"})
            .await?
            .into_iter()
            .map(|x| {
                x.as_str()
                    .map(|s| s.to_string())
                    .ok_or(FilterError::InvalidFilterId)
            })
            .collect::<Result<Vec<String>, FilterError>>()?;

        let mut filters: Vec<ZtfFilter> = Vec::new();
        for filter_id in filter_ids {
            filters.push(ZtfFilter::build(&filter_id, &filter_collection).await?);
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
                entry.push(filter.id.to_string());
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

    #[instrument(skip(self, filter_results), err)]
    async fn build_alert(
        &self,
        candid: i64,
        filter_results: Vec<FilterResults>,
    ) -> Result<Alert, FilterWorkerError> {
        let pipeline = vec![
            doc! {
                "$match": {
                    "_id": candid
                }
            },
            doc! {
                "$project": {
                    "objectId": 1,
                    "jd": "$candidate.jd",
                    "ra": "$candidate.ra",
                    "dec": "$candidate.dec",
                    "cutoutScience": 1,
                    "cutoutTemplate": 1,
                    "cutoutDifference": 1,
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
                    "prv_candidates": {
                        "$arrayElemAt": [
                            "$aux.prv_candidates",
                            0
                        ]
                    },
                    "prv_nondetections": {
                        "$arrayElemAt": [
                            "$aux.prv_nondetections",
                            0
                        ]
                    },
                    "cutoutScience": {
                        "$arrayElemAt": [
                            "$cutouts.cutoutScience",
                            0
                        ]
                    },
                    "cutoutTemplate": {
                        "$arrayElemAt": [
                            "$cutouts.cutoutTemplate",
                            0
                        ]
                    },
                    "cutoutDifference": {
                        "$arrayElemAt": [
                            "$cutouts.cutoutDifference",
                            0
                        ]
                    },
                    "classifications": 1,
                }
            },
        ];

        // Execute the aggregation pipeline
        let mut cursor = self.alert_collection.aggregate(pipeline).await?;

        let alert_document = cursor
            .next()
            .await
            .ok_or(FilterWorkerError::AlertNotFound)??;

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

        // we ignore the forced photometry for now, but will add it later

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

        let alert = Alert {
            candid,
            object_id,
            jd,
            ra,
            dec,
            filters: filter_results, // assuming you have filter results to attach
            classifications,
            photometry,
            cutout_science,
            cutout_template,
            cutout_difference,
        };

        Ok(alert)
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

                // if the array is empty, continue
                if out_documents.is_empty() {
                    continue;
                } else {
                    // if we have output documents, we need to process them
                    // and create filter results for each document (which contain annotations)
                    info!(
                        "{} alerts passed ztf filter {} with programid {}",
                        out_documents.len(),
                        filter.id,
                        programid,
                    );
                }

                let now_ts = chrono::Utc::now().timestamp_millis() as f64;

                for doc in out_documents {
                    let candid = doc.get_i64("_id")?;
                    // might want to have the annotations as an optional field instead of empty
                    let annotations =
                        serde_json::to_string(doc.get_document("annotations").unwrap_or(&doc! {}))?;
                    let filter_result = FilterResults {
                        filter_id: filter.id.to_string(),
                        passed_at: now_ts,
                        annotations,
                    };
                    let entry = results_map.entry(candid).or_insert(Vec::new());
                    entry.push(filter_result);
                }
            }

            // now we've basically combined the filter results for each candid
            for (candid, filter_results) in &results_map {
                let alert = self.build_alert(*candid, filter_results.clone()).await?;
                alerts_output.push(alert);
            }
        }

        Ok(alerts_output)
    }
}
