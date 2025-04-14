use futures::stream::StreamExt;
use mongodb::bson::{doc, Document};
use std::collections::HashMap;
use tracing::info;

use crate::filter::{
    get_filter_object, run_filter, Alert, Filter, FilterError, FilterResults, FilterWorker,
    FilterWorkerError, Origin, Photometry, Survey,
};

pub struct LsstFilter {
    id: i32,
    pipeline: Vec<Document>,
}

#[async_trait::async_trait]
impl Filter for LsstFilter {
    async fn build(
        filter_id: i32,
        filter_collection: &mongodb::Collection<mongodb::bson::Document>,
    ) -> Result<Self, FilterError> {
        // get filter object
        let filter_obj = get_filter_object(filter_id, "LSST_alerts", filter_collection).await?;

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
                    "from": format!("LSST_alerts_aux"),
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
                                    { // maximum 1 year of past data
                                        "$lt": [
                                            {
                                                "$subtract": [
                                                    "$candidate.mjd",
                                                    "$$x.mjd"
                                                ]
                                            },
                                            365
                                        ]
                                    },
                                    { // only datapoints up to (and including) current alert
                                        "$lte": [
                                            "$$x.mjd",
                                            "$candidate.mjd"
                                        ]
                                    }
                                ]
                            }
                        }
                    },
                }
            },
        ];

        let filter_pipeline = filter_obj
            .get("pipeline")
            .ok_or(FilterError::FilterNotFound)?
            .as_str()
            .ok_or(FilterError::FilterNotFound)?;

        let filter_pipeline = serde_json::from_str::<serde_json::Value>(filter_pipeline)
            .map_err(FilterError::DeserializePipelineError)?;
        let filter_pipeline = filter_pipeline
            .as_array()
            .ok_or(FilterError::InvalidFilterPipeline)?;
        // append stages to prefix
        for stage in filter_pipeline {
            let x = mongodb::bson::to_document(stage)
                .map_err(FilterError::InvalidFilterPipelineStage)?;
            pipeline.push(x);
        }

        let filter = LsstFilter {
            id: filter_id,
            pipeline: pipeline,
        };

        Ok(filter)
    }
}

pub struct LsstFilterWorker {
    alert_collection: mongodb::Collection<mongodb::bson::Document>,
    input_queue: String,
    output_topic: String,
    filters: Vec<LsstFilter>,
}

#[async_trait::async_trait]
impl FilterWorker for LsstFilterWorker {
    async fn new(config_path: &str) -> Result<Self, FilterWorkerError> {
        let config_file = crate::conf::load_config(&config_path)?;
        let db: mongodb::Database = crate::conf::build_db(&config_file).await?;
        let alert_collection = db.collection("LSST_alerts");
        let filter_collection = db.collection("filters");

        let input_queue = "LSST_alerts_filter_queue".to_string();
        let output_topic = "LSST_alerts_results".to_string();

        let filter_ids: Vec<i32> = filter_collection
            .distinct("filter_id", doc! {"active": true, "catalog": "LSST_alerts"})
            .await
            .map_err(FilterWorkerError::GetFiltersError)?
            .into_iter()
            .map(|x| x.as_i32().ok_or(FilterError::InvalidFilterId))
            .filter_map(Result::ok)
            .collect();

        let mut filters: Vec<LsstFilter> = Vec::new();
        for filter_id in filter_ids {
            filters.push(LsstFilter::build(filter_id, &filter_collection).await?);
        }

        Ok(LsstFilterWorker {
            alert_collection,
            input_queue,
            output_topic,
            filters,
        })
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
                    "cutoutDifference": 1
                }
            },
            doc! {
                "$lookup": {
                    "from": "LSST_alerts_aux",
                    "localField": "objectId",
                    "foreignField": "_id",
                    "as": "aux"
                }
            },
            doc! {
                "$lookup": {
                    "from": "LSST_alerts_cutouts",
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
                    }
                }
            },
        ];

        // Execute the aggregation pipeline
        let mut cursor = self
            .alert_collection
            .aggregate(pipeline)
            .await
            .map_err(FilterWorkerError::GetAlertByCandidError)?;

        let alert_document = if let Some(result) = cursor.next().await {
            result.map_err(FilterWorkerError::GetAlertByCandidError)?
        } else {
            return Err(FilterWorkerError::AlertNotFound);
        };

        let object_id = alert_document.get_i64("objectId")?;
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
            let flux = doc.get_f64("psfFlux")?;
            let flux_err = doc.get_f64("psfFluxErr")?;
            let band = doc.get_str("band")?.to_string();
            let ra = doc.get_f64("ra").ok(); // optional, might not be present
            let dec = doc.get_f64("dec").ok(); // optional, might not be present

            photometry.push(Photometry {
                jd,
                flux: Some(flux),
                flux_err,
                band: format!("lsst{}", band),
                zero_point: 8.9,
                origin: Origin::Alert,
                programid: 1, // only one public stream for LSST
                survey: Survey::LSST,
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
            let flux_err = doc.get_f64("noise")?;
            let band = doc.get_str("band")?.to_string();

            photometry.push(Photometry {
                jd,
                flux: None, // for non-detections, flux is None
                flux_err,
                band: format!("lsst{}", band),
                zero_point: 8.9,
                origin: Origin::Alert,
                programid: 1, // only one public stream for LSST
                survey: Survey::LSST,
                ra: None,
                dec: None,
            });
        }

        // we ignore the forced photometry for now, but will add it later

        let alert = Alert {
            candid,
            object_id: format!("{}", object_id),
            jd,
            ra,
            dec,
            filters: filter_results, // assuming you have filter results to attach
            photometry,
            cutout_science,
            cutout_template,
            cutout_difference,
        };

        Ok(alert)
    }

    async fn process_alerts(&mut self, alerts: &[String]) -> Result<Vec<Alert>, FilterWorkerError> {
        let mut alerts_output = Vec::new();

        // unlike ZTF where we get a tuple of (programid, candid) from redis
        // LSST has only one public stream, meaning there are no programids
        // so we simply convert the array of String to Vec<i64>
        let candids: Vec<i64> = alerts.iter().map(|alert| alert.parse().unwrap()).collect();

        // run the filters
        let mut results_map: HashMap<i64, Vec<FilterResults>> = HashMap::new();
        for filter in &self.filters {
            let out_documents = run_filter(
                candids.clone(),
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
                    "{} alerts passed lsst filter {}",
                    out_documents.len(),
                    filter.id,
                );
            }

            let now_ts = chrono::Utc::now().timestamp_millis() as f64;

            for doc in out_documents {
                let candid = doc.get_i64("_id")?;
                // might want to have the annotations as an optional field instead of empty
                let annotations =
                    serde_json::to_string(doc.get_document("annotations").unwrap_or(&doc! {}))
                        .map_err(FilterWorkerError::SerializeFilterResultError)?;
                let filter_result = FilterResults {
                    filter_id: filter.id,
                    passed_at: now_ts,
                    annotations,
                };
                let entry = results_map.entry(candid).or_insert(Vec::new());
                entry.push(filter_result);
            }
        }

        // now we've basically combined the filter results for each candid
        // we build the alert output and send it to Kafka
        for (candid, filter_results) in &results_map {
            let alert = self.build_alert(*candid, filter_results.clone()).await?;

            alerts_output.push(alert);
        }

        Ok(alerts_output)
    }
}
