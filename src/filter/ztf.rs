use apache_avro::Schema;
use flare::phot::{limmag_to_fluxerr, mag_to_flux};
use futures::stream::StreamExt;
use mongodb::bson::{doc, Document};
use rdkafka::producer::FutureProducer;
use redis::AsyncCommands;
use std::{collections::HashMap, num::NonZero};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{info, warn};

use crate::filter::{
    create_producer, get_filter_object, load_alert_schema, parse_programid_candid_tuple,
    process_alerts, send_alert_to_kafka, Alert, Filter, FilterError, FilterResults, FilterWorker,
    FilterWorkerError, Origin, Photometry, Survey,
};
use crate::utils::worker::WorkerCmd;

#[derive(Debug)]
pub struct ZtfFilter {
    pub id: i32,
    pub pipeline: Vec<Document>,
    pub permissions: Vec<i32>,
}

#[async_trait::async_trait]
impl Filter for ZtfFilter {
    async fn build(
        filter_id: i32,
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
                    // during filter::run proper candis are inserted here
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

        let filter = ZtfFilter {
            id: filter_id,
            pipeline: pipeline,
            permissions: permissions,
        };

        Ok(filter)
    }
}

pub struct ZtfFilterWorker {
    id: String,
    receiver: mpsc::Receiver<WorkerCmd>,
    filter_collection: mongodb::Collection<mongodb::bson::Document>,
    alert_collection: mongodb::Collection<mongodb::bson::Document>,
    producer: FutureProducer, // Kafka producer for sending messages
    schema: Schema,           // Avro schema for serialization
}

#[async_trait::async_trait]
impl FilterWorker for ZtfFilterWorker {
    async fn new(
        id: String,
        receiver: mpsc::Receiver<WorkerCmd>,
        config_path: &str,
    ) -> Result<Self, FilterWorkerError> {
        let config_file = crate::conf::load_config(&config_path)?;
        let db: mongodb::Database = crate::conf::build_db(&config_file).await?;
        let alert_collection = db.collection("ZTF_alerts");
        let filter_collection = db.collection("filters");

        let producer = create_producer().await?;
        let schema = load_alert_schema()?;

        Ok(ZtfFilterWorker {
            id,
            receiver,
            filter_collection,
            alert_collection,
            producer,
            schema,
        })
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
                survey: Survey::ZTF,
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
                survey: Survey::ZTF,
                ra: None,
                dec: None,
            });
        }

        // we ignore the forced photometry for now, but will add it later

        let alert = Alert {
            candid,
            object_id,
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

    async fn run(&mut self) -> Result<(), FilterWorkerError> {
        // query the DB to find the ids of all the filters for ZTF that are active
        let filter_ids: Vec<i32> = self
            .filter_collection
            .distinct("filter_id", doc! {"active": true, "catalog": "ZTF_alerts"})
            .await
            .map_err(FilterWorkerError::GetFiltersError)?
            .into_iter()
            .map(|x| x.as_i32().ok_or(FilterError::InvalidFilterId))
            .filter_map(Result::ok)
            .collect();

        let mut filters: Vec<ZtfFilter> = Vec::new();
        for filter_id in filter_ids {
            filters.push(ZtfFilter::build(filter_id, &self.filter_collection).await?);
        }

        if filters.is_empty() {
            warn!("no filters found for ZTF");
            return Ok(());
        }

        info!("filterworker {} found {} filters", self.id, filters.len());

        // get the highest permissions accross all filters
        let mut max_permission = 0;
        for filter in &filters {
            for permission in &filter.permissions {
                if *permission > max_permission {
                    max_permission = *permission;
                }
            }
        }
        // create a list of queues to read from
        let input_queue = "ZTF_alerts_filter_queue".to_string();
        let output_topic = "ZTF_alerts_results".to_string();

        // create a hashmap of filters per programid (permissions)
        // basically we'll have the 4 programid (from 0 to 3) as keys
        // and the idx of the filters that have that programid in their permissions as values
        let mut filters_by_permission: HashMap<i32, Vec<usize>> = HashMap::new();
        for (i, filter) in filters.iter().enumerate() {
            for permission in &filter.permissions {
                let entry = filters_by_permission
                    .entry(*permission)
                    .or_insert(Vec::new());
                entry.push(i);
            }
        }

        // in a never ending loop, loop over the queues
        let client_redis = redis::Client::open("redis://localhost:6379".to_string())
            .map_err(FilterWorkerError::ConnectRedisError)?;
        let mut con = client_redis
            .get_multiplexed_async_connection()
            .await
            .map_err(FilterWorkerError::ConnectRedisError)?;

        let command_interval: i64 = 500;
        let mut command_check_countdown = command_interval;

        loop {
            if command_check_countdown == 0 {
                match self.receiver.try_recv() {
                    Ok(WorkerCmd::TERM) => {
                        info!("filterworker {} received termination command", self.id);
                        break;
                    }
                    Err(TryRecvError::Disconnected) => {
                        warn!(
                            "filter worker {} receiver disconnected, terminating",
                            self.id
                        );
                        break;
                    }
                    Err(TryRecvError::Empty) => {
                        command_check_countdown = command_interval;
                    }
                }
            }
            // if the queue is empty, wait for a bit and continue the loop
            let queue_len: i64 = con
                .llen(&input_queue)
                .await
                .map_err(FilterWorkerError::ConnectRedisError)?;
            if queue_len == 0 {
                tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                continue;
            }

            // get candids from redis
            let alerts: Vec<String> = con
                .rpop::<&str, Vec<String>>(&input_queue, NonZero::new(1000))
                .await
                .map_err(FilterWorkerError::PopCandidError)?;

            let nb_alerts = alerts.len();
            if nb_alerts == 0 {
                // sleep for a bit if no alerts were found
                tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                continue;
            }

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

            // for each programid, get the filters that have that programid in their permissions
            // and run the filters
            for (programid, candids) in alerts_by_programid {
                let mut results_map: HashMap<i64, Vec<FilterResults>> = HashMap::new();

                let filter_indices = filters_by_permission
                    .get(&programid)
                    .ok_or(FilterWorkerError::GetFilterByQueueError)?;

                for i in filter_indices {
                    let filter = &filters[*i];
                    let out_documents = process_alerts(
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
                        let annotations = serde_json::to_string(
                            doc.get_document("annotations").unwrap_or(&doc! {}),
                        )
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

                    send_alert_to_kafka(
                        &alert,
                        &self.schema,
                        &self.producer,
                        &output_topic,
                        &self.id,
                    )
                    .await?;
                    info!(
                        "Sent alert with candid {} to Kafka topic {}",
                        candid, output_topic
                    );
                }
            }

            command_check_countdown -= nb_alerts as i64;
        }

        Ok(())
    }
}
