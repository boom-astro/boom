use mongodb::bson::{doc, Document};
use redis::AsyncCommands;
use std::{collections::HashMap, num::NonZero};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TryRecvError;
use tracing::{info, warn};

use crate::filter::{
    get_filter_object, process_alerts, Filter, FilterError, FilterWorker, FilterWorkerError,
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
        Ok(ZtfFilterWorker {
            id,
            receiver,
            filter_collection,
            alert_collection,
        })
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

        // create a list of output queues, one for each filter
        let mut filter_results_queues: HashMap<i32, String> = HashMap::new();
        for filter in &filters {
            let queue_name = format!("ZTF_alerts_filter_{}_results_queue", filter.id);
            filter_results_queues.insert(filter.id, queue_name);
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
            // get candids from redis
            let alerts: Vec<(i64, i32)> = con
                .rpop::<&str, Vec<(i64, i32)>>(&input_queue, NonZero::new(1000))
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
            for (candid, programid) in alerts {
                let entry = alerts_by_programid.entry(programid).or_insert(Vec::new());
                entry.push(candid);
            }

            // for each programid, get the filters that have that programid in their permissions
            // and run the filters
            for (programid, candids) in alerts_by_programid {
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
                    }
                    // convert the documents to json
                    let out_documents: Vec<String> = out_documents
                        .iter()
                        .map(|x| {
                            serde_json::to_string(x)
                                .map_err(FilterWorkerError::SerializeFilterResultError)
                        })
                        .filter_map(Result::ok)
                        .collect();
                    // push results to redis
                    let queue_name = filter_results_queues
                        .get(&filter.id)
                        .ok_or(FilterWorkerError::GetQueueNameError)?;
                    // push them all at once
                    let _: () = match con.lpush(queue_name, out_documents).await {
                        Ok(x) => x,
                        Err(e) => {
                            warn!("error pushing filter results to redis: {}", e);
                            continue;
                        }
                    };
                }
            }
            command_check_countdown -= nb_alerts as i64;
        }

        Ok(())
    }
}
