use mongodb::bson::{doc, Document};
use redis::AsyncCommands;
use std::{collections::HashMap, error::Error, num::NonZero};
use crate::filter::{Filter, FilterWorker, get_filter_object, process_alerts};
use crate::worker_util::WorkerCmd;


struct LsstFilter {
    id: i32,
    pipeline: Vec<Document>,
}

impl Filter for LsstFilter {
    async fn build(
        filter_id: i32,
        filter_collection: &mongodb::Collection<mongodb::bson::Document>,
    ) -> Result<LsstFilter, Box<dyn Error>> {
        // get filter object
        let filter_obj = get_filter_object(filter_id, "LSST", filter_collection).await?;

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

        // get filter pipeline as str and convert to Vec<Bson>
        let filter_pipeline = filter_obj.get("pipeline").unwrap().as_str().unwrap();
        let filter_pipeline = serde_json::from_str::<serde_json::Value>(filter_pipeline).expect("Invalid input");
        let filter_pipeline = filter_pipeline.as_array().unwrap();

        // append stages to prefix
        for stage in filter_pipeline {
            let x = mongodb::bson::to_document(stage).expect("invalid filter BSON");
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
    id: String,
    receiver: std::sync::mpsc::Receiver<WorkerCmd>,
    filter_collection: mongodb::Collection<mongodb::bson::Document>,
    alert_collection: mongodb::Collection<mongodb::bson::Document>
}

impl FilterWorker for LsstFilterWorker {
    async fn new(
        id: String,
        receiver: std::sync::mpsc::Receiver<WorkerCmd>,
        config_path: &str
    ) -> Self {
        let config_file = crate::conf::load_config(&config_path).unwrap();
        let db: mongodb::Database = crate::conf::build_db(&config_file).await;
        let alert_collection = db.collection("LSST_alerts");
        let filter_collection = db.collection("filters");

        LsstFilterWorker { 
            id,
            receiver,
            filter_collection,
            alert_collection
        }
    }

    async fn run(&self) -> Result<(), Box<dyn Error>> {

        // query the DB to find the ids of all the filters for LSST that are active
        let filter_ids: Vec<i32> = self.filter_collection
            .distinct("filter_id", doc! {"active": true, "catalog": "LSST"})
            .await
            .unwrap()
            .into_iter()
            .map(|x| x.as_i32().unwrap())
            .collect();
    
        let mut filters: Vec<LsstFilter> = Vec::new();
        for filter_id in filter_ids {
            let filter = LsstFilter::build(filter_id, &self.filter_collection).await.unwrap();
            filters.push(filter);
        }
        
        // LSST is simpler, there are no permissions so there is only one queue
        let queue_name = "lsst_filter_queue";
        // create a list of output queues, one for each filter
        let mut filter_results_queues: HashMap<i32, String> = HashMap::new();
        for filter in &filters {
            let queue_name = format!("lsst_filter_{}_results_queue", filter.id);
            filter_results_queues.insert(filter.id, queue_name);
        }
    
        // in a never ending loop, get candids from redis
        let client_redis = redis::Client::open("redis://localhost:6379".to_string()).unwrap();
        let mut con = client_redis
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        loop {
            // get candids from redis
            let candids: Vec<i64> = con
                .rpop::<&str, Vec<i64>>(queue_name, NonZero::new(1000))
                .await
                .unwrap();
            if candids.len() == 0 {
                continue;
            }
            // run the filters
            for filter in &filters {
                let out_documents = process_alerts(candids.clone(), filter.pipeline.clone(), &self.alert_collection).await.unwrap();
                // convert the documents to json
                let out_documents: Vec<String> = out_documents.iter().map(|x| serde_json::to_string(x).unwrap()).collect();
                // push results to redis
                let queue_name = filter_results_queues.get(&filter.id).unwrap();
                // push them all at once
                let _: () = con.lpush(queue_name, out_documents).await.unwrap();
            }
        }
    }
}
