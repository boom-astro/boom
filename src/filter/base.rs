use futures::stream::StreamExt;
use mongodb::bson::{doc, Document};
use std::{error::Error, fmt};
use tokio::sync::mpsc;
use tracing::error;

use crate::utils::worker::WorkerCmd;

#[derive(Debug)]
pub struct FilterError {
    message: String,
}

impl Error for FilterError {}
impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Filter Error {}", self.message)
    }
}

pub async fn get_filter_object(
    filter_id: i32,
    catalog: &str,
    filter_collection: &mongodb::Collection<mongodb::bson::Document>,
) -> Result<Document, Box<dyn Error>> {
    let filter_obj = filter_collection
        .aggregate(vec![
            doc! {
                "$match": doc! {
                    "filter_id": filter_id,
                    "active": true,
                    "catalog": catalog
                }
            },
            doc! {
                "$project": doc! {
                    "fv": doc! {
                        "$filter": doc! {
                            "input": "$fv",
                            "as": "x",
                            "cond": doc! {
                                "$eq": [
                                    "$$x.fid",
                                    "$active_fid"
                                ]
                            }
                        }
                    },
                    "group_id": 1,
                    "permissions": 1,
                    "catalog": 1
                }
            },
            doc! {
                "$project": doc! {
                    "pipeline": doc! {
                        "$arrayElemAt": [
                            "$fv.pipeline",
                            0
                        ]
                    },
                    "group_id": 1,
                    "permissions": 1,
                    "catalog": 1
                }
            },
        ])
        .await;

    if let Err(e) = filter_obj {
        error!("Got ERROR when retrieving filter from database: {}", e);
        return Result::Err(Box::new(e));
    }

    // get document from cursor
    let mut filter_obj = filter_obj.unwrap();
    let advance = filter_obj.advance().await.unwrap();
    let filter_obj = if advance {
        filter_obj.deserialize_current().unwrap()
    } else {
        return Err(Box::new(FilterError {
            message: format!("Filter {} not found in database", filter_id),
        }));
    };

    Ok(filter_obj)
}

pub async fn process_alerts(
    candids: Vec<i64>,
    mut pipeline: Vec<Document>,
    alert_collection: &mongodb::Collection<Document>,
) -> Result<Vec<Document>, Box<dyn Error>> {
    if candids.len() == 0 {
        return Ok(vec![]);
    }
    if pipeline.len() == 0 {
        panic!("filter pipeline is empty, ensure filter has been built before running");
    }

    // insert candids into filter
    pipeline[0].get_document_mut("$match").unwrap().insert(
        "_id",
        doc! {
            "$in": candids
        },
    );

    // run filter
    let mut result = alert_collection.aggregate(pipeline).await?; // is to_owned the fastest way to access self fields?

    let mut out_documents: Vec<Document> = Vec::new();

    while let Some(doc) = result.next().await {
        let doc = doc.unwrap();
        out_documents.push(doc);
    }
    Ok(out_documents)
}

#[async_trait::async_trait]
pub trait Filter {
    async fn build(
        filter_id: i32,
        filter_collection: &mongodb::Collection<mongodb::bson::Document>,
    ) -> Result<Self, Box<dyn Error>>
    where
        Self: Sized;
}

#[async_trait::async_trait]
pub trait FilterWorker {
    async fn new(id: String, receiver: mpsc::Receiver<WorkerCmd>, config_path: &str) -> Self;

    async fn run(&mut self) -> Result<(), Box<dyn Error>>;
}

#[tokio::main]
pub async fn run_filter_worker<T: FilterWorker>(
    id: String,
    receiver: mpsc::Receiver<WorkerCmd>,
    config_path: &str,
) -> Result<(), Box<dyn Error>> {
    let mut filter_worker = T::new(id, receiver, config_path).await;
    filter_worker.run().await
}
