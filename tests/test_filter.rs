use mongodb::bson::{doc, Document};
use tracing::{error, info};

use boom::{
    alert::{AlertWorker, ZtfAlertWorker},
    conf,
    filter::{process_alerts, Filter, ZtfFilter},
    utils::testing,
};

const CONFIG_FILE: &str = "tests/config.test.yaml";

#[tokio::test]
async fn test_build_filter() {
    let config_file = conf::load_config(CONFIG_FILE).unwrap();
    let db = conf::build_db(&config_file).await;
    let filter_collection = db.collection("filters");
    testing::insert_test_filter().await;
    let filter_result = ZtfFilter::build(-1, &filter_collection).await;
    testing::remove_test_filter().await;
    assert!(filter_result.is_ok());
    let filter = filter_result.unwrap();
    let pipeline: Vec<Document> = vec![
        doc! { "$match": {} },
        doc! { "$lookup": { "from": "ZTF_alerts_aux", "localField": "objectId", "foreignField": "_id", "as": "aux" } },
        doc! {
            "$project": {
                "objectId": 1, "candidate": 1, "classifications": 1, "coordinates": 1,
                "cross_matches": { "$arrayElemAt": ["$aux.cross_matches", 0] },
                "prv_candidates": {
                    "$filter": {
                        "input": { "$arrayElemAt": ["$aux.prv_candidates", 0] },
                        "as": "x",
                        "cond": {
                            "$and": [
                                { "$in": ["$$x.programid", [1_i64]] },
                                { "$lt": [{ "$subtract": ["$candidate.jd", "$$x.jd"] }, 365] },
                                { "$lte": ["$$x.jd", "$candidate.jd"]}
                            ]
                        }
                    }
                }
            }
        },
        doc! { "$match": { "candidate.drb": { "$gt": 0.5 }, "candidate.ndethist": { "$gt": 1_f64 }, "candidate.magpsf": { "$lte": 18.5 } } },
    ];
    assert_eq!(pipeline, filter.pipeline);
    assert_eq!(vec![1], filter.permissions);
}

#[tokio::test]
async fn test_filter_found() {
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    testing::insert_test_filter().await;
    let filter_collection = db.collection("filters");
    let filter_result = ZtfFilter::build(-1, &filter_collection).await;
    testing::remove_test_filter().await;
    assert!(filter_result.is_ok());
}

#[tokio::test]
async fn test_no_filter_found() {
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    let filter_collection = db.collection("filters");
    let filter_result = ZtfFilter::build(-2, &filter_collection).await;
    assert!(filter_result.is_err());
}

// checks result of running filter
#[tokio::test]
async fn test_run_filter() {
    testing::insert_test_filter().await;
    let config_file = conf::load_config(CONFIG_FILE).unwrap();
    let db = conf::build_db(&config_file).await;
    let alert_collection = db.collection("ZTF_alerts");
    let filter_collection = db.collection("filters");
    let filter_result = ZtfFilter::build(-1, &filter_collection).await;
    testing::remove_test_filter().await;
    assert!(filter_result.is_ok());
    let filter = filter_result.unwrap();

    let test_col_name = "ZTF_alerts";
    let test_cutout_col_name = "ZTF_alerts_cutouts";
    let test_aux_col_name = "ZTF_alerts_aux";
    let _ =
        testing::drop_alert_collections(&test_col_name, &test_cutout_col_name, &test_aux_col_name)
            .await;

    let mut alert_worker = ZtfAlertWorker::new(CONFIG_FILE).await.unwrap();
    let file_name = "tests/data/alerts/ztf/2695378462115010012.avro";
    let bytes_content = std::fs::read(file_name).unwrap();
    let result = alert_worker.process_alert(&bytes_content).await;
    assert!(result.is_ok(), "Error processing alert: {:?}", result);

    let candids = vec![result.unwrap()];

    info!("received {} candids from redis", candids.len());
    let out_candids = process_alerts(candids, filter.pipeline, &alert_collection).await;
    match out_candids {
        Ok(out) => {
            assert_eq!(out.len(), 1);
        }
        Err(e) => {
            panic!("Got error: {}", e);
        }
    }
}

#[tokio::test]
async fn test_filter_no_alerts() {
    testing::insert_test_filter().await;
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    let alert_collection = db.collection("ZTF_alerts");
    let filter_collection = db.collection("filters");
    let filter_result = ZtfFilter::build(-1, &filter_collection).await;
    testing::remove_test_filter().await;
    assert!(filter_result.is_ok());
    let filter = filter_result.unwrap();

    let candids: Vec<i64> = vec![1];

    let out_candids = process_alerts(candids, filter.pipeline, &alert_collection).await;
    match out_candids {
        Err(e) => {
            error!("Error running filter: {}", e);
        }
        _ => {}
    }
}
