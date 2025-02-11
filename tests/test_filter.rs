use mongodb::bson::{doc, Document};
use redis::AsyncCommands;
use std::num::NonZero;
use tracing::{error, info};

use boom::{conf, filter, kafka, testing_util as tu};

#[tokio::test]
async fn test_build_filter() {
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    tu::insert_test_filter().await;
    let filter_result = filter::Filter::build(-1, &db).await;
    tu::remove_test_filter().await;
    assert!(filter_result.is_ok());
    let filter = filter_result.unwrap();
    let pipeline: Vec<Document> = vec![
        doc! { "$match": {} },
        doc! { "$project": { "cutoutScience": 0, "cutoutDifference": 0, "cutoutTemplate": 0, "publisher": 0, "schemavsn": 0 } },
        doc! { "$lookup": { "from": "ZTF_alerts_aux", "localField": "objectId", "foreignField": "_id", "as": "aux" } },
        doc! { "$project": { "objectId": 1, "candid": 1, "candidate": 1, "classifications": 1, "coordinates": 1, "cross_matches": { "$arrayElemAt": ["$aux.cross_matches", 0] }, "prv_candidates": { "$filter": { "input": { "$arrayElemAt": ["$aux.prv_candidates", 0] }, "as": "x", "cond": { "$and": [{ "$in": ["$$x.programid", [1_i64]] }, { "$lt": [{ "$subtract": ["$candidate.jd", "$$x.jd"] }, 365] }] } } } } },
        doc! { "$match": { "candidate.drb": { "$gt": 0.5 }, "candidate.ndethist": { "$gt": 1_f64 }, "candidate.magpsf": { "$lte": 18.5 } } },
    ];
    assert_eq!(pipeline, filter.pipeline);
    assert_eq!(vec![1], filter.permissions);
    assert_eq!("ZTF_alerts", filter.catalog);
}

// checks result of running filter
#[tokio::test]
async fn test_run_filter() {
    tu::insert_test_filter().await;
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    let filter_result = filter::Filter::build(-1, &db).await;
    tu::remove_test_filter().await;
    assert!(filter_result.is_ok());
    let mut filter = filter_result.unwrap();

    let topic = uuid::Uuid::new_v4().to_string();
    let queue = uuid::Uuid::new_v4().to_string();
    let output_queue = uuid::Uuid::new_v4().to_string();
    let test_col_name = "ZTF_alerts";
    let test_aux_col_name = "ZTF_alerts_aux";
    let _ = tu::drop_alert_collections(&test_col_name, &test_aux_col_name).await;
    let result = kafka::produce_from_archive("20240617", 0, Some(topic.clone())).await;
    assert!(result.is_ok());
    let result = kafka::consume_alerts(&topic, None, true, 0, Some(queue.clone())).await;
    assert!(result.is_ok());

    let _ = tu::alert_worker(&queue, &output_queue, &test_col_name, &test_aux_col_name).await;

    let client_redis = redis::Client::open("redis://localhost:6379".to_string()).unwrap();
    let mut con: redis::aio::MultiplexedConnection = client_redis
        .get_multiplexed_async_connection()
        .await
        .unwrap();

    let res: Result<Vec<i64>, redis::RedisError> = con
        .rpop::<&str, Vec<i64>>(&output_queue, NonZero::new(1000))
        .await;

    match res {
        Ok(candids) => {
            if candids.len() == 0 {
                panic!("test_run_filter failed: no candids in queue");
            }
            info!("received {} candids from redis", candids.len());
            let out_candids = filter.run(candids.clone(), &db).await;
            match out_candids {
                Ok(out) => {
                    assert_eq!(out.len(), 194);
                }
                Err(e) => {
                    panic!("Got error: {}", e);
                }
            }
        }
        Err(e) => {
            panic!("Got Error: {}", e);
        }
    }
}

// run filter on 0 candids
#[tokio::test]
async fn test_filter_no_alerts() {
    tu::insert_test_filter().await;
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    let filter_result = filter::Filter::build(-1, &db).await;
    tu::remove_test_filter().await;
    assert!(filter_result.is_ok());
    let mut filter = filter_result.unwrap();

    let client_redis = redis::Client::open("redis://localhost:6379".to_string()).unwrap();
    let mut con: redis::aio::MultiplexedConnection = client_redis
        .get_multiplexed_async_connection()
        .await
        .unwrap();

    let res: Result<Vec<i64>, redis::RedisError> = con
        .rpop::<&str, Vec<i64>>("worker_output_queue", NonZero::new(1000))
        .await;

    match res {
        Ok(candids) => {
            let out_candids = filter.run(candids.clone(), &db).await;
            match out_candids {
                Err(e) => {
                    error!("Error running filter: {}", e);
                }
                _ => {}
            }
        }
        Err(e) => {
            panic!("Got Error when getting candids from redis: {}", e);
        }
    }
}

#[tokio::test]
async fn test_no_filter_found() {
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    let filter = filter::Filter::build(-2, &db).await;
    match filter {
        Err(e) => {
            assert!(e.is::<filter::FilterError>());
            error!("error: {}", e);
        }
        _ => {
            panic!("Was supposed to get error");
        }
    }
}

#[tokio::test]
async fn test_filter_found() {
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    tu::insert_test_filter().await;
    let filter = filter::Filter::build(-1, &db).await;
    tu::remove_test_filter().await;
    match filter {
        Ok(_) => {
            info!("successfully got filter from db");
        }
        Err(e) => {
            panic!("ERROR, was supposed to find filter in database. {}", e);
        }
    }
}
