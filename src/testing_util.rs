use crate::alert;
use crate::conf;
use crate::types;
use mongodb::bson::doc;
use redis::AsyncCommands;
use tracing::{error, info};
// Utility for unit tests

// drops alert collections from the database
pub async fn drop_alert_collections(
    alert_collection_name: &str,
    alert_aux_collection_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    db.collection::<mongodb::bson::Document>(alert_collection_name)
        .drop()
        .await?;
    db.collection::<mongodb::bson::Document>(alert_aux_collection_name)
        .drop()
        .await?;
    Ok(())
}

// run alert worker on input_queue_name
pub async fn alert_worker(
    input_packet_queue: &str,
    output_packet_queue: &str,
    alert_collection_name: &str,
    alert_aux_collection_name: &str,
) {
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let stream_name = "ZTF";
    let xmatch_configs = conf::build_xmatch_configs(&config_file, stream_name);
    let db: mongodb::Database = conf::build_db(&config_file).await;

    if let Err(e) = db.list_collection_names().await {
        error!("Error connecting to the database: {}", e);
        return;
    }
    let alert_collection = db.collection(alert_collection_name);
    let alert_aux_collection = db.collection(alert_aux_collection_name);

    let input_packet_queue_temp = format!("{}_temp", input_packet_queue);
    let schema = types::ztf_alert_schema().unwrap();

    let client_redis = redis::Client::open("redis://localhost:6379".to_string()).unwrap();
    let mut con = client_redis
        .get_multiplexed_async_connection()
        .await
        .unwrap();

    loop {
        let result: Option<Vec<Vec<u8>>> = con
            .rpoplpush(input_packet_queue, &input_packet_queue_temp)
            .await
            .unwrap();
        match result {
            Some(value) => {
                let candid = alert::process_alert(
                    &value[0],
                    &xmatch_configs,
                    &db,
                    &alert_collection,
                    &alert_aux_collection,
                    &schema,
                )
                .await;
                match candid {
                    Ok(Some(candid)) => {
                        // queue the candid for processing by the classifier
                        con.lpush::<&str, i64, isize>(&output_packet_queue, candid)
                            .await
                            .unwrap();
                        con.lrem::<&str, Vec<u8>, isize>(
                            &input_packet_queue_temp,
                            1,
                            value[0].clone(),
                        )
                        .await
                        .unwrap();
                    }
                    Ok(None) => {
                        info!("Alert already exists");
                        // remove the alert from the queue
                        con.lrem::<&str, Vec<u8>, isize>(
                            &input_packet_queue_temp,
                            1,
                            value[0].clone(),
                        )
                        .await
                        .unwrap();
                    }
                    Err(_e) => {
                        // put it back in the input_packet_queue, to the left (pop from the right, push to the left)
                        con.lrem::<&str, Vec<u8>, isize>(
                            &input_packet_queue_temp,
                            1,
                            value[0].clone(),
                        )
                        .await
                        .unwrap();
                        con.lpush::<&str, Vec<u8>, isize>(&input_packet_queue, value[0].clone())
                            .await
                            .unwrap();
                    }
                }
            }
            None => {
                return;
            }
        }
    }
}

// insert a test filter with id -1 into the database
pub async fn insert_test_filter() {
    let filter_obj: mongodb::bson::Document = doc! {
      "_id": mongodb::bson::oid::ObjectId::new(),
      "group_id": 41,
      "filter_id": -1,
      "catalog": "ZTF_alerts",
      "permissions": [
        1
      ],
      "active": true,
      "active_fid": "v2e0fs",
      "fv": [
        {
          "fid": "v2e0fs",
          "pipeline": "[{\"$match\": {\"candidate.drb\": {\"$gt\": 0.5}, \"candidate.ndethist\": {\"$gt\": 1.0}, \"candidate.magpsf\": {\"$lte\": 18.5}}}]",
          "created_at": {
            "$date": "2020-10-21T08:39:43.693Z"
          }
        }
      ],
      "autosave": false,
      "update_annotations": true,
      "created_at": {
        "$date": "2021-02-20T08:18:28.324Z"
      },
      "last_modified": {
        "$date": "2023-05-04T23:39:07.090Z"
      }
    };

    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    let x = db
        .collection::<mongodb::bson::Document>("filters")
        .insert_one(filter_obj)
        .await;
    match x {
        Err(e) => {
            error!("error inserting filter obj: {}", e);
        }
        _ => {}
    }
}

// remove test filter with id -1 from the database
pub async fn remove_test_filter() {
    let config_file = conf::load_config("tests/config.test.yaml").unwrap();
    let db = conf::build_db(&config_file).await;
    let _ = db
        .collection::<mongodb::bson::Document>("filters")
        .delete_one(doc! {"filter_id": -1})
        .await;
}

pub async fn empty_processed_alerts_queue(
    input_queue_name: &str,
    output_queue_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let client_redis = redis::Client::open("redis://localhost:6379".to_string()).unwrap();
    let mut con = client_redis
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    con.del::<&str, usize>(input_queue_name).await.unwrap();
    con.del::<&str, usize>("{}_temp").await.unwrap();
    con.del::<&str, usize>(output_queue_name).await.unwrap();

    Ok(())
}
