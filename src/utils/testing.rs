use crate::conf;
use mongodb::bson::doc;
use redis::AsyncCommands;
use tracing::error;
// Utility for unit tests

// drops alert collections from the database
pub async fn drop_alert_collections(
    alert_collection_name: &str,
    alert_cutout_collection_name: &str,
    alert_aux_collection_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let config_file = conf::load_config("tests/config.test.yaml")?;
    let db = conf::build_db(&config_file).await?;
    db.collection::<mongodb::bson::Document>(alert_collection_name)
        .drop()
        .await?;
    db.collection::<mongodb::bson::Document>(alert_cutout_collection_name)
        .drop()
        .await?;
    db.collection::<mongodb::bson::Document>(alert_aux_collection_name)
        .drop()
        .await?;
    Ok(())
}

pub async fn drop_alert_from_collections(
    candid: i64,
    stream_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let config_file = conf::load_config("tests/config.test.yaml")?;
    let db = conf::build_db(&config_file).await?;
    let alert_collection_name = format!("{}_alerts", stream_name);
    let alert_cutout_collection_name = format!("{}_alerts_cutouts", stream_name);
    let alert_aux_collection_name = format!("{}_alerts_aux", stream_name);

    let filter = doc! {"_id": candid};
    let alert = db
        .collection::<mongodb::bson::Document>(&alert_collection_name)
        .find_one(filter.clone())
        .await?;

    if let Some(alert) = alert {
        // delete the alert from the alerts collection
        db.collection::<mongodb::bson::Document>(&alert_collection_name)
            .delete_one(filter.clone())
            .await?;

        // delete the alert from the cutouts collection
        db.collection::<mongodb::bson::Document>(&alert_cutout_collection_name)
            .delete_one(filter.clone())
            .await?;

        // 1. if the stream name is ZTF we read the object id as a string and drop the aux entry
        // 2. otherwise we consider it an i64 and drop the aux entry
        match stream_name {
            "ZTF" => {
                let object_id = alert.get_str("objectId")?;
                db.collection::<mongodb::bson::Document>(&alert_aux_collection_name)
                    .delete_one(doc! {"_id": object_id})
                    .await?;
            }
            _ => {
                let object_id = alert.get_i64("objectId")?;
                db.collection::<mongodb::bson::Document>(&alert_aux_collection_name)
                    .delete_one(doc! {"_id": object_id})
                    .await?;
            }
        }
    }

    Ok(())
}

// insert a test filter with id -1 into the database
pub async fn insert_test_filter() -> Result<(), Box<dyn std::error::Error>> {
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

    let config_file = conf::load_config("tests/config.test.yaml")?;
    let db = conf::build_db(&config_file).await?;
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

    Ok(())
}

// remove test filter with id -1 from the database
pub async fn remove_test_filter() -> Result<(), Box<dyn std::error::Error>> {
    let config_file = conf::load_config("tests/config.test.yaml")?;
    let db = conf::build_db(&config_file).await?;
    let _ = db
        .collection::<mongodb::bson::Document>("filters")
        .delete_one(doc! {"filter_id": -1})
        .await;

    Ok(())
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
