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

// download alerts from date <alert_date> and place alerts into redis queue <output_packet_queue>
pub async fn fake_kafka_consumer(
    output_packet_queue: &str,
    alert_date: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let date = alert_date;
    let total_nb_alerts = download_alerts_from_archive(date)?;

    let client = redis::Client::open("redis://localhost:6379".to_string())?;
    let mut con = client.get_multiplexed_async_connection().await.unwrap();

    // empty the queue
    con.del::<&str, usize>(output_packet_queue).await.unwrap();

    let mut total = 0;

    info!("Pushing {} alerts to the queue", total_nb_alerts);
    // start timer
    let start = std::time::Instant::now();
    // poll one message at a time
    for entry in std::fs::read_dir(format!("data/alerts/ztf/{}", date))? {
        let entry = entry?;
        let path = entry.path();
        let payload = std::fs::read(path)?;

        con.rpush::<&str, Vec<u8>, usize>(output_packet_queue, payload.to_vec())
            .await
            .unwrap();
        total += 1;
        if total % 1000 == 0 {
            info!("Pushed {} items since {:?}", total, start.elapsed());
        }
    }
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
                    value[0].clone(),
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
          "pipeline": "[{\"$project\": {\"cutoutScience\": 0, \"cutoutDifference\": 0, \"cutoutTemplate\": 0, \"publisher\": 0, \"schemavsn\": 0}}, {\"$lookup\": {\"from\": \"alerts_aux\", \"localField\": \"objectId\", \"foreignField\": \"_id\", \"as\": \"aux\"}}, {\"$project\": {\"objectId\": 1, \"candid\": 1, \"candidate\": 1, \"classifications\": 1, \"coordinates\": 1, \"prv_candidates\": {\"$arrayElemAt\": [\"$aux.prv_candidates\", 0]}, \"cross_matches\": {\"$arrayElemAt\": [\"$aux.cross_matches\", 0]}}}, {\"$match\": {\"candidate.drb\": {\"$gt\": 0.5}, \"candidate.ndethist\": {\"$gt\": 1.0}, \"candidate.magpsf\": {\"$lte\": 18.5}}}]",
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

fn download_alerts_from_archive(date: &str) -> Result<i64, Box<dyn std::error::Error>> {
    // given a date in the format YYYYMMDD, download public ZTF alerts from the archive
    // in this method we just validate the date format,
    // then use wget to download the alerts from the archive
    // and finally we extract the alerts to a folder

    // validate the date format
    if date.len() != 8 {
        return Err("Invalid date format".into());
    }

    // create the data folder if it doesn't exist, in data/alerts/ztf/<date>
    let data_folder = format!("data/alerts/ztf/{}", date);

    // if it already exists and has the alerts, we don't need to download them again
    if std::path::Path::new(&data_folder).exists() && std::fs::read_dir(&data_folder)?.count() > 0 {
        info!("Alerts already downloaded to {}", data_folder);
        let count = std::fs::read_dir(&data_folder)?.count();
        return Ok(count as i64);
    }

    std::fs::create_dir_all(&data_folder)?;

    info!("Downloading alerts for date {}", date);
    // download the alerts to data folder
    let url = format!(
        "https://ztf.uw.edu/alerts/public/ztf_public_{}.tar.gz",
        date
    );
    let output = std::process::Command::new("wget")
        .arg(&url)
        .arg("-P")
        .arg(&data_folder)
        .output()?;
    if !output.status.success() {
        return Err("Failed to download alerts".into());
    } else {
        info!("Downloaded alerts to {}", data_folder);
    }

    // extract the alerts
    let output = std::process::Command::new("tar")
        .arg("-xzf")
        .arg(format!("{}/ztf_public_{}.tar.gz", data_folder, date))
        .arg("-C")
        .arg(&data_folder)
        .output()?;
    if !output.status.success() {
        return Err("Failed to extract alerts".into());
    } else {
        info!("Extracted alerts to {}", data_folder);
    }

    // remove the tar.gz file
    std::fs::remove_file(format!("{}/ztf_public_{}.tar.gz", data_folder, date))?;

    // count the number of alerts
    let count = std::fs::read_dir(&data_folder)?.count();

    Ok(count as i64)
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
