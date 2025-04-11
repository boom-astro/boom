use crate::conf;
use mongodb::bson::doc;
use rand::Rng;
use redis::AsyncCommands;
use std::fs;
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

pub trait AlertRandomizerTrait {
    fn default() -> Self;
    fn new() -> Self;
    fn objectid(self, object_id: impl Into<Self::ObjectId>) -> Self;
    fn candid(self, candid: i64) -> Self;
    fn get(self) -> (i64, Self::ObjectId, Vec<u8>);
    fn zigzag_encode_i64(n: i64) -> u64 {
        ((n << 1) ^ (n >> 63)) as u64
    }

    fn var_encode_u64(n: u64) -> Vec<u8> {
        let mut bytes = Vec::new();
        let mut value = n;

        loop {
            let mut byte = (value & 0x7F) as u8;
            value >>= 7;

            if value != 0 {
                byte |= 0x80;
            }

            bytes.push(byte);

            if value == 0 {
                break;
            }
        }

        bytes
    }

    fn encode_i64(n: i64) -> Vec<u8> {
        let zigzagged = Self::zigzag_encode_i64(n);
        Self::var_encode_u64(zigzagged)
    }
    fn find_bytes(payload: &[u8], bytes: &[u8]) -> Option<usize> {
        payload
            .windows(bytes.len())
            .position(|window| window == bytes)
    }
    type ObjectId;
}

pub struct ZtfAlertRandomizer {
    original_candid: i64,
    original_object_id: String,
    payload: Vec<u8>,
    candid: Option<i64>,
    object_id: Option<String>,
}

impl AlertRandomizerTrait for ZtfAlertRandomizer {
    type ObjectId = String;

    fn default() -> Self {
        let payload = fs::read("tests/data/alerts/ztf/2695378462115010012.avro").unwrap();
        let original_candid = 2695378462115010012;
        let original_object_id = "ZTF18abudxnw".to_string();
        let candid = Self::randomize_candid();
        let object_id = Self::randomize_object_id();
        Self {
            original_candid,
            original_object_id,
            payload,
            candid: Some(candid),
            object_id: Some(object_id),
        }
    }

    fn new() -> Self {
        let payload = fs::read("tests/data/alerts/ztf/2695378462115010012.avro").unwrap();
        let original_candid = 2695378462115010012;
        let original_object_id = "ZTF18abudxnw".to_string();
        Self {
            original_candid,
            original_object_id,
            payload,
            candid: None,
            object_id: None,
        }
    }

    fn objectid(mut self, object_id: impl Into<Self::ObjectId>) -> Self {
        self.object_id = Some(object_id.into());
        self
    }

    fn candid(mut self, candid: i64) -> Self {
        match candid {
            id if id < 2000000000000000000 || id > 3000000000000000000 => {
                panic!("Candid must be between 2000000000000000000 and 3000000000000000000");
            }
            _ => {
                self.candid = Some(candid);
            }
        }
        self
    }

    fn get(self) -> (i64, Self::ObjectId, Vec<u8>) {
        let original_candid_bytes = Self::encode_i64(self.original_candid);
        let original_object_id_bytes = self.original_object_id.as_bytes();
        let mut payload = self.payload;

        let candid = self.candid.unwrap_or_else(Self::randomize_candid);
        let object_id = self.object_id.unwrap_or_else(Self::randomize_object_id);

        let candid_bytes = Self::encode_i64(candid);
        let object_id_bytes = object_id.as_bytes();

        // Replace candid in the payload
        let mut found = false;
        loop {
            if let Some(candid_idx) = Self::find_bytes(&payload, &original_candid_bytes) {
                payload[candid_idx..candid_idx + original_candid_bytes.len()]
                    .copy_from_slice(&candid_bytes);
                found = true;
            } else {
                break;
            }
        }
        if !found {
            panic!("Candid not found in payload");
        }

        // Replace object_id in the payload
        let mut found = false;
        loop {
            if let Some(object_idx) = Self::find_bytes(&payload, original_object_id_bytes) {
                payload[object_idx..object_idx + original_object_id_bytes.len()]
                    .copy_from_slice(object_id_bytes);
                found = true;
            } else {
                break;
            }
        }
        if !found {
            panic!("Object ID not found in payload");
        }

        (candid, object_id, payload)
    }
}

impl ZtfAlertRandomizer {
    fn randomize_object_id() -> String {
        // format is ZTF + 2 digits + 7 lowercase letters
        let mut rng = rand::rng();
        let mut object_id = String::from("ZTF");
        for _ in 0..2 {
            object_id.push(rng.random_range('0'..='9'));
        }
        for _ in 0..7 {
            object_id.push(rng.random_range('a'..='z'));
        }
        object_id
    }

    fn randomize_candid() -> i64 {
        // variable length encoding means that small numbers result in less bytes
        // so, we picked that range to make sure it creates a 9 byte number
        // just like the original candid
        rand::rng().random_range(2000000000000000000..3000000000000000000)
    }
}

pub struct LsstAlertRandomizer {
    original_candid: i64,
    original_object_id: i64,
    payload: Vec<u8>,
    candid: Option<i64>,
    object_id: Option<i64>,
}

impl AlertRandomizerTrait for LsstAlertRandomizer {
    type ObjectId = i64;

    fn default() -> Self {
        let payload = fs::read("tests/data/alerts/lsst/0.avro").unwrap();
        let original_candid = 25409136044802067;
        let original_object_id = 25401295582003262;
        let candid = Self::randomize_candid();
        let object_id = Self::randomize_object_id();
        Self {
            original_candid,
            original_object_id,
            payload,
            candid: Some(candid),
            object_id: Some(object_id),
        }
    }

    fn new() -> Self {
        let payload = fs::read("tests/data/alerts/lsst/0.avro").unwrap();
        let original_candid = 25409136044802067;
        let original_object_id = 25401295582003262;
        Self {
            original_candid,
            original_object_id,
            payload,
            candid: None,
            object_id: None,
        }
    }

    fn objectid(mut self, object_id: impl Into<Self::ObjectId>) -> Self {
        let object_id = object_id.into();
        match object_id {
            id if id < 20000000000000000 || id > 30000000000000000 => {
                panic!("Object ID must be between 20000000000000000 and 30000000000000000");
            }
            _ => {
                self.object_id = Some(object_id);
            }
        }
        self
    }

    fn candid(mut self, candid: i64) -> Self {
        match candid {
            id if id < 20000000000000000 || id > 30000000000000000 => {
                panic!("Candid must be between 20000000000000000 and 30000000000000000");
            }
            _ => {
                self.candid = Some(candid);
            }
        }
        self
    }

    fn get(self) -> (i64, Self::ObjectId, Vec<u8>) {
        let original_candid_bytes = Self::encode_i64(self.original_candid);
        let original_object_id_bytes = Self::encode_i64(self.original_object_id);
        let mut payload = self.payload;

        let candid = self.candid.unwrap_or_else(Self::randomize_candid);
        let object_id = self.object_id.unwrap_or_else(Self::randomize_object_id);

        let candid_bytes = Self::encode_i64(candid);
        let object_id_bytes = Self::encode_i64(object_id);

        // Replace candid in the payload
        let mut found = false;
        loop {
            if let Some(candid_idx) = Self::find_bytes(&payload, &original_candid_bytes) {
                payload[candid_idx..candid_idx + original_candid_bytes.len()]
                    .copy_from_slice(&candid_bytes);
                found = true;
            } else {
                break;
            }
        }
        if !found {
            panic!("Candid not found in payload");
        }

        // Replace object_id in the payload
        let mut found = false;
        loop {
            if let Some(object_idx) = Self::find_bytes(&payload, &original_object_id_bytes) {
                payload[object_idx..object_idx + original_object_id_bytes.len()]
                    .copy_from_slice(&object_id_bytes);
                found = true;
            } else {
                break;
            }
        }
        if !found {
            panic!("Object ID not found in payload");
        }

        (candid, object_id, payload)
    }
}

impl LsstAlertRandomizer {
    fn randomize_object_id() -> i64 {
        // 8 bytes long
        rand::rng().random_range(20000000000000000..30000000000000000)
    }

    fn randomize_candid() -> i64 {
        // 8 bytes long
        rand::rng().random_range(20000000000000000..30000000000000000)
    }
}
