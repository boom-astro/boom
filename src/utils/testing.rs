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
    fn ra(self, ra: f64) -> Self;
    fn dec(self, dec: f64) -> Self;
    fn get(self) -> (i64, Self::ObjectId, f64, f64, Vec<u8>);
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
    fn encode_f64(n: f64) -> Vec<u8> {
        let bits = n.to_bits() as i64;
        bits.to_le_bytes().to_vec()
    }
    fn find_bytes(payload: &[u8], bytes: &[u8]) -> Option<usize> {
        payload
            .windows(bytes.len())
            .position(|window| window == bytes)
    }
    fn randomize_i64() -> i64 {
        rand::rng().random_range(i64::MIN..i64::MAX)
    }
    fn randomize_ra() -> f64 {
        rand::rng().random_range(0.0..360.0)
    }
    fn randomize_dec() -> f64 {
        rand::rng().random_range(-90.0..90.0)
    }
    type ObjectId;
}

pub struct ZtfAlertRandomizer {
    original_candid: i64,
    original_object_id: String,
    original_ra: f64,
    original_dec: f64,
    payload: Vec<u8>,
    candid: Option<i64>,
    object_id: Option<String>,
    ra: Option<f64>,
    dec: Option<f64>,
}

impl AlertRandomizerTrait for ZtfAlertRandomizer {
    type ObjectId = String;

    fn default() -> Self {
        let payload = fs::read("tests/data/alerts/ztf/2695378462115010012.avro").unwrap();
        Self {
            original_candid: 2695378462115010012,
            original_object_id: "ZTF18abudxnw".to_string(),
            original_ra: 295.3031995,
            original_dec: -10.3958989,
            payload,
            candid: Some(Self::randomize_i64()),
            object_id: Some(Self::randomize_object_id()),
            ra: None,
            dec: None,
        }
    }

    fn new() -> Self {
        let payload = fs::read("tests/data/alerts/ztf/2695378462115010012.avro").unwrap();
        Self {
            original_candid: 2695378462115010012,
            original_object_id: "ZTF18abudxnw".to_string(),
            original_ra: 295.3031995,
            original_dec: -10.3958989,
            payload,
            candid: None,
            object_id: None,
            ra: None,
            dec: None,
        }
    }

    fn objectid(mut self, object_id: impl Into<Self::ObjectId>) -> Self {
        self.object_id = Some(object_id.into());
        self
    }

    fn candid(mut self, candid: i64) -> Self {
        self.candid = Some(candid);
        self
    }

    fn ra(mut self, ra: f64) -> Self {
        self.ra = Some(ra);
        self
    }
    fn dec(mut self, dec: f64) -> Self {
        self.dec = Some(dec);
        self
    }

    fn get(self) -> (i64, Self::ObjectId, f64, f64, Vec<u8>) {
        let original_candid_bytes = Self::encode_i64(self.original_candid);
        let original_object_id_bytes = self.original_object_id.as_bytes();
        let original_ra_bytes = Self::encode_f64(self.original_ra);
        let original_dec_bytes = Self::encode_f64(self.original_dec);
        let mut payload = self.payload;

        let candid = self.candid.unwrap_or_else(Self::randomize_i64);
        let object_id = self.object_id.unwrap_or_else(Self::randomize_object_id);
        let ra = self.ra.unwrap_or_else(Self::randomize_ra);
        let dec = self.dec.unwrap_or_else(Self::randomize_dec);

        let candid_bytes = Self::encode_i64(candid);
        let object_id_bytes = object_id.as_bytes();
        let ra_bytes = Self::encode_f64(ra);
        let dec_bytes = Self::encode_f64(dec);

        // Replace candid in the payload
        let mut found = false;
        loop {
            if let Some(candid_idx) = Self::find_bytes(&payload, &original_candid_bytes) {
                let left = &payload[..candid_idx];
                let right = &payload[candid_idx + original_candid_bytes.len()..];
                payload = [left, &candid_bytes, right].concat();
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

        // replace ra in the payload
        let mut found = false;
        loop {
            if let Some(ra_idx) = Self::find_bytes(&payload, &original_ra_bytes) {
                let left = &payload[..ra_idx];
                let right = &payload[ra_idx + original_ra_bytes.len()..];
                payload = [left, &ra_bytes, right].concat();
                found = true;
            } else {
                break;
            }
        }
        if !found {
            panic!("RA not found in payload");
        }
        // replace dec in the payload
        let mut found = false;
        loop {
            if let Some(dec_idx) = Self::find_bytes(&payload, &original_dec_bytes) {
                let left = &payload[..dec_idx];
                let right = &payload[dec_idx + original_dec_bytes.len()..];
                payload = [left, &dec_bytes, right].concat();
                found = true;
            } else {
                break;
            }
        }
        if !found {
            panic!("Dec not found in payload");
        }

        (candid, object_id, ra, dec, payload)
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
}

pub struct LsstAlertRandomizer {
    original_candid: i64,
    original_object_id: i64,
    original_ra: f64,
    original_dec: f64,
    payload: Vec<u8>,
    candid: Option<i64>,
    object_id: Option<i64>,
    ra: Option<f64>,
    dec: Option<f64>,
}

impl AlertRandomizerTrait for LsstAlertRandomizer {
    type ObjectId = i64;

    fn default() -> Self {
        let payload = fs::read("tests/data/alerts/lsst/0.avro").unwrap();
        Self {
            original_candid: 25409136044802067,
            original_object_id: 25401295582003262,
            original_ra: 149.8021056712687,
            original_dec: 2.2486503003111813,
            payload,
            candid: Some(Self::randomize_i64()),
            object_id: Some(Self::randomize_i64()),
            ra: None,
            dec: None,
        }
    }

    fn new() -> Self {
        let payload = fs::read("tests/data/alerts/lsst/0.avro").unwrap();
        Self {
            original_candid: 25409136044802067,
            original_object_id: 25401295582003262,
            original_ra: 149.8021056712687,
            original_dec: 2.2486503003111813,
            payload,
            candid: None,
            object_id: None,
            ra: None,
            dec: None,
        }
    }

    fn objectid(mut self, object_id: impl Into<Self::ObjectId>) -> Self {
        self.object_id = Some(object_id.into());
        self
    }

    fn candid(mut self, candid: i64) -> Self {
        self.candid = Some(candid);
        self
    }

    fn ra(mut self, ra: f64) -> Self {
        self.ra = Some(ra);
        self
    }
    fn dec(mut self, dec: f64) -> Self {
        self.dec = Some(dec);
        self
    }

    fn get(self) -> (i64, Self::ObjectId, f64, f64, Vec<u8>) {
        let original_candid_bytes = Self::encode_i64(self.original_candid);
        let original_object_id_bytes = Self::encode_i64(self.original_object_id);
        let original_ra_bytes = Self::encode_f64(self.original_ra);
        let original_dec_bytes = Self::encode_f64(self.original_dec);
        let mut payload = self.payload;

        let candid = self.candid.unwrap_or_else(Self::randomize_i64);
        let object_id = self.object_id.unwrap_or_else(Self::randomize_i64);
        let ra = self.ra.unwrap_or_else(Self::randomize_ra);
        let dec = self.dec.unwrap_or_else(Self::randomize_dec);

        let candid_bytes = Self::encode_i64(candid);
        let object_id_bytes = Self::encode_i64(object_id);
        let ra_bytes = Self::encode_f64(ra);
        let dec_bytes = Self::encode_f64(dec);

        // Replace candid in the payload
        let mut found = false;
        loop {
            if let Some(candid_idx) = Self::find_bytes(&payload, &original_candid_bytes) {
                let left = &payload[..candid_idx];
                let right = &payload[candid_idx + original_candid_bytes.len()..];
                payload = [left, &candid_bytes, right].concat();
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
                let left = &payload[..object_idx];
                let right = &payload[object_idx + original_object_id_bytes.len()..];
                payload = [left, &object_id_bytes, right].concat();
                found = true;
            } else {
                break;
            }
        }
        if !found {
            panic!("Object ID not found in payload");
        }

        // replace ra in the payload
        let mut found = false;
        loop {
            if let Some(ra_idx) = Self::find_bytes(&payload, &original_ra_bytes) {
                let left = &payload[..ra_idx];
                let right = &payload[ra_idx + original_ra_bytes.len()..];
                payload = [left, &ra_bytes, right].concat();
                found = true;
            } else {
                break;
            }
        }
        if !found {
            panic!("RA not found in payload");
        }
        // replace dec in the payload
        let mut found = false;
        loop {
            if let Some(dec_idx) = Self::find_bytes(&payload, &original_dec_bytes) {
                let left = &payload[..dec_idx];
                let right = &payload[dec_idx + original_dec_bytes.len()..];
                payload = [left, &dec_bytes, right].concat();
                found = true;
            } else {
                break;
            }
        }
        if !found {
            panic!("Dec not found in payload");
        }

        (candid, object_id, ra, dec, payload)
    }
}
