use crate::{
    alert::{SchemaRegistry, LSST_SCHEMA_REGISTRY_URL},
    conf,
};
use apache_avro::{
    from_avro_datum,
    types::{Record, Value},
    Reader, Schema, Writer,
};
use mongodb::bson::doc;
use rand::Rng;
use redis::AsyncCommands;
use std::fs;
use std::io::Read;
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

pub async fn insert_test_ztf_filter() -> Result<i32, Box<dyn std::error::Error>> {
    // we randomize the filter id
    let filter_id = rand::random::<i32>();
    let filter_obj: mongodb::bson::Document = doc! {
      "_id": mongodb::bson::oid::ObjectId::new(),
      "group_id": 41,
      "filter_id": filter_id,
      "catalog": "ZTF_alerts",
      "permissions": [
        1
      ],
      "active": true,
      "active_fid": "v2e0fs",
      "fv": [
        {
            "fid": "v2e0fs",
            "pipeline": "[{\"$match\": {\"candidate.drb\": {\"$gt\": 0.5}, \"candidate.ndethist\": {\"$gt\": 1.0}, \"candidate.magpsf\": {\"$lte\": 18.5}}}, {\"$project\": {\"annotations.mag_now\": {\"$round\": [\"$candidate.magpsf\", 2]}}}]",
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

    Ok(filter_id)
}

pub async fn remove_test_ztf_filter(filter_id: i32) -> Result<(), Box<dyn std::error::Error>> {
    let config_file = conf::load_config("tests/config.test.yaml")?;
    let db = conf::build_db(&config_file).await?;
    let _ = db
        .collection::<mongodb::bson::Document>("filters")
        .delete_many(doc! {"filter_id": filter_id, "catalog": "ZTF_alerts"})
        .await;

    Ok(())
}

pub async fn insert_test_lsst_filter() -> Result<i32, Box<dyn std::error::Error>> {
    // we randomize the filter id
    let filter_id = rand::random::<i32>();
    let filter_obj: mongodb::bson::Document = doc! {
      "_id": mongodb::bson::oid::ObjectId::new(),
      "group_id": 41,
      "filter_id": filter_id,
      "catalog": "LSST_alerts",
      "permissions": [
        1
      ],
      "active": true,
      "active_fid": "v2e0fs",
      "fv": [
        {
            "fid": "v2e0fs",
            "pipeline": "[{\"$match\": {\"candidate.reliability\": {\"$gt\": 0.5}, \"candidate.snr\": {\"$gt\": 5.0}, \"candidate.magpsf\": {\"$lte\": 25.0}}}, {\"$project\": {\"annotations.mag_now\": {\"$round\": [\"$candidate.magpsf\", 2]}}}]",
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

    Ok(filter_id)
}

pub async fn remove_test_lsst_filter(filter_id: i32) -> Result<(), Box<dyn std::error::Error>> {
    let config_file = conf::load_config("tests/config.test.yaml")?;
    let db = conf::build_db(&config_file).await?;
    let _ = db
        .collection::<mongodb::bson::Document>("filters")
        .delete_many(doc! {"filter_id": filter_id, "catalog": "LSST_alerts"})
        .await;

    Ok(())
}

pub async fn empty_processed_alerts_queue(
    input_queue_name: &str,
    output_queue_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let config = conf::load_config("tests/config.test.yaml")?;
    let mut con = conf::build_redis(&config).await?;
    con.del::<&str, usize>(input_queue_name).await.unwrap();
    con.del::<&str, usize>("{}_temp").await.unwrap();
    con.del::<&str, usize>(output_queue_name).await.unwrap();

    Ok(())
}

#[async_trait::async_trait]
pub trait AlertRandomizerTrait {
    fn default() -> Self;
    fn new() -> Self;
    fn path(self, path: &str) -> Self;
    fn objectid(self, object_id: impl Into<Self::ObjectId>) -> Self;
    fn candid(self, candid: i64) -> Self;
    fn ra(self, ra: f64) -> Self;
    fn dec(self, dec: f64) -> Self;
    fn rand_object_id(self) -> Self;
    fn rand_candid(self) -> Self;
    fn rand_ra(self) -> Self;
    fn rand_dec(self) -> Self;
    fn validate_ra(ra: f64) -> bool {
        ra >= 0.0 && ra <= 360.0
    }
    fn validate_dec(dec: f64) -> bool {
        dec >= -90.0 && dec <= 90.0
    }
    async fn get(self) -> (i64, Self::ObjectId, f64, f64, Vec<u8>);

    fn randomize_i64() -> i64 {
        rand::rng().random_range(0..i64::MAX)
    }
    fn randomize_ra() -> f64 {
        rand::rng().random_range(0.0..360.0)
    }
    fn randomize_dec() -> f64 {
        rand::rng().random_range(-90.0..90.0)
    }
    fn value_to_string(value: &Value) -> String {
        match value {
            Value::String(s) => s.clone(),
            _ => panic!("Not a string"),
        }
    }

    fn value_to_i64(value: &Value) -> i64 {
        match value {
            Value::Long(l) => *l,
            Value::Union(_, box_value) => match box_value.as_ref() {
                Value::Long(l) => *l,
                _ => panic!("Not a long"),
            },
            _ => panic!("Not a long"),
        }
    }
    fn value_to_f64(value: &Value) -> f64 {
        match value {
            Value::Double(d) => *d,
            _ => panic!("Not a double"),
        }
    }
    type ObjectId;
}

#[derive(Clone, Debug)]
pub struct ZtfAlertRandomizer {
    payload: Option<Vec<u8>>,
    schema: Option<Schema>,
    candid: Option<i64>,
    object_id: Option<String>,
    ra: Option<f64>,
    dec: Option<f64>,
}

#[async_trait::async_trait]
impl AlertRandomizerTrait for ZtfAlertRandomizer {
    type ObjectId = String;

    fn default() -> Self {
        let payload = fs::read("tests/data/alerts/ztf/2695378462115010012.avro").unwrap();
        let reader = Reader::new(&payload[..]).unwrap();
        let schema = reader.writer_schema().clone();
        Self {
            payload: Some(payload),
            schema: Some(schema),
            candid: Some(Self::randomize_i64()),
            object_id: Some(Self::randomize_object_id()),
            ra: Some(Self::randomize_ra()),
            dec: Some(Self::randomize_dec()),
        }
    }

    fn new() -> Self {
        Self {
            payload: None,
            schema: None,
            candid: None,
            object_id: None,
            ra: None,
            dec: None,
        }
    }

    fn path(mut self, path: &str) -> Self {
        let payload = fs::read(path).unwrap();
        let reader = Reader::new(&payload[..]).unwrap();
        let schema = reader.writer_schema().clone();
        self.payload = Some(payload);
        self.schema = Some(schema);
        self
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
        match Self::validate_ra(ra) {
            true => self.ra = Some(ra),
            false => panic!("RA must be between 0 and 360"),
        }
        self
    }
    fn dec(mut self, dec: f64) -> Self {
        match Self::validate_dec(dec) {
            true => self.dec = Some(dec),
            false => panic!("Dec must be between -90 and 90"),
        }
        self
    }

    fn rand_object_id(mut self) -> Self {
        self.object_id = Some(Self::randomize_object_id());
        self
    }
    fn rand_candid(mut self) -> Self {
        self.candid = Some(Self::randomize_i64());
        self
    }
    fn rand_ra(mut self) -> Self {
        self.ra = Some(Self::randomize_ra());
        self
    }
    fn rand_dec(mut self) -> Self {
        self.dec = Some(Self::randomize_dec());
        self
    }

    async fn get(self) -> (i64, Self::ObjectId, f64, f64, Vec<u8>) {
        let mut candid = self.candid;
        let mut object_id = self.object_id;
        let mut ra = self.ra;
        let mut dec = self.dec;
        let (payload, schema) = match (self.payload, self.schema) {
            (Some(payload), Some(schema)) => (payload, schema),
            _ => {
                let payload = fs::read("tests/data/alerts/ztf/2695378462115010012.avro").unwrap();
                let reader = Reader::new(&payload[..]).unwrap();
                let schema = reader.writer_schema().clone();
                (payload, schema)
            }
        };

        let reader = Reader::new(&payload[..]).unwrap();
        let value = reader.into_iter().next().unwrap().unwrap();
        let mut record = match value {
            Value::Record(record) => record,
            _ => {
                panic!("Not a record");
            }
        };

        for i in 0..record.len() {
            let (key, value) = &mut record[i];
            if key == "objectId" {
                match object_id {
                    Some(ref id) => *value = Value::String(id.clone()),
                    None => object_id = Some(Self::value_to_string(value)),
                }
            } else if key == "candid" {
                match candid {
                    Some(id) => *value = Value::Long(id),
                    None => candid = Some(Self::value_to_i64(value)),
                }
            } else if key == "candidate" {
                let candidate_record = match value {
                    Value::Record(record) => record,
                    _ => {
                        panic!("Not a record");
                    }
                };
                for i in 0..candidate_record.len() {
                    let (key, value) = &mut candidate_record[i];
                    if key == "ra" {
                        match ra {
                            Some(r) => *value = Value::Double(r),
                            None => ra = Some(Self::value_to_f64(value)),
                        }
                    } else if key == "dec" {
                        match dec {
                            Some(d) => *value = Value::Double(d),
                            None => dec = Some(Self::value_to_f64(value)),
                        }
                    } else if key == "candid" {
                        match candid {
                            Some(c) => *value = Value::Long(c),
                            None => candid = Some(Self::value_to_i64(value)),
                        }
                    }
                }
            }
        }

        let mut writer = Writer::new(&schema, Vec::new());
        let mut new_record = Record::new(writer.schema()).unwrap();
        for (key, value) in record {
            new_record.put(&key, value);
        }

        writer.append(new_record).unwrap();
        let new_payload = writer.into_inner().unwrap();

        (
            candid.unwrap(),
            object_id.unwrap(),
            ra.unwrap(),
            dec.unwrap(),
            new_payload,
        )
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

#[derive(Clone, Debug)]
pub struct LsstAlertRandomizer {
    payload: Option<Vec<u8>>,
    schema_registry: SchemaRegistry,
    candid: Option<i64>,
    object_id: Option<i64>,
    ra: Option<f64>,
    dec: Option<f64>,
}

#[async_trait::async_trait]
impl AlertRandomizerTrait for LsstAlertRandomizer {
    type ObjectId = i64;

    fn default() -> Self {
        let payload = fs::read("tests/data/alerts/lsst/25409136044802067.avro").unwrap();
        Self {
            payload: Some(payload),
            schema_registry: SchemaRegistry::new(LSST_SCHEMA_REGISTRY_URL),
            candid: Some(Self::randomize_i64()),
            object_id: Some(Self::randomize_i64()),
            ra: Some(Self::randomize_ra()),
            dec: Some(Self::randomize_dec()),
        }
    }

    fn new() -> Self {
        let payload = fs::read("tests/data/alerts/lsst/25409136044802067.avro").unwrap();
        Self {
            payload: Some(payload),
            schema_registry: SchemaRegistry::new(LSST_SCHEMA_REGISTRY_URL),
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

    fn path(mut self, path: &str) -> Self {
        let payload = fs::read(path).unwrap();
        self.payload = Some(payload);
        self
    }

    fn ra(mut self, ra: f64) -> Self {
        match Self::validate_ra(ra) {
            true => self.ra = Some(ra),
            false => panic!("RA must be between 0 and 360"),
        }
        self
    }
    fn dec(mut self, dec: f64) -> Self {
        match Self::validate_dec(dec) {
            true => self.dec = Some(dec),
            false => panic!("Dec must be between -90 and 90"),
        }
        self
    }

    fn rand_object_id(mut self) -> Self {
        self.object_id = Some(Self::randomize_i64());
        self
    }
    fn rand_candid(mut self) -> Self {
        self.candid = Some(Self::randomize_i64());
        self
    }
    fn rand_ra(mut self) -> Self {
        self.ra = Some(Self::randomize_ra());
        self
    }
    fn rand_dec(mut self) -> Self {
        self.dec = Some(Self::randomize_dec());
        self
    }

    async fn get(mut self) -> (i64, Self::ObjectId, f64, f64, Vec<u8>) {
        let mut candid = self.candid;
        let mut object_id = self.object_id;
        let mut ra = self.ra;
        let mut dec = self.dec;
        let payload = self
            .payload
            .unwrap_or_else(|| fs::read("tests/data/alerts/lsst/25409136044802067.avro").unwrap());

        let header = payload[0..5].to_vec();

        let magic = header[0];
        if magic != 0_u8 {
            panic!("Not a valid avro file");
        }
        let schema_id = u32::from_be_bytes([header[1], header[2], header[3], header[4]]);

        let schema = self
            .schema_registry
            .get_schema("alert-packet", schema_id)
            .await
            .unwrap();

        let value = from_avro_datum(&schema, &mut &payload[5..], None).unwrap();
        let mut record = match value {
            Value::Record(record) => record,
            _ => {
                panic!("Not a record");
            }
        };
        for i in 0..record.len() {
            let (key, value) = &mut record[i];
            if key == "alertId" {
                match candid {
                    Some(id) => *value = Value::Long(id),
                    None => candid = Some(Self::value_to_i64(value)),
                }
            } else if key == "diaSource" {
                let candidate_record = match value {
                    Value::Record(record) => record,
                    _ => {
                        panic!("Not a record");
                    }
                };
                for i in 0..candidate_record.len() {
                    let (key, value) = &mut candidate_record[i];
                    if key == "diaSourceId" {
                        match object_id {
                            Some(id) => *value = Value::Long(id),
                            None => object_id = Some(Self::value_to_i64(value)),
                        }
                    } else if key == "diaObjectId" {
                        match object_id {
                            Some(id) => *value = Value::Union(1_u32, Box::new(Value::Long(id))),
                            None => object_id = Some(Self::value_to_i64(value)),
                        }
                    } else if key == "ra" {
                        match ra {
                            Some(r) => *value = Value::Double(r),
                            None => ra = Some(Self::value_to_f64(value)),
                        }
                    } else if key == "dec" {
                        match dec {
                            Some(d) => *value = Value::Double(d),
                            None => dec = Some(Self::value_to_f64(value)),
                        }
                    }
                }
            }
        }
        let mut writer = Writer::new(&schema, Vec::new());
        let mut new_record = Record::new(&schema).unwrap();
        for (key, value) in record {
            new_record.put(&key, value);
        }
        writer.append(new_record).unwrap();
        let new_payload = writer.into_inner().unwrap();

        // We find the start idx of the data
        let mut cursor = std::io::Cursor::new(&new_payload);
        let mut buf = [0; 4];
        cursor.read_exact(&mut buf).unwrap();
        if buf != [b'O', b'b', b'j', 1u8] {
            panic!("Not a valid avro file");
        }
        let meta_schema = Schema::map(Schema::Bytes);
        from_avro_datum(&meta_schema, &mut cursor, None).unwrap();
        let mut buf = [0; 16];
        cursor.read_exact(&mut buf).unwrap();
        let mut buf: [u8; 4] = [0; 4];
        cursor.read_exact(&mut buf).unwrap();
        let start_idx = cursor.position();

        // conform with the schema registry-like format
        let new_payload = [&header, &new_payload[start_idx as usize..]].concat();

        (
            candid.unwrap(),
            object_id.unwrap(),
            ra.unwrap(),
            dec.unwrap(),
            new_payload,
        )
    }
}
