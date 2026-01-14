use crate::utils::enums::Survey;
use futures::stream::{self, StreamExt};
use serde::{de::Deserializer, Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, instrument, warn};

#[derive(thiserror::Error, Debug)]
pub enum CutoutStorageError {
    #[error("Could not list buckets")]
    BucketListFailed,
    #[error("Could not create bucket")]
    BucketCreateFailed,
    #[error("cutout insert failed")]
    CutoutInsertFailed,
    #[error("cutout already exists for candid {0}")]
    CutoutAlreadyExists(i64),
    #[error("cutout retrieve failed")]
    CutoutRetrieveFailed,
    #[error("cutout delete failed")]
    CutoutDeleteFailed,
    #[error("bincode serialization error: {0}")]
    BincodeError(#[from] bincode::error::EncodeError),
    #[error("bincode deserialization error: {0}")]
    BincodeDecodeError(#[from] bincode::error::DecodeError),
}

#[derive(Serialize, Deserialize, Clone)]
pub struct AlertCutout {
    pub candid: i64,
    pub object_id: String,
    #[serde(with = "serde_bytes")]
    pub science: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub template: Vec<u8>,
    #[serde(with = "serde_bytes")]
    pub difference: Vec<u8>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct MongoAlertCutout {
    #[serde(rename = "_id")]
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    #[serde(rename = "cutoutScience")]
    #[serde(serialize_with = "serialize_cutout")]
    #[serde(deserialize_with = "deserialize_cutout")]
    pub cutout_science: Vec<u8>,
    #[serde(serialize_with = "serialize_cutout")]
    #[serde(deserialize_with = "deserialize_cutout")]
    #[serde(rename = "cutoutTemplate")]
    pub cutout_template: Vec<u8>,
    #[serde(serialize_with = "serialize_cutout")]
    #[serde(deserialize_with = "deserialize_cutout")]
    #[serde(rename = "cutoutDifference")]
    pub cutout_difference: Vec<u8>,
}

fn deserialize_cutout<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
where
    D: Deserializer<'de>,
{
    let binary = <mongodb::bson::Binary as Deserialize>::deserialize(deserializer)?;
    Ok(binary.bytes)
}

fn serialize_cutout<S>(cutout: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let binary = mongodb::bson::Binary {
        subtype: mongodb::bson::spec::BinarySubtype::Generic,
        bytes: cutout.clone(),
    };
    binary.serialize(serializer)
}

// implement From for MongoAlertCutout to AlertCutout
impl From<MongoAlertCutout> for AlertCutout {
    fn from(mongo_cutout: MongoAlertCutout) -> Self {
        AlertCutout {
            candid: mongo_cutout.candid,
            object_id: mongo_cutout.object_id,
            science: mongo_cutout.cutout_science,
            template: mongo_cutout.cutout_template,
            difference: mongo_cutout.cutout_difference,
        }
    }
}

// have a function that creates a bucket, if it doesn't exist yet
async fn create_bucket_if_not_exists(
    bucket_name: &str,
    s3_client: &aws_sdk_s3::Client,
) -> Result<(), CutoutStorageError> {
    let buckets = match s3_client.list_buckets().send().await {
        Ok(b) => b,
        Err(e) => {
            error!("Failed to list buckets: {:?}", e);
            return Err(CutoutStorageError::BucketListFailed);
        }
    };

    let bucket_exists = buckets
        .buckets()
        .iter()
        .any(|b| b.name().unwrap_or_default() == bucket_name);

    if bucket_exists {
        debug!("Bucket {} already exists", bucket_name);
        return Ok(());
    }
    // we may have some concurrency issues here, so let's just try to create the bucket and ignore errors if it already exists
    match s3_client.create_bucket().bucket(bucket_name).send().await {
        Ok(_) => {
            debug!("Created bucket: {}", bucket_name);
        }
        Err(e) => {
            warn!(
                "Bucket {} may already exist or failed to create: {:?}",
                bucket_name, e
            );
        }
    };

    Ok(())
}

#[instrument(skip_all, err)]
async fn insert_alert_cutouts(
    candid: i64,
    object_id: &str,
    cutout_science: Vec<u8>,
    cutout_template: Vec<u8>,
    cutout_difference: Vec<u8>,
    // cutout_storage: &CutoutStorage,
    bucket_name: &str,
    s3_client: &aws_sdk_s3::Client,
) -> Result<(), CutoutStorageError> {
    let alert_cutout = AlertCutout {
        candid,
        object_id: object_id.to_string(),
        science: cutout_science,
        template: cutout_template,
        difference: cutout_difference,
    };

    let key = format!("{}.cutouts", alert_cutout.candid);

    // Serialize using bincode
    let encoded = bincode::serde::encode_to_vec(alert_cutout, bincode::config::standard())?;
    let body = aws_sdk_s3::primitives::ByteStream::from(encoded);

    match s3_client
        .put_object()
        .bucket(bucket_name)
        .key(&key)
        .body(body)
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("Failed to insert cutout for candid {}: {:?}", candid, e);
            Err(CutoutStorageError::CutoutInsertFailed)
        }
    }
}

#[instrument(skip_all, err)]
async fn retrieve_alert_cutouts(
    candid: i64,
    bucket_name: &str,
    s3_client: &aws_sdk_s3::Client,
) -> Result<AlertCutout, CutoutStorageError> {
    let key = format!("{}.cutouts", candid);

    let resp = s3_client
        .get_object()
        .bucket(bucket_name)
        .key(&key)
        .send()
        .await
        .map_err(|_| CutoutStorageError::CutoutRetrieveFailed)?;

    let data = resp
        .body
        .collect()
        .await
        .map_err(|_| CutoutStorageError::CutoutRetrieveFailed)?;
    let bytes = data.into_bytes();
    let (cutout_data, _): (AlertCutout, _) =
        bincode::serde::decode_from_slice(&bytes, bincode::config::standard())?;
    Ok(cutout_data)
}

#[instrument(skip_all, err)]
async fn delete_alert_cutouts(
    candid: i64,
    bucket_name: &str,
    s3_client: &aws_sdk_s3::Client,
) -> Result<(), CutoutStorageError> {
    let key = format!("{}.cutouts", candid);

    match s3_client
        .delete_object()
        .bucket(bucket_name)
        .key(&key)
        .send()
        .await
    {
        Ok(_) => Ok(()),
        Err(e) => {
            error!("Failed to delete cutout for candid {}: {:?}", candid, e);
            Err(CutoutStorageError::CutoutDeleteFailed)
        }
    }
}

#[async_trait::async_trait]
pub trait CutoutStorageBackend {
    async fn insert_cutouts(
        &self,
        candid: i64,
        object_id: &str,
        cutout_science: Vec<u8>,
        cutout_template: Vec<u8>,
        cutout_difference: Vec<u8>,
    ) -> Result<(), CutoutStorageError>;

    async fn retrieve_cutouts(&self, candid: i64) -> Result<AlertCutout, CutoutStorageError>;

    async fn retrieve_multiple_cutouts(
        &self,
        candids: &[i64],
    ) -> Result<HashMap<i64, AlertCutout>, CutoutStorageError>;

    async fn delete_cutouts(&self, candid: i64) -> Result<(), CutoutStorageError>;
}

pub struct S3CutoutStorage {
    s3_client: aws_sdk_s3::Client,
    bucket_name: String,
    // Limit concurrency to avoid overwhelming the S3-compatible service
    concurrency_limit: usize,
}

impl S3CutoutStorage {
    #[instrument(skip_all, err)]
    pub async fn new(
        s3_client: aws_sdk_s3::Client,
        bucket_name: String,
        concurrency_limit: Option<usize>,
    ) -> Result<Self, CutoutStorageError> {
        // Ensure the bucket exists
        create_bucket_if_not_exists(&bucket_name, &s3_client).await?;
        Ok(Self {
            s3_client,
            bucket_name,
            concurrency_limit: concurrency_limit.unwrap_or(1),
        })
    }
}

#[async_trait::async_trait]
impl CutoutStorageBackend for S3CutoutStorage {
    #[instrument(skip_all, err)]
    async fn insert_cutouts(
        &self,
        candid: i64,
        object_id: &str,
        cutout_science: Vec<u8>,
        cutout_template: Vec<u8>,
        cutout_difference: Vec<u8>,
    ) -> Result<(), CutoutStorageError> {
        insert_alert_cutouts(
            candid,
            object_id,
            cutout_science,
            cutout_template,
            cutout_difference,
            &self.bucket_name,
            &self.s3_client,
        )
        .await
    }

    #[instrument(skip_all, err)]
    async fn retrieve_cutouts(&self, candid: i64) -> Result<AlertCutout, CutoutStorageError> {
        retrieve_alert_cutouts(candid, &self.bucket_name, &self.s3_client).await
    }

    #[instrument(skip_all, err)]
    async fn retrieve_multiple_cutouts(
        &self,
        candids: &[i64],
    ) -> Result<HashMap<i64, AlertCutout>, CutoutStorageError> {
        let mut cutouts = HashMap::with_capacity(candids.len());
        let results = stream::iter(candids.iter().copied())
            .map(|candid| async move {
                let res = retrieve_alert_cutouts(candid, &self.bucket_name, &self.s3_client).await;
                (candid, res)
            })
            .buffer_unordered(self.concurrency_limit)
            .collect::<Vec<_>>()
            .await;
        for (candid, res) in results {
            match res {
                Ok(cutout) => {
                    cutouts.insert(candid, cutout);
                }
                Err(_) => {
                    debug!("Cutout with candid {} not found", candid);
                }
            }
        }

        Ok(cutouts)
    }

    #[instrument(skip_all, err)]
    async fn delete_cutouts(&self, candid: i64) -> Result<(), CutoutStorageError> {
        delete_alert_cutouts(candid, &self.bucket_name, &self.s3_client).await
    }
}

pub struct MongoCutoutStorage {
    // Placeholder for MongoDB client and collection
    collection: mongodb::Collection<MongoAlertCutout>,
}

impl MongoCutoutStorage {
    pub fn new(db: mongodb::Database, survey: &Survey) -> Self {
        let collection_name = format!("{}_alerts_cutouts", survey);
        let collection = db.collection::<MongoAlertCutout>(&collection_name);
        Self { collection }
    }
}

#[async_trait::async_trait]
impl CutoutStorageBackend for MongoCutoutStorage {
    #[instrument(skip_all, err)]
    async fn insert_cutouts(
        &self,
        candid: i64,
        object_id: &str,
        cutout_science: Vec<u8>,
        cutout_template: Vec<u8>,
        cutout_difference: Vec<u8>,
    ) -> Result<(), CutoutStorageError> {
        let alert_cutout = MongoAlertCutout {
            candid,
            object_id: object_id.to_string(),
            cutout_science,
            cutout_template,
            cutout_difference,
        };

        match self.collection.insert_one(alert_cutout).await {
            Ok(_) => Ok(()),
            Err(e) => match *e.kind {
                mongodb::error::ErrorKind::Write(mongodb::error::WriteFailure::WriteError(
                    write_error,
                )) if write_error.code == 11000 => {
                    Err(CutoutStorageError::CutoutAlreadyExists(candid))
                }
                _ => {
                    error!("Failed to insert cutout for candid {}: {:?}", candid, e);
                    Err(CutoutStorageError::CutoutInsertFailed)
                }
            },
        }
    }

    #[instrument(skip_all, err)]
    async fn retrieve_cutouts(&self, candid: i64) -> Result<AlertCutout, CutoutStorageError> {
        let filter = mongodb::bson::doc! { "_id": candid };
        match self.collection.find_one(filter).await {
            Ok(Some(cutout)) => Ok(cutout.into()),
            Ok(None) => Err(CutoutStorageError::CutoutRetrieveFailed),
            Err(e) => {
                error!("Failed to retrieve cutout for candid {}: {:?}", candid, e);
                Err(CutoutStorageError::CutoutRetrieveFailed)
            }
        }
    }

    #[instrument(skip_all, err)]
    async fn retrieve_multiple_cutouts(
        &self,
        candids: &[i64],
    ) -> Result<HashMap<i64, AlertCutout>, CutoutStorageError> {
        error!(
            "Retrieving multiple cutouts for candids: {:?} (from MongoDB)",
            candids
        );
        let filter = mongodb::bson::doc! { "_id": { "$in": candids } };
        let mut cursor = self
            .collection
            .find(filter)
            .await
            .map_err(|_| CutoutStorageError::CutoutRetrieveFailed)?;

        let mut cutouts = HashMap::new();
        while let Some(cutout) = cursor.next().await {
            match cutout {
                Ok(c) => {
                    cutouts.insert(c.candid, c.into());
                }
                Err(e) => {
                    warn!(
                        "Failed to retrieve a cutout (when retrieving a batch): {:?}",
                        e
                    );
                }
            }
        }
        Ok(cutouts)
    }

    #[instrument(skip_all, err)]
    async fn delete_cutouts(&self, candid: i64) -> Result<(), CutoutStorageError> {
        let filter = mongodb::bson::doc! { "_id": candid };
        match self.collection.delete_one(filter).await {
            Ok(_) => Ok(()),
            Err(e) => {
                error!("Failed to delete cutout for candid {}: {:?}", candid, e);
                Err(CutoutStorageError::CutoutDeleteFailed)
            }
        }
    }
}

pub enum CutoutStorage {
    S3(S3CutoutStorage),
    Mongo(MongoCutoutStorage),
}

impl CutoutStorage {
    pub async fn insert_cutouts(
        &self,
        candid: i64,
        object_id: &str,
        cutout_science: Vec<u8>,
        cutout_template: Vec<u8>,
        cutout_difference: Vec<u8>,
    ) -> Result<(), CutoutStorageError> {
        match self {
            CutoutStorage::S3(storage) => {
                storage
                    .insert_cutouts(
                        candid,
                        object_id,
                        cutout_science,
                        cutout_template,
                        cutout_difference,
                    )
                    .await
            }
            CutoutStorage::Mongo(storage) => {
                storage
                    .insert_cutouts(
                        candid,
                        object_id,
                        cutout_science,
                        cutout_template,
                        cutout_difference,
                    )
                    .await
            }
        }
    }
    pub async fn retrieve_cutouts(&self, candid: i64) -> Result<AlertCutout, CutoutStorageError> {
        match self {
            CutoutStorage::S3(storage) => storage.retrieve_cutouts(candid).await,
            CutoutStorage::Mongo(storage) => storage.retrieve_cutouts(candid).await,
        }
    }
    pub async fn retrieve_multiple_cutouts(
        &self,
        candids: &[i64],
    ) -> Result<HashMap<i64, AlertCutout>, CutoutStorageError> {
        match self {
            CutoutStorage::S3(storage) => storage.retrieve_multiple_cutouts(candids).await,
            CutoutStorage::Mongo(storage) => storage.retrieve_multiple_cutouts(candids).await,
        }
    }
    pub async fn delete_cutouts(&self, candid: i64) -> Result<(), CutoutStorageError> {
        match self {
            CutoutStorage::S3(storage) => storage.delete_cutouts(candid).await,
            CutoutStorage::Mongo(storage) => storage.delete_cutouts(candid).await,
        }
    }
    pub async fn from_s3(
        s3_client: aws_sdk_s3::Client,
        bucket_name: String,
        concurrency_limit: Option<usize>,
    ) -> Result<Self, CutoutStorageError> {
        let s3_storage = S3CutoutStorage::new(s3_client, bucket_name, concurrency_limit).await?;
        Ok(CutoutStorage::S3(s3_storage))
    }
    pub async fn from_mongo(db: mongodb::Database, survey: &Survey) -> Self {
        let mongo_storage = MongoCutoutStorage::new(db, survey);
        CutoutStorage::Mongo(mongo_storage)
    }
}
