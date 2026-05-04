use crate::utils::enums::Survey;
use futures::stream::{self, StreamExt};
use redis::AsyncCommands;
use serde::{de::Deserializer, Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;
use tracing::{debug, error, instrument, warn};

#[derive(thiserror::Error, Debug)]
pub enum CutoutStorageError {
    #[error("Could not list buckets")]
    BucketListFailed,
    #[error("cutout insert failed")]
    CutoutInsertFailed,
    #[error("cutout already exists for candid {0}")]
    CutoutAlreadyExists(i64),
    #[error("cutout retrieve failed")]
    CutoutRetrieveFailed,
    #[error("cutout delete failed")]
    CutoutDeleteFailed,
    #[error("bucket create failed")]
    BucketCreateFailed,
    #[error("json serialization error: {0}")]
    JsonError(#[from] serde_json::Error),
    #[error("cutout compress failed")]
    CutoutCompressFailed,
    #[error("cutout decompress failed")]
    CutoutDecompressFailed,
    #[error("cutouts not found")]
    CutoutsNotFound,
}

#[serde_as]
#[derive(Serialize, Deserialize, Clone)]
pub struct S3AlertCutout {
    pub candid: i64,
    #[serde_as(as = "Base64")]
    #[serde(rename = "cutoutScience")]
    pub cutout_science: Vec<u8>,
    #[serde_as(as = "Base64")]
    #[serde(rename = "cutoutTemplate")]
    pub cutout_template: Vec<u8>,
    #[serde_as(as = "Base64")]
    #[serde(rename = "cutoutDifference")]
    pub cutout_difference: Vec<u8>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct AlertCutout {
    #[serde(rename = "_id")]
    pub candid: i64,
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

fn serialize_cutout<S>(cutout: &[u8], serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    let binary = mongodb::bson::Binary {
        subtype: mongodb::bson::spec::BinarySubtype::Generic,
        bytes: cutout.to_vec(),
    };
    binary.serialize(serializer)
}

impl From<S3AlertCutout> for AlertCutout {
    fn from(s3_cutout: S3AlertCutout) -> Self {
        AlertCutout {
            candid: s3_cutout.candid,
            cutout_science: s3_cutout.cutout_science,
            cutout_template: s3_cutout.cutout_template,
            cutout_difference: s3_cutout.cutout_difference,
        }
    }
}

impl From<&AlertCutout> for S3AlertCutout {
    fn from(cutout: &AlertCutout) -> Self {
        S3AlertCutout {
            candid: cutout.candid,
            cutout_science: cutout.cutout_science.clone(),
            cutout_template: cutout.cutout_template.clone(),
            cutout_difference: cutout.cutout_difference.clone(),
        }
    }
}

fn compress_stamp(data: &[u8]) -> Result<Vec<u8>, CutoutStorageError> {
    zstd::encode_all(data, 0).map_err(|_| CutoutStorageError::CutoutCompressFailed)
}

fn decompress_stamp(data: &[u8]) -> Result<Vec<u8>, CutoutStorageError> {
    zstd::decode_all(data).map_err(|_| CutoutStorageError::CutoutDecompressFailed)
}

fn compress_stamps(cutout: AlertCutout) -> Result<AlertCutout, CutoutStorageError> {
    Ok(AlertCutout {
        candid: cutout.candid,
        cutout_science: compress_stamp(&cutout.cutout_science)?,
        cutout_template: compress_stamp(&cutout.cutout_template)?,
        cutout_difference: compress_stamp(&cutout.cutout_difference)?,
    })
}

fn decompress_stamps(cutout: AlertCutout) -> Result<AlertCutout, CutoutStorageError> {
    Ok(AlertCutout {
        candid: cutout.candid,
        cutout_science: decompress_stamp(&cutout.cutout_science)?,
        cutout_template: decompress_stamp(&cutout.cutout_template)?,
        cutout_difference: decompress_stamp(&cutout.cutout_difference)?,
    })
}

pub struct CutoutCache {
    connection: redis::aio::MultiplexedConnection,
    ttl_seconds: u64,
    key_prefix: String,
}

impl CutoutCache {
    pub fn new(
        connection: redis::aio::MultiplexedConnection,
        ttl_seconds: u64,
        key_prefix: String,
    ) -> Self {
        Self {
            connection,
            ttl_seconds,
            key_prefix,
        }
    }

    fn pack(cutout: &AlertCutout) -> Vec<u8> {
        let s = &cutout.cutout_science;
        let t = &cutout.cutout_template;
        let d = &cutout.cutout_difference;
        let mut buf = Vec::with_capacity(24 + s.len() + t.len() + d.len());
        buf.extend_from_slice(&(s.len() as u64).to_le_bytes());
        buf.extend_from_slice(s);
        buf.extend_from_slice(&(t.len() as u64).to_le_bytes());
        buf.extend_from_slice(t);
        buf.extend_from_slice(&(d.len() as u64).to_le_bytes());
        buf.extend_from_slice(d);
        buf
    }

    fn unpack(candid: i64, buf: &[u8]) -> Option<AlertCutout> {
        let mut pos = 0;

        let s_len = u64::from_le_bytes(buf.get(pos..pos + 8)?.try_into().ok()?) as usize;
        pos += 8;
        let cutout_science = buf.get(pos..pos + s_len)?.to_vec();
        pos += s_len;

        let t_len = u64::from_le_bytes(buf.get(pos..pos + 8)?.try_into().ok()?) as usize;
        pos += 8;
        let cutout_template = buf.get(pos..pos + t_len)?.to_vec();
        pos += t_len;

        let d_len = u64::from_le_bytes(buf.get(pos..pos + 8)?.try_into().ok()?) as usize;
        pos += 8;
        let cutout_difference = buf.get(pos..pos + d_len)?.to_vec();

        Some(AlertCutout {
            candid,
            cutout_science,
            cutout_template,
            cutout_difference,
        })
    }

    fn cache_key(&self, candid: i64) -> String {
        format!("{}:cutout:{}", self.key_prefix, candid)
    }

    async fn set(&self, cutout: &AlertCutout) {
        let key = self.cache_key(cutout.candid);
        let mut conn = self.connection.clone();
        if let Err(e) = conn
            .set_ex::<_, _, ()>(&key, Self::pack(cutout), self.ttl_seconds)
            .await
        {
            warn!("Failed to cache cutout {}: {:?}", cutout.candid, e);
        }
    }

    async fn get(&self, candid: i64) -> Option<AlertCutout> {
        let key = self.cache_key(candid);
        let mut conn = self.connection.clone();
        let bytes: Option<Vec<u8>> = match conn.get(&key).await {
            Ok(b) => b,
            Err(e) => {
                warn!("Cache GET failed for candid {}: {:?}", candid, e);
                return None;
            }
        };
        bytes.and_then(|b| {
            let result = Self::unpack(candid, &b);
            if result.is_none() {
                warn!(
                    "Cache unpack failed for candid {} (corrupt or stale format)",
                    candid
                );
            }
            result
        })
    }

    async fn mget(&self, candids: &[i64]) -> HashMap<i64, AlertCutout> {
        if candids.is_empty() {
            return HashMap::new();
        }
        let keys: Vec<String> = candids.iter().map(|c| self.cache_key(*c)).collect();
        let mut conn = self.connection.clone();
        let values: Vec<Option<Vec<u8>>> = match conn.mget(&keys).await {
            Ok(v) => v,
            Err(e) => {
                warn!("Cache MGET failed: {:?}", e);
                return HashMap::new();
            }
        };
        candids
            .iter()
            .zip(values)
            .filter_map(|(candid, bytes_opt)| {
                let bytes = bytes_opt?;
                let result = Self::unpack(*candid, &bytes);
                if result.is_none() {
                    warn!(
                        "Cache unpack failed for candid {} (corrupt or stale format)",
                        candid
                    );
                }
                Some((*candid, result?))
            })
            .collect()
    }

    async fn set_many(&self, cutouts: &[AlertCutout]) {
        if cutouts.is_empty() {
            return;
        }
        let mut pipe = redis::pipe();
        for cutout in cutouts {
            let key = self.cache_key(cutout.candid);
            pipe.set_ex(&key, Self::pack(cutout), self.ttl_seconds)
                .ignore();
        }
        let mut conn = self.connection.clone();
        if let Err(e) = pipe.query_async::<()>(&mut conn).await {
            warn!("Failed to batch cache {} cutouts: {:?}", cutouts.len(), e);
        }
    }

    async fn del(&self, candid: i64) {
        let key = self.cache_key(candid);
        let mut conn = self.connection.clone();
        if let Err(e) = conn.del::<_, ()>(&key).await {
            warn!("Failed to delete cached cutout {}: {:?}", candid, e);
        }
    }

    async fn del_many(&self, candids: &[i64]) {
        if candids.is_empty() {
            return;
        }
        let keys: Vec<String> = candids.iter().map(|c| self.cache_key(*c)).collect();
        let mut conn = self.connection.clone();
        if let Err(e) = conn.del::<_, ()>(&keys).await {
            warn!("Failed to evict {} cached cutouts: {:?}", candids.len(), e);
        }
    }
}

async fn create_bucket_if_not_exists(
    bucket_name: &str,
    s3_client: &aws_sdk_s3::Client,
) -> Result<(), CutoutStorageError> {
    match s3_client.head_bucket().bucket(bucket_name).send().await {
        Ok(_) => {
            debug!("Bucket {} already exists", bucket_name);
            return Ok(());
        }
        Err(e) => {
            let is_not_found = e
                .as_service_error()
                .map(|se| se.is_not_found())
                .unwrap_or(false);
            if !is_not_found {
                error!("Failed to check if bucket {} exists: {:?}", bucket_name, e);
                return Err(CutoutStorageError::BucketListFailed);
            }
        }
    }

    match s3_client.create_bucket().bucket(bucket_name).send().await {
        Ok(_) => {
            debug!("Created bucket: {}", bucket_name);
            Ok(())
        }
        Err(e) => {
            let already_exists = e
                .as_service_error()
                .map(|se| se.is_bucket_already_exists() || se.is_bucket_already_owned_by_you())
                .unwrap_or(false);
            if already_exists {
                debug!("Bucket {} was created concurrently", bucket_name);
                Ok(())
            } else {
                error!("Failed to create bucket {}: {:?}", bucket_name, e);
                Err(CutoutStorageError::BucketCreateFailed)
            }
        }
    }
}

#[instrument(skip_all, err)]
async fn insert_alert_cutouts(
    cutouts: &AlertCutout,
    bucket_name: &str,
    key_prefix: &str,
    s3_client: &aws_sdk_s3::Client,
) -> Result<(), CutoutStorageError> {
    let candid = cutouts.candid;
    let key = format!("{}/{}.json", key_prefix, candid);

    let encoded = serde_json::to_vec(&S3AlertCutout::from(cutouts))?;
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
    key_prefix: &str,
    s3_client: &aws_sdk_s3::Client,
) -> Result<AlertCutout, CutoutStorageError> {
    let key = format!("{}/{}.json", key_prefix, candid);

    let resp = match s3_client
        .get_object()
        .bucket(bucket_name)
        .key(&key)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            if e.as_service_error()
                .map(|se| se.is_no_such_key())
                .unwrap_or(false)
            {
                return Err(CutoutStorageError::CutoutsNotFound);
            }
            error!("Failed to retrieve cutout for candid {}: {:?}", candid, e);
            return Err(CutoutStorageError::CutoutRetrieveFailed);
        }
    };

    let data = resp
        .body
        .collect()
        .await
        .map_err(|_| CutoutStorageError::CutoutRetrieveFailed)?;
    let bytes = data.into_bytes();
    let s3_cutout_data: S3AlertCutout = serde_json::from_slice(&bytes)?;
    Ok(AlertCutout::from(s3_cutout_data))
}

#[instrument(skip_all, err)]
async fn delete_alert_cutouts(
    candid: i64,
    bucket_name: &str,
    key_prefix: &str,
    s3_client: &aws_sdk_s3::Client,
) -> Result<(), CutoutStorageError> {
    let key = format!("{}/{}.json", key_prefix, candid);

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

pub struct S3CutoutStorage {
    s3_client: aws_sdk_s3::Client,
    bucket_name: String,
    key_prefix: String,
    concurrency_limit: usize,
    cache: CutoutCache,
    compress_stamps: bool,
}

impl S3CutoutStorage {
    #[instrument(skip_all, err)]
    pub async fn new(
        s3_client: aws_sdk_s3::Client,
        bucket_name: String,
        key_prefix: String,
        concurrency_limit: Option<usize>,
        cache: CutoutCache,
        compress_stamps: bool,
    ) -> Result<Self, CutoutStorageError> {
        create_bucket_if_not_exists(&bucket_name, &s3_client).await?;
        Ok(Self {
            s3_client,
            bucket_name,
            key_prefix,
            concurrency_limit: concurrency_limit.unwrap_or(1),
            cache,
            compress_stamps,
        })
    }

    pub async fn evict_from_cache(&self, candids: &[i64]) {
        self.cache.del_many(candids).await;
    }

    #[instrument(skip_all, err)]
    pub async fn insert_cutouts(&self, cutouts: AlertCutout) -> Result<(), CutoutStorageError> {
        let cutouts = if self.compress_stamps {
            tokio::task::spawn_blocking(move || compress_stamps(cutouts))
                .await
                .map_err(|_| CutoutStorageError::CutoutCompressFailed)??
            // first ? propagates the JoinError-mapped outer Result, yielding Result<AlertCutout, E>
            // second ? unwraps the inner Result
        } else {
            cutouts
        };
        insert_alert_cutouts(
            &cutouts,
            &self.bucket_name,
            &self.key_prefix,
            &self.s3_client,
        )
        .await?;
        self.cache.set(&cutouts).await;
        Ok(())
    }

    #[instrument(skip_all, err)]
    pub async fn retrieve_cutouts(
        &self,
        candid: i64,
        populate_cache: bool,
    ) -> Result<AlertCutout, CutoutStorageError> {
        let raw = if let Some(cutout) = self.cache.get(candid).await {
            cutout
        } else {
            let cutout = retrieve_alert_cutouts(
                candid,
                &self.bucket_name,
                &self.key_prefix,
                &self.s3_client,
            )
            .await?;
            if populate_cache {
                self.cache.set(&cutout).await;
            }
            cutout
        };
        if self.compress_stamps {
            tokio::task::spawn_blocking(move || decompress_stamps(raw))
                .await
                .map_err(|_| CutoutStorageError::CutoutDecompressFailed)?
        } else {
            Ok(raw)
        }
    }

    #[instrument(skip_all, err)]
    pub async fn retrieve_multiple_cutouts(
        &self,
        candids: &[i64],
        populate_cache: bool,
    ) -> Result<HashMap<i64, AlertCutout>, CutoutStorageError> {
        let cached = self.cache.mget(candids).await;
        let missing: Vec<i64> = candids
            .iter()
            .filter(|c| !cached.contains_key(*c))
            .copied()
            .collect();

        let mut result = cached;
        if !missing.is_empty() {
            let s3_results = stream::iter(missing.iter().copied())
                .map(|candid| async move {
                    let res = retrieve_alert_cutouts(
                        candid,
                        &self.bucket_name,
                        &self.key_prefix,
                        &self.s3_client,
                    )
                    .await;
                    (candid, res)
                })
                .buffer_unordered(self.concurrency_limit)
                .collect::<Vec<_>>()
                .await;
            let mut to_cache: Vec<AlertCutout> = Vec::new();
            let mut fetch_error: Option<CutoutStorageError> = None;
            for (candid, res) in s3_results {
                match res {
                    Ok(cutout) => {
                        result.insert(candid, cutout.clone());
                        to_cache.push(cutout);
                    }
                    Err(CutoutStorageError::CutoutsNotFound) => {
                        debug!("Cutout with candid {} not found in S3", candid);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to retrieve cutout for candid {} from S3: {:?}",
                            candid, e
                        );
                        fetch_error.get_or_insert(e);
                    }
                }
            }
            if populate_cache {
                self.cache.set_many(&to_cache).await;
            }
            if let Some(e) = fetch_error {
                return Err(e);
            }
        }

        if self.compress_stamps {
            tokio::task::spawn_blocking(move || {
                let mut decompressed = HashMap::with_capacity(result.len());
                for (candid, cutout) in result {
                    decompressed.insert(candid, decompress_stamps(cutout)?);
                }
                Ok(decompressed)
            })
            .await
            .map_err(|_| CutoutStorageError::CutoutDecompressFailed)?
        } else {
            Ok(result)
        }
    }

    #[instrument(skip_all, err)]
    pub async fn delete_cutouts(&self, candid: i64) -> Result<(), CutoutStorageError> {
        self.cache.del(candid).await;
        delete_alert_cutouts(candid, &self.bucket_name, &self.key_prefix, &self.s3_client).await
    }
}

pub struct MongoCutoutStorage {
    collection: mongodb::Collection<AlertCutout>,
}

impl MongoCutoutStorage {
    pub fn new(db: mongodb::Database, survey: &Survey) -> Self {
        let collection_name = format!("{}_alerts_cutouts", survey);
        let collection = db.collection::<AlertCutout>(&collection_name);
        Self { collection }
    }

    #[instrument(skip_all, err)]
    pub async fn insert_cutouts(&self, cutouts: AlertCutout) -> Result<(), CutoutStorageError> {
        let candid = cutouts.candid;
        match self.collection.insert_one(cutouts).await {
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
    pub async fn retrieve_cutouts(
        &self,
        candid: i64,
        _populate_cache: bool,
    ) -> Result<AlertCutout, CutoutStorageError> {
        let filter = mongodb::bson::doc! { "_id": candid };
        match self.collection.find_one(filter).await {
            Ok(Some(cutout)) => Ok(cutout),
            Ok(None) => Err(CutoutStorageError::CutoutsNotFound),
            Err(e) => {
                error!("Failed to retrieve cutout for candid {}: {:?}", candid, e);
                Err(CutoutStorageError::CutoutRetrieveFailed)
            }
        }
    }

    #[instrument(skip_all, err)]
    pub async fn retrieve_multiple_cutouts(
        &self,
        candids: &[i64],
        _populate_cache: bool,
    ) -> Result<HashMap<i64, AlertCutout>, CutoutStorageError> {
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
                    cutouts.insert(c.candid, c);
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
    pub async fn delete_cutouts(&self, candid: i64) -> Result<(), CutoutStorageError> {
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
    pub async fn insert_cutouts(&self, cutouts: AlertCutout) -> Result<(), CutoutStorageError> {
        match self {
            CutoutStorage::S3(s) => s.insert_cutouts(cutouts).await,
            CutoutStorage::Mongo(s) => s.insert_cutouts(cutouts).await,
        }
    }

    pub async fn retrieve_cutouts(
        &self,
        candid: i64,
        populate_cache: bool,
    ) -> Result<AlertCutout, CutoutStorageError> {
        match self {
            CutoutStorage::S3(s) => s.retrieve_cutouts(candid, populate_cache).await,
            CutoutStorage::Mongo(s) => s.retrieve_cutouts(candid, populate_cache).await,
        }
    }

    pub async fn retrieve_multiple_cutouts(
        &self,
        candids: &[i64],
        populate_cache: bool,
    ) -> Result<HashMap<i64, AlertCutout>, CutoutStorageError> {
        match self {
            CutoutStorage::S3(s) => s.retrieve_multiple_cutouts(candids, populate_cache).await,
            CutoutStorage::Mongo(s) => s.retrieve_multiple_cutouts(candids, populate_cache).await,
        }
    }

    pub async fn delete_cutouts(&self, candid: i64) -> Result<(), CutoutStorageError> {
        match self {
            CutoutStorage::S3(s) => s.delete_cutouts(candid).await,
            CutoutStorage::Mongo(s) => s.delete_cutouts(candid).await,
        }
    }

    pub async fn evict_from_cache(&self, candids: &[i64]) {
        if let CutoutStorage::S3(s) = self {
            s.evict_from_cache(candids).await;
        }
    }

    pub async fn from_s3(
        s3_client: aws_sdk_s3::Client,
        bucket_name: String,
        key_prefix: String,
        concurrency_limit: Option<usize>,
        cache: CutoutCache,
        compress_stamps: bool,
    ) -> Result<Self, CutoutStorageError> {
        let s3_storage = S3CutoutStorage::new(
            s3_client,
            bucket_name,
            key_prefix,
            concurrency_limit,
            cache,
            compress_stamps,
        )
        .await?;
        Ok(CutoutStorage::S3(s3_storage))
    }

    pub async fn from_mongo(db: mongodb::Database, survey: &Survey) -> Self {
        CutoutStorage::Mongo(MongoCutoutStorage::new(db, survey))
    }
}
