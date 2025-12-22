use futures::stream::{self, StreamExt};
use std::collections::HashMap;
use tracing::{debug, instrument, warn};

#[derive(thiserror::Error, Debug)]
pub enum CutoutStorageError {
    #[error("Could not list buckets")]
    BucketListFailed,
    #[error("Could not create bucket")]
    BucketCreateFailed,
    #[error("cutout insert failed")]
    CutoutInsertFailed,
    #[error("cutout retrieve failed")]
    CutoutRetrieveFailed,
    #[error("bincode serialization error: {0}")]
    BincodeError(#[from] bincode::error::EncodeError),
    #[error("bincode deserialization error: {0}")]
    BincodeDecodeError(#[from] bincode::error::DecodeError),
}

#[derive(serde::Serialize, serde::Deserialize, Clone)]
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

pub struct CutoutStorage {
    s3_client: aws_sdk_s3::Client,
    bucket_name: String,
    // Limit concurrency to avoid overwhelming the S3-compatible service
    concurrency_limit: usize,
}

impl CutoutStorage {
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

// have a function that creates a bucket, if it doesn't exist yet
async fn create_bucket_if_not_exists(
    bucket_name: &str,
    s3_client: &aws_sdk_s3::Client,
) -> Result<(), CutoutStorageError> {
    let buckets = match s3_client.list_buckets().send().await {
        Ok(b) => b,
        Err(e) => {
            println!("Failed to list buckets: {:?}", e);
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
            println!("Failed to insert cutout for candid {}: {:?}", candid, e);
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

impl CutoutStorage {
    #[instrument(skip_all, err)]
    pub async fn insert_cutouts(
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
    pub async fn retrieve_cutouts(&self, candid: i64) -> Result<AlertCutout, CutoutStorageError> {
        retrieve_alert_cutouts(candid, &self.bucket_name, &self.s3_client).await
    }

    #[instrument(skip_all, err)]
    pub async fn retrieve_multiple_cutouts(
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
}
