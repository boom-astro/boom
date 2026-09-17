//! Schema for the collection holding CIDER fusion-model embeddings.
//!
//! The vectors come from `data/models/cider_fusion_plus_embedding.onnx`, whose
//! `fusion_embedding` output is 384 floats. The model's final operation divides
//! by the L2 norm, so the vectors are unit length — which is why the default
//! metric is `COSINE` (equivalent to inner product for normalized vectors).
//!
//! The primary key is `object_id`, so the collection holds **one vector per
//! astronomical object, not per alert**. Writing an object that already exists
//! replaces its vector, so the collection tracks each object's most recently
//! ingested embedding. `candid` and `jd` record which alert that was.

use prost::Message;
use tracing::{info, instrument, warn};

use super::client::{check_status, MilvusClient, MilvusError};
use super::proto::common::KeyValuePair;
use super::proto::milvus::{
    CreateCollectionRequest, CreateIndexRequest, DescribeCollectionRequest, HasCollectionRequest,
    LoadCollectionRequest,
};
use super::proto::schema::{CollectionSchema, DataType, FieldSchema};

/// Primary key: the survey object identifier (e.g. a ZTF `objectId`).
pub const FIELD_OBJECT_ID: &str = "object_id";
/// The 384-dimensional fusion embedding.
pub const FIELD_EMBEDDING: &str = "embedding";
/// Candid of the alert the stored embedding was computed from.
pub const FIELD_CANDID: &str = "candid";
/// Julian date of that alert.
pub const FIELD_JD: &str = "jd";

const INDEX_NAME: &str = "embedding_idx";

/// Every field an upsert sends. A collection missing any of these cannot accept
/// BOOM's writes.
const REQUIRED_FIELDS: [&str; 4] = [FIELD_OBJECT_ID, FIELD_EMBEDDING, FIELD_CANDID, FIELD_JD];

impl MilvusClient {
    /// Create the embeddings collection, its vector index, and load it into
    /// memory. Does nothing if the collection already exists.
    ///
    /// Returns `true` if the collection was created by this call.
    #[instrument(skip_all, err, fields(collection = %self.config().collection.name))]
    pub async fn ensure_embedding_collection(&mut self) -> Result<bool, MilvusError> {
        let config = self.config().clone();
        let collection_name = config.collection.name.clone();

        if self.has_collection(&collection_name).await? {
            info!(collection = %collection_name, "collection already exists");
            // A collection that exists is not necessarily one we can write to:
            // if it was created by hand or by another project, its schema may
            // not match ours and every upsert will fail. Warn rather than
            // abort, so a mismatch does not take down alert ingestion.
            self.warn_on_schema_mismatch().await;
            return Ok(false);
        }

        let schema = embedding_collection_schema(
            &collection_name,
            config.collection.dim,
            config.collection.object_id_max_length,
        );

        let request = CreateCollectionRequest {
            db_name: config.database.clone(),
            collection_name: collection_name.clone(),
            // The field is a serialized schema.CollectionSchema, not a nested message.
            schema: schema.encode_to_vec(),
            ..Default::default()
        };

        let status = self
            .service()
            .create_collection(request)
            .await?
            .into_inner();
        check_status(Some(&status), "CreateCollection")?;

        info!(collection = %collection_name, dim = config.collection.dim, "created collection");

        self.create_embedding_index().await?;
        self.load_collection().await?;

        Ok(true)
    }

    #[instrument(skip_all, err)]
    pub async fn has_collection(&mut self, collection_name: &str) -> Result<bool, MilvusError> {
        let db_name = self.config().database.clone();
        let request = HasCollectionRequest {
            db_name,
            collection_name: collection_name.to_string(),
            ..Default::default()
        };

        let response = self.service().has_collection(request).await?.into_inner();
        check_status(response.status.as_ref(), "HasCollection")?;

        Ok(response.value)
    }

    /// Warn if the collection is missing any field [`super::insert`] writes.
    async fn warn_on_schema_mismatch(&mut self) {
        let collection = self.config().collection.name.clone();

        let schema = match self.describe_collection().await {
            Ok(schema) => schema,
            Err(error) => {
                warn!(%collection, %error, "could not verify collection schema");
                return;
            }
        };

        let present: Vec<&str> = schema.fields.iter().map(|f| f.name.as_str()).collect();
        let missing: Vec<&str> = REQUIRED_FIELDS
            .iter()
            .copied()
            .filter(|name| !present.contains(name))
            .collect();

        if missing.is_empty() {
            return;
        }

        warn!(
            %collection,
            missing = %missing.join(", "),
            found = %present.join(", "),
            "collection is missing fields BOOM writes to Milvus, so every upsert will fail."
        );
    }

    /// The schema the server actually holds for the configured collection.
    ///
    /// Useful when a collection was provisioned by an older version of BOOM (or
    /// by hand): the field names here must match what [`super::insert`] sends,
    /// or every upsert fails with "fieldName ... not exist in collection schema".
    #[instrument(skip_all, err)]
    pub async fn describe_collection(&mut self) -> Result<CollectionSchema, MilvusError> {
        let config = self.config().clone();
        let request = DescribeCollectionRequest {
            db_name: config.database.clone(),
            collection_name: config.collection.name.clone(),
            ..Default::default()
        };

        let response = self
            .service()
            .describe_collection(request)
            .await?
            .into_inner();
        check_status(response.status.as_ref(), "DescribeCollection")?;

        response
            .schema
            .ok_or(MilvusError::MissingStatus("DescribeCollection"))
    }

    /// Build the vector index on the embedding field.
    #[instrument(skip_all, err)]
    pub async fn create_embedding_index(&mut self) -> Result<(), MilvusError> {
        let config = self.config().clone();

        let mut extra_params = vec![
            kv("index_type", &config.collection.index_type),
            kv("metric_type", &config.collection.metric_type),
        ];

        // HNSW needs build parameters; other index types use their defaults.
        if config.collection.index_type.eq_ignore_ascii_case("HNSW") {
            extra_params.push(kv("params", r#"{"M":16,"efConstruction":200}"#));
        }

        let request = CreateIndexRequest {
            db_name: config.database.clone(),
            collection_name: config.collection.name.clone(),
            field_name: FIELD_EMBEDDING.to_string(),
            index_name: INDEX_NAME.to_string(),
            extra_params,
            ..Default::default()
        };

        let status = self.service().create_index(request).await?.into_inner();
        check_status(Some(&status), "CreateIndex")?;

        info!(
            index_type = %config.collection.index_type,
            metric_type = %config.collection.metric_type,
            "created vector index"
        );

        Ok(())
    }

    /// Load the collection into memory. Milvus refuses searches until this
    /// has been done at least once.
    #[instrument(skip_all, err)]
    pub async fn load_collection(&mut self) -> Result<(), MilvusError> {
        let config = self.config().clone();

        let request = LoadCollectionRequest {
            db_name: config.database.clone(),
            collection_name: config.collection.name.clone(),
            ..Default::default()
        };

        let status = self.service().load_collection(request).await?.into_inner();
        check_status(Some(&status), "LoadCollection")?;

        info!(collection = %config.collection.name, "load requested");

        Ok(())
    }
}

/// The collection schema: one row per object, keyed by `object_id`.
pub fn embedding_collection_schema(
    name: &str,
    dim: i64,
    object_id_max_length: i64,
) -> CollectionSchema {
    CollectionSchema {
        name: name.to_string(),
        description: "CIDER fusion model embeddings, one per object".to_string(),
        fields: vec![
            FieldSchema {
                name: FIELD_OBJECT_ID.to_string(),
                description: "Survey object identifier".to_string(),
                data_type: DataType::VarChar as i32,
                is_primary_key: true,
                // Keys are supplied by BOOM, never generated by Milvus, so that
                // re-ingesting an object updates its row instead of duplicating it.
                auto_id: false,
                type_params: vec![kv("max_length", &object_id_max_length.to_string())],
                ..Default::default()
            },
            FieldSchema {
                name: FIELD_EMBEDDING.to_string(),
                description: "L2-normalized fusion embedding".to_string(),
                data_type: DataType::FloatVector as i32,
                type_params: vec![kv("dim", &dim.to_string())],
                ..Default::default()
            },
            FieldSchema {
                name: FIELD_CANDID.to_string(),
                description: "Candid of the alert this embedding came from".to_string(),
                data_type: DataType::Int64 as i32,
                ..Default::default()
            },
            FieldSchema {
                name: FIELD_JD.to_string(),
                description: "Julian date of that alert".to_string(),
                data_type: DataType::Double as i32,
                ..Default::default()
            },
        ],
        ..Default::default()
    }
}

fn kv(key: &str, value: &str) -> KeyValuePair {
    KeyValuePair {
        key: key.to_string(),
        value: value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_id_is_non_auto_primary_key() {
        let schema = embedding_collection_schema("test_collection", 384, 64);

        let pk = schema
            .fields
            .iter()
            .find(|f| f.is_primary_key)
            .expect("schema must define a primary key");

        assert_eq!(pk.name, FIELD_OBJECT_ID);
        assert_eq!(pk.data_type, DataType::VarChar as i32);
        // auto_id would make every write insert a new row rather than replace.
        assert!(!pk.auto_id);
    }

    #[test]
    fn embedding_field_carries_the_configured_dimension() {
        let schema = embedding_collection_schema("test_collection", 384, 64);

        let embedding = schema
            .fields
            .iter()
            .find(|f| f.name == FIELD_EMBEDDING)
            .expect("schema must define an embedding field");

        assert_eq!(embedding.data_type, DataType::FloatVector as i32);
        let dim = embedding
            .type_params
            .iter()
            .find(|p| p.key == "dim")
            .expect("embedding field must declare a dim");
        assert_eq!(dim.value, "384");
    }
}
