//! Writing CIDER fusion embeddings into the collection.
//!
//! Uses Milvus's `Upsert` RPC rather than `Insert`: the primary key is the
//! survey `object_id` and is supplied by BOOM (never auto-generated), so
//! upsert gives the "one vector per object, latest wins" semantics described
//! in [`super::collection`]. Re-ingesting an object replaces its row instead
//! of accumulating duplicates.
//!
//! Milvus is column-oriented on the wire: a batch of N rows is sent as one
//! [`FieldData`] per column, each holding N values in row order. The embedding
//! column is a single flat `FloatArray` of `N * dim` floats plus the `dim`.

use tracing::{debug, instrument};

use super::client::{check_status, MilvusClient, MilvusError};
use super::collection::{FIELD_CANDID, FIELD_EMBEDDING, FIELD_JD, FIELD_OBJECT_ID};
use super::proto::milvus::UpsertRequest;
use super::proto::schema::{
    field_data::Field, scalar_field, vector_field, DataType, DoubleArray, FieldData, FloatArray,
    LongArray, ScalarField, StringArray, VectorField,
};

/// One embedding to be written — becomes one row in the collection.
#[derive(Debug, Clone)]
pub struct EmbeddingRow {
    /// Survey object identifier, the collection's primary key.
    pub object_id: String,
    /// The L2-normalized fusion embedding; its length must equal the
    /// collection's configured `dim`.
    pub embedding: Vec<f32>,
    /// Candid of the alert this embedding was computed from.
    pub candid: i64,
    /// Julian date of that alert.
    pub jd: f64,
}

impl MilvusClient {
    /// Upsert a batch of fusion embeddings, one row per object. Existing rows
    /// with the same `object_id` are replaced. Returns the number of rows
    /// Milvus reports as upserted.
    ///
    /// Every embedding must have exactly `collection.dim` floats; a mismatch
    /// is rejected before anything is sent, since Milvus would reject the whole
    /// batch anyway.
    #[instrument(
        skip_all,
        err,
        fields(collection = %self.config().collection.name, rows = rows.len())
    )]
    pub async fn upsert_embeddings(&mut self, rows: &[EmbeddingRow]) -> Result<u64, MilvusError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let config = self.config().clone();
        let dim = config.collection.dim;

        // Validate up front: one bad row would otherwise fail the whole batch
        // server-side with a far less specific error.
        for row in rows {
            if row.embedding.len() as i64 != dim {
                return Err(MilvusError::DimensionMismatch {
                    expected: dim,
                    got: row.embedding.len(),
                });
            }
        }

        // Transpose the rows into columns (Milvus's wire format).
        let object_ids: Vec<String> = rows.iter().map(|r| r.object_id.clone()).collect();
        let embeddings: Vec<f32> = rows
            .iter()
            .flat_map(|r| r.embedding.iter().copied())
            .collect();
        let candids: Vec<i64> = rows.iter().map(|r| r.candid).collect();
        let jds: Vec<f64> = rows.iter().map(|r| r.jd).collect();

        let fields_data = vec![
            string_field(FIELD_OBJECT_ID, object_ids),
            float_vector_field(FIELD_EMBEDDING, dim, embeddings),
            long_field(FIELD_CANDID, candids),
            double_field(FIELD_JD, jds),
        ];

        let request = UpsertRequest {
            db_name: config.database.clone(),
            collection_name: config.collection.name.clone(),
            fields_data,
            num_rows: rows.len() as u32,
            ..Default::default()
        };

        let result = self.service().upsert(request).await?.into_inner();
        check_status(result.status.as_ref(), "Upsert")?;

        debug!(upsert_cnt = result.upsert_cnt, "upserted embeddings");

        Ok(result.upsert_cnt as u64)
    }
}

fn string_field(name: &str, data: Vec<String>) -> FieldData {
    FieldData {
        r#type: DataType::VarChar as i32,
        field_name: name.to_string(),
        field: Some(Field::Scalars(ScalarField {
            data: Some(scalar_field::Data::StringData(StringArray { data })),
        })),
        ..Default::default()
    }
}

fn long_field(name: &str, data: Vec<i64>) -> FieldData {
    FieldData {
        r#type: DataType::Int64 as i32,
        field_name: name.to_string(),
        field: Some(Field::Scalars(ScalarField {
            data: Some(scalar_field::Data::LongData(LongArray { data })),
        })),
        ..Default::default()
    }
}

fn double_field(name: &str, data: Vec<f64>) -> FieldData {
    FieldData {
        r#type: DataType::Double as i32,
        field_name: name.to_string(),
        field: Some(Field::Scalars(ScalarField {
            data: Some(scalar_field::Data::DoubleData(DoubleArray { data })),
        })),
        ..Default::default()
    }
}

/// A float-vector column: all rows' floats concatenated, tagged with the
/// per-vector dimension so Milvus can split them back apart.
fn float_vector_field(name: &str, dim: i64, data: Vec<f32>) -> FieldData {
    FieldData {
        r#type: DataType::FloatVector as i32,
        field_name: name.to_string(),
        field: Some(Field::Vectors(VectorField {
            dim,
            data: Some(vector_field::Data::FloatVector(FloatArray { data })),
        })),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn embedding_column_concatenates_rows_with_dim() {
        let field = float_vector_field(FIELD_EMBEDDING, 3, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);

        assert_eq!(field.field_name, FIELD_EMBEDDING);
        assert_eq!(field.r#type, DataType::FloatVector as i32);
        match field.field {
            Some(Field::Vectors(v)) => {
                assert_eq!(v.dim, 3);
                match v.data {
                    Some(vector_field::Data::FloatVector(arr)) => {
                        assert_eq!(arr.data.len(), 6);
                    }
                    _ => panic!("expected a float vector"),
                }
            }
            _ => panic!("expected a vector field"),
        }
    }

    #[test]
    fn object_id_column_preserves_row_order() {
        let field = string_field(FIELD_OBJECT_ID, vec!["a".into(), "b".into()]);
        match field.field {
            Some(Field::Scalars(s)) => match s.data {
                Some(scalar_field::Data::StringData(arr)) => {
                    assert_eq!(arr.data, vec!["a".to_string(), "b".to_string()]);
                }
                _ => panic!("expected string data"),
            },
            _ => panic!("expected a scalar field"),
        }
    }
}
