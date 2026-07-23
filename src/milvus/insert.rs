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
        let request = build_upsert_request(
            &config.database,
            &config.collection.name,
            config.collection.dim,
            rows,
        )?;

        let result = self.service().upsert(request).await?.into_inner();
        check_status(result.status.as_ref(), "Upsert")?;

        debug!(upsert_cnt = result.upsert_cnt, "upserted embeddings");

        Ok(result.upsert_cnt as u64)
    }
}

/// Validate the embeddings and transpose the rows into Milvus's column-oriented
/// `UpsertRequest`. Split out from the RPC send so it can be tested without a
/// live server. Assumes `rows` is non-empty.
fn build_upsert_request(
    db_name: &str,
    collection_name: &str,
    dim: i64,
    rows: &[EmbeddingRow],
) -> Result<UpsertRequest, MilvusError> {
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

    Ok(UpsertRequest {
        db_name: db_name.to_string(),
        collection_name: collection_name.to_string(),
        fields_data,
        num_rows: rows.len() as u32,
        ..Default::default()
    })
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

    fn row(object_id: &str, embedding: Vec<f32>, candid: i64, jd: f64) -> EmbeddingRow {
        EmbeddingRow {
            object_id: object_id.to_string(),
            embedding,
            candid,
            jd,
        }
    }

    #[test]
    fn build_upsert_request_rejects_dimension_mismatch() {
        let rows = vec![
            row("ZTF_A", vec![0.1, 0.2, 0.3], 1, 2400000.5),
            row("ZTF_B", vec![0.1, 0.2], 2, 2400001.5), // wrong length
        ];

        let err = build_upsert_request("db", "coll", 3, &rows).unwrap_err();
        match err {
            MilvusError::DimensionMismatch { expected, got } => {
                assert_eq!(expected, 3);
                assert_eq!(got, 2);
            }
            other => panic!("expected DimensionMismatch, got {other:?}"),
        }
    }

    #[test]
    fn build_upsert_request_assembles_all_columns_in_order() {
        let rows = vec![
            row("ZTF_A", vec![1.0, 2.0], 10, 2400000.5),
            row("ZTF_B", vec![3.0, 4.0], 20, 2400001.5),
        ];

        let request = build_upsert_request("mydb", "mycoll", 2, &rows).unwrap();

        assert_eq!(request.db_name, "mydb");
        assert_eq!(request.collection_name, "mycoll");
        assert_eq!(request.num_rows, 2);

        // Column order must match the schema field order.
        let names: Vec<&str> = request
            .fields_data
            .iter()
            .map(|f| f.field_name.as_str())
            .collect();
        assert_eq!(
            names,
            vec![FIELD_OBJECT_ID, FIELD_EMBEDDING, FIELD_CANDID, FIELD_JD]
        );

        // object_id column: both ids, in row order.
        match &request.fields_data[0].field {
            Some(Field::Scalars(ScalarField {
                data: Some(scalar_field::Data::StringData(arr)),
            })) => assert_eq!(arr.data, vec!["ZTF_A".to_string(), "ZTF_B".to_string()]),
            other => panic!("object_id column malformed: {other:?}"),
        }

        // embedding column: the two rows' vectors flattened, dim recorded.
        match &request.fields_data[1].field {
            Some(Field::Vectors(VectorField {
                dim,
                data: Some(vector_field::Data::FloatVector(arr)),
            })) => {
                assert_eq!(*dim, 2);
                assert_eq!(arr.data, vec![1.0, 2.0, 3.0, 4.0]);
            }
            other => panic!("embedding column malformed: {other:?}"),
        }

        // candid column.
        match &request.fields_data[2].field {
            Some(Field::Scalars(ScalarField {
                data: Some(scalar_field::Data::LongData(arr)),
            })) => assert_eq!(arr.data, vec![10, 20]),
            other => panic!("candid column malformed: {other:?}"),
        }

        // jd column.
        match &request.fields_data[3].field {
            Some(Field::Scalars(ScalarField {
                data: Some(scalar_field::Data::DoubleData(arr)),
            })) => assert_eq!(arr.data, vec![2400000.5, 2400001.5]),
            other => panic!("jd column malformed: {other:?}"),
        }
    }
}
