//! Reading embeddings back out of the collection: similarity search, lookup by
//! `object_id`, counting, deletion, and flush.
//!
//! Milvus's read RPCs are column-oriented just like writes (see
//! [`super::insert`]): results come back as one [`FieldData`] per column, which
//! these helpers transpose back into rows.
//!
//! Similarity search additionally needs the query vectors wrapped in a
//! protobuf-encoded [`PlaceholderGroup`] — Milvus does not accept raw floats on
//! the `SearchRequest`. For a float vector each query vector is serialized as
//! its little-endian `f32` bytes.

use prost::Message;
use tracing::{debug, instrument};

use super::client::{check_status, MilvusClient, MilvusError};
use super::collection::{FIELD_CANDID, FIELD_EMBEDDING, FIELD_JD, FIELD_OBJECT_ID};
use super::insert::EmbeddingRow;
use super::proto::common::{
    DslType, KeyValuePair, PlaceholderGroup, PlaceholderType, PlaceholderValue,
};
use super::proto::milvus::{
    search_request::SearchInput, DeleteRequest, FlushRequest, QueryRequest, SearchRequest,
};
use super::proto::schema::{
    field_data, i_ds, scalar_field, vector_field, FieldData, SearchResultData,
};

/// One nearest-neighbor result from a similarity search.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct SearchHit {
    /// The matched object's identifier.
    pub object_id: String,
    /// Similarity score under the collection's metric (higher is closer for
    /// `COSINE`/inner-product).
    pub score: f32,
    /// Candid stored with the matched embedding, if requested/available.
    pub candid: Option<i64>,
    /// Julian date stored with the matched embedding, if requested/available.
    pub jd: Option<f64>,
}

impl MilvusClient {
    /// Find the `top_k` objects whose stored embeddings are most similar to
    /// `query`, using the collection's configured metric. Results are ordered
    /// best-first.
    #[instrument(skip_all, err, fields(collection = %self.config().collection.name, top_k))]
    pub async fn search_embedding(
        &mut self,
        query: &[f32],
        top_k: i64,
    ) -> Result<Vec<SearchHit>, MilvusError> {
        let config = self.config().clone();

        if query.len() as i64 != config.collection.dim {
            return Err(MilvusError::DimensionMismatch {
                expected: config.collection.dim,
                got: query.len(),
            });
        }

        let search_params = vec![
            kv("anns_field", FIELD_EMBEDDING),
            kv("topk", &top_k.to_string()),
            kv("metric_type", &config.collection.metric_type),
            // Empty JSON lets Milvus use the index's default search params.
            kv("params", "{}"),
            kv("round_decimal", "-1"),
        ];

        let request = SearchRequest {
            db_name: config.database.clone(),
            collection_name: config.collection.name.clone(),
            dsl: String::new(),
            dsl_type: DslType::BoolExprV1 as i32,
            search_input: Some(SearchInput::PlaceholderGroup(encode_float_placeholder(&[
                query,
            ]))),
            output_fields: vec![FIELD_CANDID.to_string(), FIELD_JD.to_string()],
            search_params,
            nq: 1,
            use_default_consistency: true,
            ..Default::default()
        };

        let response = self.service().search(request).await?.into_inner();
        check_status(response.status.as_ref(), "Search")?;

        let hits = parse_search_hits(response.results);
        debug!(hits = hits.len(), "search returned");
        Ok(hits)
    }

    /// Fetch the stored rows for the given `object_ids`. Missing ids are simply
    /// absent from the result; order is not guaranteed to match the input.
    #[instrument(skip_all, err, fields(collection = %self.config().collection.name, ids = object_ids.len()))]
    pub async fn get_embeddings(
        &mut self,
        object_ids: &[&str],
    ) -> Result<Vec<EmbeddingRow>, MilvusError> {
        if object_ids.is_empty() {
            return Ok(vec![]);
        }
        let config = self.config().clone();

        let request = QueryRequest {
            db_name: config.database.clone(),
            collection_name: config.collection.name.clone(),
            expr: object_id_in_expr(object_ids),
            output_fields: vec![
                FIELD_OBJECT_ID.to_string(),
                FIELD_EMBEDDING.to_string(),
                FIELD_CANDID.to_string(),
                FIELD_JD.to_string(),
            ],
            use_default_consistency: true,
            ..Default::default()
        };

        let response = self.service().query(request).await?.into_inner();
        check_status(response.status.as_ref(), "Query")?;

        Ok(parse_embedding_rows(&response.fields_data))
    }

    /// Total number of rows (objects) currently in the collection.
    #[instrument(skip_all, err, fields(collection = %self.config().collection.name))]
    pub async fn count(&mut self) -> Result<i64, MilvusError> {
        let config = self.config().clone();

        let request = QueryRequest {
            db_name: config.database.clone(),
            collection_name: config.collection.name.clone(),
            expr: String::new(),
            output_fields: vec!["count(*)".to_string()],
            use_default_consistency: true,
            ..Default::default()
        };

        let response = self.service().query(request).await?.into_inner();
        check_status(response.status.as_ref(), "Query")?;

        let count = long_column(&response.fields_data, "count(*)")
            .and_then(|c| c.first().copied())
            .unwrap_or(0);
        Ok(count)
    }

    /// Delete the rows for the given `object_ids`. Returns the number deleted.
    #[instrument(skip_all, err, fields(collection = %self.config().collection.name, ids = object_ids.len()))]
    pub async fn delete_embeddings(&mut self, object_ids: &[&str]) -> Result<u64, MilvusError> {
        if object_ids.is_empty() {
            return Ok(0);
        }
        let config = self.config().clone();

        let request = DeleteRequest {
            db_name: config.database.clone(),
            collection_name: config.collection.name.clone(),
            expr: object_id_in_expr(object_ids),
            ..Default::default()
        };

        let result = self.service().delete(request).await?.into_inner();
        check_status(result.status.as_ref(), "Delete")?;
        Ok(result.delete_cnt as u64)
    }

    /// Flush the collection, sealing recent writes so they become searchable.
    /// Milvus flushes on its own schedule; call this when you need read-after-
    /// write visibility immediately (e.g. a smoke test).
    #[instrument(skip_all, err, fields(collection = %self.config().collection.name))]
    pub async fn flush(&mut self) -> Result<(), MilvusError> {
        let config = self.config().clone();

        let request = FlushRequest {
            db_name: config.database.clone(),
            collection_names: vec![config.collection.name.clone()],
            ..Default::default()
        };

        let response = self.service().flush(request).await?.into_inner();
        check_status(response.status.as_ref(), "Flush")?;
        Ok(())
    }
}

/// Build a `object_id in ["a", "b"]` boolean expression.
///
/// The ids are wrapped in quotes; any embedded `"` or `\` is stripped first so
/// a malformed id can't break the expression. Survey object ids are plain
/// alphanumerics, so this never alters a legitimate value.
fn object_id_in_expr(object_ids: &[&str]) -> String {
    let list = object_ids
        .iter()
        .map(|id| {
            let sanitized: String = id.chars().filter(|c| *c != '"' && *c != '\\').collect();
            format!("\"{sanitized}\"")
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("{FIELD_OBJECT_ID} in [{list}]")
}

/// Serialize float query vectors into a Milvus `PlaceholderGroup` (each vector
/// as little-endian `f32` bytes).
fn encode_float_placeholder(vectors: &[&[f32]]) -> Vec<u8> {
    let values: Vec<Vec<u8>> = vectors
        .iter()
        .map(|v| {
            let mut bytes = Vec::with_capacity(v.len() * 4);
            for &f in *v {
                bytes.extend_from_slice(&f.to_le_bytes());
            }
            bytes
        })
        .collect();

    let placeholder = PlaceholderValue {
        tag: "$0".to_string(),
        r#type: PlaceholderType::FloatVector as i32,
        values,
        element_level: false,
    };

    PlaceholderGroup {
        placeholders: vec![placeholder],
    }
    .encode_to_vec()
}

fn parse_search_hits(data: Option<SearchResultData>) -> Vec<SearchHit> {
    let Some(data) = data else {
        return vec![];
    };

    // The primary key is a VarChar, so ids come back as string ids.
    let object_ids = match data.ids.and_then(|i| i.id_field) {
        Some(i_ds::IdField::StrId(arr)) => arr.data,
        _ => return vec![],
    };

    let candids = long_column(&data.fields_data, FIELD_CANDID);
    let jds = double_column(&data.fields_data, FIELD_JD);

    object_ids
        .into_iter()
        .enumerate()
        .map(|(i, object_id)| SearchHit {
            object_id,
            score: data.scores.get(i).copied().unwrap_or(f32::NAN),
            candid: candids.as_ref().and_then(|c| c.get(i).copied()),
            jd: jds.as_ref().and_then(|j| j.get(i).copied()),
        })
        .collect()
}

fn parse_embedding_rows(fields: &[FieldData]) -> Vec<EmbeddingRow> {
    let object_ids = string_column(fields, FIELD_OBJECT_ID).unwrap_or_default();
    let (dim, flat) = float_vector_column(fields, FIELD_EMBEDDING).unwrap_or((0, vec![]));
    let candids = long_column(fields, FIELD_CANDID).unwrap_or_default();
    let jds = double_column(fields, FIELD_JD).unwrap_or_default();

    object_ids
        .into_iter()
        .enumerate()
        .map(|(i, object_id)| {
            let embedding = if dim > 0 {
                let start = i * dim;
                flat.get(start..start + dim)
                    .map(<[f32]>::to_vec)
                    .unwrap_or_default()
            } else {
                vec![]
            };
            EmbeddingRow {
                object_id,
                embedding,
                candid: candids.get(i).copied().unwrap_or_default(),
                jd: jds.get(i).copied().unwrap_or_default(),
            }
        })
        .collect()
}

fn find_field<'a>(fields: &'a [FieldData], name: &str) -> Option<&'a FieldData> {
    fields.iter().find(|f| f.field_name == name)
}

fn long_column(fields: &[FieldData], name: &str) -> Option<Vec<i64>> {
    match &find_field(fields, name)?.field {
        Some(field_data::Field::Scalars(s)) => match &s.data {
            Some(scalar_field::Data::LongData(a)) => Some(a.data.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn double_column(fields: &[FieldData], name: &str) -> Option<Vec<f64>> {
    match &find_field(fields, name)?.field {
        Some(field_data::Field::Scalars(s)) => match &s.data {
            Some(scalar_field::Data::DoubleData(a)) => Some(a.data.clone()),
            _ => None,
        },
        _ => None,
    }
}

fn string_column(fields: &[FieldData], name: &str) -> Option<Vec<String>> {
    match &find_field(fields, name)?.field {
        Some(field_data::Field::Scalars(s)) => match &s.data {
            Some(scalar_field::Data::StringData(a)) => Some(a.data.clone()),
            _ => None,
        },
        _ => None,
    }
}

/// Returns `(dim, flat)` for a float-vector column.
fn float_vector_column(fields: &[FieldData], name: &str) -> Option<(usize, Vec<f32>)> {
    match &find_field(fields, name)?.field {
        Some(field_data::Field::Vectors(v)) => match &v.data {
            Some(vector_field::Data::FloatVector(a)) => Some((v.dim as usize, a.data.clone())),
            _ => None,
        },
        _ => None,
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
    use crate::milvus::proto::schema::{
        i_ds, scalar_field, DoubleArray, FloatArray, IDs, LongArray, ScalarField, StringArray,
        VectorField,
    };

    fn scalar(name: &str, data: scalar_field::Data) -> FieldData {
        FieldData {
            field_name: name.to_string(),
            field: Some(field_data::Field::Scalars(ScalarField { data: Some(data) })),
            ..Default::default()
        }
    }

    #[test]
    fn object_id_in_expr_quotes_and_sanitizes() {
        assert_eq!(
            object_id_in_expr(&["ZTF_A", "ZTF_B"]),
            r#"object_id in ["ZTF_A", "ZTF_B"]"#
        );
        // Quotes/backslashes are stripped, so the expression stays well-formed.
        assert_eq!(object_id_in_expr(&["ev\"il"]), r#"object_id in ["evil"]"#);
    }

    #[test]
    fn encode_float_placeholder_lays_out_little_endian_floats() {
        let bytes = encode_float_placeholder(&[&[1.0f32, 2.0]]);
        let group = PlaceholderGroup::decode(bytes.as_slice()).unwrap();
        let ph = &group.placeholders[0];
        assert_eq!(ph.r#type, PlaceholderType::FloatVector as i32);
        assert_eq!(ph.values.len(), 1);
        assert_eq!(
            ph.values[0],
            [1.0f32.to_le_bytes(), 2.0f32.to_le_bytes()].concat()
        );
    }

    #[test]
    fn parse_search_hits_zips_ids_scores_and_metadata() {
        let data = SearchResultData {
            scores: vec![0.9, 0.8],
            ids: Some(IDs {
                id_field: Some(i_ds::IdField::StrId(StringArray {
                    data: vec!["ZTF_A".into(), "ZTF_B".into()],
                })),
            }),
            fields_data: vec![
                scalar(
                    FIELD_CANDID,
                    scalar_field::Data::LongData(LongArray { data: vec![10, 20] }),
                ),
                scalar(
                    FIELD_JD,
                    scalar_field::Data::DoubleData(DoubleArray {
                        data: vec![2400000.5, 2400001.5],
                    }),
                ),
            ],
            ..Default::default()
        };

        let hits = parse_search_hits(Some(data));
        assert_eq!(
            hits,
            vec![
                SearchHit {
                    object_id: "ZTF_A".into(),
                    score: 0.9,
                    candid: Some(10),
                    jd: Some(2400000.5),
                },
                SearchHit {
                    object_id: "ZTF_B".into(),
                    score: 0.8,
                    candid: Some(20),
                    jd: Some(2400001.5),
                },
            ]
        );
    }

    #[test]
    fn parse_embedding_rows_splits_flat_vector_by_dim() {
        let fields = vec![
            scalar(
                FIELD_OBJECT_ID,
                scalar_field::Data::StringData(StringArray {
                    data: vec!["ZTF_A".into(), "ZTF_B".into()],
                }),
            ),
            FieldData {
                field_name: FIELD_EMBEDDING.to_string(),
                field: Some(field_data::Field::Vectors(VectorField {
                    dim: 2,
                    data: Some(vector_field::Data::FloatVector(FloatArray {
                        data: vec![1.0, 2.0, 3.0, 4.0],
                    })),
                })),
                ..Default::default()
            },
            scalar(
                FIELD_CANDID,
                scalar_field::Data::LongData(LongArray { data: vec![10, 20] }),
            ),
            scalar(
                FIELD_JD,
                scalar_field::Data::DoubleData(DoubleArray {
                    data: vec![2400000.5, 2400001.5],
                }),
            ),
        ];

        let rows = parse_embedding_rows(&fields);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].object_id, "ZTF_A");
        assert_eq!(rows[0].embedding, vec![1.0, 2.0]);
        assert_eq!(rows[1].embedding, vec![3.0, 4.0]);
        assert_eq!(rows[1].candid, 20);
        assert_eq!(rows[1].jd, 2400001.5);
    }
}
