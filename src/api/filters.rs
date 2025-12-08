use mongodb::bson::Document;
use tracing::{debug, instrument, warn};

/// Functionality for working with filters

// Deserialize helper functions
#[instrument(
    name = "filters::_deserialize_filter",
    skip(mongo_filter_json),
    fields(),
    err
)]
fn _deserialize_filter(mongo_filter_json: &serde_json::Value) -> Result<Document, std::io::Error> {
    match mongo_filter_json {
        serde_json::Value::Object(_) => match mongodb::bson::to_document(mongo_filter_json) {
            Ok(doc) => Ok(doc),
            Err(e) => {
                debug!(error = %e, "failed to convert JSON value to BSON Document");
                Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Invalid MongoDB Filter: {:?}", e),
                ))
            }
        },
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "MongoDB Filter must be a JSON object",
        )),
    }
}

/// Parse a filter from a JSON value
#[instrument(name = "filters::parse_filter", skip(mongo_filter_json), err)]
pub fn parse_filter(mongo_filter_json: &serde_json::Value) -> Result<Document, std::io::Error> {
    _deserialize_filter(mongo_filter_json)
}

/// Parse an optional filter from a JSON value
#[instrument(
    name = "filters::parse_optional_filter",
    skip(mongo_filter_json_opt),
    err
)]
pub fn parse_optional_filter(
    mongo_filter_json_opt: &Option<serde_json::Value>,
) -> Result<Document, std::io::Error> {
    match mongo_filter_json_opt {
        Some(filter) => _deserialize_filter(filter),
        None => Ok(Document::new()),
    }
}

#[instrument(name = "filters::parse_pipeline", skip(mongo_pipeline_json), err)]
pub fn parse_pipeline(
    mongo_pipeline_json: &serde_json::Value,
) -> Result<Vec<Document>, std::io::Error> {
    // the value should be an array of Object
    match mongo_pipeline_json {
        serde_json::Value::Array(stages) => Ok(stages
            .iter()
            .map(|stage| _deserialize_filter(stage))
            .collect::<Result<Vec<Document>, std::io::Error>>()?),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Pipeline must be a JSON array",
        )),
    }
}

#[derive(Clone, utoipa::ToSchema)]
pub enum SortOrder {
    Ascending,
    Descending,
}
// implement a custom serde::Deserialize so we can handle numericals like 1 and -1
impl<'de> serde::Deserialize<'de> for SortOrder {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct SortOrderVisitor;
        impl<'de> serde::de::Visitor<'de> for SortOrderVisitor {
            type Value = SortOrder;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string or integer representing sort order")
            }

            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match s.to_lowercase().as_str() {
                    "ascending" | "asc" | "1" => Ok(SortOrder::Ascending),
                    "descending" | "desc" | "-1" => Ok(SortOrder::Descending),
                    _ => Err(E::custom(format!("invalid sort order: {}", s))),
                }
            }

            fn visit_i64<E>(self, i: i64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match i {
                    1 => Ok(SortOrder::Ascending),
                    -1 => Ok(SortOrder::Descending),
                    _ => Err(E::custom(format!("invalid sort order: {}", i))),
                }
            }

            fn visit_u64<E>(self, u: u64) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                match u {
                    1 => Ok(SortOrder::Ascending),
                    _ => Err(E::custom(format!("invalid sort order: {}", u))),
                }
            }

            fn visit_i32<E>(self, i: i32) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_i64(i as i64)
            }

            fn visit_u32<E>(self, u: u32) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                self.visit_u64(u as u64)
            }
        }

        deserializer.deserialize_any(SortOrderVisitor)
    }
}

// function to convert a Vec<Document> to Vec<serde_json::Value>
pub fn doc2json(docs: Vec<Document>) -> Vec<serde_json::Value> {
    docs.into_iter()
        .filter_map(|doc| match serde_json::to_value(doc) {
            Ok(value) => Some(value),
            Err(e) => {
                warn!(error = %e, "failed to serialize BSON Document to JSON"); // TODO: replace with tracing once integrated
                None
            }
        })
        .collect()
}
