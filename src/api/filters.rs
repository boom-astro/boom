use mongodb::bson::Document;
use serde_json::Value;
use std::io::{Error, ErrorKind};

/// Functionality for working with filters

// Deserialize helper functions
fn _deserialize_filter(mongo_filter_json: &serde_json::Value) -> Result<Document, std::io::Error> {
    match mongo_filter_json {
        serde_json::Value::Object(_) => {
            match mongodb::bson::to_document(&mongo_filter_json) {
                Ok(doc) => return Ok(doc),
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Invalid MongoDB Filter: {:?}", e),
                    ));
                }
            };
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "MongoDB Filter must be a JSON object",
        )),
    }
}

/// Parse a filter from a JSON value
pub fn parse_filter(mongo_filter_json: &serde_json::Value) -> Result<Document, std::io::Error> {
    _deserialize_filter(mongo_filter_json)
}

/// Parse an optional filter from a JSON value
pub fn parse_optional_filter(
    mongo_filter_json_opt: &Option<serde_json::Value>,
) -> Result<Document, std::io::Error> {
    match mongo_filter_json_opt {
        Some(filter) => _deserialize_filter(filter),
        None => Ok(Document::new()),
    }
}

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

const JOIN_STAGES: [&str; 3] = ["$lookup", "$graphLookup", "$unionWith"];

/// Collections a pipeline reads besides the one it runs on, at any depth.
pub fn joined_collections(pipeline: &Value) -> Result<Vec<&str>, Error> {
    let mut names = Vec::new();
    collect_joined_collections(pipeline, &mut names)?;
    Ok(names)
}

fn collect_joined_collections<'a>(value: &'a Value, names: &mut Vec<&'a str>) -> Result<(), Error> {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_joined_collections(item, names)?;
            }
        }
        Value::Object(map) => {
            for (key, spec) in map {
                if JOIN_STAGES.contains(&key.as_str()) {
                    let targets = match spec {
                        Value::Object(spec) => vec![spec.get("from"), spec.get("coll")],
                        other => vec![Some(other)],
                    };
                    for target in targets.into_iter().flatten() {
                        let name = target.as_str().ok_or_else(|| {
                            Error::new(
                                ErrorKind::InvalidInput,
                                format!("{} must name a collection of this database", key),
                            )
                        })?;
                        names.push(name);
                    }
                }
                collect_joined_collections(spec, names)?;
            }
        }
        _ => {}
    }
    Ok(())
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
                tracing::error!("Serialization error: {}", e); // TODO: replace with tracing once integrated
                None
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_joined_collections() {
        let pipeline = json!([
            { "$match": { "from": "not_a_join" } },
            { "$lookup": { "from": "a", "localField": "x", "foreignField": "y", "as": "z" } },
            { "$graphLookup": { "from": "b", "startWith": "$x", "connectFromField": "x",
                "connectToField": "y", "as": "z" } },
            { "$unionWith": "c" },
            { "$unionWith": { "coll": "d", "pipeline": [
                { "$lookup": { "from": "e", "pipeline": [{ "$unionWith": "f" }], "as": "z" } },
            ] } },
            { "$facet": { "g": [{ "$lookup": { "from": "h", "pipeline": [], "as": "z" } }] } },
            { "$lookup": { "pipeline": [{ "$documents": [{ "x": 1 }] }], "as": "z" } },
        ]);
        assert_eq!(
            joined_collections(&pipeline).unwrap(),
            ["a", "b", "c", "d", "e", "f", "h"]
        );
        assert!(joined_collections(&json!([{ "$match": {} }]))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn test_joined_collections_must_be_named() {
        for pipeline in [
            json!([{ "$lookup": { "from": { "db": "admin", "coll": "x" }, "as": "z" } }]),
            json!([{ "$unionWith": { "coll": 1 } }]),
            json!([{ "$facet": { "g": [{ "$unionWith": ["x"] }] } }]),
        ] {
            assert!(joined_collections(&pipeline).is_err(), "{pipeline}");
        }
    }
}
