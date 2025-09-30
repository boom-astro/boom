use mongodb::bson::Document;

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
