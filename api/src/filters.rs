use mongodb::bson::Document;

/// Functionality for working with filters

// Deserialize helper functions
fn _deserialize_filter(filter: &serde_json::Value) -> Result<Document, std::io::Error> {
    match filter {
        serde_json::Value::Object(_) => {
            match mongodb::bson::to_document(&filter) {
                Ok(doc) => return Ok(doc),
                Err(e) => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        format!("Invalid filter: {:?}", e),
                    ));
                }
            };
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Filter must be a JSON object",
        )),
    }
}

/// Parse a filter from a JSON value
pub fn parse_filter(filter: &serde_json::Value) -> Result<Document, std::io::Error> {
    _deserialize_filter(filter)
}

/// Parse an optional filter from a JSON value
pub fn parse_optional_filter(
    filter_opt: &Option<serde_json::Value>,
) -> Result<Document, std::io::Error> {
    match filter_opt {
        Some(filter) => _deserialize_filter(filter),
        None => Ok(Document::new()),
    }
}

fn _deserialize_pipeline(pipeline: &serde_json::Value) -> Result<Vec<Document>, std::io::Error> {
    // the value should be an array of Object
    match pipeline {
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

/// Parse a filter from a JSON value
pub fn parse_pipeline(filter: &serde_json::Value) -> Result<Vec<Document>, std::io::Error> {
    _deserialize_pipeline(filter)
}
