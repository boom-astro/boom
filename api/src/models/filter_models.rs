#[derive(serde::Deserialize, Clone)]
pub struct FilterSubmissionBody {
    pub pipeline: Option<Vec<mongodb::bson::Document>>,
    pub permissions: Option<Vec<i32>>,
    pub catalog: Option<String>,
    pub id: Option<i32>,
}
