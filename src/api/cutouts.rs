use crate::utils::lightcurves::Band;
use utoipa::ToSchema;

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema, Clone)]
pub enum WhichCutouts {
    #[serde(alias = "first")]
    First,
    #[serde(alias = "last")]
    Last,
    #[serde(alias = "brightest")]
    Brightest,
    #[serde(alias = "faintest")]
    Faintest,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct AlertCandidOnly {
    #[serde(rename = "_id")]
    pub candid: i64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct CutoutQuery {
    pub candid: Option<i64>,
    #[serde(rename = "objectId")]
    pub object_id: Option<String>,
    pub which: Option<WhichCutouts>,
    pub band: Option<Band>,
}
