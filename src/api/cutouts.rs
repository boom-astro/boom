use crate::utils::cutouts::{AlertCutout, CutoutStorage, CutoutStorageError};
use crate::utils::lightcurves::Band;
use base64::prelude::*;
use futures::TryStreamExt;
use std::collections::HashMap;
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

#[derive(Debug, serde::Deserialize)]
pub struct AlertJdOnly {
    #[serde(rename = "_id")]
    pub candid: i64,
    pub candidate: CandidateJdOnly,
}

#[derive(Debug, serde::Deserialize)]
pub struct CandidateJdOnly {
    pub jd: f64,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct CutoutQuery {
    pub candid: Option<i64>,
    #[serde(rename = "objectId")]
    pub object_id: Option<String>,
    pub which: Option<WhichCutouts>,
    pub band: Option<Band>,
}

/// Most candids a single batch cutout request may ask for. Each alert carries
/// three stamps, so this bounds the response to a few hundred images.
pub const MAX_BATCH_CUTOUTS: usize = 100;

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct BatchCutoutQuery {
    /// Candids of the alerts to retrieve cutouts for (at most 100)
    pub candids: Vec<i64>,
}

/// Deduplicate the requested candids, keeping request order, and enforce the
/// batch size limit. Returns the error message for a bad request.
pub fn validate_batch_candids(candids: &[i64]) -> Result<Vec<i64>, String> {
    let mut seen = std::collections::HashSet::new();
    let unique: Vec<i64> = candids
        .iter()
        .copied()
        .filter(|c| seen.insert(*c))
        .collect();
    if unique.is_empty() || unique.len() > MAX_BATCH_CUTOUTS {
        return Err(format!(
            "must provide between 1 and {} candids",
            MAX_BATCH_CUTOUTS
        ));
    }
    Ok(unique)
}

/// The JSON shape every cutout endpoint returns for one alert.
pub fn cutouts_to_json(cutouts: &AlertCutout) -> serde_json::Value {
    serde_json::json!({
        "candid": cutouts.candid,
        "cutoutScience": BASE64_STANDARD.encode(&cutouts.cutout_science),
        "cutoutTemplate": BASE64_STANDARD.encode(&cutouts.cutout_template),
        "cutoutDifference": BASE64_STANDARD.encode(&cutouts.cutout_difference),
    })
}

/// Observation times of the alerts among `candids` that match `filter`, keyed
/// by candid. Candids with no matching alert are absent from the map.
pub async fn fetch_alert_jds(
    alert_collection: &mongodb::Collection<AlertJdOnly>,
    candids: &[i64],
    mut filter: mongodb::bson::Document,
) -> Result<HashMap<i64, f64>, mongodb::error::Error> {
    filter.insert("_id", mongodb::bson::doc! { "$in": candids });
    let alerts: Vec<AlertJdOnly> = alert_collection
        .find(filter)
        .projection(mongodb::bson::doc! { "_id": 1, "candidate.jd": 1 })
        .await?
        .try_collect()
        .await?;
    Ok(alerts
        .into_iter()
        .map(|alert| (alert.candid, alert.candidate.jd))
        .collect())
}

/// Fetch cutouts for `candids` and build the batch response body: the cutouts
/// found, in request order, each with its alert's `jd` (null if the alert is
/// not in `jds`), and the candids that had none.
pub async fn retrieve_batch_cutouts(
    cutout_storage: &CutoutStorage,
    candids: &[i64],
    jds: &HashMap<i64, f64>,
) -> Result<serde_json::Value, CutoutStorageError> {
    let mut found = cutout_storage
        .retrieve_multiple_cutouts(candids, false)
        .await?;
    let mut cutouts = Vec::with_capacity(found.len());
    let mut missing = Vec::new();
    for candid in candids {
        match found.remove(candid) {
            Some(c) => {
                let mut entry = cutouts_to_json(&c);
                entry["jd"] = jds.get(candid).copied().into();
                cutouts.push(entry);
            }
            None => missing.push(*candid),
        }
    }
    Ok(serde_json::json!({ "cutouts": cutouts, "missing": missing }))
}
