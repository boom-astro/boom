use crate::{
    alert::{
        lsst_surroundings, simulated_lsst_object, AlertError, SimulatedEpoch, SimulatedSurroundings,
    },
    conf::AppConfig,
    enrichment::{
        add_decam_match, create_lsst_alert_pipeline, lsst_alert_properties, LsstAlertForEnrichment,
    },
    utils::{db::mongify, enums::Survey, lightcurves::Band},
};
use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use utoipa::ToSchema;

const LSST_ALERT_AUX_COLLECTION: &str = "LSST_alerts_aux";
const ALERTS_PER_AGGREGATE: usize = 200;

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct SimulatedPoint {
    pub band: String,
    pub mjd: f64,
    /// nJy
    pub flux: f64,
    pub flux_err: f64,
    #[serde(default)]
    pub upper_limit: bool,
}

#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct SimulatedLightcurve {
    pub id: String,
    pub ra: f64,
    pub dec: f64,
    pub t0: Option<f64>,
    pub points: Vec<SimulatedPoint>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SimulatedAlertResult {
    pub candid: i64,
    pub mjd: f64,
    pub band: Option<String>,
    #[schema(value_type = Option<Object>)]
    pub annotations: Option<Document>,
}

#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct SimulatedLightcurveResult {
    pub id: String,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub n_alerts: usize,
    pub n_passed: usize,
    pub first_passed_mjd: Option<f64>,
    pub first_passed_delay: Option<f64>,
    pub passed: Vec<SimulatedAlertResult>,
    pub error: Option<String>,
}

#[derive(thiserror::Error, Debug)]
pub enum SimulationError {
    #[error("{0}")]
    InvalidInput(String),
    #[error(transparent)]
    Alert(#[from] AlertError),
    #[error(transparent)]
    Mongodb(#[from] mongodb::error::Error),
    #[error(transparent)]
    BsonSerialization(#[from] mongodb::bson::ser::Error),
    #[error(transparent)]
    BsonDeserialization(#[from] mongodb::bson::de::Error),
    #[error(transparent)]
    BsonAccess(#[from] mongodb::bson::document::ValueAccessError),
    #[error("simulated alert {0} was not enriched")]
    NotEnriched(i64),
}

fn up_to_alert(path: &str) -> Document {
    doc! { "$filter": { "input": path, "cond": { "$lte": ["$$this.jd", "$candidate.jd"] } } }
}

fn documents_stage(documents: &[Document]) -> Document {
    doc! { "$documents": { "$literal": documents.to_vec() } }
}

pub fn join_literal_documents(pipeline: &mut [Document], collection: &str, documents: &[Document]) {
    for stage in pipeline.iter_mut() {
        let Ok(lookup) = stage.get_document_mut("$lookup") else {
            continue;
        };
        if lookup.get_str("from").is_ok_and(|from| from == collection) {
            lookup.remove("from");
            lookup.insert("pipeline", vec![documents_stage(documents)]);
        }
    }
}

async fn run_on_documents(
    db: &mongodb::Database,
    pipeline: &[Document],
    alerts: &[Document],
    aux: &Document,
    mut handle: impl FnMut(Document) -> Result<(), SimulationError>,
) -> Result<(), SimulationError> {
    for chunk in alerts.chunks(ALERTS_PER_AGGREGATE) {
        let mut pipeline = pipeline.to_vec();
        pipeline[0] = documents_stage(chunk);
        join_literal_documents(
            &mut pipeline,
            LSST_ALERT_AUX_COLLECTION,
            std::slice::from_ref(aux),
        );
        let mut cursor = db.aggregate(pipeline).await?;
        while let Some(document) = cursor.try_next().await? {
            handle(document)?;
        }
    }
    Ok(())
}

async fn simulate_lsst_lightcurve(
    db: &mongodb::Database,
    filter_pipeline: &[Document],
    lightcurve: &SimulatedLightcurve,
    dia_object_id: i64,
    detection_fields: &Map<String, Value>,
    surroundings: &SimulatedSurroundings,
    now: f64,
) -> Result<SimulatedLightcurveResult, SimulationError> {
    let mut epochs = lightcurve
        .points
        .iter()
        .map(|point| {
            let band: Band =
                serde_json::from_value(Value::String(point.band.clone())).map_err(|_| {
                    SimulationError::InvalidInput(format!("unknown band {}", point.band))
                })?;
            Ok(SimulatedEpoch {
                mjd: point.mjd,
                band,
                flux: point.flux as f32,
                flux_err: point.flux_err as f32,
                detected: !point.upper_limit,
            })
        })
        .collect::<Result<Vec<_>, SimulationError>>()?;
    epochs.sort_by(|a, b| a.mjd.total_cmp(&b.mjd));

    let object = simulated_lsst_object(
        dia_object_id,
        lightcurve.ra,
        lightcurve.dec,
        &epochs,
        detection_fields,
        surroundings,
        now,
    )?;
    let aux = mongodb::bson::to_document(&object.aux)?;
    let mut alerts = object
        .alerts
        .iter()
        .map(mongodb::bson::to_document)
        .collect::<Result<Vec<_>, _>>()?;

    let mut enrichment_pipeline = add_decam_match(create_lsst_alert_pipeline());
    // Properties ignore later points; trimming them only saves transfer.
    if let Some(unwind) = enrichment_pipeline
        .iter()
        .position(|stage| stage.contains_key("$unwind"))
    {
        enrichment_pipeline.insert(
            unwind + 1,
            doc! { "$set": {
                "aux.prv_candidates": up_to_alert("$aux.prv_candidates"),
                "aux.fp_hists": up_to_alert("$aux.fp_hists"),
            }},
        );
    }
    let mut properties: HashMap<i64, Document> = HashMap::new();
    run_on_documents(db, &enrichment_pipeline, &alerts, &aux, |document| {
        let alert: LsstAlertForEnrichment = mongodb::bson::from_document(document)?;
        properties.insert(alert.candid, mongify(&lsst_alert_properties(&alert)));
        Ok(())
    })
    .await?;
    for alert in alerts.iter_mut() {
        let candid = alert.get_i64("_id")?;
        let alert_properties = properties
            .remove(&candid)
            .ok_or(SimulationError::NotEnriched(candid))?;
        alert.insert("properties", alert_properties);
    }

    let mut passed: HashMap<i64, Option<Document>> = HashMap::new();
    run_on_documents(db, filter_pipeline, &alerts, &aux, |document| {
        let candid = document.get_i64("_id")?;
        passed.insert(candid, document.get_document("annotations").ok().cloned());
        Ok(())
    })
    .await?;

    let passed: Vec<SimulatedAlertResult> = object
        .alerts
        .iter()
        .filter_map(|alert| {
            let annotations = passed.remove(&alert.candid)?;
            let dia_source = &alert.candidate.dia_source;
            Some(SimulatedAlertResult {
                candid: alert.candid,
                mjd: dia_source.midpoint_mjd_tai,
                band: dia_source
                    .band
                    .as_ref()
                    .and_then(|band| serde_json::to_value(band).ok())
                    .and_then(|band| band.as_str().map(str::to_string)),
                annotations,
            })
        })
        .collect();
    let first_passed_mjd = passed.first().map(|alert| alert.mjd);

    Ok(SimulatedLightcurveResult {
        id: lightcurve.id.clone(),
        object_id: dia_object_id.to_string(),
        n_alerts: object.alerts.len(),
        n_passed: passed.len(),
        first_passed_mjd,
        first_passed_delay: first_passed_mjd
            .zip(lightcurve.t0)
            .map(|(mjd, t0)| mjd - t0),
        passed,
        error: None,
    })
}

pub async fn simulate_lsst_filter(
    db: &mongodb::Database,
    config: &AppConfig,
    filter_pipeline: &[Document],
    lightcurves: &[SimulatedLightcurve],
    detection_fields: &Map<String, Value>,
) -> Vec<SimulatedLightcurveResult> {
    let xmatch_configs = config
        .crossmatch
        .get(&Survey::Lsst)
        .cloned()
        .unwrap_or_default();
    let now = flare::Time::now().to_jd();
    let mut surroundings: HashMap<(u64, u64), SimulatedSurroundings> = HashMap::new();
    let mut results = Vec::with_capacity(lightcurves.len());

    for (index, lightcurve) in lightcurves.iter().enumerate() {
        let dia_object_id = -(index as i64 + 1);
        let position = (lightcurve.ra.to_bits(), lightcurve.dec.to_bits());
        if !surroundings.contains_key(&position) {
            match lsst_surroundings(
                lightcurve.ra,
                lightcurve.dec,
                &xmatch_configs,
                &config.host_galaxy,
                db,
            )
            .await
            {
                Ok(found) => {
                    surroundings.insert(position, found);
                }
                Err(e) => {
                    results.push(failed(lightcurve, dia_object_id, e.to_string()));
                    continue;
                }
            }
        }
        let result = simulate_lsst_lightcurve(
            db,
            filter_pipeline,
            lightcurve,
            dia_object_id,
            detection_fields,
            &surroundings[&position],
            now,
        )
        .await
        .unwrap_or_else(|e| failed(lightcurve, dia_object_id, e.to_string()));
        results.push(result);
    }
    results
}

fn failed(
    lightcurve: &SimulatedLightcurve,
    dia_object_id: i64,
    error: String,
) -> SimulatedLightcurveResult {
    SimulatedLightcurveResult {
        id: lightcurve.id.clone(),
        object_id: dia_object_id.to_string(),
        n_alerts: 0,
        n_passed: 0,
        first_passed_mjd: None,
        first_passed_delay: None,
        passed: Vec::new(),
        error: Some(error),
    }
}
