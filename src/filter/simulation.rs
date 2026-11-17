use crate::{
    alert::{
        simulated_object, simulated_surroundings, AlertError, SimulatedEpoch, SimulatedSurroundings,
    },
    conf::AppConfig,
    enrichment::{
        add_decam_match, create_decam_alert_pipeline, create_lsst_alert_pipeline,
        create_ztf_alert_pipeline, decam_alert_properties_without_sso, lsst_alert_properties,
        ztf_alert_properties_without_sso, DecamAlertForEnrichment, EnrichmentWorkerError,
        LsstAlertForEnrichment, ZtfAlertForEnrichment,
    },
    utils::{db::mongify, enums::Survey, lightcurve_fits, lightcurves::Band},
};
use futures::TryStreamExt;
use mongodb::bson::{doc, from_document, Bson, Document};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use utoipa::ToSchema;

const ALERTS_PER_AGGREGATE: usize = 200;
pub const SIMULATED_SURVEYS: [Survey; 3] = [Survey::Lsst, Survey::Ztf, Survey::Decam];

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
    pub programid: Option<i32>,
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
    Enrichment(#[from] EnrichmentWorkerError),
    #[error(transparent)]
    Mongodb(#[from] mongodb::error::Error),
    #[error(transparent)]
    BsonDeserialization(#[from] mongodb::bson::de::Error),
    #[error(transparent)]
    BsonAccess(#[from] mongodb::bson::document::ValueAccessError),
    #[error("simulated alert {0} was not enriched")]
    NotEnriched(i64),
}

struct Replay {
    aux_collection: &'static str,
    series: &'static [&'static str],
    enrichment_pipeline: Vec<Document>,
}

fn replay(survey: &Survey) -> Result<Replay, SimulationError> {
    match survey {
        Survey::Lsst => Ok(Replay {
            aux_collection: "LSST_alerts_aux",
            series: &["prv_candidates", "fp_hists"],
            enrichment_pipeline: add_decam_match(create_lsst_alert_pipeline()),
        }),
        Survey::Ztf => Ok(Replay {
            aux_collection: "ZTF_alerts_aux",
            series: &["prv_candidates", "prv_nondetections", "fp_hists"],
            enrichment_pipeline: add_decam_match(create_ztf_alert_pipeline(false)),
        }),
        Survey::Decam => Ok(Replay {
            aux_collection: "DECAM_alerts_aux",
            series: &["prv_candidates", "fp_hists"],
            enrichment_pipeline: create_decam_alert_pipeline(),
        }),
        _ => Err(SimulationError::InvalidInput(format!(
            "simulation is not supported for {survey}"
        ))),
    }
}

fn enrich(survey: &Survey, document: Document) -> Result<Option<Document>, SimulationError> {
    match survey {
        Survey::Lsst => {
            let alert: LsstAlertForEnrichment = from_document(document)?;
            Ok(Some(
                doc! { "properties": mongify(&lsst_alert_properties(&alert)) },
            ))
        }
        Survey::Ztf => {
            let alert: ZtfAlertForEnrichment = from_document(document)?;
            match ztf_alert_properties_without_sso(&alert) {
                Ok((properties, lightcurve)) => {
                    let mut fields = doc! { "properties": mongify(&properties) };
                    if let Some(Some(fits)) = lightcurve_fits::fit_many(&[lightcurve]).pop() {
                        fields.insert("arnett_fit", mongify(&fits.arnett));
                        fields.insert("thermal_fit", mongify(&fits.thermal));
                    }
                    Ok(Some(fields))
                }
                Err(EnrichmentWorkerError::EmptyLightcurve(_)) => Ok(None),
                Err(e) => Err(e.into()),
            }
        }
        Survey::Decam => {
            let alert: DecamAlertForEnrichment = from_document(document)?;
            Ok(Some(
                doc! { "properties": mongify(&decam_alert_properties_without_sso(&alert)?) },
            ))
        }
        _ => Ok(None),
    }
}

fn documents_stage(documents: &[Document]) -> Document {
    doc! { "$documents": { "$literal": documents.to_vec() } }
}

pub fn join_literal_aux(
    pipeline: &mut [Document],
    collection: &str,
    aux: &Document,
    series: &[&str],
) {
    let trimmed: Document = series
        .iter()
        .map(|name| {
            let filter = doc! { "$filter": {
                "input": format!("${name}"),
                "cond": { "$lte": ["$$this.jd", "$$alert_jd"] },
            }};
            (name.to_string(), Bson::Document(filter))
        })
        .collect();
    for stage in pipeline.iter_mut() {
        let Ok(lookup) = stage.get_document_mut("$lookup") else {
            continue;
        };
        let own_aux = lookup.get_str("from").is_ok_and(|from| from == collection)
            && lookup
                .get_str("localField")
                .is_ok_and(|field| field == "objectId");
        if own_aux {
            lookup.remove("from");
            lookup.insert("let", doc! { "alert_jd": "$candidate.jd" });
            lookup.insert(
                "pipeline",
                vec![
                    documents_stage(std::slice::from_ref(aux)),
                    doc! { "$set": trimmed.clone() },
                ],
            );
        }
    }
}

async fn run_on_documents(
    db: &mongodb::Database,
    replay: &Replay,
    pipeline: &[Document],
    alerts: &[Document],
    aux: &Document,
    mut handle: impl FnMut(Document) -> Result<(), SimulationError>,
) -> Result<(), SimulationError> {
    for chunk in alerts.chunks(ALERTS_PER_AGGREGATE) {
        let mut pipeline = pipeline.to_vec();
        pipeline[0] = documents_stage(chunk);
        join_literal_aux(&mut pipeline, replay.aux_collection, aux, replay.series);
        let mut cursor = db.aggregate(pipeline).await?;
        while let Some(document) = cursor.try_next().await? {
            handle(document)?;
        }
    }
    Ok(())
}

fn band_name(band: &Band) -> Option<String> {
    serde_json::to_value(band)
        .ok()
        .and_then(|value| value.as_str().map(str::to_string))
}

#[allow(clippy::too_many_arguments)]
async fn simulate_lightcurve(
    db: &mongodb::Database,
    survey: &Survey,
    replay: &Replay,
    filter_pipeline: &[Document],
    lightcurve: &SimulatedLightcurve,
    index: usize,
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

    let object = simulated_object(
        survey,
        index as i64,
        lightcurve.ra,
        lightcurve.dec,
        lightcurve.programid.unwrap_or(1),
        &epochs,
        detection_fields,
        surroundings,
        now,
    )?;
    let alert_documents: Vec<Document> = object
        .alerts
        .iter()
        .map(|alert| alert.document.clone())
        .collect();

    let mut enriched: HashMap<i64, Option<Document>> = HashMap::new();
    run_on_documents(
        db,
        replay,
        &replay.enrichment_pipeline,
        &alert_documents,
        &object.aux,
        |document| {
            let candid = document.get_i64("_id")?;
            enriched.insert(candid, enrich(survey, document)?);
            Ok(())
        },
    )
    .await?;
    let mut filterable = Vec::with_capacity(object.alerts.len());
    for alert in &object.alerts {
        match enriched.remove(&alert.candid) {
            None => return Err(SimulationError::NotEnriched(alert.candid)),
            Some(None) => {}
            Some(Some(fields)) => {
                let mut document = alert.document.clone();
                document.extend(fields);
                filterable.push(document);
            }
        }
    }

    let mut passed: HashMap<i64, Option<Document>> = HashMap::new();
    run_on_documents(
        db,
        replay,
        filter_pipeline,
        &filterable,
        &object.aux,
        |document| {
            let candid = document.get_i64("_id")?;
            passed.insert(candid, document.get_document("annotations").ok().cloned());
            Ok(())
        },
    )
    .await?;

    let passed: Vec<SimulatedAlertResult> = object
        .alerts
        .iter()
        .filter_map(|alert| {
            let annotations = passed.remove(&alert.candid)?;
            Some(SimulatedAlertResult {
                candid: alert.candid,
                mjd: alert.mjd,
                band: band_name(&alert.band),
                annotations,
            })
        })
        .collect();
    let first_passed_mjd = passed.first().map(|alert| alert.mjd);

    Ok(SimulatedLightcurveResult {
        id: lightcurve.id.clone(),
        object_id: (-(index as i64) - 1).to_string(),
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

pub async fn simulate_filter(
    db: &mongodb::Database,
    config: &AppConfig,
    survey: &Survey,
    filter_pipeline: &[Document],
    permissions: &HashMap<Survey, Vec<i32>>,
    lightcurves: &[SimulatedLightcurve],
    detection_fields: &Map<String, Value>,
) -> Result<Vec<SimulatedLightcurveResult>, SimulationError> {
    let replay = replay(survey)?;
    let ztf_programids = permissions.get(&Survey::Ztf).cloned().unwrap_or_default();
    let mut filter_pipeline = filter_pipeline.to_vec();
    if *survey == Survey::Ztf {
        filter_pipeline.insert(
            1,
            doc! { "$match": { "candidate.programid": { "$in": &ztf_programids } } },
        );
    }
    let xmatch_configs = config.crossmatch.get(survey).cloned().unwrap_or_default();
    let now = flare::Time::now().to_jd();
    let mut surroundings: HashMap<(u64, u64), SimulatedSurroundings> = HashMap::new();
    let mut results = Vec::with_capacity(lightcurves.len());

    for (index, lightcurve) in lightcurves.iter().enumerate() {
        let programid = lightcurve.programid.unwrap_or(1);
        if *survey == Survey::Ztf && !ztf_programids.contains(&programid) {
            results.push(failed(
                lightcurve,
                index,
                format!("programid {programid} is not in the filter's ZTF permissions"),
            ));
            continue;
        }
        let position = (lightcurve.ra.to_bits(), lightcurve.dec.to_bits());
        if !surroundings.contains_key(&position) {
            match simulated_surroundings(
                survey,
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
                    results.push(failed(lightcurve, index, e.to_string()));
                    continue;
                }
            }
        }
        let result = simulate_lightcurve(
            db,
            survey,
            &replay,
            &filter_pipeline,
            lightcurve,
            index,
            detection_fields,
            &surroundings[&position],
            now,
        )
        .await
        .unwrap_or_else(|e| failed(lightcurve, index, e.to_string()));
        results.push(result);
    }
    Ok(results)
}

fn failed(
    lightcurve: &SimulatedLightcurve,
    index: usize,
    error: String,
) -> SimulatedLightcurveResult {
    SimulatedLightcurveResult {
        id: lightcurve.id.clone(),
        object_id: (-(index as i64) - 1).to_string(),
        n_alerts: 0,
        n_passed: 0,
        first_passed_mjd: None,
        first_passed_delay: None,
        passed: Vec::new(),
        error: Some(error),
    }
}
