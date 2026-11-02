use super::context::TaskContext;
use super::ledger::{MutationTarget, Operation};
use super::TaskError;
use crate::conf::{AppConfig, TnsPhotometryConfig};
use crate::utils::bson_number;
use crate::utils::tns::{
    TnsClient, TnsError, TnsPhotometry, PHOTOMETRY_COLLECTION, TNS_COLLECTION,
};
use futures::TryStreamExt;
use mongodb::bson::{doc, Document};
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

pub const TASK_TYPE: &str = "tns_photometry";

const MAX_NAMES: usize = 1_000;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TnsPhotometryParams {
    /// TNS names such as 2026abc, fetched instead of the configured selection.
    #[serde(default)]
    pub names: Vec<String>,
}

impl TnsPhotometryParams {
    pub fn validate_params(&self) -> Result<(), String> {
        if self.names.len() > MAX_NAMES {
            return Err(format!("at most {MAX_NAMES} names per run"));
        }
        if self.names.iter().any(|name| bare_name(name).is_empty()) {
            return Err("names must not be empty".to_string());
        }
        Ok(())
    }
}

struct Target {
    objid: i64,
    name: String,
}

struct Crossmatch {
    aux: String,
    field: String,
    radius: f64,
}

fn failed(error: impl std::fmt::Display) -> TaskError {
    TaskError::Failed(error.to_string())
}

fn bare_name(name: &str) -> &str {
    name.trim()
        .trim_start_matches(|c: char| c.is_ascii_alphabetic())
        .trim()
}

fn tns_crossmatches(config: &AppConfig) -> Vec<Crossmatch> {
    config
        .crossmatch
        .iter()
        .filter_map(|(survey, catalogs)| {
            let tns = catalogs
                .iter()
                .find(|c| c.collection_name() == TNS_COLLECTION)?;
            Some(Crossmatch {
                aux: format!("{survey}_alerts_aux"),
                field: format!("cross_matches.{}._id", tns.catalog),
                radius: tns.radius,
            })
        })
        .collect()
}

pub async fn run(
    ctx: &TaskContext,
    params: TnsPhotometryParams,
) -> Result<serde_json::Value, TaskError> {
    params.validate_params().map_err(TaskError::InvalidParams)?;
    let config = &ctx.config().tns;
    let mut client = TnsClient::new(config).map_err(failed)?;
    let targets = if params.names.is_empty() {
        select(ctx, &config.photometry, super::models::now()).await?
    } else {
        named(ctx, &params.names).await?
    };
    ctx.info(format!(
        "fetching TNS photometry for {} object(s)",
        targets.len()
    ));

    let collection = ctx.db().collection::<TnsPhotometry>(PHOTOMETRY_COLLECTION);
    let canceled = ctx.cancel_flag();
    let (mut fetched, mut points, mut errors) = (0u64, 0u64, 0u64);
    let mut outcome = Ok(());
    for (i, target) in targets.iter().enumerate() {
        if ctx.is_canceled() {
            outcome = Err(TaskError::Canceled);
            break;
        }
        let photometry = match client.object_photometry(&target.name, &canceled).await {
            Ok(photometry) => photometry,
            Err(TnsError::Canceled) => {
                outcome = Err(TaskError::Canceled);
                break;
            }
            Err(
                error @ TnsError::Status {
                    status: StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN,
                    ..
                },
            ) => {
                outcome = Err(failed(error));
                break;
            }
            Err(error) => {
                ctx.warn(format!("{}: {error}", target.name));
                errors += 1;
                continue;
            }
        };
        points += photometry.len() as u64;
        let document = TnsPhotometry {
            objid: target.objid,
            name: target.name.clone(),
            fetched_at: super::models::now(),
            photometry,
        };
        if let Err(error) = collection
            .replace_one(doc! { "_id": target.objid }, &document)
            .upsert(true)
            .await
        {
            outcome = Err(failed(error));
            break;
        }
        fetched += 1;
        ctx.progress(i as u64 + 1, targets.len() as u64, &target.name)
            .await;
    }

    if fetched > 0 {
        ctx.record_mutation(
            MutationTarget {
                database: ctx.db().name().to_string(),
                collection: PHOTOMETRY_COLLECTION.to_string(),
                catalog: None,
                survey: None,
            },
            Operation::Ingest,
            doc! { "objects": fetched as i64, "points": points as i64 },
        )
        .await;
    }
    outcome?;
    Ok(serde_json::json!({
        "selected": targets.len(),
        "fetched": fetched,
        "points": points,
        "errors": errors,
    }))
}

async fn select(
    ctx: &TaskContext,
    config: &TnsPhotometryConfig,
    now: f64,
) -> Result<Vec<Target>, TaskError> {
    let db = ctx.db();
    let crossmatches = tns_crossmatches(ctx.config());
    let now_jd = now / 86_400.0 + 2_440_587.5;
    let mut young = db
        .collection::<Document>(TNS_COLLECTION)
        .find(doc! { "discovery_jd": { "$gte": now_jd - config.max_age_days } })
        .projection(doc! { "name": 1, "ra": 1, "dec": 1 })
        .await
        .map_err(failed)?;
    let mut matched = Vec::new();
    while let Some(object) = young.try_next().await.map_err(failed)? {
        let (Some(objid), Ok(name), Some(ra), Some(dec)) = (
            object.get("_id").and_then(bson_number),
            object.get_str("name"),
            object.get("ra").and_then(bson_number),
            object.get("dec").and_then(bson_number),
        ) else {
            continue;
        };
        let objid = objid as i64;
        for crossmatch in &crossmatches {
            let field = crossmatch.field.as_str();
            let hit = db
                .collection::<Document>(&crossmatch.aux)
                .find_one(doc! {
                    "coordinates.radec_geojson": {
                        "$geoWithin": { "$centerSphere": [[ra - 180.0, dec], crossmatch.radius] }
                    },
                    field: objid,
                    "host_galaxy.best_host.dist_mpc": { "$lte": config.max_distance_mpc },
                })
                .projection(doc! { "_id": 1 })
                .await
                .map_err(failed)?;
            if hit.is_some() {
                matched.push(Target {
                    objid,
                    name: name.to_string(),
                });
                break;
            }
        }
    }
    ctx.info(format!(
        "{} young TNS object(s) matched with a host within {} Mpc",
        matched.len(),
        config.max_distance_mpc
    ));
    due(ctx, matched, config, now).await
}

async fn due(
    ctx: &TaskContext,
    mut targets: Vec<Target>,
    config: &TnsPhotometryConfig,
    now: f64,
) -> Result<Vec<Target>, TaskError> {
    let ids: Vec<i64> = targets.iter().map(|t| t.objid).collect();
    let mut cursor = ctx
        .db()
        .collection::<Document>(PHOTOMETRY_COLLECTION)
        .find(doc! { "_id": { "$in": ids } })
        .projection(doc! { "fetched_at": 1 })
        .await
        .map_err(failed)?;
    let mut fetched_at = HashMap::new();
    while let Some(fetched) = cursor.try_next().await.map_err(failed)? {
        if let (Some(objid), Ok(at)) = (
            fetched.get("_id").and_then(bson_number),
            fetched.get_f64("fetched_at"),
        ) {
            fetched_at.insert(objid as i64, at);
        }
    }
    let stale_before = now - config.refresh_hours * 3600.0;
    let last = |target: &Target| {
        fetched_at
            .get(&target.objid)
            .copied()
            .unwrap_or(f64::NEG_INFINITY)
    };
    targets.retain(|target| last(target) < stale_before);
    targets.sort_by(|a, b| last(a).total_cmp(&last(b)));
    targets.truncate(config.max_objects);
    Ok(targets)
}

async fn named(ctx: &TaskContext, names: &[String]) -> Result<Vec<Target>, TaskError> {
    let wanted: Vec<String> = names.iter().map(|n| bare_name(n).to_string()).collect();
    let mut cursor = ctx
        .db()
        .collection::<Document>(TNS_COLLECTION)
        .find(doc! { "name": { "$in": wanted.clone() } })
        .projection(doc! { "name": 1 })
        .await
        .map_err(failed)?;
    let mut targets = Vec::new();
    while let Some(object) = cursor.try_next().await.map_err(failed)? {
        if let (Some(objid), Ok(name)) = (
            object.get("_id").and_then(bson_number),
            object.get_str("name"),
        ) {
            targets.push(Target {
                objid: objid as i64,
                name: name.to_string(),
            });
        }
    }
    let missing: Vec<&String> = wanted
        .iter()
        .filter(|name| !targets.iter().any(|t| &t.name == *name))
        .collect();
    if !missing.is_empty() {
        ctx.warn(format!(
            "not in {TNS_COLLECTION}, run tns_sync first: {missing:?}"
        ));
    }
    Ok(targets)
}
