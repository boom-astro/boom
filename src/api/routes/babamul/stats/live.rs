use super::STATS_COLLECTION;
use crate::api::models::response;
use crate::api::routes::babamul::{BabamulAcl, BabamulUser};
use crate::utils::enums::Survey;
use crate::utils::ingest_stats::{stat_id, INGEST_STATS_COLLECTION};
use actix_web::{get, web, HttpResponse};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use futures::TryStreamExt;
use mongodb::{bson::doc, Collection, Database};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::LazyLock;
use tokio::sync::Mutex;
use utoipa::ToSchema;

const LIVE_INGEST_CACHE_KEY: &str = "live_ingest";
const PREVIOUS_NIGHT_CACHE_KEY: &str = "live_ingest_previous";
const LIVE_INGEST_CACHE_SECS: f64 = 60.0;
const PREVIOUS_NIGHT_CACHE_SECS: f64 = 600.0;
const BIN_MINUTES: u32 = 10;
const LIVE_SURVEYS: [Survey; 4] = [Survey::Ztf, Survey::Lsst, Survey::Decam, Survey::Winter];

static RECOUNT: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum LiveNight {
    #[default]
    Current,
    Previous,
}

impl LiveNight {
    fn cache(self) -> (&'static str, f64) {
        match self {
            LiveNight::Current => (LIVE_INGEST_CACHE_KEY, LIVE_INGEST_CACHE_SECS),
            LiveNight::Previous => (PREVIOUS_NIGHT_CACHE_KEY, PREVIOUS_NIGHT_CACHE_SECS),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct LiveIngestQuery {
    #[serde(default)]
    pub night: LiveNight,
}

#[derive(Debug, Serialize, Deserialize)]
struct LiveIngestCache {
    #[serde(rename = "_id")]
    id: String,
    live: LiveIngest,
    cache_until: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LiveBin {
    pub start: DateTime<Utc>,
    pub received: u64,
    pub processed: u64,
    pub latency_s: Option<f64>,
    pub latency_max_s: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LiveSurvey {
    pub survey: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub received: u64,
    pub processed: u64,
    pub last_received_at: Option<DateTime<Utc>>,
    pub bins: Vec<LiveBin>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LiveIngest {
    pub date: String,
    pub bin_minutes: u32,
    pub updated_at: DateTime<Utc>,
    pub surveys: Vec<LiveSurvey>,
}

#[derive(Debug, Deserialize)]
struct MinuteRecord {
    minute: i64,
    #[serde(default)]
    received: u64,
    #[serde(default)]
    processed: u64,
    #[serde(default)]
    latency_sum: f64,
    #[serde(default)]
    latency_max: f64,
}

#[derive(Default)]
struct BinTotals {
    received: u64,
    processed: u64,
    latency_sum: f64,
    latency_max: f64,
}

fn current_night(survey: &Survey, now: DateTime<Utc>) -> NaiveDate {
    let today = now.date_naive();
    if survey.night_window(&today).0 <= now {
        today
    } else {
        today - Duration::days(1)
    }
}

async fn count_survey(
    db: &Database,
    survey: &Survey,
    date: &NaiveDate,
) -> Result<LiveSurvey, mongodb::error::Error> {
    let (start, end) = survey.night_window(date);
    let name = survey.as_str();
    let collection: Collection<MinuteRecord> = db.collection(INGEST_STATS_COLLECTION);
    let records: Vec<MinuteRecord> = collection
        .find(doc! { "_id": {
            "$gte": stat_id(name, start.timestamp_millis()),
            "$lt": stat_id(name, end.timestamp_millis()),
        } })
        .await?
        .try_collect()
        .await?;

    let bin_ms = BIN_MINUTES as i64 * 60_000;
    let mut totals: BTreeMap<i64, BinTotals> = BTreeMap::new();
    for record in &records {
        let bin = (record.minute - start.timestamp_millis()).div_euclid(bin_ms);
        let total = totals.entry(bin).or_default();
        total.received += record.received;
        total.processed += record.processed;
        total.latency_sum += record.latency_sum;
        total.latency_max = total.latency_max.max(record.latency_max);
    }

    let last_received_at = records
        .iter()
        .filter(|record| record.received > 0)
        .map(|record| record.minute)
        .max()
        .and_then(DateTime::<Utc>::from_timestamp_millis);

    let bins: Vec<LiveBin> = totals
        .into_iter()
        .map(|(bin, total)| LiveBin {
            start: start + Duration::minutes(bin * BIN_MINUTES as i64),
            received: total.received,
            processed: total.processed,
            latency_s: (total.processed > 0).then(|| total.latency_sum / total.processed as f64),
            latency_max_s: (total.processed > 0).then_some(total.latency_max),
        })
        .collect();

    Ok(LiveSurvey {
        survey: name.to_lowercase(),
        start,
        end,
        received: bins.iter().map(|bin| bin.received).sum(),
        processed: bins.iter().map(|bin| bin.processed).sum(),
        last_received_at,
        bins,
    })
}

async fn count_live_ingest(
    db: &Database,
    night: LiveNight,
) -> Result<LiveIngest, mongodb::error::Error> {
    let now = Utc::now();
    let current = LIVE_SURVEYS
        .iter()
        .map(|survey| current_night(survey, now))
        .min()
        .unwrap_or_else(|| now.date_naive());
    let date = match night {
        LiveNight::Current => current,
        LiveNight::Previous => current - Duration::days(1),
    };
    let surveys = futures::future::try_join_all(
        LIVE_SURVEYS
            .iter()
            .map(|survey| count_survey(db, survey, &date)),
    )
    .await?;
    Ok(LiveIngest {
        date: date.format("%Y-%m-%d").to_string(),
        bin_minutes: BIN_MINUTES,
        updated_at: now,
        surveys,
    })
}

async fn read_cache(collection: &Collection<LiveIngestCache>, key: &str) -> Option<LiveIngest> {
    collection
        .find_one(doc! {
            "_id": key,
            "cache_until": { "$gt": Utc::now().timestamp() as f64 },
        })
        .await
        .ok()
        .flatten()
        .map(|cache| cache.live)
}

/// Alerts BOOM received tonight or last night per survey in 10-minute bins, with its processing
/// latency, cached one minute for tonight and ten for last night.
#[utoipa::path(
    get,
    path = "/babamul/stats/live",
    params(
        ("night" = Option<LiveNight>, Query, description = "current (default) or previous night."),
    ),
    responses(
        (status = 200, description = "Live ingestion stats retrieved", body = LiveIngest),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Stats"]
)]
#[get("/stats/live")]
pub async fn get_live_ingest(
    current_user: Option<web::ReqData<BabamulUser>>,
    query: web::Query<LiveIngestQuery>,
    db: web::Data<Database>,
) -> HttpResponse {
    let stats_collection: Collection<LiveIngestCache> = db.collection(STATS_COLLECTION);
    let night = query.night;
    let (cache_key, cache_secs) = night.cache();

    let mut live = match read_cache(&stats_collection, cache_key).await {
        Some(live) => live,
        None => {
            let _recount = RECOUNT.lock().await;
            match read_cache(&stats_collection, cache_key).await {
                Some(live) => live,
                None => {
                    let live = match count_live_ingest(&db, night).await {
                        Ok(live) => live,
                        Err(e) => {
                            return response::internal_error(&format!(
                                "Error counting the night's alerts: {}",
                                e
                            ));
                        }
                    };
                    let cache = LiveIngestCache {
                        id: cache_key.to_string(),
                        live: live.clone(),
                        cache_until: live.updated_at.timestamp() as f64 + cache_secs,
                    };
                    if let Err(e) = stats_collection
                        .replace_one(doc! { "_id": cache_key }, &cache)
                        .upsert(true)
                        .await
                    {
                        tracing::warn!("Failed to upsert live ingest cache: {}", e);
                    }
                    live
                }
            }
        }
    };

    if !current_user.is_some_and(|user| user.has_acl(BabamulAcl::Winter)) {
        live.surveys.retain(|survey| survey.survey != "winter");
    }

    response::ok(
        &format!("live ingest for the night of {}", live.date),
        serde_json::json!(live),
    )
}
