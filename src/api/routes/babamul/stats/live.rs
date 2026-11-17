use super::STATS_COLLECTION;
use crate::api::models::response;
use crate::api::routes::babamul::{BabamulAcl, BabamulUser};
use crate::utils::enums::Survey;
use actix_web::{get, web, HttpResponse};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, Document},
    Collection, Database,
};
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;
use tokio::sync::Mutex;
use utoipa::ToSchema;

const LIVE_INGEST_CACHE_KEY: &str = "live_ingest";
const LIVE_INGEST_CACHE_SECS: f64 = 60.0;
const BIN_MINUTES: u32 = 10;
const LIVE_SURVEYS: [Survey; 4] = [Survey::Ztf, Survey::Lsst, Survey::Decam, Survey::Winter];

static RECOUNT: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

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
    pub n_alerts: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LatestAlert {
    pub observed_at: DateTime<Utc>,
    pub ingested_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LiveSurvey {
    pub survey: String,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub n_alerts: u64,
    pub latest: Option<LatestAlert>,
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
struct BinCount {
    #[serde(rename = "_id")]
    bin: f64,
    n: u64,
}

#[derive(Debug, Deserialize)]
struct SampleCandidate {
    jd: f64,
}

#[derive(Debug, Deserialize)]
struct Sample {
    candidate: SampleCandidate,
    created_at: Option<f64>,
}

fn current_night(survey: &Survey, now: DateTime<Utc>) -> NaiveDate {
    let today = now.date_naive();
    if survey.night_window(&today).0 <= now {
        today
    } else {
        today - Duration::days(1)
    }
}

fn jd_to_utc(jd: f64) -> DateTime<Utc> {
    flare::Time::from_jd(jd).to_utc()
}

async fn count_survey(
    db: &Database,
    survey: &Survey,
    date: &NaiveDate,
) -> Result<LiveSurvey, mongodb::error::Error> {
    let (start, end) = survey.night_window(date);
    let (start_jd, _) = survey.night_jd_window(date);
    let bin_days = BIN_MINUTES as f64 / 1440.0;
    let n_bins = (end - start).num_minutes() as f64 / BIN_MINUTES as f64;
    let window = |from: f64, to: f64| {
        let mut filter = doc! { "candidate.jd": { "$gte": from, "$lt": to } };
        if *survey == Survey::Ztf {
            filter.insert("candidate.programid", 1);
        }
        filter
    };

    let alerts: Collection<Document> = db.collection(&format!("{}_alerts", survey));
    let mut counts: Vec<(i64, u64)> = alerts
        .aggregate(vec![
            doc! { "$match": window(start_jd, start_jd + n_bins * bin_days) },
            doc! { "$group": {
                "_id": { "$floor": { "$divide": [
                    { "$subtract": ["$candidate.jd", start_jd] },
                    bin_days,
                ] } },
                "n": { "$sum": 1 },
            } },
        ])
        .with_type::<BinCount>()
        .await?
        .map_ok(|count| (count.bin as i64, count.n))
        .try_collect()
        .await?;
    counts.sort_unstable();

    let samples: Collection<Sample> = db.collection(&format!("{}_alerts", survey));
    let latest = samples
        .find_one(window(start_jd, start_jd + n_bins * bin_days))
        .sort(doc! { "candidate.jd": -1 })
        .projection(doc! { "_id": 0, "candidate.jd": 1, "created_at": 1 })
        .await?
        .map(|alert| LatestAlert {
            observed_at: jd_to_utc(alert.candidate.jd),
            ingested_at: alert.created_at.map(jd_to_utc),
        });

    let bins = counts
        .iter()
        .map(|&(bin, n_alerts)| LiveBin {
            start: start + Duration::minutes(bin * BIN_MINUTES as i64),
            n_alerts,
        })
        .collect();

    Ok(LiveSurvey {
        survey: survey.as_str().to_lowercase(),
        start,
        end,
        n_alerts: counts.iter().map(|(_, n)| n).sum(),
        latest,
        bins,
    })
}

async fn count_live_ingest(db: &Database) -> Result<LiveIngest, mongodb::error::Error> {
    let now = Utc::now();
    let date = LIVE_SURVEYS
        .iter()
        .map(|survey| current_night(survey, now))
        .min()
        .unwrap_or_else(|| now.date_naive());
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

async fn read_cache(collection: &Collection<LiveIngestCache>) -> Option<LiveIngest> {
    collection
        .find_one(doc! {
            "_id": LIVE_INGEST_CACHE_KEY,
            "cache_until": { "$gt": Utc::now().timestamp() as f64 },
        })
        .await
        .ok()
        .flatten()
        .map(|cache| cache.live)
}

/// Tonight's alerts per survey in 10-minute bins and the latest alert, cached one minute.
#[utoipa::path(
    get,
    path = "/babamul/stats/live",
    responses(
        (status = 200, description = "Live ingestion stats retrieved", body = LiveIngest),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Stats"]
)]
#[get("/stats/live")]
pub async fn get_live_ingest(
    current_user: Option<web::ReqData<BabamulUser>>,
    db: web::Data<Database>,
) -> HttpResponse {
    let stats_collection: Collection<LiveIngestCache> = db.collection(STATS_COLLECTION);

    let mut live = match read_cache(&stats_collection).await {
        Some(live) => live,
        None => {
            let _recount = RECOUNT.lock().await;
            match read_cache(&stats_collection).await {
                Some(live) => live,
                None => {
                    let live = match count_live_ingest(&db).await {
                        Ok(live) => live,
                        Err(e) => {
                            return response::internal_error(&format!(
                                "Error counting tonight's alerts: {}",
                                e
                            ));
                        }
                    };
                    let cache = LiveIngestCache {
                        id: LIVE_INGEST_CACHE_KEY.to_string(),
                        live: live.clone(),
                        cache_until: live.updated_at.timestamp() as f64 + LIVE_INGEST_CACHE_SECS,
                    };
                    if let Err(e) = stats_collection
                        .replace_one(doc! { "_id": LIVE_INGEST_CACHE_KEY }, &cache)
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
