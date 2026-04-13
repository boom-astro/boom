use super::STATS_COLLECTION;
use crate::api::models::response;
use crate::utils::enums::Survey;
use actix_web::{get, web, HttpResponse};
use chrono::{Datelike, NaiveDate, Utc};
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, Document},
    Collection, Database,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize)]
struct NightlyStatCache {
    #[serde(rename = "_id")]
    id: String,
    survey: String,
    date: String,
    n_alerts: u64,
    updated_at: f64,
    cache_until: f64,
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct NightlyStat {
    pub date: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ztf: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lsst: Option<u64>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct StatsQuery {
    pub start_date: String,
    pub end_date: String,
    pub survey: Option<String>,
}

fn cache_id(survey: &Survey, date: &NaiveDate) -> String {
    format!("nightly_stats_{}_{}", survey, date.format("%Y-%m-%d"))
}

/// Convert a calendar date to Julian Date at **local noon** for the survey's
/// observatory.
///
/// An astronomical "night" for date D spans from JD(D, local noon) to
/// JD(D+1, local noon).
fn date_to_jd_local_noon(date: &NaiveDate, survey: &Survey) -> f64 {
    let y = date.year() as f64;
    let m = date.month() as f64;
    let d = date.day() as f64;

    let (y_adj, m_adj) = if m <= 2.0 {
        (y - 1.0, m + 12.0)
    } else {
        (y, m)
    };

    let a = (y_adj / 100.0_f64).floor();
    let b = 2.0_f64 - a + (a / 4.0_f64).floor();

    // JD at 0h UT (midnight UTC)
    let jd_midnight =
        (365.25_f64 * (y_adj + 4716.0)).floor() + (30.6001_f64 * (m_adj + 1.0)).floor() + d + b
            - 1524.5;

    // Shift to local noon: local noon = (12 − utc_offset) hours UTC
    let utc_offset = survey.observatory_utc_offset();
    jd_midnight + (12.0 - utc_offset) / 24.0
}

/// Cache duration (in seconds) grows with the age of the night.
///
/// - Today (age 0): 5 min — the night is still in progress, count changes fast.
/// - Yesterday: 30 min — might still get a late ingest.
/// - 2–7 days: 2 hours.
/// - 8–30 days: 12 hours.
/// - >30 days: 30 days
fn cache_duration_secs(date: &NaiveDate, today: &NaiveDate) -> f64 {
    let age_days = (*today - *date).num_days();
    match age_days {
        ..=0 => 5.0 * 60.0,
        1 => 30.0 * 60.0,
        2..=7 => 2.0 * 3600.0,
        8..=30 => 12.0 * 3600.0,
        _ => 30.0 * 24.0 * 3600.0,
    }
}

/// Get nightly alert counts for a date range.
///
/// Returns the number of alerts processed per night (noon-to-noon JD window).
/// Without the `survey` query parameter, returns stats for all surveys (ZTF + LSST).
/// ZTF counts only include public alerts (programid = 1).
/// Results are cached in MongoDB; cache lifetime grows with the age of the night.
#[utoipa::path(
    get,
    path = "/babamul/stats/nightly",
    params(
        ("start_date" = String, Query, description = "Start date (YYYY-MM-DD)"),
        ("end_date" = String, Query, description = "End date (YYYY-MM-DD)"),
        ("survey" = Option<String>, Query, description = "Optional survey filter (ztf or lsst)."),
    ),
    responses(
        (status = 200, description = "Stats retrieved", body = Vec<NightlyStat>),
        (status = 400, description = "Invalid parameters"),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Stats"]
)]
#[get("/stats/nightly")]
pub async fn get_nightly_stats(
    query: web::Query<StatsQuery>,
    db: web::Data<Database>,
) -> HttpResponse {
    let surveys = match query.survey.as_deref() {
        Some("ztf") => vec![Survey::Ztf],
        Some("lsst") => vec![Survey::Lsst],
        Some(_) => {
            return response::bad_request("Invalid survey, expected 'ztf' or 'lsst'");
        }
        None => vec![Survey::Ztf, Survey::Lsst],
    };

    let start_date = match NaiveDate::parse_from_str(&query.start_date, "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => return response::bad_request("Invalid start_date, expected YYYY-MM-DD"),
    };
    let end_date = match NaiveDate::parse_from_str(&query.end_date, "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => return response::bad_request("Invalid end_date, expected YYYY-MM-DD"),
    };
    if end_date < start_date {
        return response::bad_request("end_date must be >= start_date");
    }
    if (end_date - start_date).num_days() > 365 {
        return response::bad_request("Date range too large, maximum is 365 days");
    }

    let today = Utc::now().date_naive();
    let now_ts = Utc::now().timestamp() as f64;

    let stats_collection: Collection<NightlyStatCache> = db.collection(STATS_COLLECTION);

    let mut all_dates: Vec<NaiveDate> = Vec::new();
    let mut d = start_date;
    while d <= end_date {
        all_dates.push(d);
        d += chrono::Duration::days(1);
    }

    // Read all relevant cache entries in a single query
    let cache_keys: Vec<String> = surveys
        .iter()
        .flat_map(|s| all_dates.iter().map(move |d| cache_id(s, d)))
        .collect();

    let mut cache_counts: HashMap<(Survey, NaiveDate), u64> = HashMap::new();
    if let Ok(cursor) = stats_collection
        .find(doc! { "_id": { "$in": &cache_keys } })
        .await
    {
        if let Ok(docs) = cursor.try_collect::<Vec<_>>().await {
            for doc in docs {
                if doc.cache_until <= now_ts {
                    continue;
                }
                let survey = match doc.survey.as_str() {
                    "ZTF" => Survey::Ztf,
                    "LSST" => Survey::Lsst,
                    _ => continue,
                };
                let Ok(date) = NaiveDate::parse_from_str(&doc.date, "%Y-%m-%d") else {
                    continue;
                };
                cache_counts.insert((survey, date), doc.n_alerts);
            }
        }
    }

    // For each survey, run one aggregation covering every missing date in the range
    let mut fresh_counts: HashMap<(Survey, NaiveDate), u64> = HashMap::new();
    for survey in &surveys {
        let missing: Vec<NaiveDate> = all_dates
            .iter()
            .copied()
            .filter(|d| !cache_counts.contains_key(&(survey.clone(), *d)))
            .collect();

        if missing.is_empty() {
            continue;
        }

        let min_missing = *missing.first().unwrap();
        let max_missing = *missing.last().unwrap();
        let base_jd = date_to_jd_local_noon(&min_missing, survey);
        let end_jd = date_to_jd_local_noon(&(max_missing + chrono::Duration::days(1)), survey);

        let mut match_doc = doc! {
            "candidate.jd": { "$gte": base_jd, "$lt": end_jd }
        };
        if *survey == Survey::Ztf {
            match_doc.insert("candidate.programid", 1);
        }

        let pipeline = vec![
            doc! { "$match": match_doc },
            doc! {
                "$group": {
                    "_id": { "$toInt": { "$floor": { "$subtract": ["$candidate.jd", base_jd] } } },
                    "count": { "$sum": 1i64 }
                }
            },
        ];

        let alerts_collection = db.collection::<Document>(&format!("{}_alerts", survey));
        let cursor = match alerts_collection.aggregate(pipeline).await {
            Ok(c) => c,
            Err(e) => {
                return response::internal_error(&format!(
                    "Error aggregating alerts for {}: {}",
                    survey, e
                ));
            }
        };
        let docs: Vec<Document> = match cursor.try_collect().await {
            Ok(d) => d,
            Err(e) => {
                return response::internal_error(&format!(
                    "Error collecting aggregation for {}: {}",
                    survey, e
                ));
            }
        };

        let mut index_counts: HashMap<i64, u64> = HashMap::new();
        for doc in docs {
            let idx = doc
                .get("_id")
                .and_then(|v| v.as_i64().or_else(|| v.as_i32().map(|i| i as i64)))
                .unwrap_or(-1);
            let count = doc
                .get("count")
                .and_then(|v| v.as_i64().or_else(|| v.as_i32().map(|i| i as i64)))
                .unwrap_or(0)
                .max(0) as u64;
            index_counts.insert(idx, count);
        }

        // Fill counts for every missing date (0 when no alerts were returned)
        for date in &missing {
            let idx = (*date - min_missing).num_days();
            let count = *index_counts.get(&idx).unwrap_or(&0);
            fresh_counts.insert((survey.clone(), *date), count);
        }
    }

    // Upsert fresh counts into the cache in parallel
    let upserts: Vec<_> = fresh_counts
        .iter()
        .map(|((survey, date), count)| {
            let cache_secs = cache_duration_secs(date, &today);
            let id = cache_id(survey, date);
            let cache_doc = NightlyStatCache {
                id: id.clone(),
                survey: survey.to_string(),
                date: date.format("%Y-%m-%d").to_string(),
                n_alerts: *count,
                updated_at: now_ts,
                cache_until: now_ts + cache_secs,
            };
            let coll = stats_collection.clone();
            async move {
                let _ = coll
                    .replace_one(doc! { "_id": id }, &cache_doc)
                    .upsert(true)
                    .await;
            }
        })
        .collect();
    futures::future::join_all(upserts).await;

    let has_ztf = surveys.contains(&Survey::Ztf);
    let has_lsst = surveys.contains(&Survey::Lsst);
    let mut results: Vec<NightlyStat> = Vec::with_capacity(all_dates.len());
    for date in &all_dates {
        let lookup = |survey: Survey| {
            let key = (survey, *date);
            *cache_counts
                .get(&key)
                .or_else(|| fresh_counts.get(&key))
                .unwrap_or(&0)
        };
        let ztf = has_ztf.then(|| lookup(Survey::Ztf));
        let lsst = has_lsst.then(|| lookup(Survey::Lsst));
        results.push(NightlyStat {
            date: date.format("%Y-%m-%d").to_string(),
            ztf,
            lsst,
        });
    }

    response::ok(
        &format!("nightly stats for {} nights", results.len()),
        serde_json::json!(results),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_date_to_jd_local_noon_ztf() {
        // 2000-01-01: JD at midnight UTC = 2451544.5
        // ZTF local noon (PDT, UTC-7) = 19:00 UTC = +19/24 day
        // Expected: 2451544.5 + 19/24 = 2451545.291666...
        let d = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let jd = date_to_jd_local_noon(&d, &Survey::Ztf);
        let expected = 2451544.5 + 19.0 / 24.0;
        assert!((jd - expected).abs() < 1e-6, "got {}", jd);
    }

    #[test]
    fn test_date_to_jd_local_noon_lsst() {
        // 2000-01-01: JD at midnight UTC = 2451544.5
        // LSST local noon (CLST, UTC-3) = 15:00 UTC = +15/24 day
        // Expected: 2451544.5 + 15/24 = 2451545.125
        let d = NaiveDate::from_ymd_opt(2000, 1, 1).unwrap();
        let jd = date_to_jd_local_noon(&d, &Survey::Lsst);
        let expected = 2451544.5 + 15.0 / 24.0;
        assert!((jd - expected).abs() < 1e-6, "got {}", jd);
    }

    #[test]
    fn test_cache_duration() {
        let today = NaiveDate::from_ymd_opt(2024, 6, 15).unwrap();

        // Today -> 5 min
        assert_eq!(cache_duration_secs(&today, &today), 300.0);

        // Yesterday -> 30 min
        let yesterday = NaiveDate::from_ymd_opt(2024, 6, 14).unwrap();
        assert_eq!(cache_duration_secs(&yesterday, &today), 1800.0);

        // 5 days ago -> 2h
        let five_ago = NaiveDate::from_ymd_opt(2024, 6, 10).unwrap();
        assert_eq!(cache_duration_secs(&five_ago, &today), 7200.0);

        // 15 days ago -> 12h
        let fifteen_ago = NaiveDate::from_ymd_opt(2024, 5, 31).unwrap();
        assert_eq!(cache_duration_secs(&fifteen_ago, &today), 43200.0);

        // 60 days ago -> 30 days
        let sixty_ago = NaiveDate::from_ymd_opt(2024, 4, 16).unwrap();
        assert_eq!(cache_duration_secs(&sixty_ago, &today), 2592000.0);
    }
}
