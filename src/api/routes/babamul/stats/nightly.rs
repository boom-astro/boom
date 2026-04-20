use super::STATS_COLLECTION;
use crate::api::models::response;
use crate::utils::enums::Survey;
use actix_web::{get, web, HttpResponse};
use chrono::{Datelike, NaiveDate, Utc};
use futures::{StreamExt, TryStreamExt};
use mongodb::{
    bson::{doc, Document},
    Collection, Database,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::ToSchema;

/// MongoDB cache document storing the alert count for a single survey/night,
/// keyed by `_id` (`nightly_stats_<survey>_<date>`) with the expiration timestamp.
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

/// Per-night alert counts returned by the `/babamul/stats/nightly` endpoint.
/// Survey fields are populated only when the corresponding survey is requested.
#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct NightlyStat {
    pub date: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ztf: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lsst: Option<u64>,
}

/// Query parameters for the nightly stats endpoint: the date range (inclusive)
/// and an optional survey filter (`ztf` or `lsst`).
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
/// - Today (age 0): 1 hour — the night is still in progress, count changes fast.
/// - 1–7 days: 1 day.
/// - >7 days: 1 year.
fn cache_duration_secs(date: &NaiveDate, today: &NaiveDate) -> f64 {
    let age_days = (*today - *date).num_days();
    match age_days {
        ..=0 => 1.0 * 3600.0,
        1..=7 => 24.0 * 3600.0,
        _ => 365.0 * 24.0 * 3600.0,
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

    // For each missing (survey, night), count alerts in parallel.
    // Relies on a compound index on (candidate.programid, candidate.jd) for ZTF
    // and on candidate.jd for LSST so Mongo can satisfy the count via COUNT_SCAN.
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

        let alerts_collection = db.collection::<Document>(&format!("{}_alerts", survey));
        let count_futures = missing.into_iter().map(|date| {
            let start_jd = date_to_jd_local_noon(&date, survey);
            let end_jd = date_to_jd_local_noon(&(date + chrono::Duration::days(1)), survey);
            let mut filter = doc! {
                "candidate.jd": { "$gte": start_jd, "$lt": end_jd }
            };
            if *survey == Survey::Ztf {
                filter.insert("candidate.programid", 1);
            }
            let coll = alerts_collection.clone();
            let survey = survey.clone();
            async move {
                coll.count_documents(filter)
                    .await
                    .map(|c| (survey, date, c))
            }
        });

        let results: Vec<(Survey, NaiveDate, u64)> = match futures::stream::iter(count_futures)
            .buffer_unordered(30)
            .try_collect()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                return response::internal_error(&format!(
                    "Error counting alerts for {}: {}",
                    survey, e
                ));
            }
        };
        for (survey, date, count) in results {
            fresh_counts.insert((survey, date), count);
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

        // Today -> 1h
        assert_eq!(cache_duration_secs(&today, &today), 3600.0);

        // Yesterday -> 1 day
        let yesterday = NaiveDate::from_ymd_opt(2024, 6, 14).unwrap();
        assert_eq!(cache_duration_secs(&yesterday, &today), 86400.0);

        // 5 days ago -> 1 day
        let five_ago = NaiveDate::from_ymd_opt(2024, 6, 10).unwrap();
        assert_eq!(cache_duration_secs(&five_ago, &today), 86400.0);

        // 7 days ago -> 1 day (upper bound of the 1–7 day window)
        let seven_ago = NaiveDate::from_ymd_opt(2024, 6, 8).unwrap();
        assert_eq!(cache_duration_secs(&seven_ago, &today), 86400.0);

        // 15 days ago -> 1 year
        let fifteen_ago = NaiveDate::from_ymd_opt(2024, 5, 31).unwrap();
        assert_eq!(cache_duration_secs(&fifteen_ago, &today), 31536000.0);

        // 60 days ago -> 1 year
        let sixty_ago = NaiveDate::from_ymd_opt(2024, 4, 16).unwrap();
        assert_eq!(cache_duration_secs(&sixty_ago, &today), 31536000.0);
    }
}
