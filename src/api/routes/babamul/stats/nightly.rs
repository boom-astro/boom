use super::STATS_COLLECTION;
use crate::api::models::response;
use crate::utils::enums::Survey;
use actix_web::{get, web, HttpResponse};
use chrono::{Datelike, NaiveDate, Utc};
use mongodb::{bson::doc, Collection, Database};
use serde::{Deserialize, Serialize};
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
/// - Yesterday: 30 min — final count might still get a late ingest.
/// - 2–7 days: 2 hours.
/// - 8–30 days: 12 hours.
/// - >30 days: 30 days — count is effectively frozen.
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
) -> Result<HttpResponse, actix_web::Error> {
    let surveys = match query.survey.as_deref() {
        Some("ztf") => vec![Survey::Ztf],
        Some("lsst") => vec![Survey::Lsst],
        Some(_) => {
            return Ok(response::bad_request(
                "Invalid survey, expected 'ztf' or 'lsst'",
            ))
        }
        None => vec![Survey::Ztf, Survey::Lsst],
    };

    let start_date = match NaiveDate::parse_from_str(&query.start_date, "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => {
            return Ok(response::bad_request(
                "Invalid start_date, expected YYYY-MM-DD",
            ))
        }
    };
    let end_date = match NaiveDate::parse_from_str(&query.end_date, "%Y-%m-%d") {
        Ok(d) => d,
        Err(_) => {
            return Ok(response::bad_request(
                "Invalid end_date, expected YYYY-MM-DD",
            ))
        }
    };
    if end_date < start_date {
        return Ok(response::bad_request("end_date must be >= start_date"));
    }
    if (end_date - start_date).num_days() > 365 {
        return Ok(response::bad_request(
            "Date range too large, maximum is 365 days",
        ));
    }

    let today = Utc::now().date_naive();
    let now_ts = Utc::now().timestamp() as f64;

    let stats_collection: Collection<NightlyStatCache> = db.collection(STATS_COLLECTION);
    let has_ztf = surveys.contains(&Survey::Ztf);
    let has_lsst = surveys.contains(&Survey::Lsst);

    let mut results: Vec<NightlyStat> = Vec::new();
    let mut date = start_date;

    while date <= end_date {
        let date_str = date.format("%Y-%m-%d").to_string();

        let ztf = if has_ztf {
            Some(
                fetch_night_count(
                    &stats_collection,
                    &db,
                    &Survey::Ztf,
                    &date,
                    &date_str,
                    now_ts,
                    &today,
                )
                .await?,
            )
        } else {
            None
        };

        let lsst = if has_lsst {
            Some(
                fetch_night_count(
                    &stats_collection,
                    &db,
                    &Survey::Lsst,
                    &date,
                    &date_str,
                    now_ts,
                    &today,
                )
                .await?,
            )
        } else {
            None
        };

        results.push(NightlyStat {
            date: date_str,
            ztf,
            lsst,
        });

        date += chrono::Duration::days(1);
    }

    Ok(response::ok(
        &format!("nightly stats for {} nights", results.len()),
        serde_json::json!(results),
    ))
}

async fn fetch_night_count(
    stats_collection: &Collection<NightlyStatCache>,
    db: &Database,
    survey: &Survey,
    date: &NaiveDate,
    date_str: &str,
    now_ts: f64,
    today: &NaiveDate,
) -> Result<u64, actix_web::Error> {
    let cache_key = format!("{}_{}", survey, date_str);

    // Try to serve from cache
    if let Ok(Some(doc)) = stats_collection.find_one(doc! { "_id": &cache_key }).await {
        if doc.cache_until > now_ts {
            return Ok(doc.n_alerts);
        }
    }

    // Count alerts for this night: local noon → next local noon
    let jd_start = date_to_jd_local_noon(date, survey);
    let jd_end = jd_start + 1.0;

    let mut filter = doc! {
        "candidate.jd": { "$gte": jd_start, "$lt": jd_end }
    };
    if *survey == Survey::Ztf {
        filter.insert("candidate.programid", 1);
    }

    let alerts_collection = db.collection::<mongodb::bson::Document>(&format!("{}_alerts", survey));

    let count = alerts_collection
        .count_documents(filter)
        .await
        .map_err(|e| {
            actix_web::error::ErrorInternalServerError(format!(
                "Error counting alerts for {}: {}",
                date_str, e
            ))
        })?;

    // Upsert into cache
    let cache_secs = cache_duration_secs(date, today);
    let cache_doc = NightlyStatCache {
        id: cache_key.clone(),
        survey: survey.to_string(),
        date: date_str.to_string(),
        n_alerts: count,
        updated_at: now_ts,
        cache_until: now_ts + cache_secs,
    };
    let _ = stats_collection
        .replace_one(doc! { "_id": &cache_key }, &cache_doc)
        .upsert(true)
        .await;

    Ok(count)
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
