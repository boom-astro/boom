use super::collections::COLLECTION_STATS_CACHE_KEY;
use super::kafka::BABAMUL_KAFKA_TOPICS_CACHE_KEY;
use super::nightly::NIGHTLY_STATS_CACHE_PREFIX;
use super::STATS_COLLECTION;
use crate::api::models::response;
use crate::api::routes::babamul::BabamulUser;
use actix_web::{post, web, HttpResponse};
use chrono::{Months, NaiveDate, Utc};
use mongodb::{
    bson::{doc, Document},
    Collection, Database,
};
use serde::Deserialize;
use utoipa::ToSchema;

const REFRESH_COOLDOWN_KEY: &str = "stats_refresh";
/// Dropping the caches makes the next dashboard load recount every night in the
/// range, so a global cooldown bounds how often that can be triggered.
const REFRESH_COOLDOWN_SECS: i64 = 5 * 60;
/// Longest range that can be recounted in one go.
const MAX_REFRESH_MONTHS: u32 = 6;

/// Query parameters for the stats refresh endpoint: the night range (inclusive)
/// whose cached counts should be dropped.
#[derive(Debug, Deserialize, ToSchema)]
pub struct RefreshQuery {
    pub start_date: String,
    pub end_date: String,
}

/// Drop the cached dashboard stats so the next read recomputes them.
///
/// Removes the nightly alert counts cached for the given range, plus the
/// collection and Kafka topic caches. The next call to the stats endpoints
/// recounts from MongoDB and Kafka. Requires a signed-in account: the stats
/// themselves are public, but paying for a recount is not. The range cannot
/// span more than 6 months, and refreshes are rate-limited globally to one
/// every 5 minutes; a refusal carries a `Retry-After` header.
#[utoipa::path(
    post,
    path = "/babamul/stats/refresh",
    params(
        ("start_date" = String, Query, description = "First night whose cached count is dropped (YYYY-MM-DD); at most 6 months before end_date."),
        ("end_date" = String, Query, description = "Last night whose cached count is dropped (YYYY-MM-DD)."),
    ),
    responses(
        (status = 200, description = "Caches dropped"),
        (status = 400, description = "Invalid parameters, or a range longer than 6 months"),
        (status = 401, description = "Unauthorized"),
        (status = 429, description = "Another refresh happened too recently"),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Stats"]
)]
#[post("/stats/refresh")]
pub async fn post_stats_refresh(
    current_user: Option<web::ReqData<BabamulUser>>,
    query: web::Query<RefreshQuery>,
    db: web::Data<Database>,
) -> HttpResponse {
    if current_user.is_none() {
        return HttpResponse::Unauthorized().body("Unauthorized");
    }

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
    let earliest = end_date
        .checked_sub_months(Months::new(MAX_REFRESH_MONTHS))
        .unwrap_or(NaiveDate::MIN);
    if start_date < earliest {
        return response::bad_request("The refreshed range cannot span more than 6 months");
    }

    let now = Utc::now().timestamp();
    let stats_collection: Collection<Document> = db.collection(STATS_COLLECTION);

    // Claim the cooldown before doing anything: the upsert only matches a stale
    // marker, so concurrent callers race on `_id` and the losers get E11000.
    match stats_collection
        .update_one(
            doc! {
                "_id": REFRESH_COOLDOWN_KEY,
                "last_refresh_at": { "$lte": now - REFRESH_COOLDOWN_SECS },
            },
            doc! { "$set": { "last_refresh_at": now } },
        )
        .upsert(true)
        .await
    {
        Ok(_) => {}
        Err(e) if e.to_string().contains("E11000 duplicate key error") => {
            return too_many_requests(&stats_collection, now).await;
        }
        Err(e) => {
            return response::internal_error(&format!("Error claiming the stats refresh: {}", e));
        }
    }

    let nightly_prefix = format!("^{}", NIGHTLY_STATS_CACHE_PREFIX);
    let deleted = match stats_collection
        .delete_many(doc! {
            "$or": [
                { "_id": { "$in": [COLLECTION_STATS_CACHE_KEY, BABAMUL_KAFKA_TOPICS_CACHE_KEY] } },
                {
                    "_id": { "$regex": nightly_prefix },
                    "date": {
                        "$gte": start_date.format("%Y-%m-%d").to_string(),
                        "$lte": end_date.format("%Y-%m-%d").to_string(),
                    },
                },
            ],
        })
        .await
    {
        Ok(result) => result.deleted_count,
        Err(e) => {
            return response::internal_error(&format!("Error dropping the stats cache: {}", e));
        }
    };

    response::ok_no_data(&format!("dropped {} cached stats entries", deleted))
}

async fn too_many_requests(stats_collection: &Collection<Document>, now: i64) -> HttpResponse {
    let retry_after = match stats_collection
        .find_one(doc! { "_id": REFRESH_COOLDOWN_KEY })
        .await
    {
        Ok(Some(marker)) => marker
            .get_i64("last_refresh_at")
            .map(|last| (last + REFRESH_COOLDOWN_SECS - now).max(1))
            .unwrap_or(REFRESH_COOLDOWN_SECS),
        _ => REFRESH_COOLDOWN_SECS,
    };
    HttpResponse::TooManyRequests()
        .insert_header(("Retry-After", retry_after.to_string()))
        .json(response::ApiResponseBody::error(&format!(
            "The dashboard stats were refreshed a moment ago. Try again in {}s.",
            retry_after
        )))
}
