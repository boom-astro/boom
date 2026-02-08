use crate::api::models::response;
use crate::api::routes::users::User;
use crate::gcn::{EventGeometry, GcnEvent};
use actix_web::{get, web, HttpResponse};
use futures::stream::StreamExt;
use mongodb::bson::{doc, Document};
use mongodb::{Collection, Database};
use serde::{Deserialize, Serialize};
use tracing::error;
use utoipa::{IntoParams, ToSchema};

/// Query parameters for listing events
#[derive(Debug, Deserialize, IntoParams)]
pub struct EventListQuery {
    /// Filter by source (e.g., "lvk", "swift", "custom")
    pub source: Option<String>,
    /// Only show active events (default: true)
    pub active_only: Option<bool>,
    /// Maximum number of results (default: 100)
    pub limit: Option<i64>,
    /// Skip this many results (for pagination)
    pub skip: Option<u64>,
}

/// Public representation of an event
#[derive(Debug, Serialize, ToSchema)]
pub struct EventPublic {
    pub id: String,
    pub source: String,
    pub event_type: String,
    pub trigger_time: f64,
    pub geometry_type: String,
    pub ra: Option<f64>,
    pub dec: Option<f64>,
    pub error_radius: Option<f64>,
    pub is_active: bool,
    pub expires_at: f64,
    pub created_at: f64,
    pub name: Option<String>,
}

impl EventPublic {
    pub(crate) fn from_event(event: &GcnEvent) -> Self {
        let (geometry_type, ra, dec, error_radius) = match &event.geometry {
            EventGeometry::Circle {
                ra,
                dec,
                error_radius,
            } => ("circle".to_string(), Some(*ra), Some(*dec), Some(*error_radius)),
            EventGeometry::HealPixMap { .. } => ("healpix".to_string(), None, None, None),
        };

        EventPublic {
            id: event.id.clone(),
            source: format!("{}", event.source),
            event_type: format!("{}", event.event_type),
            trigger_time: event.trigger_time,
            geometry_type,
            ra,
            dec,
            error_radius,
            is_active: event.is_active,
            expires_at: event.expires_at,
            created_at: event.created_at,
            name: event.name.clone(),
        }
    }
}

/// List active GCN events
#[utoipa::path(
    get,
    path = "/events",
    params(EventListQuery),
    responses(
        (status = 200, description = "Events retrieved successfully", body = [EventPublic]),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Events"]
)]
#[get("/events")]
pub async fn get_events(
    db: web::Data<Database>,
    query: web::Query<EventListQuery>,
    _current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let gcn_collection: Collection<GcnEvent> = db.collection("gcn_events");

    let mut filter = Document::new();

    // Filter by source if provided
    if let Some(source) = &query.source {
        filter.insert("source", source.to_lowercase());
    }

    // Filter by active status (default to active only)
    let active_only = query.active_only.unwrap_or(true);
    if active_only {
        filter.insert("is_active", true);
        // Also filter out expired events
        let now = flare::Time::now().to_jd();
        filter.insert("expires_at", doc! { "$gt": now });
    }

    let limit = query.limit.unwrap_or(100).min(1000);
    let skip = query.skip.unwrap_or(0);

    let options = mongodb::options::FindOptions::builder()
        .limit(limit)
        .skip(skip)
        .sort(doc! { "trigger_time": -1 })
        .build();

    match gcn_collection.find(filter).with_options(options).await {
        Ok(mut cursor) => {
            let mut events = Vec::new();
            while let Some(result) = cursor.next().await {
                match result {
                    Ok(event) => events.push(EventPublic::from_event(&event)),
                    Err(e) => {
                        error!("Error reading event from cursor: {}", e);
                        return response::internal_error("Failed to read event data");
                    }
                }
            }
            response::ok_ser("success", events)
        }
        Err(e) => {
            error!("Failed to query events: {}", e);
            response::internal_error("Failed to query events")
        }
    }
}

/// Get details of a specific event
#[utoipa::path(
    get,
    path = "/events/{event_id}",
    params(
        ("event_id" = String, Path, description = "Event ID")
    ),
    responses(
        (status = 200, description = "Event retrieved successfully", body = EventPublic),
        (status = 404, description = "Event not found"),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Events"]
)]
#[get("/events/{event_id}")]
pub async fn get_event(
    db: web::Data<Database>,
    path: web::Path<String>,
    _current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let event_id = path.into_inner();
    let gcn_collection: Collection<GcnEvent> = db.collection("gcn_events");

    match gcn_collection.find_one(doc! { "_id": &event_id }).await {
        Ok(Some(event)) => response::ok_ser("success", EventPublic::from_event(&event)),
        Ok(None) => response::not_found("Event not found"),
        Err(e) => {
            error!("Failed to query event {}: {}", event_id, e);
            response::internal_error("Failed to retrieve event")
        }
    }
}

/// Query parameters for getting alerts matched to an event
#[derive(Debug, Deserialize, IntoParams)]
pub struct EventAlertsQuery {
    /// Survey to query (e.g., "ztf", "lsst")
    pub survey: String,
    /// Maximum number of results (default: 100)
    pub limit: Option<i64>,
    /// Skip this many results (for pagination)
    pub skip: Option<u64>,
}

/// Alert match summary
#[derive(Debug, Serialize, ToSchema)]
pub struct AlertMatchSummary {
    pub object_id: String,
    pub distance_deg: Option<f64>,
    pub probability: Option<f64>,
    pub matched_at: f64,
    pub trigger_offset_days: f64,
}

/// Get alerts matched to a specific event
#[utoipa::path(
    get,
    path = "/events/{event_id}/alerts",
    params(
        ("event_id" = String, Path, description = "Event ID"),
        EventAlertsQuery
    ),
    responses(
        (status = 200, description = "Matched alerts retrieved successfully", body = [AlertMatchSummary]),
        (status = 400, description = "Invalid survey"),
        (status = 404, description = "Event not found"),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Events"]
)]
#[get("/events/{event_id}/alerts")]
pub async fn get_event_alerts(
    db: web::Data<Database>,
    path: web::Path<String>,
    query: web::Query<EventAlertsQuery>,
    _current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let event_id = path.into_inner();

    // Validate survey
    let survey = query.survey.to_lowercase();
    if survey != "ztf" && survey != "lsst" {
        return response::bad_request("Invalid survey. Must be 'ztf' or 'lsst'");
    }

    // First check if the event exists
    let gcn_collection: Collection<GcnEvent> = db.collection("gcn_events");
    match gcn_collection.find_one(doc! { "_id": &event_id }).await {
        Ok(Some(_)) => {}
        Ok(None) => return response::not_found("Event not found"),
        Err(e) => {
            error!("Failed to query event {}: {}", event_id, e);
            return response::internal_error("Failed to retrieve event");
        }
    }

    // Query the alerts_aux collection for matches
    let aux_collection_name = format!("{}_alerts_aux", survey);
    let aux_collection: Collection<Document> = db.collection(&aux_collection_name);

    let limit = query.limit.unwrap_or(100).min(1000);
    let skip = query.skip.unwrap_or(0);

    let filter = doc! {
        "event_matches.event_id": &event_id
    };

    let options = mongodb::options::FindOptions::builder()
        .limit(limit)
        .skip(skip)
        .projection(doc! {
            "_id": 1,
            "event_matches": {
                "$elemMatch": { "event_id": &event_id }
            }
        })
        .build();

    match aux_collection.find(filter).with_options(options).await {
        Ok(mut cursor) => {
            let mut matches = Vec::new();
            while let Some(result) = cursor.next().await {
                match result {
                    Ok(doc) => {
                        let object_id = doc.get_str("_id").unwrap_or_default().to_string();
                        if let Some(event_matches) = doc.get_array("event_matches").ok() {
                            if let Some(first_match) = event_matches.first() {
                                if let Some(match_doc) = first_match.as_document() {
                                    matches.push(AlertMatchSummary {
                                        object_id,
                                        distance_deg: match_doc.get_f64("distance_deg").ok(),
                                        probability: match_doc.get_f64("probability").ok(),
                                        matched_at: match_doc.get_f64("matched_at").unwrap_or(0.0),
                                        trigger_offset_days: match_doc
                                            .get_f64("trigger_offset_days")
                                            .unwrap_or(0.0),
                                    });
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Error reading alert match for event {}: {}", event_id, e);
                        return response::internal_error("Failed to read alert data");
                    }
                }
            }
            response::ok_ser("success", matches)
        }
        Err(e) => {
            error!("Failed to query alerts for event {}: {}", event_id, e);
            response::internal_error("Failed to query alerts")
        }
    }
}
