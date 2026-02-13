use crate::api::models::response;
use crate::api::routes::users::User;
use crate::conf::AppConfig;
use crate::watchlist::{EventGeometry, GcnEvent, GcnEventType, GcnSource, WatchlistError};
use crate::utils::spatial::Coordinates;
use actix_web::{delete, get, patch, post, web, HttpResponse};
use futures::stream::StreamExt;
use mongodb::bson::doc;
use mongodb::options::{FindOneAndUpdateOptions, ReturnDocument};
use mongodb::{Collection, Database};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::error;
use utoipa::ToSchema;

/// Serialized value of GcnSource::Custom for use in MongoDB queries.
/// This avoids hardcoding "custom" strings that could silently break
/// if the enum variant or serde rename_all strategy changes.
const SOURCE_CUSTOM: &str = "custom";

/// Maximum allowed length for watchlist name
const MAX_NAME_LENGTH: usize = 256;
/// Maximum allowed length for watchlist description
const MAX_DESCRIPTION_LENGTH: usize = 2048;
/// Maximum allowed expiration in days (1 year)
const MAX_EXPIRES_IN_DAYS: u32 = 365;

/// Request body for creating a watchlist entry
#[derive(Debug, Deserialize, ToSchema)]
pub struct WatchlistCreate {
    /// Name for this watchlist entry
    pub name: String,
    /// Right ascension in degrees (0-360)
    pub ra: f64,
    /// Declination in degrees (-90 to 90)
    pub dec: f64,
    /// Search radius in degrees
    pub radius_deg: f64,
    /// Optional description
    pub description: Option<String>,
    /// Expiration time in days from now (default: 30, max: 365)
    pub expires_in_days: Option<u32>,
}

/// Request body for updating a watchlist entry
#[derive(Debug, Deserialize, ToSchema)]
pub struct WatchlistUpdate {
    /// Updated name
    pub name: Option<String>,
    /// Updated description
    pub description: Option<String>,
    /// Whether the watchlist is active
    pub is_active: Option<bool>,
    /// Set expiration to this many days from now
    pub expires_in_days: Option<u32>,
}

/// Public representation of a watchlist entry
#[derive(Debug, Serialize, ToSchema)]
pub struct WatchlistPublic {
    pub id: String,
    pub name: String,
    pub ra: f64,
    pub dec: f64,
    pub radius_deg: f64,
    pub description: Option<String>,
    pub is_active: bool,
    pub expires_at: f64,
    pub created_at: f64,
    pub updated_at: f64,
}

impl WatchlistPublic {
    fn from_event(event: &GcnEvent) -> Option<Self> {
        match &event.geometry {
            EventGeometry::Circle {
                ra,
                dec,
                error_radius,
            } => Some(WatchlistPublic {
                id: event.id.clone(),
                name: event.name.clone().unwrap_or_default(),
                ra: *ra,
                dec: *dec,
                radius_deg: *error_radius,
                description: event.description.clone(),
                is_active: event.is_active,
                expires_at: event.expires_at,
                created_at: event.created_at,
                updated_at: event.updated_at,
            }),
            _ => None, // HEALPix watchlists not supported
        }
    }
}

fn validate_coordinates(ra: f64, dec: f64) -> Result<(), WatchlistError> {
    if !(0.0..=360.0).contains(&ra) {
        return Err(WatchlistError::InvalidGeometry(format!(
            "RA must be between 0 and 360 degrees, got {}",
            ra
        )));
    }
    if !(-90.0..=90.0).contains(&dec) {
        return Err(WatchlistError::InvalidGeometry(format!(
            "Dec must be between -90 and 90 degrees, got {}",
            dec
        )));
    }
    Ok(())
}

fn validate_radius(radius: f64, max_radius: f64) -> Result<(), WatchlistError> {
    if !radius.is_finite() {
        return Err(WatchlistError::InvalidGeometry(
            "Radius must be a finite number".to_string(),
        ));
    }
    if radius <= 0.0 {
        return Err(WatchlistError::InvalidGeometry(
            "Radius must be positive".to_string(),
        ));
    }
    if radius > max_radius {
        return Err(WatchlistError::RadiusTooLarge(radius, max_radius));
    }
    Ok(())
}

/// Create a new watchlist entry
#[utoipa::path(
    post,
    path = "/watchlists",
    request_body = WatchlistCreate,
    responses(
        (status = 200, description = "Watchlist created successfully", body = WatchlistPublic),
        (status = 400, description = "Invalid request"),
        (status = 403, description = "Limit exceeded"),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Watchlists"]
)]
#[post("/watchlists")]
pub async fn post_watchlist(
    db: web::Data<Database>,
    config: web::Data<AppConfig>,
    body: web::Json<WatchlistCreate>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user.into_inner(),
        None => return response::forbidden("Authentication required"),
    };

    let watchlist_config = &config.watchlist.limits;

    // Validate name length
    let name = body.name.trim();
    if name.is_empty() || name.len() > MAX_NAME_LENGTH {
        return response::bad_request(&format!(
            "Name must be between 1 and {} characters",
            MAX_NAME_LENGTH
        ));
    }

    // Validate description length (checked after trim for accurate length)
    if let Some(desc) = &body.description {
        if desc.trim().len() > MAX_DESCRIPTION_LENGTH {
            return response::bad_request(&format!(
                "Description must not exceed {} characters",
                MAX_DESCRIPTION_LENGTH
            ));
        }
    }

    // Validate expiration
    if let Some(days) = body.expires_in_days {
        if days == 0 || days > MAX_EXPIRES_IN_DAYS {
            return response::bad_request(&format!(
                "Expiration must be between 1 and {} days",
                MAX_EXPIRES_IN_DAYS
            ));
        }
    }

    // Validate coordinates
    if let Err(e) = validate_coordinates(body.ra, body.dec) {
        return response::bad_request(&e.to_string());
    }

    // Validate radius
    if let Err(e) = validate_radius(body.radius_deg, watchlist_config.max_radius_deg) {
        return response::bad_request(&e.to_string());
    }

    let gcn_collection: Collection<GcnEvent> = db.collection("gcn_events");

    // Check user's watchlist count
    let user_count = match gcn_collection
        .count_documents(doc! {
            "user_id": &current_user.id,
            "source": SOURCE_CUSTOM,
        })
        .await
    {
        Ok(count) => count as usize,
        Err(e) => {
            error!("Failed to count user watchlists: {}", e);
            return response::internal_error("Failed to check watchlist limit");
        }
    };

    if user_count >= watchlist_config.max_per_user {
        return response::forbidden(&format!(
            "Maximum watchlist limit reached: {}",
            watchlist_config.max_per_user
        ));
    }

    // Calculate expiration
    let now = flare::Time::now().to_jd();
    let expires_in_days = body.expires_in_days.unwrap_or(30);
    let expires_at = now + expires_in_days as f64;

    // Create the watchlist entry
    let id = uuid::Uuid::new_v4().to_string();
    let event = GcnEvent {
        id: id.clone(),
        source: GcnSource::Custom,
        event_type: GcnEventType::Watchlist,
        trigger_time: now,
        geometry: EventGeometry::circle(body.ra, body.dec, body.radius_deg),
        coordinates: Some(Coordinates::new(body.ra, body.dec)),
        properties: HashMap::new(),
        expires_at,
        is_active: true,
        user_id: Some(current_user.id.clone()),
        name: Some(name.to_string()),
        description: body.description.as_ref().map(|d| d.trim().to_string()),
        created_at: now,
        updated_at: now,
        supersedes: None,
        superseded_by: None,
    };

    match gcn_collection.insert_one(&event).await {
        Ok(_) => {
            match WatchlistPublic::from_event(&event) {
                Some(public) => response::ok_ser("Watchlist created", public),
                None => response::internal_error("Failed to serialize watchlist"),
            }
        }
        Err(e) => {
            error!("Failed to create watchlist: {}", e);
            response::internal_error("Failed to create watchlist")
        }
    }
}

/// List the current user's watchlists
#[utoipa::path(
    get,
    path = "/watchlists",
    responses(
        (status = 200, description = "Watchlists retrieved successfully", body = [WatchlistPublic]),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Watchlists"]
)]
#[get("/watchlists")]
pub async fn get_watchlists(
    db: web::Data<Database>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user.into_inner(),
        None => return response::forbidden("Authentication required"),
    };

    let gcn_collection: Collection<GcnEvent> = db.collection("gcn_events");

    let filter = doc! {
        "user_id": &current_user.id,
        "source": SOURCE_CUSTOM,
    };

    match gcn_collection.find(filter).await {
        Ok(mut cursor) => {
            let mut watchlists = Vec::new();
            while let Some(result) = cursor.next().await {
                match result {
                    Ok(event) => {
                        if let Some(public) = WatchlistPublic::from_event(&event) {
                            watchlists.push(public);
                        }
                    }
                    Err(e) => {
                        error!("Error reading watchlist from cursor: {}", e);
                        return response::internal_error("Failed to read watchlist data");
                    }
                }
            }
            response::ok_ser("success", watchlists)
        }
        Err(e) => {
            error!("Failed to query watchlists: {}", e);
            response::internal_error("Failed to query watchlists")
        }
    }
}

/// Get a specific watchlist entry
#[utoipa::path(
    get,
    path = "/watchlists/{watchlist_id}",
    params(
        ("watchlist_id" = String, Path, description = "Watchlist ID")
    ),
    responses(
        (status = 200, description = "Watchlist retrieved successfully", body = WatchlistPublic),
        (status = 404, description = "Watchlist not found"),
        (status = 403, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Watchlists"]
)]
#[get("/watchlists/{watchlist_id}")]
pub async fn get_watchlist(
    db: web::Data<Database>,
    path: web::Path<String>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user.into_inner(),
        None => return response::forbidden("Authentication required"),
    };

    let watchlist_id = path.into_inner();
    let gcn_collection: Collection<GcnEvent> = db.collection("gcn_events");

    let filter = doc! {
        "_id": &watchlist_id,
        "source": SOURCE_CUSTOM,
    };

    match gcn_collection.find_one(filter).await {
        Ok(Some(event)) => {
            // Check ownership — return 404 to avoid leaking existence of other users' watchlists
            if event.user_id.as_ref() != Some(&current_user.id) && !current_user.is_admin {
                return response::not_found("Watchlist not found");
            }

            match WatchlistPublic::from_event(&event) {
                Some(public) => response::ok_ser("success", public),
                None => response::internal_error("Invalid watchlist geometry"),
            }
        }
        Ok(None) => response::not_found("Watchlist not found"),
        Err(e) => {
            error!("Failed to query watchlist {}: {}", watchlist_id, e);
            response::internal_error("Failed to retrieve watchlist")
        }
    }
}

/// Update a watchlist entry
#[utoipa::path(
    patch,
    path = "/watchlists/{watchlist_id}",
    params(
        ("watchlist_id" = String, Path, description = "Watchlist ID")
    ),
    request_body = WatchlistUpdate,
    responses(
        (status = 200, description = "Watchlist updated successfully", body = WatchlistPublic),
        (status = 404, description = "Watchlist not found"),
        (status = 403, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Watchlists"]
)]
#[patch("/watchlists/{watchlist_id}")]
pub async fn patch_watchlist(
    db: web::Data<Database>,
    path: web::Path<String>,
    body: web::Json<WatchlistUpdate>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user.into_inner(),
        None => return response::forbidden("Authentication required"),
    };

    // Validate update fields
    if let Some(name) = &body.name {
        let name = name.trim();
        if name.is_empty() || name.len() > MAX_NAME_LENGTH {
            return response::bad_request(&format!(
                "Name must be between 1 and {} characters",
                MAX_NAME_LENGTH
            ));
        }
    }
    if let Some(desc) = &body.description {
        if desc.trim().len() > MAX_DESCRIPTION_LENGTH {
            return response::bad_request(&format!(
                "Description must not exceed {} characters",
                MAX_DESCRIPTION_LENGTH
            ));
        }
    }
    if let Some(days) = body.expires_in_days {
        if days == 0 || days > MAX_EXPIRES_IN_DAYS {
            return response::bad_request(&format!(
                "Expiration must be between 1 and {} days",
                MAX_EXPIRES_IN_DAYS
            ));
        }
    }

    let watchlist_id = path.into_inner();
    let gcn_collection: Collection<GcnEvent> = db.collection("gcn_events");

    // Build filter with ownership check to prevent TOCTOU races.
    // Admins can update any watchlist; regular users only their own.
    let mut filter = doc! {
        "_id": &watchlist_id,
        "source": SOURCE_CUSTOM,
    };
    if !current_user.is_admin {
        filter.insert("user_id", &current_user.id);
    }

    // Build update document
    let now = flare::Time::now().to_jd();
    let mut update_doc = doc! { "updated_at": now };

    if let Some(name) = &body.name {
        update_doc.insert("name", name.trim());
    }

    if let Some(description) = &body.description {
        update_doc.insert("description", description.trim());
    }

    if let Some(is_active) = body.is_active {
        update_doc.insert("is_active", is_active);
    }

    if let Some(expires_in_days) = body.expires_in_days {
        let new_expires_at = now + expires_in_days as f64;
        update_doc.insert("expires_at", new_expires_at);
    }

    let options = FindOneAndUpdateOptions::builder()
        .return_document(ReturnDocument::After)
        .build();

    match gcn_collection
        .find_one_and_update(filter, doc! { "$set": update_doc })
        .with_options(options)
        .await
    {
        Ok(Some(updated_event)) => match WatchlistPublic::from_event(&updated_event) {
            Some(public) => response::ok_ser("Watchlist updated", public),
            None => response::internal_error("Invalid watchlist geometry"),
        },
        Ok(None) => response::not_found("Watchlist not found"),
        Err(e) => {
            error!("Failed to update watchlist {}: {}", watchlist_id, e);
            response::internal_error("Failed to update watchlist")
        }
    }
}

/// Delete a watchlist entry
#[utoipa::path(
    delete,
    path = "/watchlists/{watchlist_id}",
    params(
        ("watchlist_id" = String, Path, description = "Watchlist ID")
    ),
    responses(
        (status = 200, description = "Watchlist deleted successfully"),
        (status = 404, description = "Watchlist not found"),
        (status = 403, description = "Unauthorized"),
        (status = 500, description = "Internal server error")
    ),
    tags = ["Watchlists"]
)]
#[delete("/watchlists/{watchlist_id}")]
pub async fn delete_watchlist(
    db: web::Data<Database>,
    path: web::Path<String>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user.into_inner(),
        None => return response::forbidden("Authentication required"),
    };

    let watchlist_id = path.into_inner();
    let gcn_collection: Collection<GcnEvent> = db.collection("gcn_events");

    // Include ownership in the filter to prevent TOCTOU races.
    // Admins can delete any watchlist; regular users only their own.
    let mut filter = doc! {
        "_id": &watchlist_id,
        "source": SOURCE_CUSTOM,
    };
    if !current_user.is_admin {
        filter.insert("user_id", &current_user.id);
    }

    match gcn_collection.delete_one(filter).await {
        Ok(result) => {
            if result.deleted_count > 0 {
                response::ok_no_data("Watchlist deleted")
            } else {
                response::not_found("Watchlist not found")
            }
        }
        Err(e) => {
            error!("Failed to delete watchlist {}: {}", watchlist_id, e);
            response::internal_error("Failed to delete watchlist")
        }
    }
}
