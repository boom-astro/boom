/// Endpoints for executing analytical queries.
use crate::{
    api::{
        catalogs::{catalog_accessible, is_cone_searchable},
        filters::parse_optional_filter,
        models::response,
        routes::users::User,
    },
    conf::AppConfig,
};

use crate::api::limits::{
    large_query_slot, next_result, too_large, ResultError, DEFAULT_QUERY_MAX_TIME,
};
use actix_web::{post, web, HttpResponse};
use mongodb::{bson::doc, Database};
use std::collections::HashMap;
use std::fmt;
use utoipa::openapi::schema::{ObjectBuilder, Schema};
use utoipa::openapi::RefOr;
use utoipa::{PartialSchema, ToSchema};

const MAX_CONE_SEARCH_COORDINATES: usize = 10_000;

#[derive(serde::Deserialize, Clone)]
pub enum Unit {
    Degrees,
    Radians,
    Arcseconds,
    Arcminutes,
}
impl fmt::Debug for Unit {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Unit::Degrees => {
                write!(f, "{}", "Degrees")
            }
            Unit::Radians => {
                write!(f, "{}", "Radians")
            }
            Unit::Arcseconds => {
                write!(f, "{}", "Arcseconds")
            }
            Unit::Arcminutes => {
                write!(f, "{}", "Arcminutes")
            }
        }
    }
}
impl ToSchema for Unit {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("Unit")
    }
}
impl PartialSchema for Unit {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .schema_type(utoipa::openapi::Type::String)
                .enum_values(Some(vec![
                    "Degrees".to_string(),
                    "Radians".to_string(),
                    "Arcseconds".to_string(),
                    "Arcminutes".to_string(),
                ]))
                .build(),
        ))
    }
}

#[derive(serde::Deserialize, Clone, ToSchema)]
struct ConeSearchQuery {
    catalog_name: String,
    filter: Option<serde_json::Value>,
    projection: Option<serde_json::Value>,
    radius: f64,
    unit: Unit,
    object_coordinates: HashMap<String, [f64; 2]>, // Map of catalog name to coordinates [RA, Dec]
    limit: Option<i64>,
    skip: Option<u64>,
    sort: Option<serde_json::Value>,
    max_time_ms: Option<u64>,
}
impl ConeSearchQuery {
    /// Convert to MongoDB Find options
    fn to_find_options(&self) -> Result<mongodb::options::FindOptions, String> {
        let mut options = mongodb::options::FindOptions::default();
        if let Some(projection) = &self.projection {
            options.projection = match mongodb::bson::to_document(projection) {
                Ok(doc) => Some(doc),
                Err(e) => {
                    return Err(format!(
                        "Error converting projection to BSON document: {:?}",
                        e
                    ));
                }
            }
        }
        if let Some(limit) = self.limit {
            options.limit = Some(limit);
        }
        if let Some(skip) = self.skip {
            options.skip = Some(skip);
        }
        if let Some(sort) = &self.sort {
            options.sort = match mongodb::bson::to_document(sort) {
                Ok(doc) => Some(doc),
                Err(e) => {
                    return Err(format!("Error converting sort to BSON document: {:?}", e));
                }
            }
        }
        options.max_time = Some(
            self.max_time_ms
                .map(std::time::Duration::from_millis)
                .unwrap_or(DEFAULT_QUERY_MAX_TIME),
        );
        Ok(options)
    }
}

/// Run a cone search query on a survey alert collection, a reference catalog or a watchlist.
/// A request whose results exceed 256 MB is rejected; use a smaller radius, paginate with limit and skip, or project fewer fields.
#[utoipa::path(
    post,
    path = "/queries/cone_search",
    request_body = ConeSearchQuery,
    responses(
        (status = 200, description = "Cone search results", body = serde_json::Value),
        (status = 400, description = "Bad request"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Queries"]
)]
#[post("/queries/cone_search")]
pub async fn post_cone_search_query(
    db: web::Data<Database>,
    body: web::Json<ConeSearchQuery>,
    current_user: Option<web::ReqData<User>>,
    config: web::Data<AppConfig>,
) -> HttpResponse {
    let current_user = match current_user {
        Some(user) => user,
        None => return HttpResponse::Unauthorized().body("Unauthorized"),
    };
    let _slot = match large_query_slot().await {
        Ok(slot) => slot,
        Err(response) => return response,
    };
    let catalog_name = body.catalog_name.trim();
    if !is_cone_searchable(catalog_name, &config) {
        return response::bad_request(&format!(
            "Catalog {} does not support cone search",
            catalog_name
        ));
    }
    if !catalog_accessible(&db, catalog_name, &current_user, &config).await {
        return response::not_found(&format!("Catalog {} does not exist", catalog_name));
    }
    let collection_name = catalog_name.to_string();
    // Get the collection
    let collection = db.collection::<mongodb::bson::Document>(&collection_name);
    // Perform cone search over each set of object coordinates
    let find_options = match body.to_find_options() {
        Ok(options) => options,
        Err(e) => return response::bad_request(&format!("Invalid find options: {}", e)),
    };
    let mut radius = body.radius;
    let unit = body.unit.clone();
    // Convert radius to radians based on unit
    match unit {
        Unit::Degrees => radius = radius.to_radians(),
        Unit::Arcseconds => radius = radius.to_radians() / 3600.0,
        Unit::Arcminutes => radius = radius.to_radians() / 60.0,
        Unit::Radians => {}
    }
    let object_coordinates = &body.object_coordinates;
    if object_coordinates.len() > MAX_CONE_SEARCH_COORDINATES {
        return response::bad_request(&format!(
            "object_coordinates holds more than {} entries; split the request",
            MAX_CONE_SEARCH_COORDINATES
        ));
    }
    let mut docs: HashMap<String, Vec<mongodb::bson::Document>> = HashMap::new();
    let mut bytes_read = 0;
    let filter = match parse_optional_filter(&body.filter) {
        Ok(f) => f,
        Err(e) => return response::bad_request(&format!("Invalid filter: {}", e)),
    };
    for (object_name, radec) in object_coordinates {
        if radec.len() != 2 {
            return response::bad_request(&format!(
                "Invalid coordinates for object {}: expected [RA, Dec]",
                object_name
            ));
        }
        let ra = radec[0];
        let dec = radec[1];
        if ra < 0.0 || ra >= 360.0 {
            return response::bad_request(&format!(
                "Invalid RA for object {}: must be in [0, 360)",
                object_name
            ));
        }
        if dec < -90.0 || dec > 90.0 {
            return response::bad_request(&format!(
                "Invalid Dec for object {}: must be in [-90, 90]",
                object_name
            ));
        }
        let center_sphere = doc! {
            "$centerSphere": [[ra - 180.0, dec], radius]
        };
        let geo_within = doc! {
            "$geoWithin": center_sphere
        };
        let mut conesearch_filter = filter.clone();
        conesearch_filter.insert("coordinates.radec_geojson", geo_within);
        let mut cursor = match collection
            .find(conesearch_filter)
            .with_options(find_options.clone())
            .await
        {
            Ok(c) => c,
            Err(e) => {
                return super::query_error(e, "Error finding documents");
            }
        };
        let mut data = Vec::new();
        loop {
            match next_result(&mut cursor, &mut bytes_read).await {
                Ok(Some(doc)) => data.push(doc),
                Ok(None) => break,
                Err(ResultError::TooLarge) => return too_large(),
                Err(ResultError::Database(e)) => {
                    return super::query_error(e, "Error collecting documents")
                }
            }
        }
        docs.insert(object_name.clone(), data);
    }
    return response::ok_ser(&format!("Cone Search on {} completed", catalog_name), &docs);
}
