use crate::{models::response, routes::users::User};

use actix_web::{HttpResponse, get, patch, post, web};
use futures::stream::StreamExt;
use mongodb::{
    Collection, Database,
    bson::{Document, doc},
};
use std::vec;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(serde::Deserialize, serde::Serialize, Clone, ToSchema)]
pub struct FilterVersion {
    fid: String,
    pipeline: String,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, ToSchema)]
pub struct Filter {
    #[serde(rename = "_id")]
    pub id: String,
    pub permissions: Vec<i32>,
    pub user_id: String,
    pub catalog: String,
    pub active: bool,
    pub active_fid: String,
    pub fv: Vec<FilterVersion>,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, ToSchema)]
pub struct FilterPublic {
    #[serde(rename(serialize = "id", deserialize = "_id"))]
    pub id: String,
    pub permissions: Vec<i32>,
    pub user_id: String,
    pub catalog: String,
    pub active: bool,
    pub active_fid: String,
    pub fv: Vec<FilterVersion>,
}

impl From<Filter> for FilterPublic {
    fn from(filter: Filter) -> Self {
        Self {
            id: filter.id,
            permissions: filter.permissions,
            user_id: filter.user_id,
            catalog: filter.catalog,
            active: filter.active,
            active_fid: filter.active_fid,
            fv: filter.fv,
        }
    }
}

fn build_test_pipeline(
    filter_catalog: String,
    filter_perms: Vec<i32>,
    mut filter_pipeline: Vec<Document>,
) -> Vec<Document> {
    let mut out_pipeline = vec![
        doc! {
            "$match": doc! {
                // during filter::run proper candis are inserted here
            }
        },
        doc! {
            "$lookup": doc! {
                "from": format!("{}_aux", filter_catalog),
                "localField": "objectId",
                "foreignField": "_id",
                "as": "aux"
            }
        },
        doc! {
            "$project": doc! {
                "objectId": 1,
                "candidate": 1,
                "classifications": 1,
                "coordinates": 1,
                "cross_matches": doc! {
                    "$arrayElemAt": [
                        "$aux.cross_matches",
                        0
                    ]
                },
                "prv_candidates": doc! {
                    "$filter": doc! {
                        "input": doc! {
                            "$arrayElemAt": [
                                "$aux.prv_candidates",
                                0
                            ]
                        },
                        "as": "x",
                        "cond": doc! {
                            "$and": [
                                {
                                    "$in": [
                                        "$$x.programid",
                                        &filter_perms
                                    ]
                                },
                                { // maximum 1 year of past data
                                    "$lt": [
                                        {
                                            "$subtract": [
                                                "$candidate.jd",
                                                "$$x.jd"
                                            ]
                                        },
                                        365
                                    ]
                                },
                                { // only datapoints up to (and including) current alert
                                    "$lte": [
                                        "$$x.jd",
                                        "$candidate.jd"
                                    ]
                                }

                            ]
                        }
                    }
                },
            }
        },
    ];
    out_pipeline.append(&mut filter_pipeline);
    return out_pipeline;
}

// tests the functionality of a filter by running it on alerts in database
async fn run_test_pipeline(
    db: web::Data<Database>,
    catalog: String,
    pipeline: Vec<mongodb::bson::Document>,
) -> Result<(), mongodb::error::Error> {
    let collection: Collection<mongodb::bson::Document> =
        db.collection(format!("{}_alerts", catalog).as_str());
    let result = collection.aggregate(pipeline).await;
    match result {
        Ok(_) => {
            return Ok(());
        }
        Err(e) => {
            return Err(e);
        }
    }
}

#[derive(serde::Deserialize, Clone, ToSchema)]
struct FilterVersionPost {
    pipeline: Vec<serde_json::Value>,
    set_as_active: Option<bool>,
}

/// Create a new version for a filter
#[utoipa::path(
    post,
    path = "/filters/{filter_id}/versions",
    request_body = FilterVersionPost,
    responses(
        (status = 200, description = "Filter version added successfully"),
        (status = 400, description = "Invalid filter submitted"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Filters"]
)]
#[post("/filters/{filter_id}/versions")]
pub async fn post_filter_version(
    db: web::Data<Database>,
    filter_id: web::Path<String>,
    body: web::Json<FilterVersionPost>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let current_user = current_user.unwrap();

    let filter_id = filter_id.into_inner();
    let collection: Collection<Filter> = db.collection("filters");
    let filter = match collection.find_one(doc! {"_id": filter_id.clone()}).await {
        Ok(Some(filter)) => filter,
        Ok(None) => {
            return response::not_found(&format!("filter with id {} does not exist", filter_id));
        }
        Err(e) => {
            return response::internal_error(&format!(
                "failed to find filter with id {}. error: {}",
                &filter_id, e
            ));
        }
    };
    if filter.user_id != current_user.id && !current_user.is_admin {
        return response::forbidden("only the filter owner or an admin can modify a filter");
    }

    let catalog = filter.catalog.clone();
    let permissions = filter.permissions.clone();
    // Create test version of filter and test it
    // Convert pipeline from JSON to an array of Documents
    let pipeline = body.pipeline.clone();
    let pipeline: Vec<mongodb::bson::Document> = pipeline
        .into_iter()
        .map(|v| {
            mongodb::bson::to_document(&v).map_err(|e| {
                response::bad_request(&format!(
                    "Failed to convert new filter version to a valid BSON Document: {}",
                    e
                ))
            })
        })
        .collect::<Result<_, _>>()
        .unwrap();
    let test_pipeline = build_test_pipeline(catalog.to_string(), permissions, pipeline.clone());

    match run_test_pipeline(db.clone(), catalog.to_string(), test_pipeline).await {
        Ok(()) => {}
        Err(e) => {
            return response::bad_request(&format!(
                "Invalid filter submitted, filter test failed with error: {}",
                e
            ));
        }
    }

    let new_pipeline_id = Uuid::new_v4().to_string();
    let date_time = mongodb::bson::DateTime::now();
    let new_pipeline_json: String = serde_json::to_string(&pipeline).unwrap();
    let mut update_doc = doc! {
        "$push": {
            "fv": {
                "fid": &new_pipeline_id,
                "pipeline": new_pipeline_json,
                "created_at": date_time,
            }
        },
    };
    if body.set_as_active.unwrap_or(true) {
        update_doc.insert("$set", doc! { "active_fid": &new_pipeline_id });
    }
    let update_result = collection
        .update_one(doc! {"_id": filter_id.clone()}, update_doc)
        .await;
    match update_result {
        Ok(_) => {
            return response::ok(
                &format!(
                    "successfully added new version {} to filter id: {}",
                    &new_pipeline_id, &filter_id
                ),
                serde_json::json!({"fid": new_pipeline_id}),
            );
        }
        Err(e) => {
            return response::internal_error(&format!(
                "failed to add new version to filter. error: {}",
                e
            ));
        }
    }
}

#[derive(serde::Deserialize, Clone, ToSchema)]
pub struct FilterPost {
    pub pipeline: Vec<serde_json::Value>,
    pub permissions: Vec<i32>,
    pub catalog: String,
}

/// Create a new filter
#[utoipa::path(
    post,
    path = "/filters",
    request_body = FilterPost,
    responses(
        (status = 200, description = "Filter created successfully", body = FilterPublic),
        (status = 400, description = "Invalid filter submitted"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Filters"]
)]
#[post("/filters")]
pub async fn post_filter(
    db: web::Data<Database>,
    body: web::Json<FilterPost>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let current_user = current_user.unwrap();
    let body = body.clone();

    let catalog = body.catalog;
    let permissions = body.permissions;
    let pipeline = body.pipeline;

    // Test filter received from user
    // Create production version of filter
    let pipeline_bson: Vec<Document> = pipeline
        .clone()
        .into_iter()
        .map(|v| {
            mongodb::bson::to_document(&v).map_err(|e| {
                response::bad_request(&format!(
                    "Failed to convert pipeline step to BSON Document: {}",
                    e
                ))
            })
        })
        .collect::<Result<_, _>>()
        .unwrap();
    let test_pipeline =
        build_test_pipeline(catalog.clone(), permissions.clone(), pipeline_bson.clone());

    // Test the filter to ensure it works
    match run_test_pipeline(db.clone(), catalog.clone(), test_pipeline).await {
        Ok(()) => {}
        Err(e) => {
            return response::bad_request(&format!(
                "Invalid filter submitted, filter test failed with error: {}",
                e
            ));
        }
    }

    // Save filter to database
    let filter_id = uuid::Uuid::new_v4().to_string();
    let filter_version: String = uuid::Uuid::new_v4().to_string();
    let filter_collection: Collection<Filter> = db.collection("filters");
    // Pipeline needs to be a string
    let pipeline_json = serde_json::to_string(&pipeline_bson).unwrap();
    let filter = Filter {
        permissions,
        catalog,
        id: filter_id,
        user_id: current_user.id.clone(),
        active: true,
        active_fid: filter_version.clone(),
        fv: vec![FilterVersion {
            fid: filter_version,
            pipeline: pipeline_json,
        }],
    };
    match filter_collection.insert_one(&filter).await {
        Ok(_) => {
            return response::ok(
                "successfully created new filter",
                serde_json::to_value(FilterPublic::from(filter)).unwrap(),
            );
        }
        Err(e) => {
            return response::internal_error(&format!(
                "failed to insert filter into database. error: {}",
                e
            ));
        }
    }
}

// we want a PATCH, that let's a user change fields like active, active_fid, permissions
#[derive(serde::Deserialize, Clone, ToSchema)]
struct FilterPatch {
    active: Option<bool>,
    active_fid: Option<String>,
    permissions: Option<Vec<i32>>,
}
/// Update a filter's metadata
#[utoipa::path(
    patch,
    path = "/filters/{filter_id}",
    request_body = FilterPatch,
    responses(
        (status = 200, description = "Filter updated successfully"),
        (status = 400, description = "Invalid filter update submitted"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Filters"]
)]
#[patch("/filters/{filter_id}")]
pub async fn patch_filter(
    db: web::Data<Database>,
    filter_id: web::Path<String>,
    body: web::Json<FilterPatch>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let current_user = current_user.unwrap();

    let filter_id = filter_id.into_inner();
    let collection: Collection<Filter> = db.collection("filters");
    let filter = match collection.find_one(doc! {"_id": filter_id.clone()}).await {
        Ok(Some(filter)) => filter,
        Ok(None) => {
            return response::not_found(&format!("filter with id {} does not exist", filter_id));
        }
        Err(e) => {
            return response::internal_error(&format!(
                "failed to find filter with id {}. error: {}",
                &filter_id, e
            ));
        }
    };
    if filter.user_id != current_user.id && !current_user.is_admin {
        return response::forbidden("only the filter owner or an admin can modify a filter");
    }

    let mut update_doc = Document::new();

    if let Some(active) = body.active {
        update_doc.insert("active", active);
    }
    if let Some(active_fid) = body.active_fid.clone() {
        // Ensure the fid exists in the filter versions
        if !filter.fv.iter().any(|fv| fv.fid == active_fid) {
            return response::bad_request(
                "active_fid must be one of the existing filter version IDs",
            );
        }
        update_doc.insert("active_fid", active_fid);
    }
    if let Some(permissions) = body.permissions.clone() {
        update_doc.insert("permissions", permissions);
    }
    if update_doc.is_empty() {
        return response::bad_request("no valid fields to update");
    }
    let update_result = collection
        .update_one(doc! {"_id": filter_id.clone()}, doc! {"$set": update_doc})
        .await;
    match update_result {
        Ok(_) => {
            return response::ok(
                &format!("successfully updated filter id: {}", &filter_id),
                serde_json::Value::Null,
            );
        }
        Err(e) => {
            return response::internal_error(&format!("failed to update filter. error: {}", e));
        }
    }
}

/// Get multiple filters
#[utoipa::path(
    get,
    path = "/filters",
    responses(
        (status = 200, description = "Filters retrieved successfully", body = [FilterPublic]),
        (status = 500, description = "Internal server error")
    ),
    tags=["Filters"]
)]
#[get("/filters")]
pub async fn get_filters(
    db: web::Data<Database>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let current_user = current_user.unwrap();

    let filter_collection: Collection<FilterPublic> = db.collection("filters");
    let filter_query = if current_user.is_admin {
        doc! {}
    } else {
        doc! { "user_id": &current_user.id }
    };
    let filters = filter_collection.find(filter_query).await;

    match filters {
        Ok(mut cursor) => {
            let mut filter_list = Vec::<FilterPublic>::new();
            while let Some(filter_in_db) = cursor.next().await {
                match filter_in_db {
                    Ok(filter) => {
                        filter_list.push(filter);
                    }
                    Err(e) => {
                        return response::internal_error(&format!("error reading filter: {}", e));
                    }
                }
            }
            response::ok(
                "retrieved filters successfully",
                serde_json::to_value(&filter_list).unwrap(),
            )
        }
        Err(e) => {
            return response::internal_error(&format!("failed to query filters: {}", e));
        }
    }
}

/// Get a single filter
#[utoipa::path(
    get,
    path = "/filters/{filter_id}",
    responses(
        (status = 200, description = "Filter retrieved successfully", body = FilterPublic),
        (status = 404, description = "Filter not found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Filters"]
)]
#[get("/filters/{filter_id}")]
pub async fn get_filter(
    db: web::Data<Database>,
    path: web::Path<String>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let current_user = current_user.unwrap();

    let filter_id = path.into_inner();
    let filter_query = if current_user.is_admin {
        doc! { "_id": &filter_id }
    } else {
        doc! { "_id": &filter_id, "user_id": &current_user.id }
    };
    let filter_collection: Collection<FilterPublic> = db.collection("filters");

    match filter_collection.find_one(filter_query).await {
        Ok(Some(filter)) => response::ok(
            "retrieved filter successfully",
            serde_json::to_value(filter).unwrap(),
        ),
        Ok(None) => response::not_found(&format!("filter with id {} does not exist", filter_id)),
        Err(e) => {
            return response::internal_error(&format!("failed to query filter: {}", e));
        }
    }
}
