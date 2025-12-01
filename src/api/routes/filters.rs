use crate::api::{models::response, routes::users::User};
use crate::filter::{
    build_filter_pipeline, Filter, FilterError, FilterVersion, SURVEYS_REQUIRING_PERMISSIONS,
};
use crate::utils::db::mongify;
use crate::utils::enums::Survey;

use actix_web::{get, patch, post, web, HttpResponse};
use flare::Time;
use futures::stream::StreamExt;
use mongodb::{
    bson::{doc, Document},
    Collection, Database,
};
use std::collections::HashMap;
use std::vec;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(serde::Deserialize, serde::Serialize, Clone, ToSchema)]
pub struct FilterPublic {
    #[serde(rename(serialize = "id", deserialize = "_id"))]
    pub id: String,
    pub name: String,
    pub description: Option<String>,
    pub permissions: HashMap<Survey, Vec<i32>>,
    pub user_id: String,
    pub survey: Survey,
    pub active: bool,
    pub active_fid: String,
    pub fv: Vec<FilterVersion>,
    created_at: f64,
    updated_at: f64,
}

impl From<Filter> for FilterPublic {
    fn from(filter: Filter) -> Self {
        Self {
            id: filter.id,
            name: filter.name,
            description: filter.description,
            permissions: filter.permissions,
            user_id: filter.user_id,
            survey: filter.survey,
            active: filter.active,
            active_fid: filter.active_fid,
            fv: filter.fv,
            created_at: filter.created_at,
            updated_at: filter.updated_at,
        }
    }
}

async fn run_test_pipeline(
    db: web::Data<Database>,
    catalog: &Survey,
    mut pipeline: Vec<Document>,
) -> Result<(), FilterError> {
    let collection: Collection<Document> = db.collection(format!("{}_alerts", catalog).as_str());
    // get the latest candid from the alerts collection
    let result = collection
        .find_one(doc! {})
        .projection(doc! { "_id": 1 })
        .sort(doc! { "candidate.jd": -1 })
        .await?;
    let candid = match result {
        Some(doc) => match doc.get_i64("_id").ok() {
            Some(id) => Some(id),
            None => {
                return Err(FilterError::FilterExecutionError(
                    "Document missing _id field or _id is not an i64. Could not determine latest candid.".to_string(),
                ));
            }
        },
        None => None,
    };
    if candid.is_some() {
        match pipeline.get_mut(0) {
            Some(first_stage) => {
                if first_stage.get("$match").is_none() {
                    return Err(FilterError::InvalidFilterPipeline(
                        "first stage of pipeline must be a $match stage".to_string(),
                    ));
                }
                first_stage.insert("$match", doc! { "_id": candid.as_ref().unwrap() });
            }
            None => {
                return Err(FilterError::InvalidFilterPipeline(
                    "pipeline must have at least one stage".to_string(),
                ));
            }
        }
    }
    match collection.aggregate(pipeline).await {
        Ok(_) => Ok(()),
        Err(e) => Err(FilterError::FilterExecutionError(format!(
            "failed to run test filter on alert with candid {:?}: {}",
            candid, e
        ))),
    }
}

async fn build_and_test_filter_version(
    db: web::Data<Database>,
    survey: &Survey,
    pipeline: &Vec<serde_json::Value>,
    permissions: &HashMap<Survey, Vec<i32>>,
) -> Result<(), FilterError> {
    let test_pipeline = build_filter_pipeline(pipeline, permissions, survey).await?;
    run_test_pipeline(db, survey, test_pipeline).await
}

#[derive(serde::Deserialize, Clone, ToSchema)]
struct FilterVersionPost {
    pipeline: Vec<serde_json::Value>,
    changelog: Option<String>,
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

    let survey = filter.survey;
    let permissions = filter.permissions.clone();
    let new_pipeline = body.pipeline.clone();

    // Test the filter to ensure it works
    match build_and_test_filter_version(db.clone(), &survey, &new_pipeline, &permissions).await {
        Ok(()) => {}
        Err(e) => {
            return response::bad_request(&format!(
                "Invalid filter submitted, filter test failed with error: {}",
                e
            ));
        }
    }

    let new_pipeline_id = Uuid::new_v4().to_string();
    let mut fv_update = doc! {
        "fid": &new_pipeline_id,
        "pipeline": serde_json::to_string(&new_pipeline).unwrap(),
        "created_at": Time::now().to_jd(),
    };
    if let Some(changelog) = body.changelog.clone() {
        fv_update.insert("changelog", changelog);
    }
    let mut update_doc = doc! {
        "$push": {
            "fv": fv_update
        },
    };
    if body.set_as_active.unwrap_or(true) {
        update_doc.insert("$set", doc! { "active_fid": &new_pipeline_id });
    }
    let update_result = collection
        .update_one(doc! {"_id": filter_id.clone()}, update_doc)
        .await;
    match update_result {
        Ok(_) => response::ok(
            &format!(
                "successfully added new version {} to filter id: {}",
                &new_pipeline_id, &filter_id
            ),
            serde_json::json!({"fid": new_pipeline_id}),
        ),
        Err(e) => response::internal_error(&format!(
            "failed to add new version to filter. error: {}",
            e
        )),
    }
}

#[derive(serde::Deserialize, Clone, ToSchema)]
pub struct FilterPost {
    pub name: String,
    pub description: Option<String>,
    pub pipeline: Vec<serde_json::Value>,
    pub permissions: HashMap<Survey, Vec<i32>>,
    pub survey: Survey,
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

    let survey = body.survey;
    let permissions = body.permissions;
    if permissions.get(&survey).is_none() && SURVEYS_REQUIRING_PERMISSIONS.contains(&survey) {
        return response::bad_request(&format!(
            "Filters running on survey {:?} must have permissions defined for that survey",
            survey
        ));
    }
    let pipeline = body.pipeline;

    // Test the filter to ensure it works
    match build_and_test_filter_version(db.clone(), &survey, &pipeline, &permissions).await {
        Ok(()) => {}
        Err(e) => {
            return response::bad_request(&format!(
                "Invalid filter submitted, filter test failed with error: {}",
                e
            ));
        }
    }

    // Save filter to database
    let filter_id = Uuid::new_v4().to_string();
    let filter_version: String = Uuid::new_v4().to_string();
    let filter_collection: Collection<Filter> = db.collection("filters");
    // Pipeline needs to be a string
    let pipeline_json = serde_json::to_string(&pipeline).unwrap();
    let now = Time::now().to_jd();
    let filter = Filter {
        name: body.name,
        description: body.description,
        permissions,
        survey,
        id: filter_id,
        user_id: current_user.id.clone(),
        active: true,
        active_fid: filter_version.clone(),
        fv: vec![FilterVersion {
            fid: filter_version,
            pipeline: pipeline_json,
            changelog: None,
            created_at: now,
        }],
        created_at: now,
        updated_at: now,
    };
    match filter_collection.insert_one(&filter).await {
        Ok(_) => response::ok(
            "successfully created new filter",
            serde_json::to_value(FilterPublic::from(filter)).unwrap(),
        ),
        Err(e) => response::internal_error(&format!(
            "failed to insert filter into database. error: {}",
            e
        )),
    }
}

// we want a PATCH, that lets a user change fields like active, active_fid, permissions
#[derive(serde::Deserialize, Clone, ToSchema)]
struct FilterPatch {
    name: Option<String>,
    description: Option<String>,
    active: Option<bool>,
    active_fid: Option<String>,
    permissions: Option<HashMap<Survey, Vec<i32>>>,
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
    if let Some(name) = body.name.clone() {
        if !name.is_empty() {
            update_doc.insert("name", name);
        }
    }
    if let Some(description) = body.description.clone() {
        if !description.is_empty() {
            update_doc.insert("description", description);
        }
    }
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
        if permissions.get(&filter.survey).is_none()
            && SURVEYS_REQUIRING_PERMISSIONS.contains(&filter.survey)
        {
            return response::bad_request(&format!(
                "Filters running on survey {:?} must have permissions defined for that survey",
                filter.survey
            ));
        }
        update_doc.insert("permissions", mongify(&permissions));
    }
    if update_doc.is_empty() {
        return response::bad_request("no valid fields to update");
    }
    update_doc.insert("updated_at", Time::now().to_jd());
    let update_result = collection
        .update_one(doc! {"_id": filter_id.clone()}, doc! {"$set": update_doc})
        .await;
    match update_result {
        Ok(_) => response::ok_no_data(&format!("successfully updated filter id: {}", &filter_id)),
        Err(e) => response::internal_error(&format!("failed to update filter. error: {}", e)),
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
        Err(e) => response::internal_error(&format!("failed to query filters: {}", e)),
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
        Err(e) => response::internal_error(&format!("failed to query filter: {}", e)),
    }
}

#[derive(serde::Deserialize, Clone, ToSchema)]
pub struct FilterTestRequest {
    pub pipeline: Vec<serde_json::Value>,
    pub permissions: HashMap<Survey, Vec<i32>>,
    pub survey: Survey,
    pub start_jd: Option<f64>,
    pub end_jd: Option<f64>,
    #[schema(max_items = 1000)]
    pub object_ids: Option<Vec<String>>,
    #[schema(max_items = 100000)]
    pub candids: Option<Vec<String>>,
}

/// Test a filter pipeline
#[utoipa::path(
    post,
    path = "/filters/test",
    request_body = FilterTestRequest,
    responses(
        (status = 200, description = "Filter test executed successfully", body = Vec::<serde_json::Value>),
        (status = 400, description = "Invalid filter submitted"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Filters"]
)]
#[post("/filters/test")]
pub async fn post_filter_test(
    db: web::Data<Database>,
    body: web::Json<FilterTestRequest>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let _current_user = current_user.unwrap();
    let body = body.clone();
    let survey = body.survey;
    let permissions = body.permissions;
    if permissions.get(&survey).is_none() && SURVEYS_REQUIRING_PERMISSIONS.contains(&survey) {
        return response::bad_request(&format!(
            "Filters running on survey {:?} must have permissions defined for that survey",
            survey
        ));
    }
    let pipeline = body.pipeline;

    // the first stage of test_pipeline is a match stage, we can overwrite it based on the test criteria
    let mut match_stage = Document::new();

    if let (Some(start_jd), Some(end_jd)) = (body.start_jd, body.end_jd) {
        if end_jd <= start_jd {
            return response::bad_request("end_jd cannot be less than or equal to start_jd");
        }
        if end_jd - start_jd > 7.0 {
            return response::bad_request("JD window for filter test cannot exceed 7.0 JD");
        }
        match_stage.insert("candidate.jd", doc! { "$gte": start_jd, "$lte": end_jd });
    }

    let obj_ids: Vec<String> = body
        .object_ids
        .unwrap_or_default()
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect();
    if obj_ids.len() > 1000 {
        return response::bad_request("maximum of 1000 object_ids allowed for filter test");
    }
    if !obj_ids.is_empty() {
        match_stage.insert("objectId", doc! { "$in": obj_ids });
    }

    let candid_ids: Vec<String> = body
        .candids
        .unwrap_or_default()
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect();
    if candid_ids.len() > 100000 {
        return response::bad_request("maximum of 100000 candids allowed for filter test");
    }
    if !candid_ids.is_empty() {
        let candids_i64: Vec<i64> = candid_ids
            .iter()
            .filter_map(|id| id.parse::<i64>().ok())
            .collect();
        match_stage.insert("_id", doc! { "$in": candids_i64 });
    }

    if match_stage.is_empty() {
        return response::bad_request(
            "at least one of (start_jd and end_jd), object_ids, or candid_ids must be provided",
        );
    }

    let mut test_pipeline = match build_filter_pipeline(&pipeline, &permissions, &survey).await {
        Ok(p) => p,
        Err(e) => {
            return response::bad_request(&format!(
                "Invalid filter submitted, filter build failed with error: {}",
                e
            ));
        }
    };
    match test_pipeline.get(0) {
        Some(first_stage) => {
            if first_stage.get("$match").is_none() {
                return response::bad_request("first stage of pipeline must be a $match stage");
            }
        }
        None => {
            return response::bad_request("pipeline must have at least one stage");
        }
    }
    test_pipeline[0].insert("$match", match_stage);

    let collection: Collection<Document> = db.collection(format!("{}_alerts", survey).as_str());
    let mut cursor = match collection.aggregate(test_pipeline).await {
        Ok(c) => c,
        Err(e) => {
            return response::bad_request(&format!(
                "Invalid filter submitted, filter test failed with error: {}",
                e
            ))
        }
    };

    let mut results = Vec::new();
    while let Some(result) = cursor.next().await {
        match result {
            Ok(doc) => results.push(doc),
            Err(e) => {
                return response::internal_error(&format!(
                    "error retrieving test filter results: {}",
                    e
                ));
            }
        }
    }
    response::ok(
        "filter test executed successfully",
        serde_json::to_value(results).unwrap(),
    )
}
