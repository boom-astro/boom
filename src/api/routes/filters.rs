use crate::{
    alert::{
        LsstAliases, LsstCandidate, LsstForcedPhot, ZtfAliases, ZtfCandidate, ZtfForcedPhot,
        ZtfPrvCandidate,
    },
    api::{
        filters::{doc2json, SortOrder},
        models::response,
        routes::users::User,
    },
    enrichment::{LsstAlertProperties, ZtfAlertClassifications, ZtfAlertProperties},
    filter::{
        build_filter_pipeline, Filter, FilterError, FilterVersion, SURVEYS_REQUIRING_PERMISSIONS,
    },
    utils::{db::mongify, enums::Survey},
};

use actix_web::{get, patch, post, web, HttpResponse};
use apache_avro::AvroSchema;
use apache_avro_macros::serdavro;
use flare::Time;
use futures::stream::StreamExt;
use mongodb::{
    bson::{doc, Document},
    Collection, Database,
};
use serde::{Deserialize, Serialize};
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

async fn build_test_filter_pipeline(
    survey: &Survey,
    permissions: &HashMap<Survey, Vec<i32>>,
    pipeline: &Vec<serde_json::Value>,
    start_jd: Option<f64>,
    end_jd: Option<f64>,
    object_ids: Option<Vec<String>>,
    candids: Option<Vec<String>>,
) -> Result<Vec<Document>, FilterError> {
    if SURVEYS_REQUIRING_PERMISSIONS.contains(&survey) && permissions.get(&survey).is_none() {
        return Err(FilterError::InvalidFilterPipeline(format!(
            "Filters running on survey {:?} must have permissions defined for that survey",
            survey
        )));
    }

    // the first stage of test_pipeline is a match stage, we can overwrite it based on the test criteria
    let mut match_stage = Document::new();

    if let (Some(start_jd), Some(end_jd)) = (start_jd, end_jd) {
        if end_jd <= start_jd {
            return Err(FilterError::InvalidFilterPipeline(
                "end_jd cannot be less than or equal to start_jd".to_string(),
            ));
        }
        if end_jd - start_jd > 7.0 {
            return Err(FilterError::InvalidFilterPipeline(
                "JD window for filter test cannot exceed 7.0 JD".to_string(),
            ));
        }
        match_stage.insert("candidate.jd", doc! { "$gte": start_jd, "$lte": end_jd });
    }

    let obj_ids: Vec<String> = object_ids
        .unwrap_or_default()
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect();
    if obj_ids.len() > 1000 {
        return Err(FilterError::InvalidFilterPipeline(
            "maximum of 1000 object_ids allowed for filter test".to_string(),
        ));
    }
    if !obj_ids.is_empty() {
        match_stage.insert("objectId", doc! { "$in": obj_ids });
    }

    let candid_ids: Vec<String> = candids
        .unwrap_or_default()
        .into_iter()
        .filter(|s| !s.is_empty())
        .collect();
    if candid_ids.len() > 100000 {
        return Err(FilterError::InvalidFilterPipeline(
            "maximum of 100000 candids allowed for filter test".to_string(),
        ));
    }
    if !candid_ids.is_empty() {
        let candids_i64: Vec<i64> = candid_ids
            .iter()
            .filter_map(|id| id.parse::<i64>().ok())
            .collect();
        match_stage.insert("_id", doc! { "$in": candids_i64 });
    }

    if match_stage.is_empty() {
        return Err(FilterError::InvalidFilterPipeline(
            "at least one of (start_jd and end_jd), object_ids, or candid_ids must be provided"
                .to_string(),
        ));
    }

    let mut test_pipeline = match build_filter_pipeline(&pipeline, &permissions, &survey).await {
        Ok(p) => p,
        Err(e) => {
            return Err(FilterError::InvalidFilterPipeline(format!(
                "Filter build failed with error: {}",
                e
            )));
        }
    };
    match test_pipeline.get(0) {
        Some(first_stage) => {
            if first_stage.get("$match").is_none() {
                return Err(FilterError::InvalidFilterPipeline(
                    "first stage of pipeline must be a $match stage".to_string(),
                ));
            }
        }
        None => {
            return Err(FilterError::InvalidFilterPipeline(
                "pipeline must have at least one stage".to_string(),
            ));
        }
    }

    if SURVEYS_REQUIRING_PERMISSIONS.contains(&survey) {
        // ZTF survey uses programid for permissions
        match_stage.insert(
            "candidate.programid",
            doc! { "$in": permissions.get(&survey).unwrap() },
        );
    }
    test_pipeline[0].insert("$match", match_stage);
    Ok(test_pipeline)
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
    pub sort_by: Option<String>,
    pub sort_order: Option<SortOrder>,
    pub limit: Option<u32>,
}

#[derive(serde::Serialize, ToSchema)]
pub struct FilterTestResponse {
    pub pipeline: Vec<serde_json::Value>,
    pub results: Vec<serde_json::Value>,
}

impl FilterTestResponse {
    pub fn new(pipeline: Vec<Document>, results: Vec<Document>) -> Self {
        Self {
            pipeline: doc2json(pipeline),
            results: doc2json(results),
        }
    }
}

/// Test a filter pipeline
#[utoipa::path(
    post,
    path = "/filters/test",
    request_body = FilterTestRequest,
    responses(
        (status = 200, description = "Filter test executed successfully", body = FilterTestResponse),
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
    let pipeline = body.pipeline;

    let mut test_pipeline = match build_test_filter_pipeline(
        &survey,
        &permissions,
        &pipeline,
        body.start_jd,
        body.end_jd,
        body.object_ids,
        body.candids,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => match e {
            FilterError::InvalidFilterPipeline(msg) => {
                return response::bad_request(msg.as_str());
            }
            _ => {
                return response::internal_error(&format!(
                    "failed to build test filter pipeline: {}",
                    e
                ));
            }
        },
    };

    // Add sort stage if specified, right after the match stage
    if let Some(sort_by) = body.sort_by {
        if sort_by.is_empty() {
            return response::bad_request("sort_by cannot be an empty string");
        }
        let sort_order = match body.sort_order {
            Some(SortOrder::Ascending) => 1,
            Some(SortOrder::Descending) => -1,
            None => 1,
        };
        let sort_stage = doc! { "$sort": { sort_by: sort_order } };
        test_pipeline.insert(1, sort_stage);
    }

    // Add limit stage if specified, at the very end of the pipeline
    if let Some(limit) = body.limit {
        if limit == 0 {
            return response::bad_request("limit must be greater than 0");
        }
        let limit_stage = doc! { "$limit": limit as i64 };
        test_pipeline.push(limit_stage);
    }

    let collection: Collection<Document> = db.collection(format!("{}_alerts", survey).as_str());
    let mut cursor = match collection.aggregate(test_pipeline.clone()).await {
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
    response::ok_ser(
        "filter test executed successfully",
        FilterTestResponse::new(test_pipeline, results),
    )
}

#[derive(serde::Deserialize, Clone, ToSchema)]
pub struct FilterTestCountRequest {
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

#[derive(serde::Serialize, ToSchema)]
pub struct FilterTestCountResponse {
    pub count: i64,
    pub pipeline: Vec<serde_json::Value>,
}

impl FilterTestCountResponse {
    pub fn new(pipeline: Vec<Document>, count: i64) -> Self {
        Self {
            pipeline: doc2json(pipeline),
            count,
        }
    }
}

/// Test a filter pipeline and get count of matching alerts
#[utoipa::path(
    post,
    path = "/filters/test/count",
    request_body = FilterTestCountRequest,
    responses(
        (status = 200, description = "Filter test executed successfully", body = FilterTestCountResponse),
        (status = 400, description = "Invalid filter submitted"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Filters"]
)]
#[post("/filters/test/count")]
pub async fn post_filter_test_count(
    db: web::Data<Database>,
    body: web::Json<FilterTestCountRequest>,
    current_user: Option<web::ReqData<User>>,
) -> HttpResponse {
    let _current_user = current_user.unwrap();
    let body = body.clone();
    let survey = body.survey;
    let permissions = body.permissions;
    let pipeline = body.pipeline;

    let mut test_pipeline = match build_test_filter_pipeline(
        &survey,
        &permissions,
        &pipeline,
        body.start_jd,
        body.end_jd,
        body.object_ids,
        body.candids,
    )
    .await
    {
        Ok(p) => p,
        Err(e) => match e {
            FilterError::InvalidFilterPipeline(msg) => {
                return response::bad_request(msg.as_str());
            }
            _ => {
                return response::internal_error(&format!(
                    "failed to build test filter pipeline: {}",
                    e
                ));
            }
        },
    };

    // Add count stage at the end of the pipeline
    let count_stage = doc! { "$count": "count" };
    test_pipeline.push(count_stage);

    let collection: Collection<mongodb::bson::Document> =
        db.collection(format!("{}_alerts", survey).as_str());
    let mut cursor = match collection.aggregate(test_pipeline.clone()).await {
        Ok(c) => c,
        Err(e) => {
            return response::bad_request(&format!(
                "Invalid filter submitted, filter test failed with error: {}",
                e
            ))
        }
    };
    // there is no Vec of results, just one document with the count
    let count =
        match cursor.next().await {
            Some(res) => match res {
                Ok(doc) => match doc.get("count") {
                    Some(mongodb::bson::Bson::Int32(c)) => *c as i64,
                    Some(mongodb::bson::Bson::Int64(c)) => *c,
                    _ => return response::internal_error(
                        "error retrieving test filter count result: count field missing or invalid",
                    ),
                },
                Err(e) => {
                    // TODO: instead of returning an internal error, log it
                    // with tracing (once we have that set up in the API)
                    return response::internal_error(&format!(
                        "error retrieving test filter count result: {}",
                        e
                    ));
                }
            },
            None => 0,
        };

    response::ok_ser(
        "filter test count executed successfully",
        FilterTestCountResponse::new(test_pipeline, count),
    )
}

#[serdavro]
#[derive(Debug, Deserialize, Serialize)]
pub struct GalacticCoordinates {
    pub l: f64,
    pub b: f64,
}

#[serdavro]
#[derive(Debug, Deserialize, Serialize)]
pub struct ZtfFilterMatch {
    pub prv_candidates: Vec<ZtfCandidate>,
    pub prv_nondetections: Vec<ZtfPrvCandidate>,
    pub fp_hists: Vec<ZtfForcedPhot>,
}

#[serdavro]
#[derive(Debug, Deserialize, Serialize)]
pub struct LsstFilterMatch {
    pub prv_candidates: Vec<LsstCandidate>,
    pub fp_hists: Vec<LsstForcedPhot>,
}

#[serdavro]
#[derive(Debug, Deserialize, Serialize)]
pub struct ZtfAlertToFilter {
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: ZtfCandidate,
    pub classifications: ZtfAlertClassifications,
    pub properties: ZtfAlertProperties,
    pub coordinates: GalacticCoordinates,
    pub prv_candidates: Vec<ZtfPrvCandidate>,
    pub prv_nondetections: Vec<ZtfPrvCandidate>,
    pub fp_hists: Vec<ZtfForcedPhot>,
    pub aliases: ZtfAliases,
    #[serde(rename = "LSST")]
    pub lsst: Option<LsstFilterMatch>,
}

#[serdavro]
#[derive(Debug, Deserialize, Serialize)]
pub struct LsstAlertToFilter {
    pub candid: i64,
    #[serde(rename = "objectId")]
    pub object_id: String,
    pub candidate: LsstCandidate,
    pub properties: LsstAlertProperties,
    pub coordinates: GalacticCoordinates,
    pub prv_candidates: Vec<LsstCandidate>,
    pub fp_hists: Vec<LsstForcedPhot>,
    pub aliases: LsstAliases,
    #[serde(rename = "ZTF")]
    pub ztf: Option<ZtfFilterMatch>,
}

/// Get a schema of a survey's data available at filtering time
#[utoipa::path(
    get,
    path = "/filters/schema/{survey_name}",
    params(
        ("survey_name" = Survey, Path, description = "Name of the survey (e.g., 'ZTF')"),
    ),
    responses(
        (status = 200, description = "Schema found", body = serde_json::Value),
        (status = 404, description = "Schema not found"),
    ),
    tags=["Filters"]
)]
#[get("/filters/schema/{survey_name}")]
pub async fn get_filter_schema(path: web::Path<(Survey,)>) -> HttpResponse {
    // return the avro schema
    let survey_name = path.into_inner().0;
    match survey_name {
        Survey::Ztf => {
            let schema = ZtfAlertToFilter::get_schema();
            return response::ok(
                &format!("avro schema for survey {}", survey_name),
                serde_json::json!(schema),
            );
        }
        _ => {
            return response::not_found(&format!(
                "no filter data schema found for survey {}",
                survey_name
            ));
        }
    }
}
