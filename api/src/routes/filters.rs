use crate::{models::response, routes::users::User};

use actix_web::{HttpResponse, patch, post, web};
use mongodb::{
    Collection, Database,
    bson::{Document, doc},
};
use std::vec;
use utoipa::ToSchema;
use uuid::Uuid;

#[derive(serde::Deserialize, Clone, ToSchema)]
pub struct FilterSubmissionBody {
    pub pipeline: Option<Vec<serde_json::Value>>,
    pub permissions: Option<Vec<i32>>,
    pub catalog: Option<String>,
}

#[derive(serde::Deserialize, serde::Serialize, Clone, ToSchema)]
struct Filter {
    pub pipeline: Vec<serde_json::Value>,
    pub permissions: Vec<i32>,
    pub catalog: String,
    pub id: String,
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

/// Takes a verified filter and builds the properly formatted bson document for the database
fn build_filter_bson(
    filter: Filter,
    user_id: &str,
) -> Result<mongodb::bson::Document, mongodb::error::Error> {
    // generate new object id
    let id = mongodb::bson::oid::ObjectId::new();
    let date_time = mongodb::bson::DateTime::now();
    let pipeline_id = Uuid::new_v4().to_string(); // generate random pipeline id
    // Convert the filter pipeline from JSON into a vector of BSON Documents
    let pipeline: Vec<mongodb::bson::Document> = filter
        .pipeline
        .into_iter()
        .map(|v| {
            mongodb::bson::to_document(&v).map_err(|e| {
                mongodb::error::Error::from(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("Failed to convert pipeline step to BSON Document: {}", e),
                ))
            })
        })
        .collect::<Result<_, _>>()?;
    let database_filter_bson = doc! {
        "_id": id,
        "id": filter.id,
        "catalog": filter.catalog,
        "permissions": filter.permissions,
        "active": true,
        "active_fid": pipeline_id.clone(),
        "fv": [
            {
                "fid": pipeline_id,
                "pipeline": pipeline,
                "created_at": date_time,
            }
        ],
        "autosave": false,
        "update_annotations": true,
        "created_by": user_id,
        "created_at": date_time,
        "last_modified": date_time,
    };
    Ok(database_filter_bson)
}

#[derive(serde::Deserialize, Clone, ToSchema)]
struct FilterPipelinePatch {
    pipeline: Vec<serde_json::Value>,
}

/// Add a new version to an existing filter
#[utoipa::path(
    patch,
    path = "/filters/{filter_id}",
    request_body = FilterPipelinePatch,
    responses(
        (status = 200, description = "Filter version added successfully"),
        (status = 400, description = "Invalid filter submitted"),
        (status = 500, description = "Internal server error")
    )
)]
#[patch("/filters/{filter_id}")]
pub async fn add_filter_version(
    db: web::Data<Database>,
    filter_id: web::Path<String>,
    body: web::Json<FilterPipelinePatch>,
) -> HttpResponse {
    let filter_id = filter_id.into_inner();
    let collection: Collection<Document> = db.collection("filters");
    let owner_filter = match collection.find_one(doc! {"id": filter_id.clone()}).await {
        Ok(Some(filter)) => filter,
        Ok(None) => {
            return HttpResponse::BadRequest()
                .body(format!("filter with id {} does not exist", filter_id));
        }
        Err(e) => {
            return HttpResponse::InternalServerError().body(format!(
                "failed to find filter with id {}. error: {}",
                &filter_id, e
            ));
        }
    };
    let catalog = owner_filter.get_str("catalog").unwrap();
    let permissions = owner_filter.get_array("permissions").unwrap();
    let permissions: Vec<i32> = permissions
        .iter()
        .map(|perm| perm.as_i32().unwrap())
        .collect();
    // Create test version of filter and test it
    // Convert pipeline from JSON to an array of Documents
    let pipeline = body.pipeline.clone();
    let pipeline: Vec<mongodb::bson::Document> = pipeline
        .into_iter()
        .map(|v| {
            mongodb::bson::to_document(&v).map_err(|e| {
                HttpResponse::BadRequest().body(format!(
                    "Failed to convert pipeline step to BSON Document: {}",
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
            return HttpResponse::BadRequest().body(format!(
                "Invalid filter submitted, filter test failed with error: {}",
                e
            ));
        }
    }

    let new_pipeline_id = Uuid::new_v4().to_string();
    let date_time = mongodb::bson::DateTime::now();
    let new_pipeline_bson = doc! {
        "fid": new_pipeline_id,
        "pipeline": pipeline,
        "created_at": date_time,
    };
    let update_result = collection
        .update_one(
            doc! {"id": filter_id.clone()},
            doc! {
                "$push": {
                    "fv": new_pipeline_bson
                }
            },
        )
        .await;
    match update_result {
        Ok(_) => {
            return HttpResponse::Ok().body(format!(
                "successfully added new pipeline version to filter id: {}",
                &filter_id
            ));
        }
        Err(e) => {
            return HttpResponse::InternalServerError().body(format!(
                "failed to add new pipeline to filter. error: {}",
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
        (status = 200, description = "Filter created successfully", body = Filter),
        (status = 400, description = "Invalid filter submitted"),
        (status = 500, description = "Internal server error")
    )
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
                HttpResponse::BadRequest().body(format!(
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
            return HttpResponse::BadRequest().body(format!(
                "Invalid filter submitted, filter test failed with error: {}",
                e
            ));
        }
    }

    // Save filter to database
    let filter_id = uuid::Uuid::new_v4().to_string();
    let filter_collection: Collection<mongodb::bson::Document> = db.collection("filters");
    let database_filter = Filter {
        pipeline,
        permissions,
        catalog,
        id: filter_id,
    };
    let filter_bson = match build_filter_bson(database_filter.clone(), &current_user.id) {
        Ok(bson) => bson,
        Err(e) => {
            return HttpResponse::BadRequest()
                .body(format!("unable to create filter bson, got error: {}", e));
        }
    };
    match filter_collection.insert_one(filter_bson).await {
        Ok(_) => {
            return response::ok("success", serde_json::to_value(database_filter).unwrap());
        }
        Err(e) => {
            return HttpResponse::BadRequest().body(format!(
                "failed to insert filter into database. error: {}",
                e
            ));
        }
    }
}
