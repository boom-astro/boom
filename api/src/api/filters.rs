use crate::models::filter_models::*;
use actix_web::{HttpResponse, patch, post, web};
use mongodb::{
    Client, Collection,
    bson::{Document, doc},
};
use std::vec;
use uuid::Uuid;

const DB_NAME: &str = "boom";

struct Filter {
    pub pipeline: Vec<mongodb::bson::Document>,
    pub permissions: Vec<i32>,
    pub catalog: String,
    pub id: i32,
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
    client: web::Data<Client>,
    catalog: String,
    pipeline: Vec<mongodb::bson::Document>,
) -> Result<(), mongodb::error::Error> {
    let collection: Collection<mongodb::bson::Document> = client
        .database(DB_NAME)
        .collection(format!("{}_alerts", catalog).as_str());

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

// takes a verified filter and builds the properly formatted bson document for the database
fn build_filter_bson(filter: Filter) -> Result<mongodb::bson::Document, mongodb::error::Error> {
    // generate new object id
    let id = mongodb::bson::oid::ObjectId::new();
    let date_time = mongodb::bson::DateTime::now();
    let pipeline_id = Uuid::new_v4().to_string(); // generate random pipeline id
    let database_filter_bson = doc! {
        "_id": id,
        "group_id": 41, // consistent with other test filters
        "filter_id": filter.id,
        "catalog": filter.catalog,
        "permissions": filter.permissions,
        "active": true,
        "active_fid": pipeline_id.clone(),
        "fv": [
            {
                "fid": pipeline_id,
                "pipeline": filter.pipeline,
                "created_at": date_time,
            }
        ],
        "autosave": false,
        "update_annotations": true,
        "created_at": date_time,
        "last_modified": date_time,
    };
    Ok(database_filter_bson)
}

#[patch("/filters/{filter_id}")]
pub async fn add_filter_version(
    client: web::Data<Client>,
    filter_id: web::Path<i32>,
    body: web::Json<FilterSubmissionBody>,
) -> HttpResponse {
    let filter_id = filter_id.into_inner();
    let pipeline = match body.clone().pipeline {
        Some(pipeline) => pipeline,
        None => {
            return HttpResponse::BadRequest()
                .body("pipeline not provided. pipeline required for adding a filter version");
        }
    };

    let collection: Collection<Document> = client.database(DB_NAME).collection("filters");
    let owner_filter = match collection.find_one(doc! {"filter_id": filter_id}).await {
        Ok(Some(filter)) => filter,
        Ok(None) => {
            return HttpResponse::BadRequest()
                .body(format!("filter with id {} does not exist", filter_id));
        }
        Err(e) => {
            return HttpResponse::InternalServerError().body(format!(
                "failed to find filter with id {}. error: {}",
                filter_id, e
            ));
        }
    };
    let catalog = owner_filter.get_str("catalog").unwrap();
    let permissions = owner_filter.get_array("permissions").unwrap();
    let permissions: Vec<i32> = permissions
        .iter()
        .map(|perm| perm.as_i32().unwrap())
        .collect();
    // create test version of filter and test it
    let test_pipeline = build_test_pipeline(catalog.to_string(), permissions, pipeline.clone());

    match run_test_pipeline(client.clone(), catalog.to_string(), test_pipeline).await {
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
            doc! {"filter_id": filter_id},
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
                filter_id
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

#[post("/filters")]
pub async fn post_filter(
    client: web::Data<Client>,
    body: web::Json<FilterSubmissionBody>,
) -> HttpResponse {
    let body = body.clone();
    // grab user filter
    let catalog = match body.catalog {
        Some(catalog) => catalog,
        None => {
            return HttpResponse::BadRequest().body("catalog not provided");
        }
    };
    let id = match body.id {
        Some(id) => id,
        None => {
            return HttpResponse::BadRequest().body("filter id not provided");
        }
    };
    let permissions = match body.permissions {
        Some(permissions) => permissions,
        None => {
            return HttpResponse::BadRequest().body("permissions not provided");
        }
    };
    let pipeline = match body.pipeline {
        Some(pipeline) => pipeline,
        None => {
            return HttpResponse::BadRequest().body("pipeline not provided");
        }
    };

    // Test filter received from user
    // create production version of filter
    let test_pipeline = build_test_pipeline(catalog.clone(), permissions.clone(), pipeline.clone());

    // perform test run to ensure no errors
    match run_test_pipeline(client.clone(), catalog.clone(), test_pipeline).await {
        Ok(()) => {}
        Err(e) => {
            return HttpResponse::BadRequest().body(format!(
                "Invalid filter submitted, filter test failed with error: {}",
                e
            ));
        }
    }

    // save original filter to database
    let filter_collection: Collection<mongodb::bson::Document> =
        client.database(DB_NAME).collection("filters");
    let database_filter = Filter {
        pipeline,
        permissions,
        catalog,
        id,
    };
    let filter_bson = match build_filter_bson(database_filter) {
        Ok(bson) => bson,
        Err(e) => {
            return HttpResponse::BadRequest()
                .body(format!("unable to create filter bson, got error: {}", e));
        }
    };
    match filter_collection.insert_one(filter_bson).await {
        Ok(_) => {
            return HttpResponse::Ok().body("successfully submitted filter to database");
        }
        Err(e) => {
            return HttpResponse::BadRequest().body(format!(
                "failed to insert filter into database. error: {}",
                e
            ));
        }
    }
}
