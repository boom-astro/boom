use std::collections::HashMap;

use crate::api::models::response;
use crate::api::routes::babamul::BabamulUser;
use crate::utils::enums::Survey;
use actix_web::{get, web, HttpResponse};
use mongodb::{
    bson::{doc, Document},
    Collection, Database,
};
use utoipa::ToSchema;

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
struct XmatchResponse {
    status: String,
    message: String,
    data: serde_json::Value,
}

/// Fetch an object's cross-matches (from a given survey's alert stream) by its object ID
#[utoipa::path(
    get,
    path = "/babamul/surveys/{survey}/cross_matches/{object_id}",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (e.g., ztf, lsst)"),
        ("object_id" = String, Path, description = "ID of the object to retrieve"),
    ),
    responses(
        (status = 200, description = "Object found", body = XmatchResponse),
        (status = 404, description = "Object not found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[get("/surveys/{survey}/cross_matches/{object_id}")]
pub async fn get_object_xmatches(
    path: web::Path<(Survey, String)>,
    current_user: Option<web::ReqData<BabamulUser>>,
    db: web::Data<Database>,
) -> HttpResponse {
    // TODO: implement permissions for Babamul users, so we
    // can constrain access to certain surveys' datapoints
    let _current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };

    let (survey, object_id) = path.into_inner();
    if survey != Survey::Ztf && survey != Survey::Lsst {
        return response::bad_request(&format!(
            "Unsupported survey: {}. Supported surveys are: ztf, lsst",
            survey
        ));
    }
    let aux_collection: Collection<Document> = db.collection(&format!("{}_alerts_aux", survey));

    let aux_entry = match aux_collection
        .find_one(doc! {
            "_id": &object_id,
        })
        .projection(doc! {
            "_id": 1,
            "cross_matches": 1,
        })
        .await
    {
        Ok(entry) => match entry {
            Some(doc) => doc,
            None => {
                return response::not_found(&format!(
                    "no aux entry found for object id {}",
                    object_id
                ));
            }
        },
        Err(error) => {
            return response::internal_error(&format!("error getting documents: {}", error));
        }
    };

    let cross_matches: HashMap<String, Vec<Document>> =
        match aux_entry.get_document("cross_matches") {
            Ok(matches_doc) => matches_doc
                .iter()
                .map(|(catalog, matches)| {
                    let matches_array = match matches.as_array() {
                        Some(arr) => arr
                            .iter()
                            .filter_map(|m| m.as_document().cloned())
                            .collect::<Vec<Document>>(),
                        None => vec![],
                    };
                    (catalog.clone(), matches_array)
                })
                .collect::<HashMap<String, Vec<Document>>>(),
            Err(_) => HashMap::new(),
        };

    let response = XmatchResponse {
        status: "success".to_string(),
        message: format!("Found cross-matches for object {}", object_id),
        data: serde_json::json!(cross_matches),
    };
    HttpResponse::Ok().json(response)
}
