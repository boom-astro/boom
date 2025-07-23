use crate::models::response;
use actix_web::{HttpResponse, get, web};
use base64::prelude::*;
use futures::TryStreamExt;
use mongodb::{
    Collection, Database,
    bson::{Document, doc, document::ValueAccessResult},
};
use utoipa::ToSchema;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
struct Obj {
    object_id: String,
    candidate: serde_json::Value,
    cutout_science: serde_json::Value,
    cutout_template: serde_json::Value,
    cutout_difference: serde_json::Value,
    prv_candidates: Vec<serde_json::Value>,
    prv_nondetections: Vec<serde_json::Value>,
    fp_hists: Vec<serde_json::Value>,
    classifications: serde_json::Value,
    cross_matches: serde_json::Value,
    aliases: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
struct ObjResponse {
    status: String,
    message: String,
    data: Obj,
}

fn bson_docs_to_json_values(
    array: ValueAccessResult<&Vec<mongodb::bson::Bson>>,
) -> Vec<serde_json::Value> {
    array
        .unwrap_or(&vec![])
        .iter()
        .map(|doc| serde_json::json!(doc.clone()))
        .collect()
}

/// Fetch an object from a given survey's alert stream
///
/// Ultimately, this endpoint should format the object nicely,
/// in a way that is useful for a frontend to display object-level information.
#[utoipa::path(
    get,
    path = "/surveys/{survey_name}/objects/{object_id}",
    params(
        ("survey_name" = String, Path, description = "Name of the survey (e.g., 'ZTF')"),
        ("object_id" = String, Path, description = "ID of the object to retrieve"),
    ),
    responses(
        (status = 200, description = "Object found", body = ObjResponse),
        (status = 404, description = "Object not found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[get("/surveys/{survey_name}/objects/{object_id}")]
pub async fn get_object(
    db: web::Data<Database>,
    path: web::Path<(String, String)>,
) -> HttpResponse {
    let (survey_name, object_id) = path.into_inner();
    let survey_name = survey_name.to_uppercase(); // All alert collections are uppercase
    let alerts_collection: Collection<Document> = db.collection(&format!("{}_alerts", survey_name));
    let cutout_collection: Collection<Document> =
        db.collection(&format!("{}_alerts_cutouts", survey_name));
    let aux_collection: Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey_name));
    // Find options for getting most recent alert from alerts collection
    let find_options_recent = mongodb::options::FindOptions::builder()
        .sort(doc! {
            "candidate.jd": -1,
        })
        .projection(doc! {
            "candidate": 1,
            "classifications": 1,
        })
        .limit(1)
        .build();

    // Get the most recent alert for the object
    let mut alert_cursor = match alerts_collection
        .find(doc! {
            "objectId": &object_id,
        })
        .with_options(find_options_recent)
        .await
    {
        Ok(cursor) => cursor,
        Err(error) => {
            return response::internal_error(&format!(
                "error retrieving latest alert for object {}: {}",
                object_id, error
            ));
        }
    };
    let newest_alert = match alert_cursor.try_next().await {
        Ok(Some(alert)) => alert,
        Ok(None) => {
            return response::ok(
                &format!("no object found with id {}", object_id),
                serde_json::Value::Null,
            );
        }
        Err(error) => {
            return response::internal_error(&format!("error getting documents: {}", error));
        }
    };

    // using the candid, query the cutout collection for the cutouts
    let candid = match newest_alert.get_i64("_id") {
        Ok(candid) => candid,
        Err(_) => {
            return response::ok("no candid found", serde_json::Value::Null);
        }
    };
    let cutouts = match cutout_collection
        .find_one(doc! {
            "_id": candid,
        })
        .await
    {
        Ok(Some(cutouts)) => cutouts,
        Ok(None) => {
            return response::ok("no cutouts found", serde_json::Value::Null);
        }
        Err(error) => {
            return response::internal_error(&format!("error getting documents: {}", error));
        }
    };

    let find_options_aux = mongodb::options::FindOneOptions::builder()
        .projection(doc! {
            "_id": 0,
            "prv_candidates": 1,
            "prv_nondetections": 1,
            "fp_hists": 1,
            "cross_matches": 1,
            "aliases": 1,
        })
        .build();

    // Get crossmatches and light curve data from aux collection
    let aux_entry = match aux_collection
        .find_one(doc! {
            "_id": &object_id,
        })
        .with_options(find_options_aux)
        .await
    {
        Ok(entry) => match entry {
            Some(doc) => doc,
            None => {
                return response::ok("no aux entry found", serde_json::Value::Null);
            }
        },
        Err(error) => {
            return response::internal_error(&format!("error getting documents: {}", error));
        }
    };

    // Prepare the response
    // Convert prv_candidates into an array of JSON values
    let prv_candidates = bson_docs_to_json_values(aux_entry.get_array("prv_candidates"));
    let prv_nondetections = bson_docs_to_json_values(aux_entry.get_array("prv_nondetections"));
    let fp_hists = bson_docs_to_json_values(aux_entry.get_array("fp_hists"));

    let cutout_science = match cutouts.get_binary_generic("cutoutScience") {
        Ok(cutout) => cutout,
        Err(_) => {
            return response::ok("no cutoutScience found", serde_json::Value::Null);
        }
    };
    let cutout_template = match cutouts.get_binary_generic("cutoutTemplate") {
        Ok(cutout) => cutout,
        Err(_) => {
            return response::ok("no cutoutTemplate found", serde_json::Value::Null);
        }
    };
    let cutout_difference = match cutouts.get_binary_generic("cutoutDifference") {
        Ok(cutout) => cutout,
        Err(_) => {
            return response::ok("no cutoutDifference found", serde_json::Value::Null);
        }
    };
    let resp = Obj {
        object_id: object_id.clone(),
        candidate: serde_json::json!(newest_alert.get_document("candidate").unwrap().clone()),
        cutout_science: serde_json::json!(BASE64_STANDARD.encode(cutout_science)),
        cutout_template: serde_json::json!(BASE64_STANDARD.encode(cutout_template)),
        cutout_difference: serde_json::json!(BASE64_STANDARD.encode(cutout_difference)),
        prv_candidates,
        prv_nondetections,
        fp_hists,
        classifications: serde_json::json!(
            newest_alert
                .get_document("classifications")
                .unwrap()
                .clone()
        ),
        cross_matches: serde_json::json!(aux_entry.get_document("cross_matches").unwrap().clone()),
        aliases: serde_json::json!(aux_entry.get_document("aliases").unwrap().clone()),
    };
    return response::ok(
        &format!("object found with object_id: {}", object_id),
        serde_json::json!(resp),
    );
}
