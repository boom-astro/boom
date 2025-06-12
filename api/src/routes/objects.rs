use crate::models::response;
use actix_web::{HttpResponse, get, web};
use futures::TryStreamExt;
use mongodb::{
    Collection, Database,
    bson::{Document, doc},
};
use utoipa::ToSchema;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
struct MostRecentAlert {
    object_id: String,
    alert_metadata: serde_json::Value,
    cutout_science: serde_json::Value,
    cutout_template: serde_json::Value,
    cutout_difference: serde_json::Value,
    prv_candidates: Vec<serde_json::Value>,
    cross_matches: serde_json::Value,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
struct MostRecentAlertResponse {
    status: String,
    message: String,
    data: MostRecentAlert,
}

/// Fetch the most recent alert for a given object in a specified survey
#[utoipa::path(
    get,
    path = "/objects/{object_id}/alerts/latest-by-survey/{survey_name}",
    params(
        ("object_id" = String, Path, description = "ID of the object to retrieve"),
        ("survey_name" = String, Path, description = "Name of the survey (e.g., 'ZTF')"),
    ),
    responses(
        (status = 200, description = "Object found", body = MostRecentAlertResponse),
        (status = 404, description = "Object not found"),
        (status = 500, description = "Internal server error")
    )
)]
#[get("/objects/{object_id}/alerts/latest-by-survey/{survey_name}")]
pub async fn get_latest_alert_for_object(
    db: web::Data<Database>,
    path: web::Path<(String, String)>,
) -> HttpResponse {
    let (object_id, survey_name) = path.into_inner();
    let survey_name = survey_name.to_uppercase(); // (TEMP) to match with "ZTF"
    let alerts_collection: Collection<Document> = db.collection(&format!("{}_alerts", survey_name));
    let aux_collection: Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey_name));
    // Find options for getting most recent alert from alerts collection
    let find_options_recent = mongodb::options::FindOptions::builder()
        .sort(doc! {
            "candidate.jd": -1,
        })
        .projection(doc! {
            "_id": 1,
            "candidate": 1,
            "cutoutScience": 1,
            "cutoutTemplate": 1,
            "cutoutDifference": 1,
        })
        .limit(1)
        .build();

    // Get the most recent alert for the object
    let mut alert_cursor = match alerts_collection
        .find(doc! {
            "objectId": object_id.clone(),
        })
        .with_options(find_options_recent)
        .await
    {
        Ok(cursor) => cursor,
        Err(error) => {
            return response::internal_error(&format!("error getting documents: {}", error));
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

    let find_options_aux = mongodb::options::FindOneOptions::builder()
        .projection(doc! {
            "_id": 0,
            "prv_candidates": 1,
            "cross_matches": 1,
        })
        .build();

    // Get crossmatches and light curve data from aux collection
    let aux_entry = match aux_collection
        .find_one(doc! {
            "_id": object_id.clone(),
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
    let empty_vec = Vec::new();
    let prv_candidates = aux_entry.get_array("prv_candidates").unwrap_or(&empty_vec);
    let prv_candidates: Vec<serde_json::Value> = prv_candidates
        .iter()
        .map(|doc| serde_json::json!(doc.clone()))
        .collect();
    let resp = MostRecentAlert {
        object_id: object_id.clone(),
        alert_metadata: serde_json::json!(newest_alert.get_document("candidate").unwrap().clone()),
        cutout_science: serde_json::json!(
            newest_alert
                .get_document("cutoutScience")
                .unwrap_or(&doc! {})
                .clone()
        ),
        cutout_template: serde_json::json!(
            newest_alert
                .get_document("cutoutTemplate")
                .unwrap_or(&doc! {})
                .clone()
        ),
        cutout_difference: serde_json::json!(
            newest_alert
                .get_document("cutoutDifference")
                .unwrap_or(&doc! {})
                .clone()
        ),
        prv_candidates: prv_candidates,
        cross_matches: serde_json::json!(aux_entry.get_document("cross_matches").unwrap().clone()),
    };
    return response::ok(
        &format!("object found with object_id: {}", object_id),
        serde_json::json!(resp),
    );
}
