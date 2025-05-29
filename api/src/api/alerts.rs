use crate::models::response;
use actix_web::{HttpResponse, get, web};
use futures::TryStreamExt;
use mongodb::{
    Collection, Database,
    bson::{Document, doc},
};

#[get("/alerts/{survey_name}/get_object/{object_id}")]
pub async fn get_object(
    db: web::Data<Database>,
    path: web::Path<(String, String)>,
) -> HttpResponse {
    let (survey_name, object_id) = path.into_inner();
    let survey_name = survey_name.to_uppercase(); // (TEMP) to match with "ZTF"
    let alerts_collection: Collection<Document> = db.collection(&format!("{}_alerts", survey_name));
    let aux_collection: Collection<Document> =
        db.collection(&format!("{}_alerts_aux", survey_name));
    // find options for getting most recent alert from alerts collection
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

    // get the most recent alert for the object
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

    // get crossmatches and light curve data from aux collection
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

    let mut candidate = doc! {};
    // organize response
    candidate.insert("objectId", object_id.clone());
    candidate.insert(
        "alert_metadata",
        newest_alert.get_document("candidate").unwrap(),
    );
    candidate.insert(
        "cutoutScience",
        newest_alert.get_document("cutoutScience").unwrap(),
    );
    candidate.insert(
        "cutoutTemplate",
        newest_alert.get_document("cutoutTemplate").unwrap(),
    );
    candidate.insert(
        "cutoutDifference",
        newest_alert.get_document("cutoutDifference").unwrap(),
    );
    candidate.insert(
        "prv_candidates",
        aux_entry.get_array("prv_candidates").unwrap(),
    );
    candidate.insert(
        "cross_matches",
        aux_entry.get_document("cross_matches").unwrap(),
    );

    return response::ok(
        &format!("object found with object_id: {}", object_id),
        serde_json::json!(candidate),
    );
}
