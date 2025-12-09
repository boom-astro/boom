use crate::alert::{AlertCutout, ZtfCandidate};
use crate::api::models::response;
use crate::api::routes::babamul::users::BabamulUser;
use crate::enrichment::ZtfAlertProperties;
use actix_web::{get, web, HttpResponse};
use base64::prelude::*;
use futures::TryStreamExt;
use mongodb::{
    bson::{doc, document::ValueAccessResult, Document},
    Collection, Database,
};
use utoipa::ToSchema;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
struct Obj {
    object_id: String,
    candidate: serde_json::Value,
    properties: serde_json::Value,
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
    path = "/babamul/surveys/{survey_name}/objects/{object_id}",
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
#[get("/babamul/surveys/{survey_name}/objects/{object_id}")]
pub async fn get_object(
    path: web::Path<(String, String)>,
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
            "properties": 1,
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
            return response::not_found(&format!("no object found with id {}", object_id));
        }
        Err(error) => {
            return response::internal_error(&format!("error getting documents: {}", error));
        }
    };

    // using the candid, query the cutout collection for the cutouts
    let candid = match newest_alert.get_i64("_id") {
        Ok(candid) => candid,
        Err(_) => {
            return response::internal_error("error getting candid from newest alert");
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
            return response::not_found(&format!("no cutouts found for candid {}", candid));
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

    // Prepare the response
    // Convert prv_candidates into an array of JSON values
    let prv_candidates = bson_docs_to_json_values(aux_entry.get_array("prv_candidates"));
    let prv_nondetections = bson_docs_to_json_values(aux_entry.get_array("prv_nondetections"));
    let fp_hists = bson_docs_to_json_values(aux_entry.get_array("fp_hists"));

    let cutout_science = match cutouts.get_binary_generic("cutoutScience") {
        Ok(cutout) => cutout,
        Err(_) => {
            return response::internal_error("error getting cutoutScience from cutouts");
        }
    };
    let cutout_template = match cutouts.get_binary_generic("cutoutTemplate") {
        Ok(cutout) => cutout,
        Err(_) => {
            return response::internal_error("error getting cutoutTemplate from cutouts");
        }
    };
    let cutout_difference = match cutouts.get_binary_generic("cutoutDifference") {
        Ok(cutout) => cutout,
        Err(_) => {
            return response::internal_error("error getting cutoutDifference from cutouts");
        }
    };
    let resp = Obj {
        object_id: object_id.clone(),
        candidate: serde_json::json!(newest_alert.get_document("candidate").unwrap().clone()),
        properties: serde_json::json!(newest_alert.get_document("properties").unwrap().clone()),
        cutout_science: serde_json::json!(BASE64_STANDARD.encode(cutout_science)),
        cutout_template: serde_json::json!(BASE64_STANDARD.encode(cutout_template)),
        cutout_difference: serde_json::json!(BASE64_STANDARD.encode(cutout_difference)),
        prv_candidates,
        prv_nondetections,
        fp_hists,
        classifications: serde_json::json!(newest_alert
            .get_document("classifications")
            .unwrap()
            .clone()),
        cross_matches: serde_json::json!(aux_entry.get_document("cross_matches").unwrap().clone()),
        aliases: serde_json::json!(aux_entry.get_document("aliases").unwrap().clone()),
    };
    return response::ok(
        &format!("object found with object_id: {}", object_id),
        serde_json::json!(resp),
    );
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, ToSchema)]
struct ObjectQuery {
    ra: Option<f64>,
    dec: Option<f64>,
    radius_arcsec: Option<f64>,
    start_jd: Option<f64>,
    end_jd: Option<f64>,
    min_magpsf: Option<f64>,
    max_magpsf: Option<f64>,
    min_drb: Option<f64>,
    max_drb: Option<f64>,
    min_sgscore1: Option<f64>,
    max_sgscore1: Option<f64>,
    min_distpsnr1: Option<f64>,
    max_distpsnr1: Option<f64>,
}

#[get("/babamul/surveys/{survey_name}/objects")]
pub async fn get_objects(
    path: web::Path<String>,
    query: web::Query<ObjectQuery>,
    current_user: Option<web::ReqData<BabamulUser>>,
    db: web::Data<Database>,
) -> HttpResponse {
    let _current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };
    let survey_name = path.into_inner();
    if survey_name.to_uppercase() != "ZTF" {
        return response::bad_request("only ZTF survey is supported for get_objects endpoint");
    }
    let mut filter_doc = Document::new();
    // Build the filter document based on the query parameters
    if let (Some(ra), Some(dec), Some(radius_arcsec)) = (query.ra, query.dec, query.radius_arcsec) {
        // Add cone search filter
        filter_doc.insert(
            "coordinates.radec_geojson",
            doc! {
                "$geoWithin": {
                    "$centerSphere": [
                        [ra, dec],
                        (radius_arcsec / 3600.0) * (std::f64::consts::PI / 180.0)
                    ]
                }
            },
        );
    }
    if let (Some(start_jd), Some(end_jd)) = (query.start_jd, query.end_jd) {
        filter_doc.insert(
            "candidate.jd",
            doc! {
                "$gte": start_jd,
                "$lte": end_jd,
            },
        );
    }
    if let (Some(min_magpsf), Some(max_magpsf)) = (query.min_magpsf, query.max_magpsf) {
        filter_doc.insert(
            "candidate.magpsf",
            doc! {
                "$gte": min_magpsf,
                "$lte": max_magpsf,
            },
        );
    }
    if let (Some(min_drb), Some(max_drb)) = (query.min_drb, query.max_drb) {
        filter_doc.insert(
            "classifications.drb",
            doc! {
                "$gte": min_drb,
                "$lte": max_drb,
            },
        );
    }
    if let (Some(min_sgscore1), Some(max_sgscore1), Some(min_distpsnr1), Some(max_distpsnr1)) = (
        query.min_sgscore1,
        query.max_sgscore1,
        query.min_distpsnr1,
        query.max_distpsnr1,
    ) {
        filter_doc.insert(
            "classifications.sgscore1",
            doc! {
                "$gte": min_sgscore1,
                "$lte": max_sgscore1,
            },
        );
        filter_doc.insert(
            "classifications.distpsnr1",
            doc! {
                "$gte": min_distpsnr1,
                "$lte": max_distpsnr1,
            },
        );
    }

    let projection_doc = doc! {
        "candid": "_id",
        "objectId": 1,
        "candidate": 1,
        "properties": 1,
        "classifications": 1,
    };

    #[derive(Debug, serde::Deserialize, serde::Serialize)]
    pub struct EnrichedZtfAlert {
        // #[serde(rename = "_id")]
        pub candid: i64,
        #[serde(rename = "objectId")]
        pub object_id: String,
        pub candidate: ZtfCandidate,
        pub properties: ZtfAlertProperties,
        pub classifications: Document,
    }

    let alerts_collection: Collection<EnrichedZtfAlert> = db.collection("ZTF_alerts");
    // Query the alerts collection with the constructed filter
    let mut alert_cursor = match alerts_collection
        .find(filter_doc)
        .projection(projection_doc)
        .sort(doc! { "_id": 1 })
        .await
    {
        Ok(cursor) => cursor,
        Err(error) => {
            return response::internal_error(&format!(
                "error retrieving alerts for objects: {}",
                error
            ));
        }
    };

    let mut results: Vec<EnrichedZtfAlert> = Vec::new();
    while let Some(alert_doc) = match alert_cursor.try_next().await {
        Ok(Some(doc)) => Some(doc),
        Ok(None) => None,
        Err(error) => {
            return response::internal_error(&format!("error getting documents: {}", error));
        }
    } {
        results.push(alert_doc);
    }
    return response::ok(
        &format!("found {} objects matching query", results.len()),
        serde_json::json!(results),
    );
}

// we also want an endpoint that given a candid, grabs the cutouts from the cutouts collection
#[get("/babamul/surveys/{survey_name}/objects/cutouts/{candid}")]
pub async fn get_object_cutouts(
    path: web::Path<(String, i64)>,
    current_user: Option<web::ReqData<BabamulUser>>,
    db: web::Data<Database>,
) -> HttpResponse {
    let _current_user = match current_user {
        Some(user) => user,
        None => {
            return HttpResponse::Unauthorized().body("Unauthorized");
        }
    };
    let (survey_name, candid) = path.into_inner();
    let survey_name = survey_name.to_uppercase(); // All alert collections are uppercase

    let cutout_collection: Collection<AlertCutout> =
        db.collection(&format!("{}_alerts_cutouts", survey_name));
    let cutouts = match cutout_collection
        .find_one(doc! {
            "_id": candid,
        })
        .await
    {
        Ok(Some(cutouts)) => cutouts,
        Ok(None) => {
            return response::not_found(&format!("no cutouts found for candid {}", candid));
        }
        Err(error) => {
            return response::internal_error(&format!("error getting documents: {}", error));
        }
    };
    let resp = serde_json::json!({
        "candid": candid,
        "cutout_science": BASE64_STANDARD.encode(&cutouts.cutout_science),
        "cutout_template": BASE64_STANDARD.encode(&cutouts.cutout_template),
        "cutout_difference": BASE64_STANDARD.encode(&cutouts.cutout_difference),
    });
    return response::ok(
        &format!("cutouts found for candid: {}", candid),
        serde_json::json!(resp),
    );
}
