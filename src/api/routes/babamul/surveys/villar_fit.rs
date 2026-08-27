use crate::api::cutouts::{AlertCandidOnly, WhichCutouts};
use crate::api::models::response;
use crate::api::routes::babamul::BabamulUser;
use crate::utils::enums::Survey;
use actix_web::{get, web, HttpResponse};
use mongodb::{bson::doc, Collection, Database};
use std::collections::HashMap;
use utoipa::ToSchema;

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct VillarFitQuery {
    pub candid: Option<i64>,
    #[serde(rename = "objectId")]
    pub object_id: Option<String>,
    pub which: Option<WhichCutouts>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, ToSchema)]
pub struct VillarFitResponse {
    pub candid: i64,
    /// Reduced chi-squared of the fit. `null` if no fit could be produced for this alert.
    pub reduced_chi2: Option<f64>,
    /// Villar model parameters, keyed as `{param}_{filter}` (e.g. `amplitude_g`).
    /// Values are `null` where a fit could not be produced.
    pub params: HashMap<String, f64>,
}

/// Fetch the GPU-computed Villar light curve fit for a ZTF alert
#[utoipa::path(
    get,
    path = "/babamul/surveys/{survey}/villar-fit",
    params(
        ("survey" = Survey, Path, description = "Name of the survey (currently only ztf is supported)"),
        ("candid" = Option<i64>, Query, description = "Candid of the alert to retrieve the Villar fit for"),
        ("objectId" = Option<String>, Query, description = "Object ID to retrieve the Villar fit for"),
        ("which" = Option<WhichCutouts>, Query, description = "Which alert to use if multiple match the objectId (first, last, brightest, faintest); defaults to last"),
    ),
    responses(
        (status = 200, description = "Villar fit found", body = VillarFitResponse),
        (status = 400, description = "Invalid survey or missing candid/objectId"),
        (status = 404, description = "No alert or Villar fit found"),
        (status = 500, description = "Internal server error")
    ),
    tags=["Surveys"]
)]
#[get("/surveys/{survey}/villar-fit")]
pub async fn get_villar_fit(
    path: web::Path<Survey>,
    query: web::Query<VillarFitQuery>,
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

    let survey = path.into_inner();
    if survey != Survey::Ztf {
        return response::bad_request(&format!(
            "Unsupported survey: {}. Villar fits are currently only computed for ztf",
            survey
        ));
    }

    let candid = if let Some(candid) = query.candid {
        candid
    } else if let Some(object_id) = &query.object_id {
        let candid_collection: Collection<AlertCandidOnly> =
            db.collection(&format!("{}_alerts", survey));
        let which = query.which.clone().unwrap_or(WhichCutouts::Last);
        let find_options = match which {
            WhichCutouts::First => mongodb::options::FindOneOptions::builder()
                .sort(doc! { "candidate.jd": 1 })
                .build(),
            WhichCutouts::Last => mongodb::options::FindOneOptions::builder()
                .sort(doc! { "candidate.jd": -1 })
                .build(),
            WhichCutouts::Brightest => mongodb::options::FindOneOptions::builder()
                .sort(doc! { "candidate.magpsf": 1 })
                .build(),
            WhichCutouts::Faintest => mongodb::options::FindOneOptions::builder()
                .sort(doc! { "candidate.magpsf": -1 })
                .build(),
        };

        match candid_collection
            .find_one(doc! {
                "objectId": object_id,
                "candidate.programid": 1, // Babamul only returns public ZTF alerts
            })
            .projection(doc! { "_id": 1 })
            .with_options(find_options)
            .await
        {
            Ok(Some(alert)) => alert.candid,
            Ok(None) => {
                return response::not_found(&format!("no alerts found for objectId {}", object_id));
            }
            Err(error) => {
                return response::internal_error(&format!("error getting documents: {}", error));
            }
        }
    } else {
        return response::bad_request("candid or objectId query parameter must be provided");
    };

    let alert_collection: Collection<mongodb::bson::Document> =
        db.collection(&format!("{}_alerts", survey));
    let alert_doc = match alert_collection
        .find_one(doc! {
            "_id": candid,
            "candidate.programid": 1, // Babamul only returns public ZTF alerts
        })
        .projection(doc! { "_id": 1, "villar_fit": 1 })
        .await
    {
        Ok(Some(doc)) => doc,
        Ok(None) => {
            return response::not_found(&format!("no alert found for candid {}", candid));
        }
        Err(error) => {
            return response::internal_error(&format!("error getting documents: {}", error));
        }
    };

    let villar_fit_doc = match alert_doc.get_document("villar_fit") {
        Ok(doc) => doc,
        Err(_) => {
            return response::not_found(&format!(
                "no Villar fit found for candid {} (alert may not have been processed by the GPU enrichment worker yet)",
                candid
            ));
        }
    };

    let mut reduced_chi2 = None;
    let mut params = HashMap::new();
    for (key, value) in villar_fit_doc.iter() {
        let value = match value.as_f64() {
            Some(v) => v,
            None => continue,
        };
        if key == "reduced_chi2" {
            reduced_chi2 = Some(value);
        } else {
            params.insert(key.clone(), value);
        }
    }

    let response = VillarFitResponse {
        candid,
        reduced_chi2,
        params,
    };
    response::ok_ser(&format!("found Villar fit for candid {}", candid), response)
}
