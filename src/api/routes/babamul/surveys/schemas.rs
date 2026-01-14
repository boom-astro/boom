use crate::api::models::response;
use crate::utils::enums::Survey;
use actix_web::{web, HttpResponse};
use mongodb::bson::doc;
pub struct BabamulAvroSchemas {
    lsst_schema: serde_json::Value,
    ztf_schema: serde_json::Value,
}

impl BabamulAvroSchemas {
    pub fn new() -> Self {
        use apache_avro::AvroSchema;
        let lsst_schema = crate::enrichment::babamul::EnrichedLsstAlert::get_schema();
        let ztf_schema = crate::enrichment::babamul::EnrichedZtfAlert::get_schema();
        let lsst_schema = serde_json::to_value(&lsst_schema).unwrap();
        let ztf_schema = serde_json::to_value(&ztf_schema).unwrap();
        Self {
            lsst_schema,
            ztf_schema,
        }
    }
}

/// Get the Avro schema used by Babamul for the specified survey
#[utoipa::path(
    get,
    path = "/babamul/surveys/{survey}/schemas",
    responses(
        (status = 200, description = "Schema retrieved successfully", body = String),
        (status = 400, description = "Invalid survey"),
        (status = 500, description = "Internal server error")
    ),
    params(
        ("survey" = Survey, Path, description = "Survey name (e.g., ztf, lsst)")
    ),
    tags=["Schemas"]
)]
#[actix_web::get("/babamul/surveys/{survey}/schemas")]
pub async fn get_babamul_schema(
    survey: web::Path<Survey>,
    babamul_avro_schemas: web::Data<BabamulAvroSchemas>,
) -> HttpResponse {
    match survey.into_inner() {
        Survey::Lsst => HttpResponse::Ok().json(babamul_avro_schemas.lsst_schema.clone()),
        Survey::Ztf => HttpResponse::Ok().json(babamul_avro_schemas.ztf_schema.clone()),
        _ => response::bad_request("Invalid survey specified"),
    }
}
