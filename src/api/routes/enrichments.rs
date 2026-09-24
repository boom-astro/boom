use crate::api::models::response;
use crate::conf::AppConfig;
use crate::utils::enrichment_schema::{enrichment_fields, EnabledEnrichers};
use crate::utils::enums::Survey;
use actix_web::{get, web, HttpResponse};

/// Fields BOOM adds to an alert after ingestion
///
/// The Avro schema describes the packet IPAC ships. Every consumer of an alert
/// reads the enriched document instead, and these are the paths it carries that
/// the packet does not describe.
#[utoipa::path(
    get,
    path = "/enrichments/{survey_name}",
    params(
        ("survey_name" = Survey, Path, description = "Name of the survey (e.g., 'ZTF')"),
    ),
    responses(
        (status = 200, description = "Enrichment fields returned", body = serde_json::Value),
    ),
    tags=["Enrichments"]
)]
#[get("/enrichments/{survey_name}")]
pub async fn get_enrichments(
    path: web::Path<(Survey,)>,
    config: web::Data<AppConfig>,
) -> HttpResponse {
    let survey = path.into_inner().0;
    let crossmatch = config.crossmatch.get(&survey).cloned().unwrap_or_default();
    let fields = enrichment_fields(
        &survey,
        &crossmatch,
        EnabledEnrichers {
            host_galaxy: config.host_galaxy.enabled,
            villar: config.gpu.is_active(),
        },
    );
    response::ok(
        &format!("enrichment fields for survey {}", survey),
        serde_json::json!({ "survey": survey.to_string(), "fields": fields }),
    )
}
