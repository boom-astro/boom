use crate::routes;
use utoipa::Modify;
use utoipa::OpenApi;
use utoipa::openapi::Components;
use utoipa::openapi::security::{HttpAuthScheme, HttpBuilder, SecurityScheme};

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if openapi.components.is_none() {
            openapi.components = Some(Components::new());
        }

        openapi.components.as_mut().unwrap().add_security_scheme(
            "api_jwt_token",
            SecurityScheme::Http(
                HttpBuilder::new()
                    .scheme(HttpAuthScheme::Bearer)
                    .bearer_format("JWT")
                    .build(),
            ),
        );
    }
}

#[derive(OpenApi)]
#[openapi(
    info(
        title = "BOOM API",
        version = "0.1.0",
        description = "An HTTP REST interface to BOOM."
    ),
    paths(
        routes::info::get_health,
        routes::info::get_db_info,
        routes::users::post_user,
        routes::users::get_users,
        routes::users::delete_user,
        routes::auth::post_auth,
        routes::surveys::get_object,
        routes::catalogs::get_catalogs,
        routes::catalogs::get_catalog_indexes,
        routes::catalogs::get_catalog_sample,
        routes::filters::post_filter,
        routes::filters::add_filter_version,
        routes::queries::post_count_query,
        routes::queries::post_estimated_count_query,
        routes::queries::post_find_query,
        routes::queries::post_cone_search_query,
    ),
    security(
        ("api_jwt_token" = [])
    ),
    modifiers(&SecurityAddon)
)]
pub struct ApiDoc;
