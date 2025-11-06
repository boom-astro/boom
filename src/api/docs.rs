use crate::api::routes;
use utoipa::openapi::security::{Flow, OAuth2, Password, Scopes, SecurityScheme};
use utoipa::openapi::Components;
use utoipa::Modify;
use utoipa::OpenApi;

struct SecurityAddon;

impl Modify for SecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if openapi.components.is_none() {
            openapi.components = Some(Components::new());
        }

        openapi.components.as_mut().unwrap().add_security_scheme(
            "api_jwt_token",
            SecurityScheme::OAuth2(OAuth2::new([Flow::Password(Password::new(
                "/auth",
                Scopes::default(),
            ))])),
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
        routes::kafka::get_kafka_acls,
        routes::kafka::post_kafka_acl,
        routes::users::post_user,
        routes::users::get_users,
        routes::users::delete_user,
        routes::auth::post_auth,
        routes::babamul::post_babamul_signup,
        routes::babamul::post_babamul_activate,
        routes::babamul::post_babamul_auth,
        routes::surveys::get_object,
        routes::catalogs::get_catalogs,
        routes::catalogs::get_catalog_indexes,
        routes::catalogs::get_catalog_sample,
        routes::filters::post_filter,
        routes::filters::patch_filter,
        routes::filters::get_filters,
        routes::filters::get_filter,
        routes::filters::post_filter_version,
        routes::queries::count::post_count_query,
        routes::queries::count::post_estimated_count_query,
        routes::queries::find::post_find_query,
        routes::queries::cone_search::post_cone_search_query,
        routes::queries::pipeline::post_pipeline_query
    ),
    security(
        ("api_jwt_token" = [])
    ),
    modifiers(&SecurityAddon)
)]
pub struct ApiDoc;
