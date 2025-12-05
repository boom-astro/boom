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

struct BabamulSecurityAddon;

impl Modify for BabamulSecurityAddon {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        if openapi.components.is_none() {
            openapi.components = Some(Components::new());
        }

        openapi.components.as_mut().unwrap().add_security_scheme(
            "babamul_jwt_token",
            SecurityScheme::OAuth2(OAuth2::new([Flow::Password(Password::new(
                "/babamul/auth",
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
        description = "An HTTP REST interface to BOOM.\n\n\
        **Note**: For the public Babamul API, see the separate documentation at `/babamul/docs`."
    ),
    paths(
        routes::info::get_health,
        routes::info::get_db_info,
        routes::kafka::get_kafka_acls,
        routes::kafka::delete_kafka_acls_for_user,
        routes::users::post_user,
        routes::users::get_users,
        routes::users::delete_user,
        routes::auth::post_auth,
        routes::catalogs::get_catalogs,
        routes::catalogs::get_catalog_indexes,
        routes::catalogs::get_catalog_sample,
        routes::filters::post_filter,
        routes::filters::patch_filter,
        routes::filters::get_filters,
        routes::filters::get_filter,
        routes::filters::post_filter_version,
        routes::filters::post_filter_test,
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

#[derive(OpenApi)]
#[openapi(
    info(
        title = "BOOM's Babamul API",
        version = "0.1.0",
        description = "The Public REST API for Babamul."
    ),
    paths(
        routes::babamul::users::post_babamul_signup,
        routes::babamul::users::post_babamul_activate,
        routes::babamul::users::post_babamul_auth,
        routes::babamul::users::get_babamul_profile,
        routes::babamul::schemas::get_babamul_schema,
        routes::babamul::surveys::get_object,
    ),
    security(
        ("babamul_jwt_token" = [])
    ),
    modifiers(&BabamulSecurityAddon)
)]
pub struct BabamulApiDoc;
