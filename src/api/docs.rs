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
        **Note**: For the public Babamul API (email-based signup for Kafka streams), see the separate documentation at `/babamul/docs`."
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

#[derive(OpenApi)]
#[openapi(
    info(
        title = "Babamul API",
        version = "0.1.0",
        description = "Public API for Babamul - Email-based signup for accessing transient alert streams via Kafka.\n\n\
        ## Getting Started\n\n\
        1. **Sign up** with your email at `/babamul/signup`\n\
        2. **Activate** your account with the code sent to your email at `/babamul/activate`\n\
        3. **Authenticate** with your email and password at `/babamul/auth` to get a JWT token\n\
        4. **Connect to Kafka** using your email as username and password for SCRAM-SHA-512 authentication\n\n\
        ## Kafka Access\n\n\
        After activation, you can access Kafka topics matching `babamul.*` pattern:\n\n\
        - **Username**: Your email address\n\
        - **Password**: The password received during activation\n\
        - **Authentication**: SCRAM-SHA-512\n\
        - **Topics**: `babamul.*` (READ, DESCRIBE)\n\
        - **Consumer Groups**: `babamul-*` (READ)\n\n\
        ## Important Notes\n\n\
        - Your password is shown **only once** during activation - save it securely!\n\
        - The same password works for both Kafka authentication and API authentication\n\
        - JWT tokens expire after 24 hours - use `/babamul/auth` to get a new one"
    ),
    paths(
        routes::babamul::post_babamul_signup,
        routes::babamul::post_babamul_activate,
        routes::babamul::post_babamul_auth,
    ),
    security(
        ("babamul_jwt_token" = [])
    ),
    modifiers(&BabamulSecurityAddon)
)]
pub struct BabamulApiDoc;
