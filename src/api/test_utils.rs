use crate::api::auth::{get_test_auth, AuthProvider};
use crate::conf::AppConfig;
use mongodb::{bson, Database};

/// Auth provider plus a bearer token for the config's admin user, for tests
/// that need to exercise endpoints behind `auth_middleware`.
pub async fn get_admin_auth(db: &Database) -> (AuthProvider, String) {
    let auth = get_test_auth(db).await.unwrap();
    let auth_config = AppConfig::from_test_config().unwrap().api.auth;
    let (token, _) = auth
        .create_token_for_user(&auth_config.admin_username, &auth_config.admin_password)
        .await
        .unwrap();
    (auth, token)
}

pub async fn create_test_catalog(db: &Database) -> String {
    let name = format!("test_catalog_{}", uuid::Uuid::new_v4());
    let collection = db.collection(&name);
    // add an index to the collection
    collection
        .create_index(
            mongodb::IndexModel::builder()
                .keys(mongodb::bson::doc! { "coordinates.radec_geojson": "2dsphere" })
                .options(None)
                .build(),
        )
        .await
        .unwrap();
    // insert a dummy document
    let doc = bson::doc! {
        "test_field": "test_value",
        "test_other_field": 42,
        "coordinates": { "radec_geojson": { "type": "Point", "coordinates": [-170.0, 20.0] } }
    };
    collection.insert_one(doc).await.unwrap();
    name
}

pub async fn delete_test_catalog(db: &Database, name: &str) {
    db.collection::<bson::Document>(name).drop().await.unwrap();
}

pub async fn read_str_response<B>(resp: actix_web::dev::ServiceResponse<B>) -> String
where
    B: actix_web::body::MessageBody,
{
    let body = actix_web::test::read_body(resp).await;
    String::from_utf8_lossy(&body).to_string()
}

// we read a json response in all of our tests, so let's make a helper function for that
pub async fn read_json_response<B>(resp: actix_web::dev::ServiceResponse<B>) -> serde_json::Value
where
    B: actix_web::body::MessageBody,
{
    let body_str = read_str_response(resp).await;
    serde_json::from_str(&body_str).expect("failed to parse JSON")
}
