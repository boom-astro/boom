use mongodb::{bson, Database};
use tracing::instrument;

#[instrument(name = "test_utils::create_test_catalog", skip(db), ret)]
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

#[instrument(name = "test_utils::delete_test_catalog", skip(db))]
pub async fn delete_test_catalog(db: &Database, name: &str) {
    db.collection::<bson::Document>(name).drop().await.unwrap();
}

#[instrument(name = "test_utils::read_str_response", skip(resp), ret)]
pub async fn read_str_response<B>(resp: actix_web::dev::ServiceResponse<B>) -> String
where
    B: actix_web::body::MessageBody,
{
    let body = actix_web::test::read_body(resp).await;
    String::from_utf8_lossy(&body).to_string()
}

// we read a json response in all of our tests, so let's make a helper function for that
#[instrument(name = "test_utils::read_json_response", skip(resp), ret)]
pub async fn read_json_response<B>(resp: actix_web::dev::ServiceResponse<B>) -> serde_json::Value
where
    B: actix_web::body::MessageBody,
{
    let body_str = read_str_response(resp).await;
    serde_json::from_str(&body_str).expect("failed to parse JSON")
}
