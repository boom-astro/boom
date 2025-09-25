// Tests for catalogs endpoints
#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::{App, test, web};
    use boom_api::db::get_default_db;
    use boom_api::routes;
    use mongodb::{Database, bson};

    // let's make a function that creates a new catalog for testing
    // the catalog should have a randomized name to avoid conflicts
    // and a function to delete the catalog after the test is done
    async fn create_test_catalog(db: &Database) -> String {
        let name = format!("test_catalog_{}", uuid::Uuid::new_v4());
        let collection = db.collection(&name);
        // add an index to the collection
        collection
            .create_index(
                mongodb::IndexModel::builder()
                    .keys(mongodb::bson::doc! { "test_field": 1 })
                    .options(None)
                    .build(),
            )
            .await
            .unwrap();
        // insert a dummy document
        let doc = bson::doc! { "test_field": "test_value" };
        collection.insert_one(doc).await.unwrap();
        name
    }

    async fn delete_test_catalog(db: &Database, name: &str) {
        db.collection::<bson::Document>(name).drop().await.unwrap();
    }

    /// Test GET /catalogs
    #[actix_rt::test]
    async fn test_get_catalogs() {
        let database: Database = get_default_db().await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::catalogs::get_catalogs),
        )
        .await;

        let req = test::TestRequest::get().uri("/catalogs").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::OK);

        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);

        // Parse response body JSON
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");

        assert!(resp["data"].is_array());
    }
    // next we test the get_catalog_indexes endpoint
    #[actix_rt::test]
    async fn test_get_catalog_indexes() {
        let database: Database = get_default_db().await;
        let test_catalog_name = create_test_catalog(&database).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::catalogs::get_catalog_indexes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri(&format!("/catalogs/{}/indexes", test_catalog_name))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Parse response body JSON
        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);

        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");

        assert!(resp["data"].is_array());

        // Clean up test catalog
        delete_test_catalog(&database, &test_catalog_name).await;
    }
    // next we test the get_catalog_sample endpoint
    #[actix_rt::test]
    async fn test_get_catalog_sample() {
        let database: Database = get_default_db().await;
        let test_catalog_name = create_test_catalog(&database).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::catalogs::get_catalog_sample),
        )
        .await;
        let req = test::TestRequest::get()
            .uri(&format!("/catalogs/{}/sample", test_catalog_name))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        // Parse response body JSON
        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");
        assert!(resp["data"].is_array());
        assert_eq!(resp["data"].as_array().unwrap().len(), 1);
        // Clean up test catalog
        delete_test_catalog(&database, &test_catalog_name).await;
    }
}
