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
                    .keys(mongodb::bson::doc! { "coordinates.radec_geojson": "2dsphere" })
                    .options(None)
                    .build(),
            )
            .await
            .unwrap();
        // insert a dummy document
        let doc = bson::doc! { "test_field": "test_value", "test_other_field": 42, "coordinates": { "radec_geojson": { "type": "Point", "coordinates": [-170.0, 20.0] } } };
        collection.insert_one(doc).await.unwrap();
        name
    }

    async fn delete_test_catalog(db: &Database, name: &str) {
        db.collection::<bson::Document>(name).drop().await.unwrap();
    }

    /// Test GET /catalogs
    #[actix_rt::test]
    async fn test_post_count_query() {
        let database: Database = get_default_db().await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::queries::count::post_count_query),
        )
        .await;

        let catalog_name = create_test_catalog(&database).await;
        let req = test::TestRequest::post()
            .uri("/queries/count")
            .set_json(&serde_json::json!({
                "catalog_name": catalog_name,
                "filter": {}
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);

        // Parse response body JSON
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");
        assert_eq!(resp["data"], 1);
        // clean up
        delete_test_catalog(&database, &catalog_name).await;
    }

    /// Test POST /queries/estimated_count
    #[actix_rt::test]
    async fn test_post_estimated_count_query() {
        let database: Database = get_default_db().await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::queries::count::post_estimated_count_query),
        )
        .await;
        let catalog_name = create_test_catalog(&database).await;
        let req = test::TestRequest::post()
            .uri("/queries/estimated_count")
            .set_json(&serde_json::json!({
                "catalog_name": catalog_name,
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");
        assert!(resp["data"].as_u64().unwrap() >= 1);
        // clean up
        delete_test_catalog(&database, &catalog_name).await;
    }

    // next, let's test the /queries/find endpoint
    #[actix_rt::test]
    async fn test_post_find_query() {
        let database: Database = get_default_db().await;
        let test_catalog_name = create_test_catalog(&database).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::queries::find::post_find_query),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/queries/find")
            .set_json(&serde_json::json!({
                "catalog_name": test_catalog_name,
                "filter": {}
            }))
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

        let req = test::TestRequest::post()
            .uri("/queries/find")
            .set_json(&serde_json::json!({
                "catalog_name": test_catalog_name,
                "filter": { "non_existent_field": "no_value" }
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        // Parse response body JSON
        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");
        assert!(resp["data"].is_array());
        assert_eq!(resp["data"].as_array().unwrap().len(), 0);

        // test it with a filter that does match
        let req = test::TestRequest::post()
            .uri("/queries/find")
            .set_json(&serde_json::json!({
                "catalog_name": test_catalog_name,
                "filter": { "test_field": "test_value" }
            }))
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

        // test it with a projection, to only keep test_field
        let req = test::TestRequest::post()
            .uri("/queries/find")
            .set_json(&serde_json::json!({
                "catalog_name": test_catalog_name,
                "filter": { "test_field": "test_value" },
                "projection": { "test_field": 1, "_id": 0 }
            }))
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
        let first_doc = &resp["data"].as_array().unwrap()[0];
        assert_eq!(first_doc["test_field"], "test_value");
        assert!(first_doc.get("_id").is_none());
        assert!(first_doc.get("test_other_field").is_none());
        assert!(first_doc.get("coordinates").is_none());
        // clean up
        delete_test_catalog(&database, &test_catalog_name).await;
    }

    // next we test the /queries/cone-search endpoint
    #[actix_rt::test]
    async fn test_post_cone_search_query() {
        let database: Database = get_default_db().await;
        let test_catalog_name = create_test_catalog(&database).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::queries::cone_search::post_cone_search_query),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/queries/cone_search")
            .set_json(&serde_json::json!({
                "catalog_name": test_catalog_name,
                "object_coordinates": { "test": [10.0, 20.0] },
                "radius": 1.0,
                "unit": "Degrees",
                "filter": {}
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        // Parse response body JSON
        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);
        println!("Response body: {}", body_str);
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");
        assert!(resp["data"].is_object());
        assert!(resp["data"]["test"].is_array());
        assert_eq!(resp["data"]["test"].as_array().unwrap().len(), 1);

        // test > 1 deg away, should return 0 results
        let req = test::TestRequest::post()
            .uri("/queries/cone_search")
            .set_json(&serde_json::json!({
                "catalog_name": test_catalog_name,
                "object_coordinates": { "test": [0.0, 0.0] },
                "radius": 1.0,
                "unit": "Degrees",
                "filter": {}
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        // Parse response body JSON
        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");
        assert!(resp["data"].is_object());
        assert!(resp["data"]["test"].is_array());
        assert_eq!(resp["data"]["test"].as_array().unwrap().len(), 0);
        // clean up
        delete_test_catalog(&database, &test_catalog_name).await;
    }

    // last but not least, we test the /queries/pipeline endpoint
    #[actix_rt::test]
    async fn test_post_pipeline_query() {
        let database: Database = get_default_db().await;
        let test_catalog_name = create_test_catalog(&database).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::queries::pipeline::post_pipeline_query),
        )
        .await;
        let req = test::TestRequest::post()
            .uri("/queries/pipeline")
            .set_json(&serde_json::json!({
                "catalog_name": test_catalog_name,
                "pipeline": [
                    { "$match": { "test_field": "test_value" } },
                    { "$project": { "test_field": 1, "_id": 0 } }
                ]
            }))
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
        let first_doc = &resp["data"].as_array().unwrap()[0];
        assert_eq!(first_doc["test_field"], "test_value");
        assert!(first_doc.get("_id").is_none());
        assert!(first_doc.get("test_other_field").is_none());
        assert!(first_doc.get("coordinates").is_none());

        // test with a pipeline that returns no results
        let req = test::TestRequest::post()
            .uri("/queries/pipeline")
            .set_json(&serde_json::json!({
                "catalog_name": test_catalog_name,
                "pipeline": [
                    { "$match": { "test_field": "non_existent_value" } },
                    { "$project": { "test_field": 1, "_id": 0 } }
                ]
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        // Parse response body JSON
        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");
        assert!(resp["data"].is_array());
        assert_eq!(resp["data"].as_array().unwrap().len(), 0);
        // clean up
        delete_test_catalog(&database, &test_catalog_name).await;
    }
}
