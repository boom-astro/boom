/// Tests for queries endpoints
#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::middleware::from_fn;
    use actix_web::{test, web, App};
    use boom::api::auth::auth_middleware;
    use boom::api::db::get_test_db_api;
    use boom::api::routes;
    use boom::api::test_utils::{
        create_test_catalog, delete_test_catalog, get_admin_auth, read_json_response,
    };
    use mongodb::bson::doc;
    use mongodb::{Collection, Database};

    /// Test GET /catalogs
    #[actix_rt::test]
    async fn test_post_count_query() {
        let database: Database = get_test_db_api().await;
        let (auth, token) = get_admin_auth(&database).await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth))
                .wrap(from_fn(auth_middleware))
                .service(routes::queries::count::post_count_query),
        )
        .await;

        let catalog_name = create_test_catalog(&database).await;
        let req = test::TestRequest::post()
            .uri("/queries/count")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&serde_json::json!({
                "catalog_name": catalog_name,
                "filter": {}
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp: serde_json::Value = read_json_response(resp).await;
        assert_eq!(resp["data"], 1);
        // clean up
        delete_test_catalog(&database, &catalog_name).await;
    }

    /// Test POST /queries/estimated_count
    #[actix_rt::test]
    async fn test_post_estimated_count_query() {
        let database: Database = get_test_db_api().await;
        let (auth, token) = get_admin_auth(&database).await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth))
                .wrap(from_fn(auth_middleware))
                .service(routes::queries::count::post_estimated_count_query),
        )
        .await;
        let catalog_name = create_test_catalog(&database).await;
        let req = test::TestRequest::post()
            .uri("/queries/estimated_count")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&serde_json::json!({
                "catalog_name": catalog_name,
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp: serde_json::Value = read_json_response(resp).await;
        assert!(resp["data"].as_u64().unwrap() >= 1);
        // clean up
        delete_test_catalog(&database, &catalog_name).await;
    }

    // next, let's test the /queries/find endpoint
    #[actix_rt::test]
    async fn test_post_find_query() {
        let database: Database = get_test_db_api().await;
        let (auth, token) = get_admin_auth(&database).await;
        let test_catalog_name = create_test_catalog(&database).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth))
                .wrap(from_fn(auth_middleware))
                .service(routes::queries::find::post_find_query),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/queries/find")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&serde_json::json!({
                "catalog_name": test_catalog_name,
                "filter": {}
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp: serde_json::Value = read_json_response(resp).await;
        assert!(resp["data"].is_array());
        assert_eq!(resp["data"].as_array().unwrap().len(), 1);

        let req = test::TestRequest::post()
            .uri("/queries/find")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&serde_json::json!({
                "catalog_name": test_catalog_name,
                "filter": { "non_existent_field": "no_value" }
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp: serde_json::Value = read_json_response(resp).await;
        assert!(resp["data"].is_array());
        assert_eq!(resp["data"].as_array().unwrap().len(), 0);

        // test it with a filter that does match
        let req = test::TestRequest::post()
            .uri("/queries/find")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&serde_json::json!({
                "catalog_name": test_catalog_name,
                "filter": { "test_field": "test_value" }
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp: serde_json::Value = read_json_response(resp).await;
        assert!(resp["data"].is_array());
        assert_eq!(resp["data"].as_array().unwrap().len(), 1);

        // test it with a projection, to only keep test_field
        let req = test::TestRequest::post()
            .uri("/queries/find")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&serde_json::json!({
                "catalog_name": test_catalog_name,
                "filter": { "test_field": "test_value" },
                "projection": { "test_field": 1, "_id": 0 }
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        // Parse response body JSON
        let resp: serde_json::Value = read_json_response(resp).await;
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
        let database: Database = get_test_db_api().await;
        let (auth, token) = get_admin_auth(&database).await;
        let test_catalog_name = create_test_catalog(&database).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth))
                .wrap(from_fn(auth_middleware))
                .service(routes::queries::cone_search::post_cone_search_query),
        )
        .await;

        let req = test::TestRequest::post()
            .uri("/queries/cone_search")
            .insert_header(("Authorization", format!("Bearer {}", token)))
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
        let resp: serde_json::Value = read_json_response(resp).await;
        assert!(resp["data"].is_object());
        assert!(resp["data"]["test"].is_array());
        assert_eq!(resp["data"]["test"].as_array().unwrap().len(), 1);

        // test > 1 deg away, should return 0 results
        let req = test::TestRequest::post()
            .uri("/queries/cone_search")
            .insert_header(("Authorization", format!("Bearer {}", token)))
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
        let resp: serde_json::Value = read_json_response(resp).await;
        assert!(resp["data"].is_object());
        assert!(resp["data"]["test"].is_array());
        assert_eq!(resp["data"]["test"].as_array().unwrap().len(), 0);
        // clean up
        delete_test_catalog(&database, &test_catalog_name).await;
    }

    // last but not least, we test the /queries/pipeline endpoint
    #[actix_rt::test]
    async fn test_post_pipeline_query() {
        let database: Database = get_test_db_api().await;
        let (auth, token) = get_admin_auth(&database).await;
        let test_catalog_name = create_test_catalog(&database).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth))
                .wrap(from_fn(auth_middleware))
                .service(routes::queries::pipeline::post_pipeline_query),
        )
        .await;
        let req = test::TestRequest::post()
            .uri("/queries/pipeline")
            .insert_header(("Authorization", format!("Bearer {}", token)))
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
        let resp: serde_json::Value = read_json_response(resp).await;
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
            .insert_header(("Authorization", format!("Bearer {}", token)))
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
        let resp: serde_json::Value = read_json_response(resp).await;
        assert!(resp["data"].is_array());
        assert_eq!(resp["data"].as_array().unwrap().len(), 0);
        // clean up
        delete_test_catalog(&database, &test_catalog_name).await;
    }

    // A watchlist catalog is hidden (404) from a user without access, accessible
    // to an admin, and accessible to a non-admin once granted via watchlist_access.
    #[actix_rt::test]
    async fn test_watchlist_access_control() {
        let database: Database = get_test_db_api().await;
        let (auth, admin_token) = get_admin_auth(&database).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth.clone()))
                .wrap(from_fn(auth_middleware))
                .service(routes::users::post_user)
                .service(routes::users::patch_watchlist_access)
                .service(routes::users::delete_user)
                .service(routes::queries::find::post_find_query),
        )
        .await;

        let watchlist_name = format!("watchlist_acl_{}", uuid::Uuid::new_v4().simple());
        let watchlist: Collection<mongodb::bson::Document> = database.collection(&watchlist_name);
        watchlist
            .insert_one(doc! { "test_field": "test_value" })
            .await
            .unwrap();

        // Create a non-admin user and a token for them.
        let username = uuid::Uuid::new_v4().to_string();
        let req = test::TestRequest::post()
            .uri("/users")
            .insert_header(("Authorization", format!("Bearer {}", admin_token)))
            .set_json(&serde_json::json!({
                "username": username,
                "email": format!("{}@example.com", username),
                "password": "password123"
            }))
            .to_request();
        let resp = read_json_response(test::call_service(&app, req).await).await;
        let user_id = resp["data"]["id"].as_str().unwrap().to_string();
        let (user_token, _) = auth
            .create_token_for_user(&username, "password123")
            .await
            .unwrap();

        let find_as = |token: &str| {
            test::TestRequest::post()
                .uri("/queries/find")
                .insert_header(("Authorization", format!("Bearer {}", token)))
                .set_json(&serde_json::json!({
                    "catalog_name": watchlist_name,
                    "filter": {}
                }))
                .to_request()
        };

        // Non-admin without access: hidden behind 404.
        let resp = test::call_service(&app, find_as(&user_token)).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // Admin bypasses the access list.
        let resp = test::call_service(&app, find_as(&admin_token)).await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Grant the watchlist to the non-admin user.
        let req = test::TestRequest::patch()
            .uri(&format!("/users/{}/watchlist_access", user_id))
            .insert_header(("Authorization", format!("Bearer {}", admin_token)))
            .set_json(&serde_json::json!({ "watchlist_access": [watchlist_name] }))
            .to_request();
        assert_eq!(test::call_service(&app, req).await.status(), StatusCode::OK);

        // Now the non-admin can read it.
        let resp = test::call_service(&app, find_as(&user_token)).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = read_json_response(resp).await;
        assert_eq!(resp["data"].as_array().unwrap().len(), 1);

        // clean up
        watchlist.drop().await.unwrap();
        let req = test::TestRequest::delete()
            .uri(&format!("/users/{}", user_id))
            .insert_header(("Authorization", format!("Bearer {}", admin_token)))
            .to_request();
        test::call_service(&app, req).await;
    }
}
