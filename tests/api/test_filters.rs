/// Tests for filters routes
#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::middleware::from_fn;
    use actix_web::{test, web, App};
    use boom::api::auth::{auth_middleware, get_test_auth};
    use boom::api::db::get_test_db_api;
    use boom::api::routes;
    use boom::api::test_utils::{read_json_response, read_str_response};
    use boom::conf::{load_dotenv, AppConfig};
    use mongodb::bson::{doc, Document};
    use mongodb::{Collection, Database};

    /// Helper function to create an auth token for the admin user
    async fn create_admin_token(database: &Database) -> String {
        load_dotenv();
        let auth_app_data = get_test_auth(database).await.unwrap();
        let auth_config = AppConfig::from_test_config().unwrap().api.auth;
        let (token, _) = auth_app_data
            .create_token_for_user(&auth_config.admin_username, &auth_config.admin_password)
            .await
            .expect("Failed to create token for admin user");
        token
    }

    /// Helper function to create a simple test filter JSON object
    fn create_test_filter_json() -> serde_json::Value {
        serde_json::json!({
            "name": "test_filter",
            "description": "Test filter",
            "pipeline": [{"$match": {"something": 5}}, {"$project": {"objectId": 1}}],
            "survey": "ZTF",
            "permissions": {"ZTF": [1, 2]}
        })
    }

    /// Helper function to create a test filter and return its ID and token
    async fn create_test_filter() -> (String, String, Database) {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let token = create_admin_token(&database).await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .wrap(from_fn(auth_middleware))
                .service(routes::filters::post_filter)
                .service(routes::filters::get_filter),
        )
        .await;

        // Create a new filter using the helper function
        let new_filter = create_test_filter_json();

        let req = test::TestRequest::post()
            .uri("/filters")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&new_filter)
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Failed to create test filter: {:?}",
            read_str_response(resp).await
        );
        let resp = read_json_response(resp).await;
        assert!(!resp["data"].as_object().unwrap().contains_key("_id"));
        let filter_id = resp["data"]["id"].as_str().unwrap().to_string();

        (filter_id, token, database)
    }

    // let's make a helper function that takes a filter_id, GETs the filter and returns it
    async fn get_test_filter(filter_id: &str, token: &str) -> serde_json::Value {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .wrap(from_fn(auth_middleware))
                .service(routes::filters::get_filter),
        )
        .await;

        // Now get this filter by ID
        let req = test::TestRequest::get()
            .uri(&format!("/filters/{}", filter_id))
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Failed to get test filter: {:?}",
            read_str_response(resp).await
        );
        let get_resp = read_json_response(resp).await;
        // Assert we have no _id field in the response
        assert!(!get_resp["data"].as_object().unwrap().contains_key("_id"));
        assert_eq!(get_resp["data"]["id"], filter_id);
        get_resp["data"].clone()
    }

    // write a wrapper method to post a new filter version, which returns the version ID
    async fn post_new_filter_version(
        filter_id: &str,
        token: &str,
        new_version: &serde_json::Value,
    ) -> String {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .wrap(from_fn(auth_middleware))
                .service(routes::filters::post_filter_version),
        )
        .await;
        let post_req = test::TestRequest::post()
            .uri(&format!("/filters/{}/versions", filter_id))
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(new_version)
            .to_request();
        let post_resp = test::call_service(&app, post_req).await;
        assert_eq!(post_resp.status(), StatusCode::OK);
        let post_resp = read_json_response(post_resp).await;
        let version_id = post_resp["data"]["fid"].as_str().unwrap().to_string();
        assert!(!version_id.is_empty());
        version_id
    }

    /// Helper function to clean up a test filter
    async fn cleanup_test_filter(database: &Database, filter_id: &str) {
        let filters_collection: Collection<Document> = database.collection("filters");
        filters_collection
            .delete_one(doc! { "_id": filter_id })
            .await
            .expect("Failed to delete filter");
    }

    /// Test POST /filters
    #[actix_rt::test]
    async fn test_post_filter() {
        let (filter_id, _token, database) = create_test_filter().await;

        // The create_test_filter function already validates the POST request,
        // so we just need to clean up
        cleanup_test_filter(&database, &filter_id).await;
    }

    /// Test GET /filters/{id}
    #[actix_rt::test]
    async fn test_get_filter() {
        let (filter_id, token, database) = create_test_filter().await;

        get_test_filter(&filter_id, &token).await;

        // Clean up the filter
        cleanup_test_filter(&database, &filter_id).await;
    }

    /// Test GET /filters
    #[actix_rt::test]
    async fn test_get_filters() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let token = create_admin_token(&database).await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .wrap(from_fn(auth_middleware))
                .service(routes::filters::get_filters),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/filters")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Failed to get filters: {:?}",
            read_str_response(resp).await
        );
        let resp = read_json_response(resp).await;

        assert!(resp["data"].is_array());
    }

    /// Test POST /filters/{id}/versions
    #[actix_rt::test]
    async fn test_post_filter_version() {
        let (filter_id, token, database) = create_test_filter().await;

        // first GET the filter and get its current active_fid
        let filter = get_test_filter(&filter_id, &token).await;
        let active_fid_before = filter["active_fid"].as_str().unwrap().to_string();

        // Now post a new version to this filter
        let new_version = serde_json::json!({
            "changelog": "Added a new test version",
            "pipeline": [{"$match": {"somethingelse": 10}}, {"$project": {"objectId": 1}}],
            "set_as_active": true
        });
        let active_fid_after = post_new_filter_version(&filter_id, &token, &new_version).await;
        assert_ne!(active_fid_before, active_fid_after);

        // GET the filter to verify the patch took effect
        let filter = get_test_filter(&filter_id, &token).await;
        assert_eq!(filter["id"], filter_id);
        let active_fid = filter["active_fid"].as_str().unwrap();
        assert_eq!(active_fid, active_fid_after);
        let versions = filter["fv"].as_array().unwrap();
        assert!(versions
            .iter()
            .any(|v| v["fid"].as_str().unwrap() == active_fid_after));

        // Post another version, but don't set it as active
        let new_version = serde_json::json!({
            "changelog": "Added another test version",
            "pipeline": [{"$match": {"somethingelseelse": 20}}, {"$project": {"objectId": 1}}],
            "set_as_active": false
        });
        let active_fid_after2 = post_new_filter_version(&filter_id, &token, &new_version).await;
        assert_ne!(active_fid_before, active_fid_after2);
        assert_ne!(active_fid_after, active_fid_after2);

        // GET the filter to verify the active_fid did NOT change
        let filter = get_test_filter(&filter_id, &token).await;
        assert_eq!(filter["id"], filter_id);
        let active_fid = filter["active_fid"].as_str().unwrap();
        assert_eq!(active_fid, active_fid_after); // should still be the same
        let versions = filter["fv"].as_array().unwrap();
        assert!(versions
            .iter()
            .any(|v| v["fid"].as_str().unwrap() == active_fid_after2));

        // Clean up the filter
        cleanup_test_filter(&database, &filter_id).await;
    }

    /// Test PATCH /filters/{id}
    #[actix_rt::test]
    async fn test_patch_filter() {
        let (filter_id, token, database) = create_test_filter().await;
        // Create app for PATCH testing
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .wrap(from_fn(auth_middleware))
                .service(routes::filters::patch_filter)
                .service(routes::filters::get_filter),
        )
        .await;

        // POST a new version to ensure we have something to patch to
        let new_version = serde_json::json!({
            "changelog": "Added a new test version",
            "pipeline": [{"$match": {"somethingelse": 10}}, {"$project": {"objectId": 1}}],
            "set_as_active": false
        });
        let active_fid_after = post_new_filter_version(&filter_id, &token, &new_version).await;

        // first GET the filter and get its current active status
        let filter = get_test_filter(&filter_id, &token).await;
        assert_eq!(filter["active"], true);
        assert_ne!(filter["active_fid"].as_str().unwrap(), active_fid_after);

        // Now patch this filter
        let patch_data = serde_json::json!({
            "active": false,
            "active_fid": active_fid_after,
            "permissions": {"ZTF": [1, 2, 3]},
            "name": "updated_test_filter",
            "description": "Updated test filter description"
        });
        let patch_req = test::TestRequest::patch()
            .uri(&format!("/filters/{}", filter_id))
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&patch_data)
            .to_request();
        let patch_resp = test::call_service(&app, patch_req).await;
        assert_eq!(
            patch_resp.status(),
            StatusCode::OK,
            "Failed to patch filter: {:?}",
            read_str_response(patch_resp).await
        );
        let patch_resp = read_json_response(patch_resp).await;

        assert_eq!(
            patch_resp["message"],
            format!("successfully updated filter id: {}", filter_id)
        );

        // GET the filter to verify the patch took effect
        let filter = get_test_filter(&filter_id, &token).await;
        assert_eq!(filter["active"], false);
        assert_eq!(filter["active_fid"].as_str().unwrap(), active_fid_after);
        let permissions = filter["permissions"].as_object().unwrap()["ZTF"]
            .as_array()
            .unwrap();
        let permissions: Vec<i32> = permissions
            .iter()
            .map(|p| p.as_i64().unwrap() as i32)
            .collect();
        assert_eq!(permissions, vec![1, 2, 3]);
        assert_eq!(filter["name"].as_str().unwrap(), "updated_test_filter");
        assert_eq!(
            filter["description"].as_str().unwrap(),
            "Updated test filter description"
        );

        // Clean up the filter
        cleanup_test_filter(&database, &filter_id).await;
    }

    // test the /filters/test endpoint
    #[actix_rt::test]
    async fn test_filter_pipeline_test_endpoint() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let token = create_admin_token(&database).await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .wrap(from_fn(auth_middleware))
                .service(routes::filters::post_filter_test),
        )
        .await;

        // Create a test pipeline
        let test_pipeline = serde_json::json!([
            { "$match": { "candidate.magpsf": { "$lt": 18 } } },
            { "$project": { "objectId": 1, "annotation": { "mag_now": "$candidate.magpsf" } } }
        ]);
        let payload = serde_json::json!({
            "pipeline": test_pipeline,
            "permissions": {"ZTF": [1, 2]},
            "survey": "ZTF",
            "start_jd": 2459000.5,
            "end_jd": 2459001.5
        });

        let req = test::TestRequest::post()
            .uri("/filters/test")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&payload)
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Failed to test filter pipeline: {:?}",
            read_str_response(resp).await
        );
        let resp = read_json_response(resp).await;

        let data = resp["data"].as_object().unwrap();
        let pipeline = data["pipeline"].as_array().unwrap();
        // should have at least the 2 stages we sent +
        // stages added by the system (e.g., permission filtering)
        assert!(pipeline.len() > 2);
        let _ = data["results"].as_array().unwrap();
    }

    // test the /filters/test/count endpoint
    #[actix_rt::test]
    async fn test_filter_pipeline_test_count_endpoint() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let token = create_admin_token(&database).await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .wrap(from_fn(auth_middleware))
                .service(routes::filters::post_filter_test_count),
        )
        .await;

        // Create a test pipeline
        let test_pipeline = serde_json::json!([
            { "$match": { "candidate.magpsf": { "$lt": 18 } } },
            { "$project": { "objectId": 1, "annotation": { "mag_now": "$candidate.magpsf" } } }
        ]);
        let payload = serde_json::json!({
            "pipeline": test_pipeline,
            "permissions": {"ZTF": [1, 2]},
            "survey": "ZTF",
            "start_jd": 2459000.5,
            "end_jd": 2459001.5
        });
        let req = test::TestRequest::post()
            .uri("/filters/test/count")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&payload)
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Failed to test filter pipeline count: {:?}",
            read_str_response(resp).await
        );
        let resp = read_json_response(resp).await;
        let data = resp["data"].as_object().unwrap();
        let pipeline = data["pipeline"].as_array().unwrap();
        // should have at least the 2 stages we sent +
        // stages added by the system (e.g., permission filtering)
        assert!(pipeline.len() > 2);
        let _ = data["count"].as_i64().unwrap();
    }

    // test the /filters/schema/{survey} endpoint
    #[actix_rt::test]
    async fn test_filter_schema_endpoint() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let token = create_admin_token(&database).await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .wrap(from_fn(auth_middleware))
                .service(routes::filters::get_filter_schema),
        )
        .await;

        // ZTF schema test
        let req = test::TestRequest::get()
            .uri("/filters/schema/ZTF")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Failed to get filter schema: {:?}",
            read_str_response(resp).await
        );
        let resp = read_json_response(resp).await;

        // let's just check we have a data field with type and fields keys
        let data = resp["data"].as_object().unwrap();
        assert!(data.contains_key("type"));
        assert!(data.contains_key("name"));
        assert!(data.contains_key("fields"));
        assert!(data["type"] == "record");
        assert!(data["name"] == "ZtfAlertToFilter");
        assert!(data["fields"].is_array());

        // LSST schema test
        let req = test::TestRequest::get()
            .uri("/filters/schema/LSST")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Failed to get filter schema: {:?}",
            read_str_response(resp).await
        );
        let resp = read_json_response(resp).await;
        let data = resp["data"].as_object().unwrap();
        assert!(data.contains_key("type"));
        assert!(data.contains_key("name"));
        assert!(data.contains_key("fields"));
        assert!(data["type"] == "record");
        assert!(data["name"] == "LsstAlertToFilter");
        assert!(data["fields"].is_array());

        // Invalid survey test (should return NOT_FOUND)
        let req = test::TestRequest::get()
            .uri("/filters/schema/INVALID_SURVEY")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "Expected NOT_FOUND for invalid survey, got: {:?}",
            read_str_response(resp).await
        );
    }
}
