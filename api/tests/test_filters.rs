/// Tests for filters routes
#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::middleware::from_fn;
    use actix_web::{App, test, web};
    use boom_api::auth::{auth_middleware, get_test_auth};
    use boom_api::conf::{AppConfig, load_dotenv};
    use boom_api::db::get_test_db;
    use boom_api::routes;
    use mongodb::bson::{Document, doc};
    use mongodb::{Collection, Database};

    /// Helper function to create an auth token for the admin user
    async fn create_admin_token(database: &Database) -> String {
        load_dotenv();
        let auth_app_data = get_test_auth(database).await.unwrap();
        let auth_config = AppConfig::from_path("../tests/config.test.yaml").auth;
        let (token, _) = auth_app_data
            .create_token_for_user(&auth_config.admin_username, &auth_config.admin_password)
            .await
            .expect("Failed to create token for admin user");
        token
    }

    /// Helper function to create a simple test filter JSON object
    fn create_test_filter_json() -> serde_json::Value {
        serde_json::json!({
            "pipeline": [{"$match": {"something": 5}}],
            "catalog": "ZTF_alerts",
            "permissions": [1, 2],
        })
    }

    /// Helper function to create a test filter and return its ID and token
    async fn create_test_filter() -> (String, String, Database) {
        load_dotenv();
        let database: Database = get_test_db().await;
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
        assert_eq!(resp.status(), StatusCode::OK);

        // Get the filter out of the response body data so we know the ID
        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");
        assert!(!resp["data"].as_object().unwrap().contains_key("_id"));
        let filter_id = resp["data"]["id"].as_str().unwrap().to_string();

        (filter_id, token, database)
    }

    // let's make a helper function that takes a filter_id, GETs the filter and returns it
    async fn get_test_filter(filter_id: &str, token: &str) -> serde_json::Value {
        load_dotenv();
        let database: Database = get_test_db().await;
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
        assert_eq!(resp.status(), StatusCode::OK);
        let get_body = test::read_body(resp).await;
        let get_body_str = String::from_utf8_lossy(&get_body);
        let get_resp: serde_json::Value =
            serde_json::from_str(&get_body_str).expect("failed to parse JSON");
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
        let database: Database = get_test_db().await;
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
        let post_body = test::read_body(post_resp).await;
        let post_body_str = String::from_utf8_lossy(&post_body);
        let post_resp: serde_json::Value =
            serde_json::from_str(&post_body_str).expect("failed to parse JSON");
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
        let database: Database = get_test_db().await;
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

        assert_eq!(resp.status(), StatusCode::OK);

        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);

        // Parse response body JSON
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");

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
            "pipeline": [{"$match": {"somethingelse": 10}}],
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
        assert!(
            versions
                .iter()
                .any(|v| v["fid"].as_str().unwrap() == active_fid_after)
        );

        // Post another version, but don't set it as active
        let new_version = serde_json::json!({
            "pipeline": [{"$match": {"somethingelseelse": 20}}],
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
        assert!(
            versions
                .iter()
                .any(|v| v["fid"].as_str().unwrap() == active_fid_after2)
        );

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
            "pipeline": [{"$match": {"somethingelse": 10}}],
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
            "permissions": [1, 2, 3]
        });
        let patch_req = test::TestRequest::patch()
            .uri(&format!("/filters/{}", filter_id))
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&patch_data)
            .to_request();
        let patch_resp = test::call_service(&app, patch_req).await;
        assert_eq!(patch_resp.status(), StatusCode::OK);
        let patch_body = test::read_body(patch_resp).await;
        let patch_body_str = String::from_utf8_lossy(&patch_body);
        let patch_resp: serde_json::Value =
            serde_json::from_str(&patch_body_str).expect("failed to parse JSON");
        assert_eq!(
            patch_resp["message"],
            format!("successfully updated filter id: {}", filter_id)
        );

        // GET the filter to verify the patch took effect
        let filter = get_test_filter(&filter_id, &token).await;
        assert_eq!(filter["active"], false);
        assert_eq!(filter["active_fid"].as_str().unwrap(), active_fid_after);
        let permissions = filter["permissions"].as_array().unwrap();
        let perm_values: Vec<i32> = permissions
            .iter()
            .map(|p| p.as_i64().unwrap() as i32)
            .collect();
        assert_eq!(perm_values, vec![1, 2, 3]);

        // Clean up the filter
        cleanup_test_filter(&database, &filter_id).await;
    }
}
