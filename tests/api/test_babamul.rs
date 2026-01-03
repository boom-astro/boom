#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::middleware::from_fn;
    use actix_web::{test, web, App};
    use boom::alert::{AlertWorker, ProcessAlertStatus};
    use boom::api::auth::{babamul_auth_middleware, get_test_auth};
    use boom::api::db::get_test_db_api;
    use boom::api::email::EmailService;
    use boom::api::kafka::delete_acls_for_user;
    use boom::api::routes;
    use boom::api::test_utils::{read_json_response, read_str_response};
    use boom::conf::{load_dotenv, AppConfig};
    use boom::enrichment::{EnrichmentWorker, LsstEnrichmentWorker};
    use boom::utils::enums::Survey;
    use boom::utils::testing::{
        drop_alert_from_collections, lsst_alert_worker, AlertRandomizer, TEST_CONFIG_FILE,
    };
    use mongodb::bson::doc;
    use mongodb::Database;

    /// Helper struct to manage test user lifecycle
    struct TestUser {
        pub user: boom::api::routes::babamul::BabamulUser,
        pub token: String,
        database: Database,
    }

    impl TestUser {
        /// Create a new test user with a unique email and JWT token
        async fn create(
            database: &Database,
            auth_app_data: &boom::api::auth::AuthProvider,
            email_suffix: &str,
        ) -> Self {
            let test_email = format!("test+{}@babamul.example.com", email_suffix);
            let babamul_users_collection: mongodb::Collection<
                boom::api::routes::babamul::BabamulUser,
            > = database.collection("babamul_users");

            // Clean up any existing user with this email
            babamul_users_collection
                .delete_one(doc! { "email": &test_email })
                .await
                .ok();

            let test_user = boom::api::routes::babamul::BabamulUser {
                id: uuid::Uuid::new_v4().to_string(),
                username: "testuser".to_string(),
                email: test_email.clone(),
                password_hash: "hash".to_string(),
                activation_code: None,
                is_activated: true,
                created_at: 0,
            };

            babamul_users_collection
                .insert_one(&test_user)
                .await
                .expect("Failed to insert test user");

            // Create JWT token for test user
            let (token, _) =
                boom::api::routes::babamul::create_babamul_jwt(auth_app_data, &test_user.id)
                    .await
                    .expect("Failed to create JWT");

            Self {
                user: test_user,
                token,
                database: database.clone(),
            }
        }
    }

    impl Drop for TestUser {
        fn drop(&mut self) {
            // Clean up the test user when the struct is dropped
            let database = self.database.clone();
            let user_id = self.user.id.clone();

            tokio::spawn(async move {
                let babamul_users_collection: mongodb::Collection<
                    boom::api::routes::babamul::BabamulUser,
                > = database.collection("babamul_users");
                babamul_users_collection
                    .delete_one(doc! { "_id": &user_id })
                    .await
                    .ok();
            });
        }
    }

    /// Helper function to generate a unique timestamp-based suffix
    fn unique_suffix() -> String {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis()
            .to_string()
    }

    /// Test POST /babamul/signup
    #[actix_rt::test]
    async fn test_babamul_signup() {
        load_dotenv();
        let config = AppConfig::from_test_config().unwrap();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(config.clone()))
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .app_data(web::Data::new(EmailService::new()))
                .service(routes::babamul::post_babamul_signup),
        )
        .await;

        // Generate a unique test email
        let test_email = format!("test+{}@babamul.example.com", unique_suffix());

        // Create a signup request
        let req = test::TestRequest::post()
            .uri("/babamul/signup")
            .set_json(serde_json::json!({
                "email": test_email
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Signup should succeed with valid email (error: {})",
            read_str_response(resp).await
        );

        let body = read_json_response(resp).await;
        assert!(
            body["message"].is_string(),
            "Response should contain message"
        );
        assert_eq!(
            body["activation_required"].as_bool().unwrap(),
            true,
            "Activation should be required"
        );

        // No password should be returned yet (only after activation)
        assert!(
            body["password"].is_null() || !body.get("password").is_some(),
            "Password should not be returned before activation"
        );

        // Verify the user was created in the database
        let babamul_users_collection: mongodb::Collection<boom::api::routes::babamul::BabamulUser> =
            database.collection("babamul_users");
        let user = babamul_users_collection
            .find_one(doc! { "email": &test_email })
            .await
            .unwrap();
        assert!(user.is_some(), "User should be created in database");

        let user = user.unwrap();
        assert_eq!(user.email, test_email);
        assert!(!user.is_activated, "User should not be activated yet");
        assert!(
            user.activation_code.is_some(),
            "Activation code should be set"
        );

        // Try to signup with the same email again - should succeed
        // (since it is not activated yet), but generate a new activation code
        let req = test::TestRequest::post()
            .uri("/babamul/signup")
            .set_json(serde_json::json!({
                "email": test_email
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Re-signup should succeed for unactivated account (error: {})",
            read_str_response(resp).await
        );

        let body = read_json_response(resp).await;
        assert!(
            body["message"].is_string(),
            "Response should contain message"
        );
        assert_eq!(
            body["activation_required"].as_bool().unwrap(),
            true,
            "Activation should be required"
        );

        let user_after = babamul_users_collection
            .find_one(doc! { "email": &test_email })
            .await
            .unwrap()
            .unwrap();
        assert_ne!(
            user.activation_code, user_after.activation_code,
            "A new activation code should be generated on re-signup"
        );

        // Clean up: delete the test user
        babamul_users_collection
            .delete_one(doc! { "email": &test_email })
            .await
            .unwrap();
    }

    /// Test POST /babamul/activate
    /// NOTE:
    /// - This test requires Kafka CLI tools (kafka-configs / kafka-acls) and a reachable Kafka broker.
    /// - Install tools with: brew install kafka (macOS) or run against a Docker Kafka.
    #[actix_rt::test]
    async fn test_babamul_activate() {
        load_dotenv();
        let config = AppConfig::from_test_config().unwrap();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(config.clone()))
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .app_data(web::Data::new(EmailService::new()))
                .service(routes::babamul::post_babamul_signup)
                .service(routes::babamul::post_babamul_activate)
                .service(routes::babamul::post_babamul_auth),
        )
        .await;

        // Generate a unique test email
        let test_email = format!("test+{}@babamul.example.com", unique_suffix());

        // First, sign up
        let req = test::TestRequest::post()
            .uri("/babamul/signup")
            .set_json(serde_json::json!({
                "email": test_email
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Get the activation code from the database
        let babamul_users_collection: mongodb::Collection<boom::api::routes::babamul::BabamulUser> =
            database.collection("babamul_users");
        let user = babamul_users_collection
            .find_one(doc! { "email": &test_email })
            .await
            .unwrap()
            .unwrap();
        let activation_code = user.activation_code.clone().unwrap();

        // Try to activate with wrong code
        let req = test::TestRequest::post()
            .uri("/babamul/activate")
            .set_json(serde_json::json!({
                "email": test_email,
                "activation_code": "wrong-code"
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Wrong activation code should be rejected"
        );

        // Activate with correct code
        let req = test::TestRequest::post()
            .uri("/babamul/activate")
            .set_json(serde_json::json!({
                "email": test_email,
                "activation_code": activation_code
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK, "Activation should succeed");

        let body = read_json_response(resp).await;
        assert_eq!(body["activated"].as_bool().unwrap(), true);
        assert!(
            body["password"].is_string(),
            "Password should be returned on activation"
        );

        let password = body["password"].as_str().unwrap();
        assert_eq!(password.len(), 32, "Password should be 32 characters");

        // Save password for later use
        let user_password = password.to_string();

        // Verify user is activated in database
        let user = babamul_users_collection
            .find_one(doc! { "email": &test_email })
            .await
            .unwrap()
            .unwrap();
        assert!(user.is_activated, "User should be activated");
        assert!(
            user.activation_code.is_none(),
            "Activation code should be cleared"
        );

        // Verify password is stored (hashed)
        assert!(
            !user.password_hash.is_empty(),
            "Password hash should be stored"
        );

        // Test authentication with the password
        let req = test::TestRequest::post()
            .uri("/babamul/auth")
            .set_form(serde_json::json!({
                "email": test_email,
                "password": user_password
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Authentication should succeed (error: {})",
            read_str_response(resp).await
        );

        let auth_body = read_json_response(resp).await;
        assert!(
            auth_body["access_token"].is_string(),
            "Should return access token"
        );
        assert_eq!(auth_body["token_type"].as_str().unwrap(), "Bearer");

        // Try to activate again - should succeed but not return password
        let req = test::TestRequest::post()
            .uri("/babamul/activate")
            .set_json(serde_json::json!({
                "email": test_email,
                "activation_code": activation_code
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_json_response(resp).await;
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("already activated"));
        assert!(
            body["password"].is_null(),
            "Password should not be returned for already-activated account"
        );

        // Clean up: delete the test user
        babamul_users_collection
            .delete_one(doc! { "email": &test_email })
            .await
            .unwrap();

        // Clean up: delete Kafka ACLs for the user
        delete_acls_for_user(&test_email, &config.kafka.producer.server).unwrap();
    }

    /// Test that invalid emails are rejected
    #[actix_rt::test]
    async fn test_babamul_signup_invalid_email() {
        load_dotenv();
        let config = AppConfig::from_test_config().unwrap();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(config.clone()))
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .app_data(web::Data::new(EmailService::new()))
                .service(routes::babamul::post_babamul_signup),
        )
        .await;

        // Test invalid emails
        for invalid_email in &["invalid", "no-at-sign", "@nodomain", ""] {
            let req = test::TestRequest::post()
                .uri("/babamul/signup")
                .set_json(serde_json::json!({
                    "email": invalid_email
                }))
                .to_request();

            let resp = test::call_service(&app, req).await;
            assert_eq!(
                resp.status(),
                StatusCode::BAD_REQUEST,
                "Invalid email '{}' should be rejected",
                invalid_email
            );
        }
    }

    /// Test GET /babamul/schema/{survey}
    #[actix_rt::test]
    async fn test_get_babamul_schema() {
        load_dotenv();
        let babamul_schemas = boom::api::routes::babamul::surveys::BabamulAvroSchemas::new();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(babamul_schemas))
                .service(routes::babamul::surveys::get_babamul_schema),
        )
        .await;

        // ZTF schema
        let req = test::TestRequest::get()
            .uri("/babamul/surveys/ztf/schemas")
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Should successfully retrieve ZTF schema"
        );

        let body = read_json_response(resp).await;
        assert!(body.is_object(), "Schema should be a JSON object");
        assert!(
            body.get("name").is_some(),
            "Schema should contain a 'name' field"
        );

        // LSST schema
        let req = test::TestRequest::get()
            .uri("/babamul/surveys/lsst/schemas")
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Should successfully retrieve LSST schema"
        );

        let body = read_json_response(resp).await;
        assert!(body.is_object(), "Schema should be a JSON object");
        assert!(
            body.get("name").is_some(),
            "Schema should contain a 'name' field"
        );

        // Invalid survey
        let req = test::TestRequest::get()
            .uri("/babamul/surveys/invalid_survey/schemas")
            .to_request();

        let resp = test::call_service(&app, req).await;
        // Invalid survey routes don't match the handler pattern, so they get 404
        assert!(
            resp.status() == StatusCode::NOT_FOUND || resp.status() == StatusCode::BAD_REQUEST,
            "Should reject invalid survey"
        );
    }

    /// Test GET /babamul/surveys/{survey_name}/objects/{candid}/cutouts success case
    #[actix_rt::test]
    async fn test_get_alert_cutouts() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(
            &database,
            &auth_app_data,
            &format!("cutouts_{}", unique_suffix()),
        )
        .await;

        // Insert test cutout data with unique ID
        let cutouts_collection =
            database.collection::<boom::alert::AlertCutout>("ZTF_alerts_cutouts");
        let test_candid: i64 = unique_suffix().parse::<i64>().unwrap() + 99999;

        let cutout = boom::alert::AlertCutout {
            candid: test_candid,
            cutout_science: vec![1, 2, 3, 4, 5],
            cutout_template: vec![6, 7, 8, 9, 10],
            cutout_difference: vec![11, 12, 13, 14, 15],
        };

        cutouts_collection
            .insert_one(&cutout)
            .await
            .expect("Failed to insert test cutout");

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .wrap(from_fn(babamul_auth_middleware))
                .service(routes::babamul::surveys::get_alert_cutouts),
        )
        .await;

        let req = test::TestRequest::get()
            .uri(&format!(
                "/babamul/surveys/ztf/alerts/{}/cutouts",
                test_candid
            ))
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Should successfully retrieve cutouts"
        );

        let body = read_json_response(resp).await;
        assert_eq!(
            body["data"]["candid"].as_i64().unwrap(),
            test_candid,
            "Response should contain correct candid"
        );
        assert!(
            body["data"]["cutout_science"].is_string(),
            "Cutout should be base64 encoded string"
        );

        // Clean up
        cutouts_collection
            .delete_one(doc! { "_id": test_candid })
            .await
            .expect("Failed to delete test cutout");

        // Test retrieval of non-existent candid
        let req = test::TestRequest::get()
            .uri("/babamul/surveys/ztf/alerts/8888888888/cutouts")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "Should return 404 for non-existent candid"
        );
    }

    /// Test GET /babamul/surveys/{survey_name}/alerts
    #[actix_rt::test]
    async fn test_get_lsst_alerts() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(
            &database,
            &auth_app_data,
            &format!("objects_{}", unique_suffix()),
        )
        .await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .wrap(from_fn(babamul_auth_middleware))
                .service(routes::babamul::surveys::get_alerts),
        )
        .await;

        let mut alert_worker = lsst_alert_worker().await;
        let (candid, object_id, _, _, bytes_content) =
            AlertRandomizer::new_randomized(Survey::Lsst)
                .ra(180.0)
                .dec(0.0)
                .get()
                .await;
        let status = alert_worker.process_alert(&bytes_content).await.unwrap();
        assert_eq!(status, ProcessAlertStatus::Added(candid));
        let mut enrichment_worker = LsstEnrichmentWorker::new(TEST_CONFIG_FILE).await.unwrap();
        let result = enrichment_worker.process_alerts(&[candid]).await;
        assert!(result.is_ok(), "Enrichment failed: {:?}", result.err());
        // Query with cone search and magnitude filters
        let req = test::TestRequest::get()
            .uri("/babamul/surveys/lsst/alerts?ra=180.0&dec=0.0&radius_arcsec=60&min_magpsf=11&max_magpsf=26")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Should successfully retrieve alerts (error: {})",
            read_str_response(resp).await
        );
        let body = read_json_response(resp).await;
        let alerts = body["data"].as_array().unwrap();
        assert!(
            !alerts.is_empty(),
            "Response should contain at least one alert"
        );
        assert!(
            alerts
                .iter()
                .any(|alert| alert["objectId"].as_str().unwrap() == object_id
                    && alert["candid"].as_i64().unwrap() == candid),
            "Response should contain the inserted alert"
        );

        // Clean up
        drop_alert_from_collections(candid, "LSST").await.unwrap();
    }

    /// Test GET /babamul/surveys/{survey_name}/objects/{object_id}
    #[actix_rt::test]
    async fn test_get_lsst_object() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(
            &database,
            &auth_app_data,
            &format!("object_{}", unique_suffix()),
        )
        .await;

        let mut alert_worker = lsst_alert_worker().await;
        let (candid, object_id, _, _, bytes_content) =
            AlertRandomizer::new_randomized(Survey::Lsst).get().await;
        let status = alert_worker.process_alert(&bytes_content).await.unwrap();
        assert_eq!(status, ProcessAlertStatus::Added(candid));
        let mut enrichment_worker = LsstEnrichmentWorker::new(TEST_CONFIG_FILE).await.unwrap();
        let result = enrichment_worker.process_alerts(&[candid]).await;
        assert!(result.is_ok(), "Enrichment failed: {:?}", result.err());

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .wrap(from_fn(babamul_auth_middleware))
                .service(routes::babamul::surveys::get_object),
        )
        .await;

        let req = test::TestRequest::get()
            .uri(&format!("/babamul/surveys/lsst/objects/{}", &object_id))
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Should successfully retrieve object (error: {})",
            read_str_response(resp).await
        );

        let body = read_json_response(resp).await;
        assert_eq!(
            body["data"]["object_id"].as_str().unwrap(),
            &object_id,
            "Response should contain correct object_id"
        );
        assert!(
            body["data"]["candidate"].is_object(),
            "Response should contain candidate"
        );
        assert!(
            body["data"]["cutout_science"].is_string(),
            "Cutout should be base64 encoded string"
        );

        // Clean up
        drop_alert_from_collections(candid, "LSST").await.unwrap();

        // Test retrieval of non-existent object
        let req = test::TestRequest::get()
            .uri("/babamul/surveys/lsst/objects/nonexistent_object")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "Should return 404 for non-existent object"
        );
    }
}
