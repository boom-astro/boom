#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::middleware::from_fn;
    use actix_web::{test, web, App};
    use boom::alert::{AlertWorker, ProcessAlertStatus};
    use boom::api::auth::{babamul_auth_middleware, get_test_auth};
    use boom::api::db::get_test_db_api;
    use boom::api::email::EmailService;
    use boom::api::routes;
    use boom::api::test_utils::{read_json_response, read_str_response};
    use boom::conf::{load_dotenv, AppConfig};
    use boom::enrichment::{EnrichmentWorker, LsstEnrichmentWorker, ZtfEnrichmentWorker};
    use boom::utils::enums::Survey;
    use boom::utils::testing::{
        drop_alert_from_collections, lsst_alert_worker, ztf_alert_worker, AlertRandomizer,
        TEST_CONFIG_FILE,
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
        ) -> Self {
            let id = uuid::Uuid::new_v4().to_string();
            let test_email = format!("test+{}@babamul.example.com", id);
            let test_user = boom::api::routes::babamul::BabamulUser {
                id: id.clone(),
                username: "testuser".to_string(),
                email: test_email.clone(),
                password_hash: "hash".to_string(),
                activation_code: None,
                is_activated: true,
                created_at: 0,
                kafka_credentials: vec![],
                tokens: vec![],
                password_reset_token_hash: None,
                password_reset_token_expires_at: None,
            };

            let babamul_users_collection: mongodb::Collection<
                boom::api::routes::babamul::BabamulUser,
            > = database.collection("babamul_users");
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

    /// Test POST /babamul/signup
    #[actix_rt::test]
    async fn test_babamul_signup() {
        load_dotenv();
        let config = AppConfig::from_test_config().unwrap();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new().service(
                actix_web::web::scope("/babamul")
                    .app_data(web::Data::new(config.clone()))
                    .app_data(web::Data::new(database.clone()))
                    .app_data(web::Data::new(auth_app_data.clone()))
                    .app_data(web::Data::new(EmailService::new()))
                    .service(routes::babamul::post_babamul_signup),
            ),
        )
        .await;

        // Generate a unique test email
        let id = uuid::Uuid::new_v4().to_string();
        let test_email = format!("test+{}@babamul.example.com", id);

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
            App::new().service(
                actix_web::web::scope("/babamul")
                    .app_data(web::Data::new(config.clone()))
                    .app_data(web::Data::new(database.clone()))
                    .app_data(web::Data::new(auth_app_data.clone()))
                    .app_data(web::Data::new(EmailService::new()))
                    .service(routes::babamul::post_babamul_signup)
                    .service(routes::babamul::post_babamul_activate)
                    .service(routes::babamul::post_babamul_auth),
            ),
        )
        .await;

        // Generate a unique test email
        let id = uuid::Uuid::new_v4().to_string();
        let test_email = format!("test+{}@babamul.example.com", id);

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
    }

    /// Test that invalid emails are rejected
    #[actix_rt::test]
    async fn test_babamul_signup_invalid_email() {
        load_dotenv();
        let config = AppConfig::from_test_config().unwrap();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new().service(
                actix_web::web::scope("/babamul")
                    .app_data(web::Data::new(config.clone()))
                    .app_data(web::Data::new(database.clone()))
                    .app_data(web::Data::new(auth_app_data.clone()))
                    .app_data(web::Data::new(EmailService::new()))
                    .service(routes::babamul::post_babamul_signup),
            ),
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
            App::new().service(
                actix_web::web::scope("/babamul")
                    .app_data(web::Data::new(babamul_schemas))
                    .service(routes::babamul::surveys::get_babamul_schema),
            ),
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

    /// Test GET /babamul/surveys/{survey_name}/cutouts success case
    #[actix_rt::test]
    async fn test_get_alert_cutouts() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        // Insert test cutout data with unique ID
        let cutouts_collection =
            database.collection::<boom::alert::AlertCutout>("ZTF_alerts_cutouts");
        let test_candid: i64 = uuid::Uuid::new_v4().as_u128() as i64;

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
            App::new().service(
                actix_web::web::scope("/babamul")
                    .app_data(web::Data::new(database.clone()))
                    .app_data(web::Data::new(auth_app_data.clone()))
                    .wrap(from_fn(babamul_auth_middleware))
                    .service(routes::babamul::surveys::get_cutouts),
            ),
        )
        .await;

        let req = test::TestRequest::get()
            .uri(&format!(
                "/babamul/surveys/ztf/cutouts?candid={}",
                test_candid
            ))
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Should successfully retrieve cutouts: {}",
            read_str_response(resp).await
        );

        let body = read_json_response(resp).await;
        assert_eq!(
            body["data"]["candid"].as_i64().unwrap(),
            test_candid,
            "Response should contain correct candid"
        );
        assert!(
            body["data"]["cutoutScience"].is_string(),
            "Cutout should be base64 encoded string"
        );

        // Clean up
        cutouts_collection
            .delete_one(doc! { "_id": test_candid })
            .await
            .expect("Failed to delete test cutout");

        // Test retrieval of non-existent candid
        let req = test::TestRequest::get()
            .uri("/babamul/surveys/ztf/cutouts?candid=8888888888")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "Should return 404 for non-existent candid"
        );
    }

    /// Test GET /babamul/surveys/lsst/alerts
    #[actix_rt::test]
    async fn test_get_lsst_alerts() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        let app = test::init_service(
            App::new().service(
                actix_web::web::scope("/babamul")
                    .app_data(web::Data::new(database.clone()))
                    .app_data(web::Data::new(auth_app_data.clone()))
                    .wrap(from_fn(babamul_auth_middleware))
                    .service(routes::babamul::surveys::get_alerts),
            ),
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

    /// Test GET /babamul/surveys/ztf/alerts
    #[actix_rt::test]
    async fn test_get_ztf_alerts() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        let app = test::init_service(
            App::new().service(
                actix_web::web::scope("/babamul")
                    .app_data(web::Data::new(database.clone()))
                    .app_data(web::Data::new(auth_app_data.clone()))
                    .wrap(from_fn(babamul_auth_middleware))
                    .service(routes::babamul::surveys::get_alerts),
            ),
        )
        .await;

        let mut alert_worker = ztf_alert_worker().await;
        let (candid, object_id, _, _, bytes_content) = AlertRandomizer::new_randomized(Survey::Ztf)
            .ra(180.0)
            .dec(0.0)
            .get()
            .await;
        let status = alert_worker.process_alert(&bytes_content).await.unwrap();
        assert_eq!(status, ProcessAlertStatus::Added(candid));
        let mut enrichment_worker = ZtfEnrichmentWorker::new(TEST_CONFIG_FILE).await.unwrap();
        let result = enrichment_worker.process_alerts(&[candid]).await;
        assert!(result.is_ok(), "Enrichment failed: {:?}", result.err());
        // Query with cone search and magnitude filters
        let req = test::TestRequest::get()
            .uri("/babamul/surveys/ztf/alerts?ra=180.0&dec=0.0&radius_arcsec=60&min_magpsf=11&max_magpsf=26")
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
        drop_alert_from_collections(candid, "ZTF").await.unwrap();
    }

    /// Test GET /babamul/surveys/lsst/objects/{object_id}
    #[actix_rt::test]
    async fn test_get_lsst_object() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        let mut alert_worker = lsst_alert_worker().await;
        let (candid, object_id, _, _, bytes_content) =
            AlertRandomizer::new_randomized(Survey::Lsst).get().await;
        let status = alert_worker.process_alert(&bytes_content).await.unwrap();
        assert_eq!(status, ProcessAlertStatus::Added(candid));
        let mut enrichment_worker = LsstEnrichmentWorker::new(TEST_CONFIG_FILE).await.unwrap();
        let result = enrichment_worker.process_alerts(&[candid]).await;
        assert!(result.is_ok(), "Enrichment failed: {:?}", result.err());

        let app = test::init_service(
            App::new().service(
                actix_web::web::scope("/babamul")
                    .app_data(web::Data::new(database.clone()))
                    .app_data(web::Data::new(auth_app_data.clone()))
                    .wrap(from_fn(babamul_auth_middleware))
                    .service(routes::babamul::surveys::get_object),
            ),
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
            body["data"]["objectId"].as_str().unwrap(),
            &object_id,
            "Response should contain correct objectId"
        );
        assert!(
            body["data"]["candidate"].is_object(),
            "Response should contain candidate"
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

    /// Test GET /babamul/surveys/ztf/objects/{object_id}
    #[actix_rt::test]
    async fn test_get_ztf_object() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        let mut alert_worker = ztf_alert_worker().await;
        let (candid, object_id, _, _, bytes_content) =
            AlertRandomizer::new_randomized(Survey::Ztf).get().await;
        let status = alert_worker.process_alert(&bytes_content).await.unwrap();
        assert_eq!(status, ProcessAlertStatus::Added(candid));
        let mut enrichment_worker = ZtfEnrichmentWorker::new(TEST_CONFIG_FILE).await.unwrap();
        let result = enrichment_worker.process_alerts(&[candid]).await;
        assert!(result.is_ok(), "Enrichment failed: {:?}", result.err());

        let app = test::init_service(
            App::new().service(
                actix_web::web::scope("/babamul")
                    .app_data(web::Data::new(database.clone()))
                    .app_data(web::Data::new(auth_app_data.clone()))
                    .wrap(from_fn(babamul_auth_middleware))
                    .service(routes::babamul::surveys::get_object),
            ),
        )
        .await;

        let req = test::TestRequest::get()
            .uri(&format!("/babamul/surveys/ztf/objects/{}", &object_id))
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
            body["data"]["objectId"].as_str().unwrap(),
            &object_id,
            "Response should contain correct objectId"
        );
        assert!(
            body["data"]["candidate"].is_object(),
            "Response should contain candidate"
        );

        // Clean up
        drop_alert_from_collections(candid, "ZTF").await.unwrap();

        // Test retrieval of non-existent object
        let req = test::TestRequest::get()
            .uri("/babamul/surveys/ztf/objects/nonexistent_object")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "Should return 404 for non-existent object"
        );
    }

    /// Test GET /babamul/objects validation for ZTF patterns
    #[actix_rt::test]
    async fn test_get_objects_validation() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        let app = test::init_service(
            App::new().service(
                actix_web::web::scope("/babamul")
                    .app_data(web::Data::new(database.clone()))
                    .app_data(web::Data::new(auth_app_data.clone()))
                    .wrap(from_fn(babamul_auth_middleware))
                    .service(routes::babamul::surveys::get_objects),
            ),
        )
        .await;

        // ZTF:
        // Acceptable values
        for value in ["Z", "ZT", "ZTF", "ZTF20a", "20a"] {
            let req = test::TestRequest::get()
                .uri(&format!("/babamul/objects?object_id={}", value))
                .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
                .to_request();

            let resp = test::call_service(&app, req).await;
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "Should accept valid object_id pattern '{}'",
                value
            );
        }

        // Invalid values
        for value in ["Z2", "ZTF231", "ZTF2a", "ZTF20aaaaaaaa"] {
            let req = test::TestRequest::get()
                .uri(&format!("/babamul/objects?object_id={}", value))
                .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
                .to_request();

            let resp = test::call_service(&app, req).await;
            assert_eq!(
                resp.status(),
                StatusCode::BAD_REQUEST,
                "Should reject invalid object_id pattern '{}'",
                value
            );
        }

        // LSST:
        // Acceptable values
        for value in ["L", "LS", "LSS", "LSST", "LSST1", "1", "LSST123", "123"] {
            let req = test::TestRequest::get()
                .uri(&format!("/babamul/objects?object_id={}", value))
                .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "Should accept valid object_id pattern '{}'",
                value
            );
        }

        // Invalid values
        for value in ["L2", "LSSTA", "1a"] {
            let req = test::TestRequest::get()
                .uri(&format!("/babamul/objects?object_id={}", value))
                .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(
                resp.status(),
                StatusCode::BAD_REQUEST,
                "Should reject invalid object_id pattern '{}'",
                value
            );
        }
    }

    /// Test POST /babamul/kafka-credentials - Create a new Kafka credential
    /// NOTE: This test requires Kafka CLI tools and a reachable Kafka broker
    #[actix_rt::test]
    async fn test_create_kafka_credential() {
        load_dotenv();
        let config = AppConfig::from_test_config().unwrap();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        let app = test::init_service(
            App::new().service(
                actix_web::web::scope("/babamul")
                    .app_data(web::Data::new(config.clone()))
                    .app_data(web::Data::new(database.clone()))
                    .app_data(web::Data::new(auth_app_data.clone()))
                    .wrap(from_fn(babamul_auth_middleware))
                    .service(routes::babamul::post_kafka_credentials)
                    .service(routes::babamul::get_kafka_credentials)
                    .service(routes::babamul::delete_kafka_credential),
            ),
        )
        .await;

        // Create a Kafka credential with a valid name
        let credential_name = "My Test Kafka Credential";
        let req = test::TestRequest::post()
            .uri("/babamul/kafka-credentials")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": credential_name
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Should successfully create Kafka credential (error: {})",
            read_str_response(resp).await
        );

        let body = read_json_response(resp).await;
        assert!(
            body["message"].is_string(),
            "Response should contain message"
        );
        assert!(
            body["credential"].is_object(),
            "Response should contain credential object"
        );

        // Verify credential structure
        let credential = &body["credential"];
        assert!(credential["id"].is_string(), "Credential should have id");
        assert_eq!(
            credential["name"].as_str().unwrap(),
            credential_name,
            "Credential name should match"
        );
        assert!(
            credential["kafka_username"].is_string(),
            "Credential should have kafka_username"
        );
        assert!(
            credential["kafka_password"].is_string(),
            "Credential should have kafka_password"
        );
        assert!(
            credential["created_at"].is_i64(),
            "Credential should have created_at timestamp"
        );

        // Verify kafka_username starts with "babamul-"
        let kafka_username = credential["kafka_username"].as_str().unwrap();
        assert!(
            kafka_username.starts_with("babamul-"),
            "Kafka username should start with 'babamul-'"
        );

        // Verify kafka_password is 32 characters
        let kafka_password = credential["kafka_password"].as_str().unwrap();
        assert_eq!(
            kafka_password.len(),
            32,
            "Kafka password should be 32 characters"
        );

        let credential_id = credential["id"].as_str().unwrap();

        // Verify credential was added to user in database
        let babamul_users_collection: mongodb::Collection<boom::api::routes::babamul::BabamulUser> =
            database.collection("babamul_users");
        let user = babamul_users_collection
            .find_one(doc! { "_id": &test_user.user.id })
            .await
            .unwrap()
            .expect("User should exist");

        assert_eq!(
            user.kafka_credentials.len(),
            1,
            "User should have 1 Kafka credential"
        );
        assert_eq!(
            user.kafka_credentials[0].id, credential_id,
            "Credential ID should match"
        );
        assert_eq!(
            user.kafka_credentials[0].name, credential_name,
            "Credential name should match"
        );

        // Clean up: delete the Kafka credential (which also deletes Kafka user/ACLs)
        let req = test::TestRequest::delete()
            .uri(&format!("/babamul/kafka-credentials/{}", credential_id))
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Should successfully delete Kafka credential"
        );

        // Test creating credential with invalid names:
        // - empty name
        let req = test::TestRequest::post()
            .uri("/babamul/kafka-credentials")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": ""
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Should reject empty credential name"
        );

        // - whitespace-only name
        let req = test::TestRequest::post()
            .uri("/babamul/kafka-credentials")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": "   "
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Should reject whitespace-only credential name"
        );
    }

    /// Test GET /babamul/kafka-credentials - List all credentials
    /// NOTE: This test requires Kafka CLI tools and a reachable Kafka broker
    #[actix_rt::test]
    async fn test_list_kafka_credentials() {
        load_dotenv();
        let config = AppConfig::from_test_config().unwrap();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        let app = test::init_service(
            App::new().service(
                actix_web::web::scope("/babamul")
                    .app_data(web::Data::new(config.clone()))
                    .app_data(web::Data::new(database.clone()))
                    .app_data(web::Data::new(auth_app_data.clone()))
                    .wrap(from_fn(babamul_auth_middleware))
                    .service(routes::babamul::post_kafka_credentials)
                    .service(routes::babamul::get_kafka_credentials)
                    .service(routes::babamul::delete_kafka_credential),
            ),
        )
        .await;

        // Initially, the user should have no Kafka credentials
        let req = test::TestRequest::get()
            .uri("/babamul/kafka-credentials")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Should successfully retrieve credentials list"
        );

        let body = read_json_response(resp).await;
        let credentials = body["data"].as_array().unwrap();
        assert_eq!(
            credentials.len(),
            0,
            "User should initially have no credentials"
        );

        // Create two Kafka credentials
        let req = test::TestRequest::post()
            .uri("/babamul/kafka-credentials")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": "Credential 1"
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body1 = read_json_response(resp).await;
        let credential_id_1 = body1["credential"]["id"].as_str().unwrap();

        let req = test::TestRequest::post()
            .uri("/babamul/kafka-credentials")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": "Credential 2"
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body2 = read_json_response(resp).await;
        let credential_id_2 = body2["credential"]["id"].as_str().unwrap();

        // Now list should show 2 credentials
        let req = test::TestRequest::get()
            .uri("/babamul/kafka-credentials")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = read_json_response(resp).await;
        let credentials = body["data"].as_array().unwrap();
        assert_eq!(credentials.len(), 2, "User should have 2 credentials");

        // Verify both credentials are present
        let cred_ids: Vec<&str> = credentials
            .iter()
            .map(|c| c["id"].as_str().unwrap())
            .collect();
        assert!(cred_ids.contains(&credential_id_1));
        assert!(cred_ids.contains(&credential_id_2));

        // Verify kafka_password is included in the list (stored in DB)
        for cred in credentials {
            assert!(
                cred["kafka_password"].is_string(),
                "Credential should include kafka_password"
            );
            // Verify kafka_username starts with "babamul-"
            assert!(cred["kafka_username"]
                .as_str()
                .unwrap()
                .starts_with("babamul-"));
        }

        // Clean up: delete both credentials
        let req = test::TestRequest::delete()
            .uri(&format!("/babamul/kafka-credentials/{}", credential_id_1))
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();
        test::call_service(&app, req).await;

        let req = test::TestRequest::delete()
            .uri(&format!("/babamul/kafka-credentials/{}", credential_id_2))
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();
        test::call_service(&app, req).await;
    }

    /// Test DELETE /babamul/kafka-credentials/{credential_id}
    /// NOTE: This test requires Kafka CLI tools and a reachable Kafka broker
    #[actix_rt::test]
    async fn test_delete_kafka_credential() {
        load_dotenv();
        let config = AppConfig::from_test_config().unwrap();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        let app = test::init_service(
            App::new().service(
                actix_web::web::scope("/babamul")
                    .app_data(web::Data::new(config.clone()))
                    .app_data(web::Data::new(database.clone()))
                    .app_data(web::Data::new(auth_app_data.clone()))
                    .wrap(from_fn(babamul_auth_middleware))
                    .service(routes::babamul::post_kafka_credentials)
                    .service(routes::babamul::get_kafka_credentials)
                    .service(routes::babamul::delete_kafka_credential),
            ),
        )
        .await;

        // Create a Kafka credential
        let req = test::TestRequest::post()
            .uri("/babamul/kafka-credentials")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": "Credential to Delete"
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = read_json_response(resp).await;
        let credential_id = body["credential"]["id"].as_str().unwrap();

        // Verify credential exists before deletion
        let req = test::TestRequest::get()
            .uri("/babamul/kafka-credentials")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        let body = read_json_response(resp).await;
        let credentials = body["data"].as_array().unwrap();
        assert_eq!(credentials.len(), 1);

        // Delete the credential
        let req = test::TestRequest::delete()
            .uri(&format!("/babamul/kafka-credentials/{}", credential_id))
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Should successfully delete credential (error: {})",
            read_str_response(resp).await
        );

        let body = read_json_response(resp).await;
        assert_eq!(
            body["deleted"].as_bool().unwrap(),
            true,
            "Response should indicate deletion"
        );
        assert!(
            body["message"].is_string(),
            "Response should contain message"
        );

        // Verify credential was removed from database
        let babamul_users_collection: mongodb::Collection<boom::api::routes::babamul::BabamulUser> =
            database.collection("babamul_users");
        let user = babamul_users_collection
            .find_one(doc! { "_id": &test_user.user.id })
            .await
            .unwrap()
            .expect("User should exist");

        assert_eq!(
            user.kafka_credentials.len(),
            0,
            "User should have no credentials after deletion"
        );

        // Verify GET list also shows no credentials
        let req = test::TestRequest::get()
            .uri("/babamul/kafka-credentials")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        let body = read_json_response(resp).await;
        let credentials = body["data"].as_array().unwrap();
        assert_eq!(credentials.len(), 0);

        // Try to delete a non-existent credential
        let fake_credential_id = uuid::Uuid::new_v4().to_string();
        let req = test::TestRequest::delete()
            .uri(&format!(
                "/babamul/kafka-credentials/{}",
                fake_credential_id
            ))
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "Should return 404 for non-existent credential"
        );
    }

    /// Test POST /babamul/tokens - Create a new PAT
    #[actix_rt::test]
    async fn test_post_token() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data))
                .service(
                    web::scope("/babamul")
                        .wrap(from_fn(babamul_auth_middleware))
                        .service(routes::babamul::tokens::post_token)
                        .service(routes::babamul::tokens::get_tokens)
                        .service(routes::babamul::get_babamul_profile),
                ),
        )
        .await;

        // Create a token with a valid name
        let req = test::TestRequest::post()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": "My First Token",
                "expires_in_days": 30
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Token creation should succeed"
        );

        let body = read_json_response(resp).await;
        assert!(body["id"].is_string(), "Response should have token id");
        assert_eq!(body["name"].as_str().unwrap(), "My First Token");
        assert!(
            body["access_token"].is_string(),
            "Response should have access_token"
        );

        let access_token = body["access_token"].as_str().unwrap();
        assert!(
            access_token.starts_with("bbml_"),
            "Token should start with bbml_"
        );
        assert_eq!(
            access_token.len(),
            41,
            "Token should be bbml_ (5 chars) + 36 random chars"
        );

        assert!(
            body["created_at"].is_i64(),
            "created_at should be timestamp"
        );
        assert!(
            body["expires_at"].is_i64(),
            "expires_at should be timestamp"
        );

        let created_at = body["created_at"].as_i64().unwrap();
        let expires_at = body["expires_at"].as_i64().unwrap();
        assert!(
            expires_at > created_at,
            "expires_at should be after created_at"
        );
        assert!(
            expires_at - created_at >= 30 * 86400 - 10
                && expires_at - created_at <= 30 * 86400 + 10,
            "Token should expire in ~30 days"
        );

        // Test that the PAT can be used to authenticate and fetch user profile
        let req = test::TestRequest::get()
            .uri("/babamul/profile")
            .insert_header(("Authorization", format!("Bearer {}", access_token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "PAT should authenticate successfully"
        );

        let profile_body = read_json_response(resp).await;
        assert_eq!(
            profile_body["data"]["email"].as_str().unwrap(),
            test_user.user.email,
            "Profile should return correct user email"
        );

        // Test creating token with empty name
        let req = test::TestRequest::post()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": "",
                "expires_in_days": 30
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Empty name should be rejected"
        );

        // Test creating token with whitespace-only name
        let req = test::TestRequest::post()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": "   ",
                "expires_in_days": 30
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Whitespace-only name should be rejected"
        );

        // Test creating token with default expiration (365 days)
        let req = test::TestRequest::post()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": "Default Expiration Token"
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Token creation with default expiration should succeed"
        );

        let body = read_json_response(resp).await;
        let created_at = body["created_at"].as_i64().unwrap();
        let expires_at = body["expires_at"].as_i64().unwrap();
        assert!(
            expires_at - created_at >= 365 * 86400 - 10
                && expires_at - created_at <= 365 * 86400 + 10,
            "Token should expire in ~365 days by default"
        );

        // Test creating token with zero days expiration
        let req = test::TestRequest::post()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": "Zero Expiration Token",
                "expires_in_days": 0
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Zero days expiration should be rejected"
        );

        // Test creating token with expiration > 3 years
        let req = test::TestRequest::post()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": "Too Long Expiration Token",
                "expires_in_days": 1096
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Expiration > 3 years should be rejected"
        );

        // Test creating token with exactly 3 years (should succeed)
        let req = test::TestRequest::post()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": "3 Year Token",
                "expires_in_days": 1095
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "3 years expiration should succeed"
        );

        let body = read_json_response(resp).await;
        let created_at = body["created_at"].as_i64().unwrap();
        let expires_at = body["expires_at"].as_i64().unwrap();
        assert!(
            expires_at - created_at >= 1095 * 86400 - 10
                && expires_at - created_at <= 1095 * 86400 + 10,
            "Token should expire in ~1095 days"
        );
    }

    /// Test POST /babamul/tokens - Token limit enforcement
    #[actix_rt::test]
    async fn test_post_token_limit() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data))
                .service(
                    web::scope("/babamul")
                        .wrap(from_fn(babamul_auth_middleware))
                        .service(routes::babamul::tokens::post_token)
                        .service(routes::babamul::tokens::delete_token),
                ),
        )
        .await;

        // Create 10 tokens (the maximum allowed)
        let mut token_ids = Vec::new();
        for i in 1..=10 {
            let req = test::TestRequest::post()
                .uri("/babamul/tokens")
                .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
                .set_json(serde_json::json!({
                    "name": format!("Token {}", i),
                    "expires_in_days": 30
                }))
                .to_request();

            let resp = test::call_service(&app, req).await;
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "Token {} creation should succeed",
                i
            );

            let body = read_json_response(resp).await;
            token_ids.push(body["id"].as_str().unwrap().to_string());
        }

        // Try to create an 11th token - should fail
        let req = test::TestRequest::post()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": "Token 11",
                "expires_in_days": 30
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "11th token creation should fail due to limit"
        );

        let body = read_str_response(resp).await;
        assert!(
            body.contains("Maximum number of tokens"),
            "Error message should mention token limit"
        );

        // Delete one token
        let req = test::TestRequest::delete()
            .uri(&format!("/babamul/tokens/{}", token_ids[0]))
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Now we should be able to create a new token
        let req = test::TestRequest::post()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": "New Token After Delete",
                "expires_in_days": 30
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Token creation should succeed after deleting one"
        );

        // Clean up: delete remaining tokens
        for token_id in &token_ids[1..] {
            let req = test::TestRequest::delete()
                .uri(&format!("/babamul/tokens/{}", token_id))
                .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
                .to_request();
            test::call_service(&app, req).await;
        }
    }

    /// Test GET /babamul/tokens - List all PATs
    #[actix_rt::test]
    async fn test_get_tokens() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data))
                .service(
                    web::scope("/babamul")
                        .wrap(from_fn(babamul_auth_middleware))
                        .service(routes::babamul::tokens::post_token)
                        .service(routes::babamul::tokens::get_tokens),
                ),
        )
        .await;

        // Initially, user should have no tokens
        let req = test::TestRequest::get()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = read_json_response(resp).await;
        let tokens = body.as_array().unwrap();
        assert_eq!(tokens.len(), 0, "User should start with no tokens");

        // Create three tokens
        let token_names = vec!["Token 1", "Token 2", "Token 3"];
        let mut created_token_ids = Vec::new();

        for name in &token_names {
            let req = test::TestRequest::post()
                .uri("/babamul/tokens")
                .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
                .set_json(serde_json::json!({
                    "name": name,
                    "expires_in_days": 30
                }))
                .to_request();

            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::OK);

            let body = read_json_response(resp).await;
            created_token_ids.push(body["id"].as_str().unwrap().to_string());
        }

        // List tokens
        let req = test::TestRequest::get()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = read_json_response(resp).await;
        let tokens = body.as_array().unwrap();
        assert_eq!(tokens.len(), 3, "User should have 3 tokens");

        // Verify token structure and content
        for (i, token) in tokens.iter().enumerate() {
            assert!(token["id"].is_string(), "Token should have id");
            assert!(token["name"].is_string(), "Token should have name");
            assert_eq!(
                token["name"].as_str().unwrap(),
                token_names[i],
                "Token name should match"
            );
            assert!(token["created_at"].is_i64(), "Token should have created_at");
            assert!(token["expires_at"].is_i64(), "Token should have expires_at");
            assert!(
                token["last_used_at"].is_null(),
                "Token should have null last_used_at initially"
            );

            // Token should NOT expose the token_hash or access_token
            assert!(
                token.get("token_hash").is_none(),
                "Token hash should not be exposed"
            );
            assert!(
                token.get("access_token").is_none(),
                "Access token should not be exposed in list"
            );
            assert!(
                token.get("user_id").is_none(),
                "User ID should not be exposed"
            );

            // Verify this is one of the tokens we created
            assert!(created_token_ids.contains(&token["id"].as_str().unwrap().to_string()));
        }

        // Verify token names are in the response (order may vary)
        let returned_names: Vec<&str> =
            tokens.iter().map(|t| t["name"].as_str().unwrap()).collect();
        for name in &token_names {
            assert!(
                returned_names.contains(name),
                "Token name should be in response"
            );
        }
    }

    /// Test DELETE /babamul/tokens/{id} - Delete a PAT
    #[actix_rt::test]
    async fn test_delete_token() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data))
                .service(
                    web::scope("/babamul")
                        .wrap(from_fn(babamul_auth_middleware))
                        .service(routes::babamul::tokens::post_token)
                        .service(routes::babamul::tokens::get_tokens)
                        .service(routes::babamul::tokens::delete_token),
                ),
        )
        .await;

        // Create a token
        let req = test::TestRequest::post()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "name": "Token to Delete",
                "expires_in_days": 30
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = read_json_response(resp).await;
        let token_id = body["id"].as_str().unwrap().to_string();

        // Verify token exists by listing
        let req = test::TestRequest::get()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = read_json_response(resp).await;
        let tokens = body.as_array().unwrap();
        assert_eq!(tokens.len(), 1, "Should have 1 token before delete");

        // Delete the token
        let req = test::TestRequest::delete()
            .uri(&format!("/babamul/tokens/{}", token_id))
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = read_json_response(resp).await;
        assert_eq!(
            body["message"].as_str().unwrap(),
            "Token deleted successfully"
        );

        // Verify token is gone by listing
        let req = test::TestRequest::get()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = read_json_response(resp).await;
        let tokens = body.as_array().unwrap();
        assert_eq!(tokens.len(), 0, "Token should be deleted");

        // Try to delete again - should get 404
        let req = test::TestRequest::delete()
            .uri(&format!("/babamul/tokens/{}", token_id))
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let body = read_json_response(resp).await;
        assert_eq!(body["error"].as_str().unwrap(), "Token not found");
    }

    /// Test DELETE /babamul/tokens/{id} with unauthorized access (token belongs to another user)
    #[actix_rt::test]
    async fn test_delete_token_unauthorized() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create two test users
        let user1 = TestUser::create(&database, &auth_app_data).await;
        let user2 = TestUser::create(&database, &auth_app_data).await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data))
                .service(
                    web::scope("/babamul")
                        .wrap(from_fn(babamul_auth_middleware))
                        .service(routes::babamul::tokens::post_token)
                        .service(routes::babamul::tokens::delete_token),
                ),
        )
        .await;

        // User 1 creates a token
        let req = test::TestRequest::post()
            .uri("/babamul/tokens")
            .insert_header(("Authorization", format!("Bearer {}", user1.token)))
            .set_json(serde_json::json!({
                "name": "User 1 Token",
                "expires_in_days": 30
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = read_json_response(resp).await;
        let token_id = body["id"].as_str().unwrap().to_string();

        // User 2 tries to delete User 1's token - should get 404
        let req = test::TestRequest::delete()
            .uri(&format!("/babamul/tokens/{}", token_id))
            .insert_header(("Authorization", format!("Bearer {}", user2.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "Should not be able to delete another user's token"
        );
    }

    /// Test GET /babamul/surveys/{survey}/objects/{object_id}/cross-matches
    #[actix_rt::test]
    async fn test_get_object_xmatches() {
        use boom::utils::spatial::Coordinates;

        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        // Create test aux data with cross_matches
        let aux_collection = database.collection::<boom::alert::ZtfObject>("ZTF_alerts_aux");
        let test_object_id = "ZTF24aaaaaaa".to_string();

        let test_object = boom::alert::ZtfObject {
            object_id: test_object_id.clone(),
            coordinates: Coordinates::new(124.5, -12.3),
            prv_candidates: vec![],
            prv_nondetections: vec![],
            fp_hists: vec![],
            aliases: None,
            created_at: 0.0,
            updated_at: 0.0,
            cross_matches: Some(
                serde_json::json!({
                    "gaia": [{"mag": 15.2, "distance": 0.5}],
                    "panstarrs": []
                })
                .as_object()
                .unwrap()
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        v.as_array()
                            .unwrap()
                            .iter()
                            .filter_map(|item| mongodb::bson::to_document(item).ok())
                            .collect::<Vec<_>>(),
                    )
                })
                .collect::<std::collections::HashMap<_, _>>(),
            ),
        };

        aux_collection
            .insert_one(&test_object)
            .await
            .expect("Failed to insert test object");

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data))
                .service(
                    web::scope("/babamul")
                        .wrap(from_fn(babamul_auth_middleware))
                        .service(routes::babamul::surveys::get_object_xmatches),
                ),
        )
        .await;

        // Test successful retrieval
        let req = test::TestRequest::get()
            .uri(&format!(
                "/babamul/surveys/ztf/objects/{}/cross-matches",
                test_object_id
            ))
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Should successfully retrieve cross-matches"
        );

        let body = read_json_response(resp).await;
        assert_eq!(body["status"].as_str().unwrap(), "success");
        assert!(
            body["data"].is_object(),
            "Should contain cross_matches data"
        );

        // Test not found
        let req = test::TestRequest::get()
            .uri("/babamul/surveys/ztf/objects/ZTF99nonexistent/cross-matches")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // Clean up
        aux_collection
            .delete_one(doc! { "_id": &test_object_id })
            .await
            .ok();
    }

    // Test POST /babamul/surveys/{survey}/objects/cross_matches endpoint (batch cross-match retrieval)
    #[actix_rt::test]
    async fn test_get_cross_matches_batch() {
        use boom::utils::spatial::Coordinates;

        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;
        // Create test aux data with cross_matches
        let aux_collection = database.collection::<boom::alert::ZtfObject>("ZTF_alerts_aux");
        let unique_suffix = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let test_objects = vec![
            boom::alert::ZtfObject {
                object_id: format!("ZTF24obj001_{}", unique_suffix),
                coordinates: Coordinates::new(125.0, -12.0),
                prv_candidates: vec![],
                prv_nondetections: vec![],
                fp_hists: vec![],
                aliases: None,
                created_at: 0.0,
                updated_at: 0.0,
                cross_matches: Some(
                    serde_json::json!({
                        "gaia": [{"mag": 15.2, "distance": 0.5}],
                        "panstarrs": []
                    })
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            v.as_array()
                                .unwrap()
                                .iter()
                                .filter_map(|item| mongodb::bson::to_document(item).ok())
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect::<std::collections::HashMap<_, _>>(),
                ),
            },
            boom::alert::ZtfObject {
                object_id: format!("ZTF24obj002_{}", unique_suffix),
                coordinates: Coordinates::new(126.0, -11.5),
                prv_candidates: vec![],
                prv_nondetections: vec![],
                fp_hists: vec![],
                aliases: None,
                created_at: 0.0,
                updated_at: 0.0,
                cross_matches: Some(
                    serde_json::json!({
                        "gaia": [{"mag": 16.5, "distance": 1.0}],
                        "panstarrs": [{"mag": 17.0, "distance": 0.8}]
                    })
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            v.as_array()
                                .unwrap()
                                .iter()
                                .filter_map(|item| mongodb::bson::to_document(item).ok())
                                .collect::<Vec<_>>(),
                        )
                    })
                    .collect::<std::collections::HashMap<_, _>>(),
                ),
            },
        ];

        for obj in &test_objects {
            aux_collection
                .insert_one(obj)
                .await
                .expect("Failed to insert test object");
        }

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data))
                .service(
                    web::scope("/babamul")
                        .wrap(from_fn(babamul_auth_middleware))
                        .service(routes::babamul::surveys::get_objects_xmatches),
                ),
        )
        .await;

        // Test successful batch retrieval
        let req = test::TestRequest::post()
            .uri("/babamul/surveys/ztf/objects/cross-matches")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "objectIds": [
                    format!("ZTF24obj001_{}", unique_suffix),
                    format!("ZTF24obj002_{}", unique_suffix)
                ]
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Should successfully retrieve batch cross-matches"
        );
        let body = read_json_response(resp).await;
        assert_eq!(body["status"].as_str().unwrap(), "success");
        assert!(
            body["data"].is_object(),
            "Should contain cross_matches data"
        );

        // Test with some non-existent object IDs
        let req = test::TestRequest::post()
            .uri("/babamul/surveys/ztf/objects/cross-matches")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "objectIds": [
                    format!("ZTF24obj001_{}", unique_suffix),
                    "ZTF99nonexistent"
                ]
            }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Should successfully retrieve cross-matches even if some object IDs do not exist"
        );
        let body = read_json_response(resp).await;
        assert_eq!(body["status"].as_str().unwrap(), "success");
        assert!(
            body["data"].is_object(),
            "Should contain cross_matches data for existing object"
        );
        assert!(
            body["data"].get("ZTF99nonexistent").is_none(),
            "Should not have data for non-existent object"
        );

        // Clean up test objects
        for obj in test_objects {
            aux_collection
                .delete_one(doc! { "_id": &obj.object_id })
                .await
                .ok();
        }
    }

    /// Test POST /babamul/surveys/{survey}/objects/cone-search
    #[actix_rt::test]
    async fn test_cone_search_objects() {
        use boom::utils::spatial::Coordinates;

        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        // Insert test objects with specific coordinates
        let aux_collection = database.collection::<boom::alert::ZtfObject>("ZTF_alerts_aux");
        let unique_suffix = uuid::Uuid::new_v4().to_string()[..8].to_string();
        let test_objects = vec![
            boom::alert::ZtfObject {
                object_id: format!("ZTF24obj001_{}", unique_suffix),
                coordinates: Coordinates::new(125.0, -12.0),
                prv_candidates: vec![],
                prv_nondetections: vec![],
                fp_hists: vec![],
                aliases: None,
                created_at: 0.0,
                updated_at: 0.0,
                cross_matches: None,
            },
            boom::alert::ZtfObject {
                object_id: format!("ZTF24obj002_{}", unique_suffix),
                coordinates: Coordinates::new(126.0, -11.5),
                prv_candidates: vec![],
                prv_nondetections: vec![],
                fp_hists: vec![],
                aliases: None,
                created_at: 0.0,
                updated_at: 0.0,
                cross_matches: None,
            },
        ];

        for obj in &test_objects {
            aux_collection
                .insert_one(obj)
                .await
                .expect("Failed to insert test object");
        }

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .service(
                    web::scope("/babamul")
                        .wrap(from_fn(babamul_auth_middleware))
                        .service(routes::babamul::surveys::cone_search_objects),
                ),
        )
        .await;

        // Test successful cone search with multiple coordinates
        let req = test::TestRequest::post()
            .uri("/babamul/surveys/ztf/objects/cone-search")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "coordinates": {
                    "search1": [125.0, -12.0],
                    "search2": [126.0, -11.5]
                },
                "radius_arcsec": 60.0
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Cone search should succeed (error: {})",
            read_str_response(resp).await
        );

        let body = read_json_response(resp).await;
        assert!(body["data"].is_object(), "Should return results as object");

        // Test with invalid radius (too large)
        let req = test::TestRequest::post()
            .uri("/babamul/surveys/ztf/objects/cone-search")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "coordinates": {
                    "search1": [125.0, -12.0]
                },
                "radius_arcsec": 700.0
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Should reject radius > 600 arcsec"
        );

        // Test with no coordinates
        let req = test::TestRequest::post()
            .uri("/babamul/surveys/ztf/objects/cone-search")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "coordinates": {},
                "radius_arcsec": 60.0
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Should reject empty coordinates"
        );

        // Test with unauthorized access
        let req = test::TestRequest::post()
            .uri("/babamul/surveys/ztf/objects/cone-search")
            .set_json(serde_json::json!({
                "coordinates": {
                    "search1": [125.0, -12.0]
                },
                "radius_arcsec": 60.0
            }))
            .to_request();

        let resp = test::try_call_service(&app, req).await;
        assert!(resp.is_err());
        assert_eq!(
            resp.err().unwrap().as_response_error().status_code(),
            StatusCode::UNAUTHORIZED
        );

        // Clean up
        for obj in test_objects {
            aux_collection
                .delete_one(doc! { "_id": &obj.object_id })
                .await
                .ok();
        }
    }

    /// Test POST /babamul/surveys/{survey}/alerts/cone-search
    #[actix_rt::test]
    async fn test_cone_search_alerts() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();

        // Create a test user
        let test_user = TestUser::create(&database, &auth_app_data).await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .service(
                    web::scope("/babamul")
                        .wrap(from_fn(babamul_auth_middleware))
                        .service(routes::babamul::surveys::cone_search_alerts),
                ),
        )
        .await;

        // Test with invalid radius (too small)
        let req = test::TestRequest::post()
            .uri("/babamul/surveys/ztf/alerts/cone-search")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "coordinates": {
                    "search1": [125.0, -12.0]
                },
                "radius_arcsec": 0.0
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Should reject radius <= 0"
        );

        // Test with invalid radius (too large)
        let req = test::TestRequest::post()
            .uri("/babamul/surveys/ztf/alerts/cone-search")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "coordinates": {
                    "search1": [125.0, -12.0]
                },
                "radius_arcsec": 700.0
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Should reject radius > 600"
        );

        // Test with no coordinates
        let req = test::TestRequest::post()
            .uri("/babamul/surveys/ztf/alerts/cone-search")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "coordinates": {},
                "radius_arcsec": 60.0
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Should reject empty coordinates"
        );

        // Test with unauthorized access
        let req = test::TestRequest::post()
            .uri("/babamul/surveys/ztf/alerts/cone-search")
            .set_json(serde_json::json!({
                "coordinates": {
                    "search1": [125.0, -12.0]
                },
                "radius_arcsec": 60.0
            }))
            .to_request();

        let resp = test::try_call_service(&app, req).await;
        assert!(resp.is_err());
        assert_eq!(
            resp.err().unwrap().as_response_error().status_code(),
            StatusCode::UNAUTHORIZED
        );

        // Test successful cone search with valid parameters
        let req = test::TestRequest::post()
            .uri("/babamul/surveys/ztf/alerts/cone-search")
            .insert_header(("Authorization", format!("Bearer {}", test_user.token)))
            .set_json(serde_json::json!({
                "coordinates": {
                    "search1": [125.0, -12.0]
                },
                "radius_arcsec": 60.0,
                "start_jd": 2450000.0,
                "end_jd": 2460000.0,
                "min_magpsf": 10.0,
                "max_magpsf": 20.0,
                "is_rock": false,
                "is_star": false
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Cone search with valid parameters should succeed (error: {})",
            read_str_response(resp).await
        );

        let body = read_json_response(resp).await;
        assert!(body["message"].is_string(), "Should include a message");
        assert!(body["data"].is_object(), "Should return results as object");
    }

    // ─── Password reset tests ─────────────────────────────────────────────────

    /// Helper: compute the SHA-256 hex digest of a string (mirrors the production code).
    fn sha256_hex(input: &str) -> String {
        use sha2::{Digest, Sha256};
        let mut h = Sha256::new();
        h.update(input.as_bytes());
        format!("{:x}", h.finalize())
    }

    /// Helper: insert a password-reset token directly into the DB for a user.
    async fn set_reset_token(database: &Database, user_id: &str, raw_token: &str, expires_at: i64) {
        let col: mongodb::Collection<boom::api::routes::babamul::BabamulUser> =
            database.collection("babamul_users");
        let token_hash = sha256_hex(raw_token);
        col.update_one(
            doc! { "_id": user_id },
            doc! {
                "$set": {
                    "password_reset_token_hash": &token_hash,
                    "password_reset_token_expires_at": expires_at
                }
            },
        )
        .await
        .expect("Failed to set reset token in DB");
    }

    /// POST /babamul/forgot-password
    ///
    /// Covers:
    /// - Known activated user: token hash and expiry are written to DB
    /// - Unknown email: returns 200 with generic message (no enumeration)
    /// - Non-activated account: returns 200 but no token is stored
    #[actix_rt::test]
    async fn test_babamul_forgot_password() {
        load_dotenv();
        let config = AppConfig::from_test_config().unwrap();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new().service(
                web::scope("/babamul")
                    .app_data(web::Data::new(config.clone()))
                    .app_data(web::Data::new(database.clone()))
                    .app_data(web::Data::new(auth_app_data.clone()))
                    .app_data(web::Data::new(EmailService::new()))
                    .service(routes::babamul::post_babamul_forgot_password)
                    .service(routes::babamul::post_babamul_reset_password)
                    .service(routes::babamul::post_babamul_auth),
            ),
        )
        .await;

        let col: mongodb::Collection<boom::api::routes::babamul::BabamulUser> =
            database.collection("babamul_users");

        // ── Case 1: activated user – token is written to DB ──────────────────
        let id_activated = uuid::Uuid::new_v4().to_string();
        let email_activated = format!("test+{}@babamul.example.com", id_activated);
        col.insert_one(&boom::api::routes::babamul::BabamulUser {
            id: id_activated.clone(),
            username: "resettest".to_string(),
            email: email_activated.clone(),
            password_hash: bcrypt::hash("hunter22hunter22", 4).unwrap(),
            activation_code: None,
            is_activated: true,
            created_at: 0,
            kafka_credentials: vec![],
            tokens: vec![],
            password_reset_token_hash: None,
            password_reset_token_expires_at: None,
        })
        .await
        .unwrap();

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/babamul/forgot-password")
                .set_json(serde_json::json!({ "email": email_activated }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "should always return 200");

        let updated = col
            .find_one(doc! { "_id": &id_activated })
            .await
            .unwrap()
            .unwrap();
        assert!(
            updated.password_reset_token_hash.is_some(),
            "reset token hash should be written to DB"
        );
        let expiry = updated.password_reset_token_expires_at.unwrap();
        let now = flare::Time::now().to_utc().timestamp();
        assert!(expiry > now, "token expiry should be in the future");
        assert!(
            expiry <= now + 3600 + 5,
            "token expiry should be ~1 hour from now"
        );

        // ── Case 2: unknown email – generic 200 response, no enumeration ─────
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/babamul/forgot-password")
                .set_json(serde_json::json!({ "email": "nobody@nowhere.example.com" }))
                .to_request(),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "unknown email must not cause a non-200 status"
        );
        let body = read_json_response(resp).await;
        assert!(
            body["message"].as_str().unwrap().contains("If an account"),
            "response must use the generic non-revealing message"
        );

        // ── Case 3: non-activated account – no token stored ──────────────────
        let id_inactive = uuid::Uuid::new_v4().to_string();
        let email_inactive = format!("test+{}@babamul.example.com", id_inactive);
        col.insert_one(&boom::api::routes::babamul::BabamulUser {
            id: id_inactive.clone(),
            username: "notactivated".to_string(),
            email: email_inactive.clone(),
            password_hash: "x".to_string(),
            activation_code: Some("code".to_string()),
            is_activated: false,
            created_at: 0,
            kafka_credentials: vec![],
            tokens: vec![],
            password_reset_token_hash: None,
            password_reset_token_expires_at: None,
        })
        .await
        .unwrap();

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/babamul/forgot-password")
                .set_json(serde_json::json!({ "email": email_inactive }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);

        let inactive = col
            .find_one(doc! { "_id": &id_inactive })
            .await
            .unwrap()
            .unwrap();
        assert!(
            inactive.password_reset_token_hash.is_none(),
            "no reset token should be stored for a non-activated account"
        );

        // Clean up
        col.delete_one(doc! { "_id": &id_activated }).await.unwrap();
        col.delete_one(doc! { "_id": &id_inactive }).await.unwrap();
    }

    /// POST /babamul/reset-password
    ///
    /// Covers:
    /// - Happy path: successful reset clears token, old password fails, new password logs in
    /// - Invalid (wrong) token → 400
    /// - Correct token but wrong email → 400
    /// - Expired token → 400
    /// - New password shorter than 8 characters → 400
    /// - Token is single-use: second attempt with the same token → 400
    #[actix_rt::test]
    async fn test_babamul_reset_password() {
        load_dotenv();
        let config = AppConfig::from_test_config().unwrap();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new().service(
                web::scope("/babamul")
                    .app_data(web::Data::new(config.clone()))
                    .app_data(web::Data::new(database.clone()))
                    .app_data(web::Data::new(auth_app_data.clone()))
                    .app_data(web::Data::new(EmailService::new()))
                    .service(routes::babamul::post_babamul_forgot_password)
                    .service(routes::babamul::post_babamul_reset_password)
                    .service(routes::babamul::post_babamul_auth),
            ),
        )
        .await;

        let col: mongodb::Collection<boom::api::routes::babamul::BabamulUser> =
            database.collection("babamul_users");
        let now = flare::Time::now().to_utc().timestamp();

        // Helper closure for inserting a plain activated user
        let insert_user =
            |id: &str, email: &str, username: &str| boom::api::routes::babamul::BabamulUser {
                id: id.to_string(),
                username: username.to_string(),
                email: email.to_string(),
                password_hash: bcrypt::hash("pw12345678", 4).unwrap(),
                activation_code: None,
                is_activated: true,
                created_at: 0,
                kafka_credentials: vec![],
                tokens: vec![],
                password_reset_token_hash: None,
                password_reset_token_expires_at: None,
            };

        let mut ids_to_cleanup: Vec<String> = Vec::new();

        // ── Happy path ────────────────────────────────────────────────────────
        let id = uuid::Uuid::new_v4().to_string();
        let email = format!("test+{}@babamul.example.com", id);
        let old_password = "oldpassword1234!";
        col.insert_one(&boom::api::routes::babamul::BabamulUser {
            password_hash: bcrypt::hash(old_password, 4).unwrap(),
            ..insert_user(&id, &email, "happypath")
        })
        .await
        .unwrap();
        ids_to_cleanup.push(id.clone());

        let raw_token = "happypathtokenXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        set_reset_token(&database, &id, raw_token, now + 3600).await;

        let new_password = "newpassword5678!";
        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/babamul/reset-password")
                .set_json(serde_json::json!({
                    "email": email,
                    "token": raw_token,
                    "new_password": new_password
                }))
                .to_request(),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "happy-path reset should succeed: {}",
            read_str_response(resp).await
        );
        let body = read_json_response(resp).await;
        assert!(body["message"]
            .as_str()
            .unwrap()
            .contains("reset successfully"));

        let updated = col.find_one(doc! { "_id": &id }).await.unwrap().unwrap();
        assert!(
            updated.password_reset_token_hash.is_none(),
            "token hash should be cleared"
        );
        assert!(
            updated.password_reset_token_expires_at.is_none(),
            "token expiry should be cleared"
        );
        assert!(
            !bcrypt::verify(old_password, &updated.password_hash).unwrap(),
            "old password should no longer work"
        );

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/babamul/auth")
                .set_form(serde_json::json!({ "email": email, "password": new_password }))
                .to_request(),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "login with new password should succeed"
        );
        assert!(read_json_response(resp).await["access_token"].is_string());

        // ── Invalid (wrong) token → 400 ───────────────────────────────────────
        let id2 = uuid::Uuid::new_v4().to_string();
        let email2 = format!("test+{}@babamul.example.com", id2);
        col.insert_one(&insert_user(&id2, &email2, "invalidtok"))
            .await
            .unwrap();
        ids_to_cleanup.push(id2.clone());
        set_reset_token(
            &database,
            &id2,
            "correct_token_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
            now + 3600,
        )
        .await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/babamul/reset-password")
                .set_json(serde_json::json!({
                    "email": email2,
                    "token": "this_is_the_wrong_token_XXXXXXXXXXXXXXXXXXXXXXXXX",
                    "new_password": "newpassword5678!"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "wrong token must be rejected"
        );
        assert_eq!(
            read_json_response(resp).await["message"].as_str().unwrap(),
            "Invalid or expired password reset token",
            "wrong-token error must use the same generic message as wrong-email to prevent oracle attacks"
        );

        // ── Correct token but wrong email → 400 ──────────────────────────────
        let id3 = uuid::Uuid::new_v4().to_string();
        let email3 = format!("test+{}@babamul.example.com", id3);
        col.insert_one(&insert_user(&id3, &email3, "wrongemail"))
            .await
            .unwrap();
        ids_to_cleanup.push(id3.clone());
        let token3 = "wrongemailtoken_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        set_reset_token(&database, &id3, token3, now + 3600).await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/babamul/reset-password")
                .set_json(serde_json::json!({
                    "email": "someone_else@other.example.com",
                    "token": token3,
                    "new_password": "newpassword5678!"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "correct token with wrong email must be rejected"
        );
        assert_eq!(
            read_json_response(resp).await["message"].as_str().unwrap(),
            "Invalid or expired password reset token",
            "wrong-email error must use the same generic message as wrong-token to prevent oracle attacks"
        );

        // ── Expired token → 400 ───────────────────────────────────────────────
        let id4 = uuid::Uuid::new_v4().to_string();
        let email4 = format!("test+{}@babamul.example.com", id4);
        col.insert_one(&insert_user(&id4, &email4, "expiredtok"))
            .await
            .unwrap();
        ids_to_cleanup.push(id4.clone());
        let token4 = "expiredtoken_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        set_reset_token(&database, &id4, token4, now - 1).await; // already expired

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/babamul/reset-password")
                .set_json(serde_json::json!({
                    "email": email4,
                    "token": token4,
                    "new_password": "newpassword5678!"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "expired token must be rejected"
        );

        // ── Password too short → 400 ──────────────────────────────────────────
        let id5 = uuid::Uuid::new_v4().to_string();
        let email5 = format!("test+{}@babamul.example.com", id5);
        col.insert_one(&insert_user(&id5, &email5, "tooshort"))
            .await
            .unwrap();
        ids_to_cleanup.push(id5.clone());
        let token5 = "tooshorttoken_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        set_reset_token(&database, &id5, token5, now + 3600).await;

        for short_pw in &["short", "seven77", ""] {
            let resp = test::call_service(
                &app,
                test::TestRequest::post()
                    .uri("/babamul/reset-password")
                    .set_json(serde_json::json!({
                        "email": email5,
                        "token": token5,
                        "new_password": short_pw
                    }))
                    .to_request(),
            )
            .await;
            assert_eq!(
                resp.status(),
                StatusCode::BAD_REQUEST,
                "password '{}' should be rejected as too short",
                short_pw
            );
        }

        // ── Token is single-use ───────────────────────────────────────────────
        let id6 = uuid::Uuid::new_v4().to_string();
        let email6 = format!("test+{}@babamul.example.com", id6);
        col.insert_one(&insert_user(&id6, &email6, "singleuse"))
            .await
            .unwrap();
        ids_to_cleanup.push(id6.clone());
        let token6 = "singleusetoken_XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX";
        set_reset_token(&database, &id6, token6, now + 3600).await;

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/babamul/reset-password")
                .set_json(serde_json::json!({
                    "email": email6,
                    "token": token6,
                    "new_password": "newpw12345678"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "first use should succeed");

        let resp = test::call_service(
            &app,
            test::TestRequest::post()
                .uri("/babamul/reset-password")
                .set_json(serde_json::json!({
                    "email": email6,
                    "token": token6,
                    "new_password": "anothernewpw5678"
                }))
                .to_request(),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "second use of consumed token must be rejected"
        );

        // Clean up all users created in this test
        for id in &ids_to_cleanup {
            col.delete_one(doc! { "_id": id }).await.unwrap();
        }
    }
}
