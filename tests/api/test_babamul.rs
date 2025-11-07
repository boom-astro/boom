#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::{test, web, App};
    use boom::api::auth::get_test_auth;
    use boom::api::db::get_test_db;
    use boom::api::email::EmailService;
    use boom::api::routes;
    use boom::api::test_utils::read_json_response;
    use boom::conf::load_dotenv;
    use mongodb::bson::doc;
    use mongodb::Database;

    /// Test POST /babamul/signup
    #[actix_rt::test]
    async fn test_babamul_signup() {
        load_dotenv();
        let database: Database = get_test_db().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .app_data(web::Data::new(EmailService::new()))
                .service(routes::babamul::post_babamul_signup),
        )
        .await;

        // Generate a unique test email
        let test_email = format!("test+{}@babamul.example.com", uuid::Uuid::new_v4());

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
            "Signup should succeed with valid email"
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

        // Try to signup with the same email again - should fail
        let req = test::TestRequest::post()
            .uri("/babamul/signup")
            .set_json(serde_json::json!({
                "email": test_email
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::CONFLICT,
            "Duplicate email should be rejected"
        );

        // Clean up: delete the test user
        babamul_users_collection
            .delete_one(doc! { "email": &test_email })
            .await
            .unwrap();
    }

    /// Test POST /babamul/activate
    #[actix_rt::test]
    async fn test_babamul_activate() {
        load_dotenv();
        let database: Database = get_test_db().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .app_data(web::Data::new(EmailService::new()))
                .service(routes::babamul::post_babamul_signup)
                .service(routes::babamul::post_babamul_activate)
                .service(routes::babamul::post_babamul_auth),
        )
        .await;

        // Generate a unique test email
        let test_email = format!("test+{}@babamul.example.com", uuid::Uuid::new_v4());

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
            .set_json(serde_json::json!({
                "email": test_email,
                "password": user_password
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Authentication should succeed"
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
        let database: Database = get_test_db().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new()
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
}
