#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::middleware::from_fn;
    use actix_web::{test, web, App};
    use boom::api::auth::{auth_middleware, get_test_auth};
    use boom::api::conf::{load_dotenv, AppConfig};
    use boom::api::db::get_test_db;
    use boom::api::routes;
    use boom::api::test_utils::read_json_response;
    use mongodb::{bson::doc, Database};

    /// Test POST /auth
    #[actix_rt::test]
    async fn test_post_auth() {
        load_dotenv();

        // Set up the database, which will create the admin user
        let database: Database = get_test_db().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .service(routes::auth::post_auth),
        )
        .await;

        // On initialization of the db connection, an admin user for the API
        // should be created if it does not exist yet, and updated if it does
        // but the password and/or email have changed.
        let auth_config = AppConfig::from_test_config().api.auth;
        let admin_username = auth_config.admin_username.clone();
        let admin_password = auth_config.admin_password.clone();

        // Now try to authenticate with the admin user, to retrieve a JWT token
        let req = test::TestRequest::post()
            .uri("/auth")
            .set_json(&serde_json::json!({
                "username": admin_username,
                "password": admin_password
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        let response_status = resp.status();
        assert_eq!(response_status, StatusCode::OK);

        let resp: serde_json::Value = read_json_response(resp).await;
        let _ = resp["access_token"]
            .as_str()
            .expect("token should be a string");

        // there should also be an access_type field
        assert!(resp.get("token_type").is_some());
        assert_eq!(resp["token_type"], "Bearer");
        let token = resp["access_token"]
            .as_str()
            .expect("token should be a string");

        if auth_app_data.token_expiration > 0 {
            assert!(resp.get("expires_in").is_some());
            let expires_in = resp["expires_in"]
                .as_u64()
                .expect("expires_in should be a u64");
            assert_eq!(expires_in, auth_app_data.token_expiration as u64);
        } else {
            // if token_expiration is 0, expires_in should not be present
            assert!(resp.get("expires_in").is_none());
        }

        // assert that the token is a valid JWT
        // (i.e. that we can decode it)
        let claims: Result<boom::api::auth::Claims, jsonwebtoken::errors::Error> =
            auth_app_data.decode_token(token).await;
        assert!(
            claims.is_ok(),
            "Failed to decode JWT token: {:?}",
            claims.err()
        );
        let user_id = claims.unwrap().sub;
        // query the user from the database to check that it exists
        let user = database
            .collection::<boom::api::routes::users::User>("users")
            .find_one(doc! { "_id": user_id })
            .await
            .unwrap();

        // check that the user exists
        assert!(user.is_some(), "User not found in database");
        let user = user.unwrap();
        // check that the user has the correct username
        assert_eq!(user.username, admin_username, "User has incorrect username");
        // check that the user is an admin, as expected
        assert!(user.is_admin, "User is not an admin");

        // check that there is a "token_type" field in the response
        assert_eq!(resp["token_type"], "Bearer");
    }

    /// Test POST /auth
    #[actix_rt::test]
    async fn test_auth_middleware() {
        load_dotenv();

        // Now set up the database, which will create the admin user
        let database: Database = get_test_db().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let auth_config = AppConfig::from_test_config().api.auth;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .service(routes::users::post_user)
                .service(routes::auth::post_auth)
                .service(
                    // we add an endpoint that requires auth to test the middleware
                    actix_web::web::scope("/auth-required")
                        .wrap(from_fn(auth_middleware))
                        .service(routes::users::get_users),
                ),
        )
        .await;

        let (token, _) = auth_app_data
            .create_token_for_user(&auth_config.admin_username, &auth_config.admin_password)
            .await
            .expect("Failed to create token for admin user");

        // Now try to access a protected endpoint with the token
        let req = test::TestRequest::get()
            .uri("/auth-required/users")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        // and without to verify the auth is enforced
        let req = test::TestRequest::get()
            .uri("/auth-required/users")
            .to_request();
        let resp = test::try_call_service(&app, req).await;
        assert!(resp.is_err());
        assert_eq!(
            resp.err().unwrap().as_response_error().status_code(),
            StatusCode::UNAUTHORIZED
        );
    }
}
