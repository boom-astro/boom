#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::middleware::from_fn;
    use actix_web::{App, test, web};
    use boom_api::api;
    use boom_api::auth::{auth_middleware, get_auth};
    use boom_api::db::get_default_db;
    use mongodb::Database;

    /// Test POST /auth
    #[actix_rt::test]
    async fn test_post_auth() {
        let database: Database = get_default_db().await;
        let auth_app_data = get_auth(&database).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .service(api::users::post_user)
                .service(api::auth::post_auth)
                .service(
                    // we add an endpoint that requires auth to test the middleware
                    actix_web::web::scope("/auth-required")
                        .wrap(from_fn(auth_middleware))
                        .service(api::users::get_users),
                ),
        )
        .await;

        // Create a new user with a UUID username
        let random_name = uuid::Uuid::new_v4().to_string();

        let new_user = serde_json::json!({
            "username": random_name,
            "email":
            format!("{}@example.com", random_name),
            "password": "password123"
        });

        let req = test::TestRequest::post()
            .uri("/users")
            .set_json(&new_user)
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        // Now try to authenticate with the new user, to retrieve a JWT token
        let req = test::TestRequest::post()
            .uri("/auth")
            .set_json(&serde_json::json!({
                "username": random_name,
                "password": "password123"
            }))
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);

        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");
        assert_eq!(resp["status"], "success");

        // the response is a jwt token
        let token = resp["token"].as_str().expect("token should be a string");

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
