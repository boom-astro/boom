#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::middleware::from_fn;
    use actix_web::{test, web, App};
    use boom::api::auth::{auth_middleware, get_test_auth};
    use boom::api::db::get_test_db_api;
    use boom::api::routes;
    use boom::api::test_utils::read_json_response;
    use boom::conf::{load_dotenv, AppConfig};
    use mongodb::Database;

    /// Test GET /users
    #[actix_rt::test]
    async fn test_get_users() {
        load_dotenv();
        let database: Database = get_test_db_api().await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::users::get_users),
        )
        .await;

        let req = test::TestRequest::get().uri("/users").to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = read_json_response(resp).await;

        assert!(resp["data"].is_array());
    }

    /// Test POST /users and DELETE /users/{username}
    #[actix_rt::test]
    async fn test_post_and_delete_user() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let auth_config = AppConfig::from_test_config().unwrap().api.auth;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .wrap(from_fn(auth_middleware))
                .service(routes::users::post_user)
                .service(routes::users::delete_user),
        )
        .await;

        let (token, _) = auth_app_data
            .create_token_for_user(&auth_config.admin_username, &auth_config.admin_password)
            .await
            .expect("Failed to create token for admin user");

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
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&new_user)
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = read_json_response(resp).await;

        let user_id = resp["data"]["id"].as_str().unwrap();

        // Test that we can't post the same user again
        let duplicate_req = test::TestRequest::post()
            .uri("/users")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&new_user)
            .to_request();
        let duplicate_resp = test::call_service(&app, duplicate_req).await;
        assert_eq!(duplicate_resp.status(), StatusCode::CONFLICT);

        // Now delete this user
        let delete_req = test::TestRequest::delete()
            .uri(&format!("/users/{}", user_id))
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .to_request();
        let delete_resp = test::call_service(&app, delete_req).await;
        assert_eq!(delete_resp.status(), StatusCode::OK);
    }

    /// Test PATCH /users/{user_id}/watchlist_access validation, dedup and sort
    #[actix_rt::test]
    async fn test_patch_watchlist_access() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let auth_app_data = get_test_auth(&database).await.unwrap();
        let auth_config = AppConfig::from_test_config().unwrap().api.auth;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .wrap(from_fn(auth_middleware))
                .service(routes::users::post_user)
                .service(routes::users::patch_watchlist_access)
                .service(routes::users::delete_user),
        )
        .await;

        let (token, _) = auth_app_data
            .create_token_for_user(&auth_config.admin_username, &auth_config.admin_password)
            .await
            .expect("Failed to create token for admin user");

        // Create a new user with a UUID username
        let random_name = uuid::Uuid::new_v4().to_string();
        let new_user = serde_json::json!({
            "username": random_name,
            "email": format!("{}@example.com", random_name),
            "password": "password123"
        });
        let req = test::TestRequest::post()
            .uri("/users")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&new_user)
            .to_request();
        let resp = read_json_response(test::call_service(&app, req).await).await;
        let user_id = resp["data"]["id"].as_str().unwrap().to_string();

        // A valid list with duplicates, blanks and unsorted entries is accepted,
        // deduplicated, sorted, and stripped of empties.
        let patch = serde_json::json!({
            "watchlist_access": ["watchlist_b", "watchlist_a", "watchlist_b", ""]
        });
        let req = test::TestRequest::patch()
            .uri(&format!("/users/{}/watchlist_access", user_id))
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&patch)
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = read_json_response(resp).await;
        assert_eq!(
            resp["data"]["watchlist_access"],
            serde_json::json!(["watchlist_a", "watchlist_b"])
        );

        // A name without the watchlist_ prefix is rejected and leaves the list unchanged.
        let bad_patch = serde_json::json!({
            "watchlist_access": ["watchlist_c", "not_a_watchlist"]
        });
        let req = test::TestRequest::patch()
            .uri(&format!("/users/{}/watchlist_access", user_id))
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .set_json(&bad_patch)
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        // Clean up.
        let delete_req = test::TestRequest::delete()
            .uri(&format!("/users/{}", user_id))
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .to_request();
        let delete_resp = test::call_service(&app, delete_req).await;
        assert_eq!(delete_resp.status(), StatusCode::OK);
    }
}
