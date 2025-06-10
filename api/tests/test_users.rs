#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::{App, test, web};
    use boom_api::api;
    use boom_api::db::get_default_db;
    use mongodb::Database;

    /// Test GET /users
    #[actix_rt::test]
    async fn test_get_users() {
        let database: Database = get_default_db().await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(api::users::get_users),
        )
        .await;

        let req = test::TestRequest::get().uri("/users").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::OK);

        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);

        // Parse response body JSON
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");

        assert_eq!(resp["status"], "success");
    }

    /// Test POST /users and DELETE /users/{username}
    #[actix_rt::test]
    async fn test_post_and_delete_user() {
        let database: Database = get_default_db().await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(api::users::post_user)
                .service(api::users::delete_user),
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
        // Get the user out of the response body data so we know the ID
        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");
        assert_eq!(resp["status"], "success");
        let user_id = resp["data"]["id"].as_str().unwrap();

        // Test that we can't post the same user again
        let duplicate_req = test::TestRequest::post()
            .uri("/users")
            .set_json(&new_user)
            .to_request();
        let duplicate_resp = test::call_service(&app, duplicate_req).await;
        assert_eq!(duplicate_resp.status(), StatusCode::CONFLICT);

        // Now delete this user
        let delete_req = test::TestRequest::delete()
            .uri(&format!("/users/{}", user_id))
            .to_request();
        let delete_resp = test::call_service(&app, delete_req).await;
        assert_eq!(delete_resp.status(), StatusCode::OK);
    }
}
