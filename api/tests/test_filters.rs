/// Tests for filters routes
#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::middleware::from_fn;
    use actix_web::{App, test, web};
    use boom_api::auth::{auth_middleware, get_default_auth};
    use boom_api::conf::AppConfig;
    use boom_api::db::get_default_db;
    use boom_api::routes;
    use mongodb::bson::{Document, doc};
    use mongodb::{Collection, Database};

    /// Test GET /filters
    #[actix_rt::test]
    async fn test_get_filters() {
        let database: Database = get_default_db().await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::filters::get_filters),
        )
        .await;

        let req = test::TestRequest::get().uri("/filters").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::OK);

        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);

        // Parse response body JSON
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");

        assert_eq!(resp["status"], "success");
    }

    /// Test POST /filters
    #[actix_rt::test]
    async fn test_post_filter() {
        let database: Database = get_default_db().await;
        let auth_app_data = get_default_auth(&database).await.unwrap();
        let auth_config = AppConfig::default().auth;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth_app_data.clone()))
                .wrap(from_fn(auth_middleware))
                .service(routes::filters::post_filter),
        )
        .await;

        let (token, _) = auth_app_data
            .create_token_for_user(&auth_config.admin_username, &auth_config.admin_password)
            .await
            .expect("Failed to create token for admin user");

        // Create a new filter
        let new_filter = serde_json::json!({
            "pipeline": [{"$match": {"something": 5}}],
            "catalog": "ZTF_alerts",
            "permissions": [1, 2],
        });

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
        assert_eq!(resp["status"], "success");
        let filter_id = resp["data"]["id"].as_str().unwrap();
        // Assert we have no _id field in the response
        assert!(!resp["data"].as_object().unwrap().contains_key("_id"));

        // Now delete this filter
        let filters_collection: Collection<Document> = database.collection("filters");
        filters_collection
            .delete_one(doc! { "_id": &filter_id })
            .await
            .expect("Failed to delete filter");
    }
}
