// Tests for catalogs endpoints
#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::{test, web, App};
    use boom_api::conf::load_dotenv;
    use boom_api::db::get_default_db;
    use boom_api::routes;
    use mongodb::Database;

    async fn get_test_db() -> Database {
        // For now, use the default DB function but we'll load test env vars
        load_dotenv();
        get_default_db().await
    }

    /// Test GET /catalogs
    #[actix_rt::test]
    async fn test_get_catalogs() {
        let database: Database = get_test_db().await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::catalogs::get_catalogs),
        )
        .await;

        let req = test::TestRequest::get().uri("/catalogs").to_request();
        let resp = test::call_service(&app, req).await;

        assert_eq!(resp.status(), StatusCode::OK);

        let body = test::read_body(resp).await;
        let body_str = String::from_utf8_lossy(&body);

        // Parse response body JSON
        let resp: serde_json::Value =
            serde_json::from_str(&body_str).expect("failed to parse JSON");

        assert!(resp["data"].is_array());
    }
}
