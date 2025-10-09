// Tests for catalogs endpoints
#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::{App, test, web};
    use boom_api::conf::load_dotenv;
    use boom_api::db::get_test_db;
    use boom_api::routes;
    use boom_api::test_utils::{create_test_catalog, delete_test_catalog, read_json_response};
    use mongodb::Database;

    /// Test GET /catalogs
    #[actix_rt::test]
    async fn test_get_catalogs() {
        load_dotenv();
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
        let resp = read_json_response(resp).await;

        assert!(resp["data"].is_array());
    }
    // next we test the get_catalog_indexes endpoint
    #[actix_rt::test]
    async fn test_get_catalog_indexes() {
        load_dotenv();
        let database: Database = get_test_db().await;
        let test_catalog_name = create_test_catalog(&database).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::catalogs::get_catalog_indexes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri(&format!("/catalogs/{}/indexes", test_catalog_name))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = read_json_response(resp).await;

        assert!(resp["data"].is_array());

        // Clean up test catalog
        delete_test_catalog(&database, &test_catalog_name).await;
    }
    // next we test the get_catalog_sample endpoint
    #[actix_rt::test]
    async fn test_get_catalog_sample() {
        load_dotenv();
        let database: Database = get_test_db().await;
        let test_catalog_name = create_test_catalog(&database).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::catalogs::get_catalog_sample),
        )
        .await;
        let req = test::TestRequest::get()
            .uri(&format!("/catalogs/{}/sample", test_catalog_name))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = read_json_response(resp).await;

        assert!(resp["data"].is_array());
        assert_eq!(resp["data"].as_array().unwrap().len(), 1);
        // Clean up test catalog
        delete_test_catalog(&database, &test_catalog_name).await;
    }
}
