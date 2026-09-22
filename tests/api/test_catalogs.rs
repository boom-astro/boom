// Tests for catalogs endpoints
#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::middleware::from_fn;
    use actix_web::{test, web, App};
    use boom::api::auth::auth_middleware;
    use boom::api::db::get_test_db_api;
    use boom::api::routes;
    use boom::api::test_utils::{
        create_test_catalog, delete_test_catalog, get_admin_auth, read_json_response,
    };
    use boom::conf::load_dotenv;
    use mongodb::Database;

    /// Test GET /catalogs
    #[actix_rt::test]
    async fn test_get_catalogs() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let (auth, token) = get_admin_auth(&database).await;

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth))
                .wrap(from_fn(auth_middleware))
                .service(routes::catalogs::get_catalogs),
        )
        .await;

        let req = test::TestRequest::get()
            .uri("/catalogs")
            .insert_header(("Authorization", format!("Bearer {}", token)))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = read_json_response(resp).await;

        assert!(resp["data"].is_array());
    }
    // next we test the get_catalog_indexes endpoint
    #[actix_rt::test]
    async fn test_get_catalog_indexes() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let (auth, token) = get_admin_auth(&database).await;
        let test_catalog_name = create_test_catalog(&database).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth))
                .wrap(from_fn(auth_middleware))
                .service(routes::catalogs::get_catalog_indexes),
        )
        .await;

        let req = test::TestRequest::get()
            .uri(&format!("/catalogs/{}/indexes", test_catalog_name))
            .insert_header(("Authorization", format!("Bearer {}", token)))
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
        let database: Database = get_test_db_api().await;
        let (auth, token) = get_admin_auth(&database).await;
        let test_catalog_name = create_test_catalog(&database).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth))
                .wrap(from_fn(auth_middleware))
                .service(routes::catalogs::get_catalog_sample),
        )
        .await;
        let req = test::TestRequest::get()
            .uri(&format!("/catalogs/{}/sample", test_catalog_name))
            .insert_header(("Authorization", format!("Bearer {}", token)))
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
