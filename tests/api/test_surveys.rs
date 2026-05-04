/// Tests for queries endpoints
#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::{test, web, App};
    use boom::api::db::get_test_db_api;
    use boom::api::routes;
    use boom::api::test_utils::{read_json_response, read_str_response};
    use boom::conf::AppConfig;
    use boom::utils::cutouts::{AlertCutout, CutoutStorage};
    use boom::utils::enums::Survey;
    use mongodb::Database;
    use std::collections::HashMap;

    /// Test GET /surveys/{survey_name}/cutouts success case
    #[actix_rt::test]
    async fn test_get_alert_cutouts() {
        let config = AppConfig::from_test_config().unwrap();
        let database: Database = get_test_db_api().await;

        // Insert test cutout data with unique ID
        let ztf_cutouts_storage = config
            .build_cutout_storage(&Survey::Ztf)
            .await
            .expect("Failed to build ZTF cutout storage");
        let test_candid = uuid::Uuid::new_v4().as_u128() as i64;

        let cutouts = AlertCutout {
            candid: test_candid,
            cutout_science: vec![1, 2, 3],
            cutout_template: vec![4, 5, 6],
            cutout_difference: vec![7, 8, 9],
        };

        ztf_cutouts_storage
            .insert_cutouts(cutouts)
            .await
            .expect("Failed to store test cutout");

        let mut cutout_storage_map: HashMap<Survey, CutoutStorage> = HashMap::new();
        cutout_storage_map.insert(Survey::Ztf, ztf_cutouts_storage);

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(config.clone()))
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(cutout_storage_map))
                .service(routes::surveys::cutouts::get_cutouts),
        )
        .await;

        let req = test::TestRequest::get()
            .uri(&format!("/surveys/ztf/cutouts?candid={}", test_candid))
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
        config
            .build_cutout_storage(&Survey::Ztf)
            .await
            .expect("Failed to build ZTF cutout storage for cleanup")
            .delete_cutouts(test_candid)
            .await
            .expect("Failed to delete test cutout");

        // Test retrieval of non-existent candid
        let req = test::TestRequest::get()
            .uri("/surveys/ztf/cutouts?candid=8888888888")
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "Should return 404 for non-existent candid"
        );
    }
}
