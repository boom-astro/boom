/// Tests for queries endpoints
#[cfg(test)]
mod tests {
    use actix_web::http::StatusCode;
    use actix_web::{test, web, App};
    use boom::api::db::get_test_db_api;
    use boom::api::routes;
    use boom::api::test_utils::{
        create_test_catalog, delete_test_catalog, read_json_response, read_str_response,
    };
    use mongodb::bson::doc;
    use mongodb::Database;

    /// Test GET /surveys/{survey_name}/cutouts success case
    #[actix_rt::test]
    async fn test_get_alert_cutouts() {
        let database: Database = get_test_db_api().await;

        // Insert test cutout data with unique ID
        let cutouts_collection =
            database.collection::<boom::alert::AlertCutout>("ZTF_alerts_cutouts");
        let test_candid: i64 = uuid::Uuid::new_v4().as_u128() as i64;

        let cutout = boom::alert::AlertCutout {
            candid: test_candid,
            cutout_science: vec![1, 2, 3, 4, 5],
            cutout_template: vec![6, 7, 8, 9, 10],
            cutout_difference: vec![11, 12, 13, 14, 15],
        };

        cutouts_collection
            .insert_one(&cutout)
            .await
            .expect("Failed to insert test cutout");

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::queries::get_cutouts),
        )
        .await;

        let req = test::TestRequest::get()
            .uri(&format!("/queries/ztf/cutouts?candid={}", test_candid))
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
        cutouts_collection
            .delete_one(doc! { "_id": test_candid })
            .await
            .expect("Failed to delete test cutout");

        // Test retrieval of non-existent candid
        let req = test::TestRequest::get()
            .uri("/queries/ztf/cutouts?candid=8888888888")
            .to_request();

        let resp = test::call_service(&app, req).await;
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "Should return 404 for non-existent candid"
        );
    }
  }