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
        create_test_catalog, create_test_user, delete_test_catalog, delete_test_user,
        get_admin_auth, read_json_response, test_config_with_crossmatch,
    };
    use boom::conf::{load_dotenv, AppConfig};
    use mongodb::{bson::doc, Database};

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
                .app_data(web::Data::new(AppConfig::from_test_config().unwrap()))
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
                .app_data(web::Data::new(AppConfig::from_test_config().unwrap()))
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
                .app_data(web::Data::new(AppConfig::from_test_config().unwrap()))
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

    fn get_as(token: &str, uri: &str) -> test::TestRequest {
        test::TestRequest::get()
            .uri(uri)
            .insert_header(("Authorization", format!("Bearer {}", token)))
    }

    fn entry<'a>(catalogs: &'a serde_json::Value, name: &str) -> Option<&'a serde_json::Value> {
        catalogs["data"]
            .as_array()
            .unwrap()
            .iter()
            .find(|c| c["name"] == name)
    }

    #[actix_rt::test]
    async fn test_get_catalogs_lists_only_queryable_catalogs() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let (auth, admin_token) = get_admin_auth(&database).await;
        let (user, user_token) = create_test_user(&database, &auth, &[]).await;
        let other_catalog = create_test_catalog(&database).await;
        database.create_collection("ZTF_alerts").await.ok();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth))
                .app_data(web::Data::new(AppConfig::from_test_config().unwrap()))
                .wrap(from_fn(auth_middleware))
                .service(routes::catalogs::get_catalogs)
                .service(routes::catalogs::get_catalog_sample),
        )
        .await;

        for uri in ["/catalogs", "/catalogs?get_details=true"] {
            let catalogs: serde_json::Value =
                test::call_and_read_body_json(&app, get_as(&user_token, uri).to_request()).await;
            assert!(entry(&catalogs, &other_catalog).is_none());
            assert_eq!(entry(&catalogs, "LSPSC").unwrap()["crossmatch"], true);
            assert_eq!(entry(&catalogs, "ZTF_alerts").unwrap()["crossmatch"], false);
        }

        let catalogs: serde_json::Value =
            test::call_and_read_body_json(&app, get_as(&admin_token, "/catalogs").to_request())
                .await;
        assert_eq!(
            entry(&catalogs, &other_catalog).unwrap()["crossmatch"],
            false
        );
        assert_eq!(entry(&catalogs, "LSPSC").unwrap()["crossmatch"], true);

        let sample_uri = format!("/catalogs/{}/sample", other_catalog);
        let resp = test::call_service(&app, get_as(&user_token, &sample_uri).to_request()).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let resp = test::call_service(&app, get_as(&admin_token, &sample_uri).to_request()).await;
        assert_eq!(resp.status(), StatusCode::OK);

        delete_test_catalog(&database, &other_catalog).await;
        delete_test_user(&database, &user).await;
    }

    #[actix_rt::test]
    async fn test_crossmatched_watchlist_keeps_its_acl() {
        load_dotenv();
        let database: Database = get_test_db_api().await;
        let (auth, admin_token) = get_admin_auth(&database).await;
        let watchlist = format!("watchlist_xmatch_{}", uuid::Uuid::new_v4().simple());
        database
            .collection(&watchlist)
            .insert_one(doc! { "test_field": "test_value" })
            .await
            .unwrap();
        let (outsider, outsider_token) = create_test_user(&database, &auth, &[]).await;
        let (member, member_token) = create_test_user(&database, &auth, &[&watchlist]).await;
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(auth))
                .app_data(web::Data::new(test_config_with_crossmatch(&[&watchlist])))
                .wrap(from_fn(auth_middleware))
                .service(routes::catalogs::get_catalogs)
                .service(routes::queries::find::post_find_query),
        )
        .await;
        let find_as = |token: &str| {
            test::TestRequest::post()
                .uri("/queries/find")
                .insert_header(("Authorization", format!("Bearer {}", token)))
                .set_json(serde_json::json!({ "catalog_name": watchlist, "filter": {} }))
                .to_request()
        };

        let catalogs: serde_json::Value =
            test::call_and_read_body_json(&app, get_as(&outsider_token, "/catalogs").to_request())
                .await;
        assert!(entry(&catalogs, &watchlist).is_none());
        let resp = test::call_service(&app, find_as(&outsider_token)).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        for token in [&member_token, &admin_token] {
            let catalogs: serde_json::Value =
                test::call_and_read_body_json(&app, get_as(token, "/catalogs").to_request()).await;
            assert_eq!(entry(&catalogs, &watchlist).unwrap()["crossmatch"], false);
            let resp = test::call_service(&app, find_as(token)).await;
            assert_eq!(resp.status(), StatusCode::OK);
        }

        delete_test_catalog(&database, &watchlist).await;
        delete_test_user(&database, &outsider).await;
        delete_test_user(&database, &member).await;
    }
}
