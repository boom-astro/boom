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
    use boom::utils::tracks::{
        commit_upsert, plan_upsert, StoredOrbit, StoredTrack, ALIASES_COLLECTION, TRACKS_COLLECTION,
    };
    use mongodb::bson::doc;
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

    /// Test POST /surveys/{survey_name}/cutouts
    #[actix_rt::test]
    async fn test_get_batch_cutouts() {
        let config = AppConfig::from_test_config().unwrap();
        let database: Database = get_test_db_api().await;
        let storage = config
            .build_cutout_storage(&Survey::Ztf)
            .await
            .expect("Failed to build ZTF cutout storage");
        let base = (uuid::Uuid::new_v4().as_u128() as i64).abs() / 16;
        let (first, second, absent) = (base, base + 1, base + 2);
        // Only `first` has an alert, so only it gets a jd.
        let alerts =
            database.collection::<mongodb::bson::Document>(&format!("{}_alerts", Survey::Ztf));
        alerts
            .insert_one(doc! { "_id": first, "candidate": { "jd": 2460000.5 } })
            .await
            .expect("Failed to insert test alert");
        for candid in [first, second] {
            storage
                .insert_cutouts(AlertCutout {
                    candid,
                    cutout_science: vec![1, 2, 3],
                    cutout_template: vec![4, 5, 6],
                    cutout_difference: vec![7, 8, 9],
                })
                .await
                .expect("Failed to store test cutout");
        }

        let mut cutout_storage_map: HashMap<Survey, CutoutStorage> = HashMap::new();
        cutout_storage_map.insert(Survey::Ztf, storage);
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .app_data(web::Data::new(cutout_storage_map))
                .service(routes::surveys::cutouts::get_batch_cutouts),
        )
        .await;

        // Request order is kept, duplicates collapse, and an unknown candid is
        // reported rather than failing the request.
        let req = test::TestRequest::post()
            .uri("/surveys/ztf/cutouts")
            .set_json(serde_json::json!({ "candids": [second, absent, first, second] }))
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = read_json_response(resp).await;
        let candids: Vec<i64> = body["data"]["cutouts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|c| c["candid"].as_i64().unwrap())
            .collect();
        assert_eq!(candids, vec![second, first]);
        assert_eq!(body["data"]["missing"], serde_json::json!([absent]));
        assert_eq!(body["data"]["cutouts"][0]["cutoutScience"], "AQID");
        assert!(body["data"]["cutouts"][0]["jd"].is_null());
        assert_eq!(body["data"]["cutouts"][1]["jd"], 2460000.5);

        for candids in [vec![], (0..101).collect::<Vec<i64>>()] {
            let req = test::TestRequest::post()
                .uri("/surveys/ztf/cutouts")
                .set_json(serde_json::json!({ "candids": candids }))
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        }

        alerts.delete_one(doc! { "_id": first }).await.unwrap();
        let storage = config.build_cutout_storage(&Survey::Ztf).await.unwrap();
        for candid in [first, second] {
            storage.delete_cutouts(candid).await.unwrap();
        }
    }

    async fn insert_track(database: &Database, members: &[i64]) -> String {
        let jds: Vec<f64> = (0..members.len()).map(|k| 2460000.0 + k as f64).collect();
        let plan = plan_upsert(database, members, &jds, None, None, None)
            .await
            .expect("Failed to plan track");
        commit_upsert(database, plan)
            .await
            .expect("Failed to store track")
            .track
            .id
    }

    #[actix_rt::test]
    async fn test_get_track_resolves_a_merged_id() {
        let database: Database = get_test_db_api().await;
        let base = (uuid::Uuid::new_v4().as_u128() as i64).abs() / 16;
        let (a, b) = ([base, base + 1], [base + 2, base + 3]);
        let first = insert_track(&database, &a).await;
        let second = insert_track(&database, &b).await;
        let survivor = insert_track(&database, &[a[0], a[1], b[0], b[1]]).await;
        let absorbed = if survivor == first { &second } else { &first };
        assert_ne!(
            &survivor, absorbed,
            "the merge should keep one of the two ids"
        );

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::surveys::tracks::get_track),
        )
        .await;

        for (id, what) in [(&survivor, "surviving"), (absorbed, "absorbed")] {
            let req = test::TestRequest::get()
                .uri(&format!("/surveys/ztf/tracks/{}", id))
                .to_request();
            let resp = test::call_service(&app, req).await;
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "Should retrieve the {} id: {}",
                what,
                read_str_response(resp).await
            );
            let body = read_json_response(resp).await;
            assert_eq!(body["data"]["_id"].as_str().unwrap(), survivor);
            assert_eq!(body["data"]["n_detections"].as_i64().unwrap(), 4);
        }

        let req = test::TestRequest::get()
            .uri("/surveys/ztf/tracks/BT_MISSING")
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        database
            .collection::<mongodb::bson::Document>(TRACKS_COLLECTION)
            .delete_one(doc! { "_id": &survivor })
            .await
            .unwrap();
        database
            .collection::<mongodb::bson::Document>(ALIASES_COLLECTION)
            .delete_many(doc! { "superseded_by": &survivor })
            .await
            .unwrap();
    }

    fn orbit_at_perihelion(e: f64, q: f64) -> StoredOrbit {
        let k2 = 0.017_202_098_95_f64.powi(2);
        StoredOrbit {
            epoch_jd: 2460000.0,
            pos: [q, 0.0, 0.0],
            vel: [0.0, (k2 * (1.0 + e) / q).sqrt(), 0.0],
        }
    }

    fn listed_track(
        id: String,
        updated_at: f64,
        orbit: Option<StoredOrbit>,
        designation: Option<&str>,
        n_nights: i32,
    ) -> StoredTrack {
        StoredTrack {
            id,
            members: Vec::new(),
            epochs: Vec::new(),
            bound_fit: None,
            bound_fit_residual_arcsec: None,
            bound_fit_detections: None,
            n_detections: 2,
            n_nights,
            arc_days: 1.0,
            first_jd: 2460000.0,
            last_jd: 2460001.0,
            designation: designation.map(str::to_string),
            updated_at,
            orbit,
        }
    }

    #[actix_rt::test]
    async fn test_list_tracks_pages_and_cuts_on_the_orbit() {
        let database: Database = get_test_db_api().await;
        let tag = uuid::Uuid::new_v4().simple().to_string();
        let since = 4e9 + (uuid::Uuid::new_v4().as_u128() % 1_000_000) as f64 * 100.0;
        let ids: Vec<String> = ["a", "b", "c", "d"]
            .iter()
            .map(|k| format!("LIST{tag}{k}"))
            .collect();
        let tracks = vec![
            listed_track(
                ids[0].clone(),
                since,
                Some(orbit_at_perihelion(3.36, 2.0)),
                None,
                3,
            ),
            listed_track(
                ids[1].clone(),
                since + 1.0,
                Some(orbit_at_perihelion(0.5, 1.0)),
                None,
                2,
            ),
            listed_track(
                ids[2].clone(),
                since + 1.0,
                Some(orbit_at_perihelion(0.1, 2.5)),
                Some("2000 AA"),
                4,
            ),
            listed_track(ids[3].clone(), since + 2.0, None, None, 5),
        ];
        let collection = database.collection::<StoredTrack>(TRACKS_COLLECTION);
        collection.insert_many(&tracks).await.unwrap();

        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(database.clone()))
                .service(routes::surveys::tracks::get_tracks),
        )
        .await;
        let list = |query: String| {
            let (app, ids) = (&app, &ids);
            async move {
                let req = test::TestRequest::get()
                    .uri(&format!("/surveys/ztf/tracks?{query}"))
                    .to_request();
                let resp = test::call_service(app, req).await;
                assert_eq!(resp.status(), StatusCode::OK);
                let body = read_json_response(resp).await;
                let listed: Vec<String> = body["data"]["tracks"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|t| t["_id"].as_str().unwrap().to_string())
                    .filter(|id| ids.contains(id))
                    .collect();
                (listed, body["data"]["next"].clone())
            }
        };

        let (first, next) = list(format!("updated_since={since}&limit=2")).await;
        assert_eq!(first, ids[..2]);
        let (second, _) = list(format!(
            "updated_since={}&after_id={}&limit=2",
            next["updated_since"],
            next["after_id"].as_str().unwrap()
        ))
        .await;
        assert_eq!(second, ids[2..]);

        let (unbound, next) = list(format!("updated_since={since}&min_e=1")).await;
        assert_eq!(unbound, ids[..1]);
        assert_eq!(
            next["after_id"],
            ids[3].as_str(),
            "the cursor moves past what the cut rejected"
        );
        let near_earth = list(format!("updated_since={since}&max_q=1.3")).await.0;
        assert_eq!(near_earth, ids[1..2]);
        let unknown = list(format!("updated_since={since}&unknown_only=true"))
            .await
            .0;
        assert_eq!(unknown, [ids[0].clone(), ids[1].clone(), ids[3].clone()]);
        let long = list(format!("updated_since={since}&min_nights=4")).await.0;
        assert_eq!(long, ids[2..]);

        collection
            .delete_many(doc! { "_id": { "$in": &ids } })
            .await
            .unwrap();
    }
}
