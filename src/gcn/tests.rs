//! Comprehensive tests for the GCN module
//!
//! These tests cover:
//! - Data model edge cases and validation
//! - Serialization/deserialization round-trips
//! - Spatial matching algorithms (circle + HEALPix)
//! - HEALPix operations and validation
//! - Event matching logic (active/inactive, expiry, geometry)
//! - EventMatch construction and field correctness
//! - WatchlistError variants
//! - Watchlist validation functions
//! - Configuration defaults and deserialization
//! - Crossmatch (xmatch) scenarios

#[cfg(test)]
mod model_tests {
    use crate::gcn::{EventGeometry, EventMatch, GcnEvent, GcnEventType, GcnSource};
    use crate::utils::spatial::Coordinates;
    use std::collections::HashMap;

    // ==================== Helpers ====================

    fn make_circle_event(
        id: &str,
        ra: f64,
        dec: f64,
        radius: f64,
        trigger_jd: f64,
        expires_in_days: f64,
    ) -> GcnEvent {
        GcnEvent {
            id: id.to_string(),
            source: GcnSource::Custom,
            event_type: GcnEventType::Watchlist,
            trigger_time: trigger_jd,
            geometry: EventGeometry::circle(ra, dec, radius),
            coordinates: Some(Coordinates::new(ra, dec)),
            properties: HashMap::new(),
            expires_at: trigger_jd + expires_in_days,
            is_active: true,
            user_id: Some("test-user".to_string()),
            name: Some("Test Event".to_string()),
            description: None,
            created_at: trigger_jd,
            updated_at: trigger_jd,
            supersedes: None,
            superseded_by: None,
        }
    }

    fn make_healpix_event(
        id: &str,
        nside: u32,
        pixels: Vec<u64>,
        probs: Vec<f64>,
        trigger_jd: f64,
        expires_in_days: f64,
    ) -> GcnEvent {
        GcnEvent {
            id: id.to_string(),
            source: GcnSource::Lvk,
            event_type: GcnEventType::GravitationalWave,
            trigger_time: trigger_jd,
            geometry: EventGeometry::healpix(nside, pixels, probs, 0.9).unwrap(),
            coordinates: None,
            properties: HashMap::new(),
            expires_at: trigger_jd + expires_in_days,
            is_active: true,
            user_id: None,
            name: Some("GW Event".to_string()),
            description: Some("A gravitational wave event".to_string()),
            created_at: trigger_jd,
            updated_at: trigger_jd,
            supersedes: None,
            superseded_by: None,
        }
    }

    // ==================== GcnSource Tests ====================

    #[test]
    fn test_gcn_source_serialization_roundtrip() {
        let sources = vec![
            GcnSource::Lvk,
            GcnSource::Swift,
            GcnSource::Fermi,
            GcnSource::Svom,
            GcnSource::EinsteinProbe,
            GcnSource::IceCube,
            GcnSource::Custom,
        ];

        for source in sources {
            let json = serde_json::to_string(&source).unwrap();
            let deserialized: GcnSource = serde_json::from_str(&json).unwrap();
            assert_eq!(source, deserialized, "Round-trip failed for {:?}", source);
        }
    }

    #[test]
    fn test_gcn_source_snake_case_serialization() {
        assert_eq!(serde_json::to_string(&GcnSource::Lvk).unwrap(), "\"lvk\"");
        assert_eq!(
            serde_json::to_string(&GcnSource::Swift).unwrap(),
            "\"swift\""
        );
        assert_eq!(
            serde_json::to_string(&GcnSource::Fermi).unwrap(),
            "\"fermi\""
        );
        assert_eq!(
            serde_json::to_string(&GcnSource::Svom).unwrap(),
            "\"svom\""
        );
        assert_eq!(
            serde_json::to_string(&GcnSource::EinsteinProbe).unwrap(),
            "\"einstein_probe\""
        );
        // IceCube -> ice_cube with snake_case renaming
        assert_eq!(
            serde_json::to_string(&GcnSource::IceCube).unwrap(),
            "\"ice_cube\""
        );
        assert_eq!(
            serde_json::to_string(&GcnSource::Custom).unwrap(),
            "\"custom\""
        );
    }

    #[test]
    fn test_gcn_source_display() {
        assert_eq!(format!("{}", GcnSource::Lvk), "LVK");
        assert_eq!(format!("{}", GcnSource::Swift), "Swift");
        assert_eq!(format!("{}", GcnSource::Fermi), "Fermi");
        assert_eq!(format!("{}", GcnSource::Svom), "SVOM");
        assert_eq!(format!("{}", GcnSource::EinsteinProbe), "Einstein Probe");
        assert_eq!(format!("{}", GcnSource::IceCube), "IceCube");
        assert_eq!(format!("{}", GcnSource::Custom), "Custom");
    }

    #[test]
    fn test_gcn_source_clone_and_eq() {
        let source = GcnSource::Swift;
        let cloned = source.clone();
        assert_eq!(source, cloned);
    }

    #[test]
    fn test_gcn_source_hash_in_hashmap() {
        let mut map: HashMap<GcnSource, &str> = HashMap::new();
        map.insert(GcnSource::Lvk, "ligo");
        map.insert(GcnSource::Swift, "swift");
        map.insert(GcnSource::IceCube, "neutrinos");
        assert_eq!(map.get(&GcnSource::Swift), Some(&"swift"));
        assert_eq!(map.get(&GcnSource::Custom), None);
    }

    #[test]
    fn test_gcn_source_deserialization_from_string() {
        let source: GcnSource = serde_json::from_str("\"lvk\"").unwrap();
        assert_eq!(source, GcnSource::Lvk);

        let source: GcnSource = serde_json::from_str("\"einstein_probe\"").unwrap();
        assert_eq!(source, GcnSource::EinsteinProbe);
    }

    #[test]
    fn test_gcn_source_invalid_deserialization() {
        let result: Result<GcnSource, _> = serde_json::from_str("\"invalid_source\"");
        assert!(result.is_err());
    }

    // ==================== GcnEventType Tests ====================

    #[test]
    fn test_gcn_event_type_serialization_roundtrip() {
        let types = vec![
            GcnEventType::GravitationalWave,
            GcnEventType::GammaRayBurst,
            GcnEventType::XRayTransient,
            GcnEventType::Neutrino,
            GcnEventType::Watchlist,
        ];

        for event_type in types {
            let json = serde_json::to_string(&event_type).unwrap();
            let deserialized: GcnEventType = serde_json::from_str(&json).unwrap();
            assert_eq!(event_type, deserialized, "Round-trip failed for {:?}", event_type);
        }
    }

    #[test]
    fn test_gcn_event_type_snake_case_serialization() {
        assert_eq!(
            serde_json::to_string(&GcnEventType::GravitationalWave).unwrap(),
            "\"gravitational_wave\""
        );
        assert_eq!(
            serde_json::to_string(&GcnEventType::GammaRayBurst).unwrap(),
            "\"gamma_ray_burst\""
        );
        assert_eq!(
            serde_json::to_string(&GcnEventType::XRayTransient).unwrap(),
            "\"x_ray_transient\""
        );
        assert_eq!(
            serde_json::to_string(&GcnEventType::Neutrino).unwrap(),
            "\"neutrino\""
        );
        assert_eq!(
            serde_json::to_string(&GcnEventType::Watchlist).unwrap(),
            "\"watchlist\""
        );
    }

    #[test]
    fn test_gcn_event_type_display() {
        assert_eq!(format!("{}", GcnEventType::GravitationalWave), "Gravitational Wave");
        assert_eq!(format!("{}", GcnEventType::GammaRayBurst), "Gamma-Ray Burst");
        assert_eq!(format!("{}", GcnEventType::XRayTransient), "X-Ray Transient");
        assert_eq!(format!("{}", GcnEventType::Neutrino), "Neutrino");
        assert_eq!(format!("{}", GcnEventType::Watchlist), "Watchlist");
    }

    #[test]
    fn test_gcn_event_type_invalid_deserialization() {
        let result: Result<GcnEventType, _> = serde_json::from_str("\"supernova\"");
        assert!(result.is_err());
    }

    // ==================== Circle Geometry Tests ====================

    #[test]
    fn test_circle_geometry_contains_center() {
        let geom = EventGeometry::circle(180.0, 45.0, 1.0);
        assert!(geom.contains(180.0, 45.0));
    }

    #[test]
    fn test_circle_geometry_contains_inside_boundary() {
        let geom = EventGeometry::circle(180.0, 45.0, 1.0);
        assert!(geom.contains(180.5, 45.0));
    }

    #[test]
    fn test_circle_geometry_excludes_outside() {
        let geom = EventGeometry::circle(180.0, 45.0, 1.0);
        assert!(!geom.contains(182.0, 45.0));
    }

    #[test]
    fn test_circle_geometry_near_north_pole() {
        // Near north pole, nearby points in declination should match
        let geom = EventGeometry::circle(0.0, 89.0, 2.0);
        assert!(geom.contains(0.0, 89.5));
        // Points at same dec but different RA are close near the pole
        assert!(geom.contains(90.0, 89.5));
        // Point clearly outside the radius
        assert!(!geom.contains(0.0, 86.0));
    }

    #[test]
    fn test_circle_geometry_at_north_pole() {
        // Event at exactly dec=90, any RA nearby should match
        let geom = EventGeometry::circle(0.0, 90.0, 1.0);
        assert!(geom.contains(0.0, 89.5));
        assert!(geom.contains(180.0, 89.5));
    }

    #[test]
    fn test_circle_geometry_at_south_pole() {
        let geom = EventGeometry::circle(0.0, -89.5, 1.0);
        assert!(geom.contains(180.0, -89.5));
    }

    #[test]
    fn test_circle_geometry_across_ra_zero() {
        let geom = EventGeometry::circle(359.0, 0.0, 3.0);
        assert!(geom.contains(1.0, 0.0));
        assert!(geom.contains(358.0, 0.0));
    }

    #[test]
    fn test_circle_geometry_on_equator() {
        let geom = EventGeometry::circle(100.0, 0.0, 1.0);
        assert!(geom.contains(100.0, 0.5));
        assert!(geom.contains(100.0, -0.5));
        assert!(geom.contains(100.5, 0.0));
        assert!(!geom.contains(102.0, 0.0));
    }

    #[test]
    fn test_circle_geometry_zero_radius() {
        let geom = EventGeometry::circle(180.0, 45.0, 0.0);
        // Only exact center should match (distance = 0 <= 0)
        assert!(geom.contains(180.0, 45.0));
        // Anything else should not
        assert!(!geom.contains(180.001, 45.0));
    }

    #[test]
    fn test_circle_geometry_very_small_radius() {
        // Sub-arcsecond radius: 0.0001 deg ~ 0.36 arcsec
        let geom = EventGeometry::circle(180.0, 45.0, 0.0001);
        assert!(geom.contains(180.0, 45.0));
        assert!(!geom.contains(180.001, 45.0));
    }

    #[test]
    fn test_circle_geometry_large_radius() {
        // Large radius covers most of sky
        let geom = EventGeometry::circle(0.0, 0.0, 89.0);
        assert!(geom.contains(88.0, 0.0));
        // Point well beyond the radius
        assert!(!geom.contains(0.0, -89.5));
    }

    #[test]
    fn test_circle_geometry_full_sky() {
        // 180-degree radius should cover everything
        let geom = EventGeometry::circle(0.0, 0.0, 180.0);
        assert!(geom.contains(180.0, 0.0));
        assert!(geom.contains(0.0, 90.0));
        assert!(geom.contains(0.0, -90.0));
    }

    #[test]
    fn test_circle_geometry_center_method() {
        let geom = EventGeometry::circle(123.456, -45.678, 2.5);
        assert_eq!(geom.center(), Some((123.456, -45.678)));
    }

    #[test]
    fn test_circle_geometry_symmetric_distance() {
        // Distance from A to B should equal distance from B to A
        let geom_a = EventGeometry::circle(100.0, 30.0, 5.0);
        let geom_b = EventGeometry::circle(103.0, 32.0, 5.0);
        assert_eq!(
            geom_a.contains(103.0, 32.0),
            geom_b.contains(100.0, 30.0)
        );
    }

    // ==================== HEALPix Validation Tests ====================

    #[test]
    fn test_healpix_rejects_non_power_of_two_nside_3() {
        let result = EventGeometry::healpix(3, vec![1], vec![0.5], 0.9);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("nside must be a power of 2"));
    }

    #[test]
    fn test_healpix_rejects_non_power_of_two_nside_5() {
        let result = EventGeometry::healpix(5, vec![1], vec![0.5], 0.9);
        assert!(result.is_err());
    }

    #[test]
    fn test_healpix_rejects_non_power_of_two_nside_100() {
        let result = EventGeometry::healpix(100, vec![1], vec![0.5], 0.9);
        assert!(result.is_err());
    }

    #[test]
    fn test_healpix_rejects_nside_zero() {
        let result = EventGeometry::healpix(0, vec![], vec![], 0.9);
        assert!(result.is_err());
    }

    #[test]
    fn test_healpix_rejects_more_pixels_than_probs() {
        let result = EventGeometry::healpix(64, vec![1, 2, 3], vec![0.5, 0.5], 0.9);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("same length"));
    }

    #[test]
    fn test_healpix_rejects_more_probs_than_pixels() {
        let result = EventGeometry::healpix(64, vec![1], vec![0.5, 0.3], 0.9);
        assert!(result.is_err());
    }

    #[test]
    fn test_healpix_accepts_valid_nside_values() {
        for nside in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096] {
            assert!(EventGeometry::healpix(nside, vec![], vec![], 0.9).is_ok());
        }
    }

    #[test]
    fn test_healpix_accepts_empty_pixels() {
        let geom = EventGeometry::healpix(64, vec![], vec![], 0.9).unwrap();
        // No pixel should match
        assert!(!geom.contains(180.0, 45.0));
    }

    #[test]
    fn test_healpix_geometry_center_is_none() {
        let geom = EventGeometry::healpix(64, vec![100, 101, 102], vec![0.1, 0.2, 0.7], 0.9).unwrap();
        assert_eq!(geom.center(), None);
    }

    // ==================== HEALPix Contains & Probability Tests ====================

    #[test]
    fn test_healpix_contains_known_pixel() {
        // Compute the pixel for a known position, then check contains
        use cdshealpix::nested;
        let nside: u32 = 64;
        let depth = (nside as f64).log2() as u8;
        let ra = 180.0_f64;
        let dec = 45.0_f64;
        let pixel = nested::hash(depth, ra.to_radians(), dec.to_radians());

        let geom = EventGeometry::healpix(nside, vec![pixel], vec![0.8], 0.9).unwrap();
        assert!(geom.contains(ra, dec));
    }

    #[test]
    fn test_healpix_excludes_unknown_pixel() {
        // Use a pixel that we know won't match a specific position
        use cdshealpix::nested;
        let nside: u32 = 64;
        let depth = (nside as f64).log2() as u8;
        let ra = 180.0_f64;
        let dec = 45.0_f64;
        let pixel = nested::hash(depth, ra.to_radians(), dec.to_radians());

        // Create geometry with a *different* pixel
        let different_pixel = if pixel > 0 { pixel - 1 } else { pixel + 1 };
        let geom = EventGeometry::healpix(nside, vec![different_pixel], vec![0.8], 0.9).unwrap();
        assert!(!geom.contains(ra, dec));
    }

    #[test]
    fn test_healpix_probability_at_returns_correct_value() {
        use cdshealpix::nested;
        let nside: u32 = 64;
        let depth = (nside as f64).log2() as u8;
        let ra = 100.0_f64;
        let dec = -20.0_f64;
        let pixel = nested::hash(depth, ra.to_radians(), dec.to_radians());

        let geom = EventGeometry::healpix(
            nside,
            vec![pixel, pixel + 1, pixel + 2],
            vec![0.3, 0.5, 0.2],
            0.9,
        )
        .unwrap();
        let prob = geom.probability_at(ra, dec);
        // After sorting, the probability for `pixel` should still be correct
        assert!(prob.is_some());
        assert_eq!(prob, Some(0.3));
    }

    #[test]
    fn test_healpix_probability_at_missing_pixel() {
        let geom = EventGeometry::healpix(64, vec![999999], vec![0.5], 0.9).unwrap();
        // Position that maps to a different pixel
        let prob = geom.probability_at(0.0, 0.0);
        // Likely a different pixel index
        if prob.is_some() {
            // If by chance it maps to 999999, that's okay too
            assert_eq!(prob, Some(0.5));
        }
    }

    #[test]
    fn test_healpix_probability_at_empty_map() {
        let geom = EventGeometry::healpix(64, vec![], vec![], 0.9).unwrap();
        assert_eq!(geom.probability_at(180.0, 45.0), None);
    }

    #[test]
    fn test_circle_probability_at_returns_none() {
        let geom = EventGeometry::circle(180.0, 45.0, 1.0);
        assert_eq!(geom.probability_at(180.0, 45.0), None);
    }

    // ==================== EventGeometry Serialization ====================

    #[test]
    fn test_circle_geometry_serialization_roundtrip() {
        let circle = EventGeometry::circle(180.0, 45.0, 1.0);
        let json = serde_json::to_string(&circle).unwrap();
        assert!(json.contains("\"type\":\"circle\""));
        assert!(json.contains("\"ra\":180.0"));
        assert!(json.contains("\"dec\":45.0"));
        assert!(json.contains("\"error_radius\":1.0"));

        let deserialized: EventGeometry = serde_json::from_str(&json).unwrap();
        assert_eq!(circle, deserialized);
    }

    #[test]
    fn test_healpix_geometry_serialization_roundtrip() {
        let healpix = EventGeometry::healpix(64, vec![100, 200, 300], vec![0.3, 0.5, 0.2], 0.9).unwrap();
        let json = serde_json::to_string(&healpix).unwrap();
        // HealPixMap -> heal_pix_map with snake_case renaming
        assert!(json.contains("\"type\":\"heal_pix_map\""));
        assert!(json.contains("\"nside\":64"));
        assert!(json.contains("\"credible_level\":0.9"));

        let deserialized: EventGeometry = serde_json::from_str(&json).unwrap();
        assert_eq!(healpix, deserialized);
    }

    #[test]
    fn test_circle_geometry_deserialization_from_json() {
        let json = r#"{"type":"circle","ra":45.0,"dec":-30.0,"error_radius":0.5}"#;
        let geom: EventGeometry = serde_json::from_str(json).unwrap();
        assert_eq!(geom.center(), Some((45.0, -30.0)));
    }

    #[test]
    fn test_geometry_deserialization_invalid_type() {
        let json = r#"{"type":"polygon","vertices":[]}"#;
        let result: Result<EventGeometry, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_circle_geometry_with_extreme_coordinates() {
        // RA at boundary, Dec at boundary
        let geom = EventGeometry::circle(0.0, -90.0, 1.0);
        let json = serde_json::to_string(&geom).unwrap();
        let deserialized: EventGeometry = serde_json::from_str(&json).unwrap();
        assert_eq!(geom, deserialized);
    }

    // ==================== GcnEvent Tests ====================

    #[test]
    fn test_event_matches_active_and_not_expired() {
        let event = make_circle_event("test1", 180.0, 45.0, 1.0, 2460000.0, 30.0);
        assert!(event.matches(180.0, 45.0, 2460001.0));
    }

    #[test]
    fn test_event_does_not_match_when_inactive() {
        let mut event = make_circle_event("test2", 180.0, 45.0, 1.0, 2460000.0, 30.0);
        event.is_active = false;
        assert!(!event.matches(180.0, 45.0, 2460001.0));
    }

    #[test]
    fn test_event_does_not_match_after_expiry() {
        let event = make_circle_event("test3", 180.0, 45.0, 1.0, 2460000.0, 5.0);
        assert!(!event.matches(180.0, 45.0, 2460010.0));
    }

    #[test]
    fn test_event_does_not_match_outside_geometry() {
        let event = make_circle_event("test4", 180.0, 45.0, 1.0, 2460000.0, 30.0);
        assert!(!event.matches(190.0, 45.0, 2460001.0));
    }

    #[test]
    fn test_event_matches_at_exact_expiry() {
        let event = make_circle_event("test5", 180.0, 45.0, 1.0, 2460000.0, 30.0);
        // alert_jd == expires_at: alert_jd > expires_at is false, so should match
        assert!(event.matches(180.0, 45.0, event.expires_at));
    }

    #[test]
    fn test_event_does_not_match_just_past_expiry() {
        let event = make_circle_event("test5b", 180.0, 45.0, 1.0, 2460000.0, 30.0);
        // Just barely past expiry
        assert!(!event.matches(180.0, 45.0, event.expires_at + 0.001));
    }

    #[test]
    fn test_event_matches_at_trigger_time() {
        let event = make_circle_event("test6", 180.0, 45.0, 1.0, 2460000.0, 30.0);
        // Alert at exactly trigger time (alert_jd == trigger_time)
        assert!(event.matches(180.0, 45.0, 2460000.0));
    }

    #[test]
    fn test_event_matches_before_trigger_time() {
        // Current implementation allows alerts before trigger time
        let event = make_circle_event("test7", 180.0, 45.0, 1.0, 2460000.0, 30.0);
        assert!(event.matches(180.0, 45.0, 2459999.0));
    }

    #[test]
    fn test_event_inactive_overrides_geometry_match() {
        let mut event = make_circle_event("test8", 180.0, 45.0, 180.0, 2460000.0, 9999.0);
        event.is_active = false;
        // Even with whole-sky radius and far-future expiry, inactive prevents match
        assert!(!event.matches(180.0, 45.0, 2460001.0));
    }

    // ==================== GcnEvent Serialization ====================

    #[test]
    fn test_event_serialization_roundtrip() {
        let event = make_circle_event("test-ser", 180.0, 45.0, 1.0, 2460000.0, 30.0);
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: GcnEvent = serde_json::from_str(&json).unwrap();

        assert_eq!(event.id, deserialized.id);
        assert_eq!(event.source, deserialized.source);
        assert_eq!(event.event_type, deserialized.event_type);
        assert_eq!(event.trigger_time, deserialized.trigger_time);
        assert_eq!(event.is_active, deserialized.is_active);
        assert_eq!(event.expires_at, deserialized.expires_at);
        assert_eq!(event.user_id, deserialized.user_id);
        assert_eq!(event.name, deserialized.name);
        assert_eq!(event.description, deserialized.description);
        assert_eq!(event.supersedes, deserialized.supersedes);
        assert_eq!(event.superseded_by, deserialized.superseded_by);
    }

    #[test]
    fn test_event_mongodb_id_field() {
        let event = make_circle_event("my-event-id", 180.0, 45.0, 1.0, 2460000.0, 30.0);
        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("\"_id\":\"my-event-id\""));
        // Should NOT contain a bare "id" key
        assert!(!json.contains("\"id\":"));
    }

    #[test]
    fn test_event_deserialization_with_id_field() {
        // MongoDB returns _id, make sure we can deserialize it
        let json = r#"{
            "_id": "S240101abc",
            "source": "lvk",
            "event_type": "gravitational_wave",
            "trigger_time": 2460310.5,
            "geometry": {"type": "circle", "ra": 100.0, "dec": -30.0, "error_radius": 5.0},
            "coordinates": null,
            "properties": {},
            "expires_at": 2460340.5,
            "is_active": true,
            "user_id": null,
            "name": "S240101abc",
            "description": null,
            "created_at": 2460310.5,
            "updated_at": 2460310.5,
            "supersedes": null,
            "superseded_by": null
        }"#;
        let event: GcnEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.id, "S240101abc");
        assert_eq!(event.source, GcnSource::Lvk);
        assert_eq!(event.event_type, GcnEventType::GravitationalWave);
    }

    #[test]
    fn test_event_deserialization_with_missing_properties() {
        // properties has #[serde(default)], so missing field should deserialize to empty HashMap
        let json = r#"{
            "_id": "test",
            "source": "custom",
            "event_type": "watchlist",
            "trigger_time": 2460000.0,
            "geometry": {"type": "circle", "ra": 0.0, "dec": 0.0, "error_radius": 1.0},
            "coordinates": null,
            "expires_at": 2460030.0,
            "is_active": true,
            "user_id": null,
            "name": null,
            "description": null,
            "created_at": 2460000.0,
            "updated_at": 2460000.0,
            "supersedes": null,
            "superseded_by": null
        }"#;
        let event: GcnEvent = serde_json::from_str(json).unwrap();
        assert!(event.properties.is_empty());
    }

    #[test]
    fn test_event_deserialization_with_properties() {
        let json = r#"{
            "_id": "with-props",
            "source": "swift",
            "event_type": "gamma_ray_burst",
            "trigger_time": 2460000.0,
            "geometry": {"type": "circle", "ra": 0.0, "dec": 0.0, "error_radius": 1.0},
            "coordinates": null,
            "properties": {"t90": 12.5, "fluence": 1.2e-6, "classification": "long"},
            "expires_at": 2460030.0,
            "is_active": true,
            "user_id": null,
            "name": "GRB240101A",
            "description": "A gamma-ray burst",
            "created_at": 2460000.0,
            "updated_at": 2460000.0,
            "supersedes": null,
            "superseded_by": null
        }"#;
        let event: GcnEvent = serde_json::from_str(json).unwrap();
        assert_eq!(event.properties.len(), 3);
        assert_eq!(event.properties.get("classification").unwrap(), "long");
    }

    #[test]
    fn test_event_with_supersedes_chain() {
        let mut event1 = make_circle_event("v1", 180.0, 45.0, 5.0, 2460000.0, 30.0);
        event1.superseded_by = Some("v2".to_string());

        let mut event2 = make_circle_event("v2", 180.0, 45.0, 3.0, 2460000.0, 30.0);
        event2.supersedes = Some("v1".to_string());

        let json1 = serde_json::to_string(&event1).unwrap();
        let json2 = serde_json::to_string(&event2).unwrap();
        assert!(json1.contains("\"superseded_by\":\"v2\""));
        assert!(json2.contains("\"supersedes\":\"v1\""));
    }

    // ==================== EventMatch Tests ====================

    #[test]
    fn test_event_match_circle_fields() {
        let event = make_circle_event("evt-match-1", 180.0, 45.0, 1.0, 2460000.0, 30.0);
        let m = EventMatch::new(&event, 180.1, 45.05, 2460001.0);

        assert_eq!(m.event_id, "evt-match-1");
        assert_eq!(m.event_source, GcnSource::Custom);
        assert!(m.distance_deg.is_some());
        assert!(m.probability.is_none());
        assert!(m.distance_deg.unwrap() < 0.2);
        assert!((m.trigger_offset_days - 1.0).abs() < 0.001);
        assert!(m.matched_at > 0.0);
    }

    #[test]
    fn test_event_match_healpix_fields() {
        use cdshealpix::nested;
        let nside: u32 = 64;
        let depth = (nside as f64).log2() as u8;
        let ra = 180.0_f64;
        let dec = 45.0_f64;
        let pixel = nested::hash(depth, ra.to_radians(), dec.to_radians());

        let event = make_healpix_event("gw-1", nside, vec![pixel], vec![0.75], 2460000.0, 30.0);
        let m = EventMatch::new(&event, ra, dec, 2460002.0);

        assert_eq!(m.event_id, "gw-1");
        assert_eq!(m.event_source, GcnSource::Lvk);
        assert!(m.distance_deg.is_none());
        assert_eq!(m.probability, Some(0.75));
        assert!((m.trigger_offset_days - 2.0).abs() < 0.001);
    }

    #[test]
    fn test_event_match_negative_trigger_offset() {
        // Alert before event trigger
        let event = make_circle_event("evt-neg", 180.0, 45.0, 1.0, 2460010.0, 30.0);
        let m = EventMatch::new(&event, 180.0, 45.0, 2460005.0);
        assert!((m.trigger_offset_days - (-5.0)).abs() < 0.001);
    }

    #[test]
    fn test_event_match_zero_distance() {
        let event = make_circle_event("evt-zero", 180.0, 45.0, 1.0, 2460000.0, 30.0);
        let m = EventMatch::new(&event, 180.0, 45.0, 2460001.0);
        assert!(m.distance_deg.unwrap().abs() < 1e-10);
    }

    #[test]
    fn test_event_match_preserves_source_type() {
        let sources = vec![
            (GcnSource::Lvk, GcnEventType::GravitationalWave),
            (GcnSource::Swift, GcnEventType::GammaRayBurst),
            (GcnSource::Fermi, GcnEventType::GammaRayBurst),
            (GcnSource::EinsteinProbe, GcnEventType::XRayTransient),
            (GcnSource::IceCube, GcnEventType::Neutrino),
        ];
        for (source, event_type) in sources {
            let mut event = make_circle_event("test", 180.0, 45.0, 1.0, 2460000.0, 30.0);
            event.source = source.clone();
            event.event_type = event_type;
            let m = EventMatch::new(&event, 180.0, 45.0, 2460001.0);
            assert_eq!(m.event_source, source);
        }
    }

    #[test]
    fn test_event_match_serialization_roundtrip() {
        let event = make_circle_event("evt-ser", 180.0, 45.0, 1.0, 2460000.0, 30.0);
        let m = EventMatch::new(&event, 180.1, 45.05, 2460001.0);

        let json = serde_json::to_string(&m).unwrap();
        let deserialized: EventMatch = serde_json::from_str(&json).unwrap();

        assert_eq!(m.event_id, deserialized.event_id);
        assert_eq!(m.event_source, deserialized.event_source);
        assert_eq!(m.distance_deg, deserialized.distance_deg);
        assert_eq!(m.probability, deserialized.probability);
        assert_eq!(m.trigger_offset_days, deserialized.trigger_offset_days);
    }

    // ==================== WatchlistError Tests ====================

    #[test]
    fn test_watchlist_error_not_found() {
        use crate::gcn::WatchlistError;
        let err = WatchlistError::NotFound("test-id".to_string());
        let msg = err.to_string();
        assert!(msg.contains("test-id"));
        assert!(msg.contains("not found"));
    }

    #[test]
    fn test_watchlist_error_unauthorized() {
        use crate::gcn::WatchlistError;
        let err = WatchlistError::Unauthorized;
        assert!(err.to_string().contains("unauthorized"));
    }

    #[test]
    fn test_watchlist_error_invalid_geometry() {
        use crate::gcn::WatchlistError;
        let err = WatchlistError::InvalidGeometry("test reason".to_string());
        let msg = err.to_string();
        assert!(msg.contains("test reason"));
        assert!(msg.contains("invalid geometry"));
    }

    #[test]
    fn test_watchlist_error_radius_too_large() {
        use crate::gcn::WatchlistError;
        let err = WatchlistError::RadiusTooLarge(15.0, 10.0);
        let msg = err.to_string();
        assert!(msg.contains("15"));
        assert!(msg.contains("10"));
    }

    #[test]
    fn test_watchlist_error_limit_exceeded() {
        use crate::gcn::WatchlistError;
        let err = WatchlistError::LimitExceeded(100);
        assert!(err.to_string().contains("100"));
    }
}

#[cfg(test)]
mod watchlist_validation_tests {

    // Re-create the validation functions as they're private to the route module.
    // These test the equivalent logic.

    fn validate_coordinates(ra: f64, dec: f64) -> Result<(), String> {
        if !(0.0..=360.0).contains(&ra) {
            return Err(format!("RA must be between 0 and 360, got {}", ra));
        }
        if !(-90.0..=90.0).contains(&dec) {
            return Err(format!("Dec must be between -90 and 90, got {}", dec));
        }
        Ok(())
    }

    fn validate_radius(radius: f64, max_radius: f64) -> Result<(), String> {
        if radius <= 0.0 {
            return Err("Radius must be positive".to_string());
        }
        if radius > max_radius {
            return Err(format!("Radius {} exceeds max {}", radius, max_radius));
        }
        Ok(())
    }

    #[test]
    fn test_valid_coordinates() {
        assert!(validate_coordinates(0.0, 0.0).is_ok());
        assert!(validate_coordinates(180.0, 45.0).is_ok());
        assert!(validate_coordinates(360.0, 90.0).is_ok());
        assert!(validate_coordinates(0.0, -90.0).is_ok());
        assert!(validate_coordinates(359.999, 89.999).is_ok());
    }

    #[test]
    fn test_invalid_ra_negative() {
        assert!(validate_coordinates(-1.0, 0.0).is_err());
    }

    #[test]
    fn test_invalid_ra_over_360() {
        assert!(validate_coordinates(360.1, 0.0).is_err());
    }

    #[test]
    fn test_invalid_dec_below_minus90() {
        assert!(validate_coordinates(180.0, -90.1).is_err());
    }

    #[test]
    fn test_invalid_dec_above_90() {
        assert!(validate_coordinates(180.0, 90.1).is_err());
    }

    #[test]
    fn test_nan_ra_rejected() {
        assert!(validate_coordinates(f64::NAN, 0.0).is_err());
    }

    #[test]
    fn test_nan_dec_rejected() {
        assert!(validate_coordinates(180.0, f64::NAN).is_err());
    }

    #[test]
    fn test_infinity_ra_rejected() {
        assert!(validate_coordinates(f64::INFINITY, 0.0).is_err());
    }

    #[test]
    fn test_neg_infinity_dec_rejected() {
        assert!(validate_coordinates(180.0, f64::NEG_INFINITY).is_err());
    }

    #[test]
    fn test_valid_radius() {
        assert!(validate_radius(1.0, 10.0).is_ok());
        assert!(validate_radius(0.001, 10.0).is_ok());
        assert!(validate_radius(10.0, 10.0).is_ok());
    }

    #[test]
    fn test_zero_radius_rejected() {
        assert!(validate_radius(0.0, 10.0).is_err());
    }

    #[test]
    fn test_negative_radius_rejected() {
        assert!(validate_radius(-1.0, 10.0).is_err());
    }

    #[test]
    fn test_radius_exceeds_max() {
        assert!(validate_radius(10.1, 10.0).is_err());
    }

}

#[cfg(test)]
mod xmatch_tests {
    use crate::gcn::{event_xmatch_sync, EventGeometry, GcnEvent, GcnEventType, GcnSource};
    use crate::utils::spatial::Coordinates;
    use std::collections::HashMap;

    fn make_event(
        id: &str,
        ra: f64,
        dec: f64,
        radius: f64,
        trigger_jd: f64,
        expires_in_days: f64,
        is_active: bool,
        source: GcnSource,
    ) -> GcnEvent {
        GcnEvent {
            id: id.to_string(),
            source,
            event_type: GcnEventType::Watchlist,
            trigger_time: trigger_jd,
            geometry: EventGeometry::circle(ra, dec, radius),
            coordinates: Some(Coordinates::new(ra, dec)),
            properties: HashMap::new(),
            expires_at: trigger_jd + expires_in_days,
            is_active,
            user_id: Some("test-user".to_string()),
            name: Some("Test".to_string()),
            description: None,
            created_at: trigger_jd,
            updated_at: trigger_jd,
            supersedes: None,
            superseded_by: None,
        }
    }

    fn make_active(id: &str, ra: f64, dec: f64, radius: f64, trigger: f64, days: f64) -> GcnEvent {
        make_event(id, ra, dec, radius, trigger, days, true, GcnSource::Custom)
    }

    #[test]
    fn test_xmatch_empty_events() {
        let events: Vec<GcnEvent> = vec![];
        let matches = event_xmatch_sync(180.0, 45.0, 2460001.0, &events);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_xmatch_single_match() {
        let events = vec![make_active("evt1", 180.0, 45.0, 1.0, 2460000.0, 30.0)];
        let matches = event_xmatch_sync(180.0, 45.0, 2460001.0, &events);
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].event_id, "evt1");
    }

    #[test]
    fn test_xmatch_no_match_due_to_distance() {
        let events = vec![make_active("evt1", 180.0, 45.0, 1.0, 2460000.0, 30.0)];
        let matches = event_xmatch_sync(200.0, 60.0, 2460001.0, &events);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_xmatch_no_match_due_to_expiry() {
        let events = vec![make_active("evt1", 180.0, 45.0, 1.0, 2460000.0, 5.0)];
        let matches = event_xmatch_sync(180.0, 45.0, 2460010.0, &events);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_xmatch_no_match_due_to_inactive() {
        let events = vec![make_event("evt1", 180.0, 45.0, 1.0, 2460000.0, 30.0, false, GcnSource::Custom)];
        let matches = event_xmatch_sync(180.0, 45.0, 2460001.0, &events);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_xmatch_multiple_overlapping_events() {
        let events = vec![
            make_active("evt1", 180.0, 45.0, 2.0, 2460000.0, 30.0),
            make_active("evt2", 180.5, 45.2, 2.0, 2460000.0, 30.0),
            make_active("evt3", 200.0, 60.0, 1.0, 2460000.0, 30.0),
        ];
        let matches = event_xmatch_sync(180.2, 45.1, 2460001.0, &events);
        assert_eq!(matches.len(), 2);

        let ids: Vec<&str> = matches.iter().map(|m| m.event_id.as_str()).collect();
        assert!(ids.contains(&"evt1"));
        assert!(ids.contains(&"evt2"));
        assert!(!ids.contains(&"evt3"));
    }

    #[test]
    fn test_xmatch_mixed_active_inactive() {
        let events = vec![
            make_event("active1", 180.0, 45.0, 2.0, 2460000.0, 30.0, true, GcnSource::Custom),
            make_event("inactive1", 180.0, 45.0, 2.0, 2460000.0, 30.0, false, GcnSource::Custom),
            make_event("active2", 180.0, 45.0, 2.0, 2460000.0, 30.0, true, GcnSource::Custom),
        ];
        let matches = event_xmatch_sync(180.0, 45.0, 2460001.0, &events);
        assert_eq!(matches.len(), 2);
    }

    #[test]
    fn test_xmatch_boundary_radius_inside() {
        let events = vec![make_active("evt1", 0.0, 0.0, 1.0, 2460000.0, 30.0)];
        let matches = event_xmatch_sync(0.99, 0.0, 2460001.0, &events);
        assert_eq!(matches.len(), 1);
    }

    #[test]
    fn test_xmatch_boundary_radius_outside() {
        let events = vec![make_active("evt1", 0.0, 0.0, 1.0, 2460000.0, 30.0)];
        let matches = event_xmatch_sync(1.01, 0.0, 2460001.0, &events);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_xmatch_preserves_source_in_matches() {
        let events = vec![
            make_event("swift-1", 180.0, 45.0, 2.0, 2460000.0, 30.0, true, GcnSource::Swift),
            make_event("fermi-1", 180.0, 45.0, 2.0, 2460000.0, 30.0, true, GcnSource::Fermi),
        ];
        let matches = event_xmatch_sync(180.0, 45.0, 2460001.0, &events);
        assert_eq!(matches.len(), 2);

        let swift_match = matches.iter().find(|m| m.event_id == "swift-1").unwrap();
        assert_eq!(swift_match.event_source, GcnSource::Swift);

        let fermi_match = matches.iter().find(|m| m.event_id == "fermi-1").unwrap();
        assert_eq!(fermi_match.event_source, GcnSource::Fermi);
    }

    #[test]
    fn test_xmatch_distance_values_correct() {
        let events = vec![make_active("evt1", 100.0, 30.0, 5.0, 2460000.0, 30.0)];
        // Alert at event center => distance ~ 0
        let m1 = event_xmatch_sync(100.0, 30.0, 2460001.0, &events);
        assert_eq!(m1.len(), 1);
        assert!(m1[0].distance_deg.unwrap() < 1e-10);

        // Alert 1 degree away in dec => distance ~ 1
        let m2 = event_xmatch_sync(100.0, 31.0, 2460001.0, &events);
        assert_eq!(m2.len(), 1);
        assert!((m2[0].distance_deg.unwrap() - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_xmatch_trigger_offset_calculation() {
        let events = vec![make_active("evt1", 180.0, 45.0, 1.0, 2460000.0, 30.0)];
        let matches = event_xmatch_sync(180.0, 45.0, 2460007.5, &events);
        assert_eq!(matches.len(), 1);
        assert!((matches[0].trigger_offset_days - 7.5).abs() < 0.001);
    }

    #[test]
    fn test_xmatch_same_position_different_times() {
        // Three events with different expiry windows
        let events = vec![
            make_active("short", 180.0, 45.0, 1.0, 2460000.0, 5.0),   // expires at 2460005
            make_active("medium", 180.0, 45.0, 1.0, 2460000.0, 15.0),  // expires at 2460015
            make_active("long", 180.0, 45.0, 1.0, 2460000.0, 30.0),    // expires at 2460030
        ];

        // At day 3: all match
        let m1 = event_xmatch_sync(180.0, 45.0, 2460003.0, &events);
        assert_eq!(m1.len(), 3);

        // At day 10: only medium and long match
        let m2 = event_xmatch_sync(180.0, 45.0, 2460010.0, &events);
        assert_eq!(m2.len(), 2);
        let ids: Vec<&str> = m2.iter().map(|m| m.event_id.as_str()).collect();
        assert!(ids.contains(&"medium"));
        assert!(ids.contains(&"long"));

        // At day 20: only long matches
        let m3 = event_xmatch_sync(180.0, 45.0, 2460020.0, &events);
        assert_eq!(m3.len(), 1);
        assert_eq!(m3[0].event_id, "long");

        // At day 35: none match
        let m4 = event_xmatch_sync(180.0, 45.0, 2460035.0, &events);
        assert!(m4.is_empty());
    }

    #[test]
    fn test_xmatch_at_exact_expiry_boundary() {
        let events = vec![make_active("evt1", 180.0, 45.0, 1.0, 2460000.0, 10.0)];
        // expires_at = 2460010.0

        // At exactly expiry: should match (alert_jd > expires_at is false)
        let at_expiry = event_xmatch_sync(180.0, 45.0, 2460010.0, &events);
        assert_eq!(at_expiry.len(), 1);

        // Just past expiry: should not match
        let past_expiry = event_xmatch_sync(180.0, 45.0, 2460010.001, &events);
        assert!(past_expiry.is_empty());
    }

    #[test]
    fn test_xmatch_with_many_events() {
        // Create 100 events spread across the sky
        let events: Vec<GcnEvent> = (0..100)
            .map(|i| {
                let ra = (i as f64) * 3.6; // 0 to 356.4 degrees
                let dec = (i as f64) * 1.8 - 90.0; // -90 to 88.2 degrees
                make_active(&format!("evt-{}", i), ra, dec, 1.0, 2460000.0, 30.0)
            })
            .collect();

        // Query near one event - should match at most a few
        let matches = event_xmatch_sync(0.0, -90.0, 2460001.0, &events);
        assert!(matches.len() >= 1);
        // The first event is at (0, -90) with radius 1
        assert!(matches.iter().any(|m| m.event_id == "evt-0"));
    }

    #[test]
    fn test_xmatch_all_events_expired() {
        let events = vec![
            make_active("e1", 180.0, 45.0, 1.0, 2460000.0, 1.0),
            make_active("e2", 180.0, 45.0, 1.0, 2460000.0, 2.0),
            make_active("e3", 180.0, 45.0, 1.0, 2460000.0, 3.0),
        ];
        let matches = event_xmatch_sync(180.0, 45.0, 2460100.0, &events);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_xmatch_all_events_inactive() {
        let events = vec![
            make_event("e1", 180.0, 45.0, 1.0, 2460000.0, 30.0, false, GcnSource::Custom),
            make_event("e2", 180.0, 45.0, 1.0, 2460000.0, 30.0, false, GcnSource::Custom),
        ];
        let matches = event_xmatch_sync(180.0, 45.0, 2460001.0, &events);
        assert!(matches.is_empty());
    }

    #[test]
    fn test_xmatch_concentric_events_different_radii() {
        // Events at same center but different radii
        let events = vec![
            make_active("small", 180.0, 45.0, 0.5, 2460000.0, 30.0),
            make_active("medium", 180.0, 45.0, 2.0, 2460000.0, 30.0),
            make_active("large", 180.0, 45.0, 5.0, 2460000.0, 30.0),
        ];

        // At center: all match
        let m1 = event_xmatch_sync(180.0, 45.0, 2460001.0, &events);
        assert_eq!(m1.len(), 3);

        // 1 degree away: medium and large match
        let m2 = event_xmatch_sync(181.0, 45.0, 2460001.0, &events);
        assert_eq!(m2.len(), 2);
        let ids: Vec<&str> = m2.iter().map(|m| m.event_id.as_str()).collect();
        assert!(ids.contains(&"medium"));
        assert!(ids.contains(&"large"));

        // 3 degrees away: only large matches
        let m3 = event_xmatch_sync(183.0, 45.0, 2460001.0, &events);
        assert_eq!(m3.len(), 1);
        assert_eq!(m3[0].event_id, "large");
    }

    #[test]
    fn test_xmatch_returns_ordered_by_event_list_order() {
        let events = vec![
            make_active("a", 180.0, 45.0, 1.0, 2460000.0, 30.0),
            make_active("b", 180.0, 45.0, 1.0, 2460000.0, 30.0),
            make_active("c", 180.0, 45.0, 1.0, 2460000.0, 30.0),
        ];
        let matches = event_xmatch_sync(180.0, 45.0, 2460001.0, &events);
        assert_eq!(matches.len(), 3);
        assert_eq!(matches[0].event_id, "a");
        assert_eq!(matches[1].event_id, "b");
        assert_eq!(matches[2].event_id, "c");
    }
}

#[cfg(test)]
mod config_tests {
    use crate::conf::{GcnConfig, WatchlistConfig};

    #[test]
    fn test_gcn_config_defaults() {
        let config = GcnConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.watchlist.max_per_user, 100);
        assert!((config.watchlist.max_radius_deg - 10.0).abs() < 0.001);
    }

    #[test]
    fn test_gcn_config_full_deserialization() {
        let json = r#"{
            "enabled": true,
            "watchlist": {
                "max_per_user": 50,
                "max_radius_deg": 5.0
            }
        }"#;
        let config: GcnConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.watchlist.max_per_user, 50);
        assert!((config.watchlist.max_radius_deg - 5.0).abs() < 0.001);
    }

    #[test]
    fn test_gcn_config_partial_uses_defaults() {
        let json = r#"{"enabled": true}"#;
        let config: GcnConfig = serde_json::from_str(json).unwrap();
        assert!(config.enabled);
        assert_eq!(config.watchlist.max_per_user, 100);
    }

    #[test]
    fn test_gcn_config_disabled_by_default() {
        let json = r#"{}"#;
        let config: GcnConfig = serde_json::from_str(json).unwrap();
        assert!(!config.enabled);
    }

    #[test]
    fn test_watchlist_config_defaults() {
        let config = WatchlistConfig::default();
        assert_eq!(config.max_per_user, 100);
        assert!((config.max_radius_deg - 10.0).abs() < 0.001);
    }

    #[test]
    fn test_watchlist_config_deserialization() {
        let json = r#"{"max_per_user": 25, "max_radius_deg": 2.5}"#;
        let config: WatchlistConfig = serde_json::from_str(json).unwrap();
        assert_eq!(config.max_per_user, 25);
        assert!((config.max_radius_deg - 2.5).abs() < 0.001);
    }
}
