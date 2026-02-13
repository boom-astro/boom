use crate::watchlist::{EventMatch, GcnEvent};
use tracing::instrument;

/// Match an alert position against a list of events synchronously.
///
/// Checks each event's geometry and time constraints, returning matches
/// with distance/probability information.
///
/// # Arguments
/// * `ra` - Alert right ascension in degrees
/// * `dec` - Alert declination in degrees
/// * `alert_jd` - Alert Julian Date
/// * `events` - Slice of events to check against
///
/// # Returns
/// Vector of EventMatch structs for all matching events
#[instrument(skip(events), fields(ra = %ra, dec = %dec))]
pub fn event_xmatch_sync(
    ra: f64,
    dec: f64,
    alert_jd: f64,
    events: &[GcnEvent],
) -> Vec<EventMatch> {
    let mut matches = Vec::new();

    for event in events {
        if event.matches(ra, dec, alert_jd) {
            let event_match = EventMatch::new(event, ra, dec, alert_jd);
            matches.push(event_match);
        }
    }

    matches
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::watchlist::{EventGeometry, GcnEventType, GcnSource};
    use std::collections::HashMap;

    fn create_test_event(ra: f64, dec: f64, radius: f64, trigger_jd: f64) -> GcnEvent {
        let now = trigger_jd;
        GcnEvent {
            id: uuid::Uuid::new_v4().to_string(),
            source: GcnSource::Custom,
            event_type: GcnEventType::Watchlist,
            trigger_time: trigger_jd,
            geometry: EventGeometry::circle(ra, dec, radius),
            coordinates: Some(crate::utils::spatial::Coordinates::new(ra, dec)),
            properties: HashMap::new(),
            expires_at: trigger_jd + 30.0,
            is_active: true,
            user_id: Some("test-user".to_string()),
            name: Some("Test Event".to_string()),
            description: None,
            created_at: now,
            updated_at: now,
            supersedes: None,
            superseded_by: None,
        }
    }

    #[test]
    fn test_xmatch_sync_match_within_radius() {
        let event = create_test_event(180.0, 45.0, 1.0, 2460000.0);
        let events = vec![event];
        let matches = event_xmatch_sync(180.1, 45.05, 2460001.0, &events);
        assert_eq!(matches.len(), 1);
        assert!(matches[0].distance_deg.unwrap() < 1.0);
    }

    #[test]
    fn test_xmatch_sync_no_match_outside_radius() {
        let event = create_test_event(180.0, 45.0, 1.0, 2460000.0);
        let events = vec![event];
        let matches = event_xmatch_sync(190.0, 50.0, 2460001.0, &events);
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn test_xmatch_sync_no_match_expired() {
        let mut event = create_test_event(180.0, 45.0, 1.0, 2460000.0);
        event.expires_at = 2460005.0;
        let events = vec![event];
        let matches = event_xmatch_sync(180.0, 45.0, 2460010.0, &events);
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn test_xmatch_sync_no_match_inactive() {
        let mut event = create_test_event(180.0, 45.0, 1.0, 2460000.0);
        event.is_active = false;
        let events = vec![event];
        let matches = event_xmatch_sync(180.0, 45.0, 2460001.0, &events);
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn test_xmatch_sync_multiple_events() {
        let trigger_jd = 2460000.0;
        let events = vec![
            create_test_event(180.0, 45.0, 1.0, trigger_jd),
            create_test_event(180.5, 45.0, 2.0, trigger_jd),
            create_test_event(200.0, 60.0, 1.0, trigger_jd),
        ];
        let matches = event_xmatch_sync(180.2, 45.0, trigger_jd + 1.0, &events);
        assert_eq!(matches.len(), 2);
    }
}
