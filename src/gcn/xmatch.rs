use crate::gcn::{cache::ActiveEventsCache, EventMatch, GcnEvent};
use std::sync::Arc;
use tracing::{debug, instrument};

/// Error type for event crossmatch operations
#[derive(thiserror::Error, Debug)]
pub enum EventXmatchError {
    #[error("cache error: {0}")]
    Cache(#[from] crate::gcn::cache::CacheError),
}

/// Match an alert position against active events in the cache.
///
/// This function:
/// 1. Gets candidate events from the cache
/// 2. Checks if the alert position falls within each event's geometry
/// 3. Returns matches with distance/probability information
///
/// # Arguments
/// * `ra` - Alert right ascension in degrees
/// * `dec` - Alert declination in degrees
/// * `alert_jd` - Alert Julian Date
/// * `cache` - Active events cache
///
/// # Returns
/// Vector of EventMatch structs for all matching events
#[instrument(skip(cache), fields(ra = %ra, dec = %dec))]
pub async fn event_xmatch(
    ra: f64,
    dec: f64,
    alert_jd: f64,
    cache: &ActiveEventsCache,
) -> Result<Vec<EventMatch>, EventXmatchError> {
    // Refresh cache if needed
    cache.refresh_if_needed().await?;

    // Get candidate events for this alert time (Arc references — no deep clone)
    let candidates = cache.get_candidates_for_time(alert_jd).await;

    let matches = event_xmatch_from_refs(ra, dec, alert_jd, &candidates);

    if !matches.is_empty() {
        debug!(
            match_count = matches.len(),
            "Alert matched {} events", matches.len()
        );
    }

    Ok(matches)
}

/// Core matching logic shared by async and sync paths.
fn event_xmatch_from_refs(
    ra: f64,
    dec: f64,
    alert_jd: f64,
    events: &[Arc<GcnEvent>],
) -> Vec<EventMatch> {
    let mut matches = Vec::new();
    for event in events {
        if event.matches(ra, dec, alert_jd) {
            matches.push(EventMatch::new(event, ra, dec, alert_jd));
        }
    }
    matches
}

/// Synchronous version of event_xmatch for use in contexts where async isn't available.
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
    use crate::gcn::{EventGeometry, GcnEventType, GcnSource};
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
            expires_at: trigger_jd + 30.0, // 30 days
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
        let event_ra = 180.0;
        let event_dec = 45.0;
        let radius = 1.0; // 1 degree
        let trigger_jd = 2460000.0;

        let event = create_test_event(event_ra, event_dec, radius, trigger_jd);
        let events = vec![event];

        // Alert very close to event center
        let alert_ra = 180.1;
        let alert_dec = 45.05;
        let alert_jd = trigger_jd + 1.0;

        let matches = event_xmatch_sync(alert_ra, alert_dec, alert_jd, &events);
        assert_eq!(matches.len(), 1);
        assert!(matches[0].distance_deg.unwrap() < radius);
    }

    #[test]
    fn test_xmatch_sync_no_match_outside_radius() {
        let event_ra = 180.0;
        let event_dec = 45.0;
        let radius = 1.0; // 1 degree
        let trigger_jd = 2460000.0;

        let event = create_test_event(event_ra, event_dec, radius, trigger_jd);
        let events = vec![event];

        // Alert far from event center
        let alert_ra = 190.0;
        let alert_dec = 50.0;
        let alert_jd = trigger_jd + 1.0;

        let matches = event_xmatch_sync(alert_ra, alert_dec, alert_jd, &events);
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn test_xmatch_sync_no_match_expired() {
        let event_ra = 180.0;
        let event_dec = 45.0;
        let radius = 1.0;
        let trigger_jd = 2460000.0;

        let mut event = create_test_event(event_ra, event_dec, radius, trigger_jd);
        event.expires_at = trigger_jd + 5.0; // Expires in 5 days
        let events = vec![event];

        // Alert after expiration
        let alert_ra = 180.0;
        let alert_dec = 45.0;
        let alert_jd = trigger_jd + 10.0; // 10 days later

        let matches = event_xmatch_sync(alert_ra, alert_dec, alert_jd, &events);
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn test_xmatch_sync_no_match_inactive() {
        let event_ra = 180.0;
        let event_dec = 45.0;
        let radius = 1.0;
        let trigger_jd = 2460000.0;

        let mut event = create_test_event(event_ra, event_dec, radius, trigger_jd);
        event.is_active = false;
        let events = vec![event];

        // Alert at event center
        let alert_ra = 180.0;
        let alert_dec = 45.0;
        let alert_jd = trigger_jd + 1.0;

        let matches = event_xmatch_sync(alert_ra, alert_dec, alert_jd, &events);
        assert_eq!(matches.len(), 0);
    }

    #[test]
    fn test_xmatch_sync_multiple_events() {
        let trigger_jd = 2460000.0;

        let events = vec![
            create_test_event(180.0, 45.0, 1.0, trigger_jd),
            create_test_event(180.5, 45.0, 2.0, trigger_jd), // Overlaps with first
            create_test_event(200.0, 60.0, 1.0, trigger_jd), // Far away
        ];

        // Alert in the overlap region of first two events
        let alert_ra = 180.2;
        let alert_dec = 45.0;
        let alert_jd = trigger_jd + 1.0;

        let matches = event_xmatch_sync(alert_ra, alert_dec, alert_jd, &events);
        assert_eq!(matches.len(), 2);
    }
}
