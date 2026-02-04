//! Tests for scheduler logging improvements.
//!
//! This module documents the behavior changes made to improve scheduler logging:
//!
//! 1. **Queue empty log level**: Changed from INFO to TRACE to reduce log noise
//!    during normal operation when workers are idle.
//!
//! 2. **Queue empty delay**: Reduced from 2 seconds to 500ms for better latency,
//!    now that log spam is no longer a concern.
//!
//! 3. **Valkey error delay**: Added a 5-second delay after valkey/redis errors
//!    to prevent log spam when the service is unavailable.
//!
//! 4. **Heartbeat format**: Shows live/total worker counts per pool type.
//!    Example: `heartbeat: workers running alert=2/2 enrichment=2/2 filter=1/2`
//!    (the filter=1/2 indicates one filter worker has crashed)

use boom::utils::worker::WorkerType;

/// Test that WorkerType displays correctly for logging.
#[test]
fn test_worker_type_display() {
    assert_eq!(format!("{}", WorkerType::Alert), "Alert");
    assert_eq!(format!("{}", WorkerType::Enrichment), "Enrichment");
    assert_eq!(format!("{}", WorkerType::Filter), "Filter");
}

/// Test that WorkerType can be copied (required for use in ThreadPool).
#[test]
fn test_worker_type_is_copy() {
    let wt1 = WorkerType::Alert;
    let wt2 = wt1; // Copy
    let _wt3 = wt1; // Can still use wt1 after copy
    assert_eq!(format!("{}", wt2), "Alert");
}
