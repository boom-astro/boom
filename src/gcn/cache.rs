use crate::gcn::GcnEvent;
use futures::stream::StreamExt;
use mongodb::bson::doc;
use mongodb::{Collection, Database};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, instrument, warn};

/// Error type for cache operations
#[derive(thiserror::Error, Debug)]
pub enum CacheError {
    #[error("database error: {0}")]
    Database(#[from] mongodb::error::Error),
}

/// In-memory cache of active GCN events for fast alert matching.
///
/// The cache is refreshed periodically from MongoDB to ensure events
/// are up-to-date without querying the database for each alert.
pub struct ActiveEventsCache {
    /// Cached active events and last refresh time, behind a single RwLock
    state: Arc<RwLock<CacheState>>,
    /// Mutex to prevent concurrent refresh operations
    refresh_mutex: Arc<Mutex<()>>,
    /// Database handle for refreshing
    db: Database,
    /// How often to refresh the cache
    refresh_interval: Duration,
}

struct CacheState {
    events: Vec<GcnEvent>,
    last_refresh: Option<Instant>,
}

impl ActiveEventsCache {
    /// Create a new cache with the given database and refresh interval.
    pub fn new(db: Database, refresh_interval_secs: u64) -> Self {
        ActiveEventsCache {
            state: Arc::new(RwLock::new(CacheState {
                events: Vec::new(),
                last_refresh: None,
            })),
            refresh_mutex: Arc::new(Mutex::new(())),
            db,
            refresh_interval: Duration::from_secs(refresh_interval_secs),
        }
    }

    /// Initialize the cache by loading events from the database.
    /// Should be called once at startup before processing alerts.
    #[instrument(skip(self), err)]
    pub async fn initialize(&self) -> Result<(), CacheError> {
        self.refresh_inner().await
    }

    /// Refresh the cache if the refresh interval has elapsed.
    /// Returns true if a refresh was performed, false if skipped.
    ///
    /// Uses a mutex to prevent concurrent refreshes (TOCTOU protection).
    #[instrument(skip(self), err)]
    pub async fn refresh_if_needed(&self) -> Result<bool, CacheError> {
        // Acquire the refresh mutex to prevent concurrent refresh attempts
        let _guard = self.refresh_mutex.lock().await;

        let should_refresh = {
            let state = self.state.read().await;
            match state.last_refresh {
                None => true,
                Some(instant) => instant.elapsed() >= self.refresh_interval,
            }
        };

        if should_refresh {
            self.refresh_inner().await?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Force a cache refresh regardless of the interval.
    #[instrument(skip(self), err)]
    pub async fn force_refresh(&self) -> Result<(), CacheError> {
        let _guard = self.refresh_mutex.lock().await;
        self.refresh_inner().await
    }

    /// Internal refresh implementation.
    /// Caller must hold the refresh_mutex.
    async fn refresh_inner(&self) -> Result<(), CacheError> {
        let now_jd = flare::Time::now().to_jd();

        let gcn_collection: Collection<GcnEvent> = self.db.collection("gcn_events");

        // Query for active, non-expired events
        let filter = doc! {
            "is_active": true,
            "expires_at": { "$gt": now_jd }
        };

        let mut cursor = gcn_collection.find(filter).await?;
        let mut events = Vec::new();

        while let Some(result) = cursor.next().await {
            match result {
                Ok(event) => events.push(event),
                Err(e) => {
                    warn!("Error reading event from cursor: {}", e);
                }
            }
        }

        let count = events.len();

        // Update events and last refresh time atomically
        {
            let mut state = self.state.write().await;
            state.events = events;
            state.last_refresh = Some(Instant::now());
        }

        debug!(event_count = count, "Cache refreshed");
        Ok(())
    }

    /// Get the number of cached events.
    pub async fn len(&self) -> usize {
        self.state.read().await.events.len()
    }

    /// Check if the cache is empty.
    pub async fn is_empty(&self) -> bool {
        self.state.read().await.events.is_empty()
    }

    /// Get events that could potentially match an alert at the given time.
    /// This filters events whose time windows include the alert time.
    pub async fn get_candidates_for_time(&self, alert_jd: f64) -> Vec<GcnEvent> {
        let state = self.state.read().await;
        state
            .events
            .iter()
            .filter(|e| {
                // Event must be active and not expired at alert time
                e.is_active && alert_jd <= e.expires_at
            })
            .cloned()
            .collect()
    }
}

impl Clone for ActiveEventsCache {
    fn clone(&self) -> Self {
        ActiveEventsCache {
            state: Arc::clone(&self.state),
            refresh_mutex: Arc::clone(&self.refresh_mutex),
            db: self.db.clone(),
            refresh_interval: self.refresh_interval,
        }
    }
}

#[cfg(test)]
mod tests {
    // Note: Full testing of ActiveEventsCache requires MongoDB integration tests.
    // The cache functionality is tested as part of the xmatch integration tests.

    #[test]
    fn test_cache_creation() {
        // Placeholder - verify the module compiles correctly.
        // Full testing requires MongoDB integration tests.
    }
}
