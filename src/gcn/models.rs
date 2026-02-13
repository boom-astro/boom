use crate::utils::spatial::Coordinates;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// GCN event sources/missions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum GcnSource {
    /// LIGO-Virgo-KAGRA gravitational wave events
    Lvk,
    /// Swift satellite (GRB, X-ray)
    Swift,
    /// Fermi satellite (GRB, gamma-ray)
    Fermi,
    /// Space Variable Objects Monitor
    Svom,
    /// Einstein Probe (X-ray transients)
    EinsteinProbe,
    /// IceCube neutrino events
    IceCube,
    /// User-defined watchlist entries
    Custom,
}

impl std::fmt::Display for GcnSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GcnSource::Lvk => write!(f, "LVK"),
            GcnSource::Swift => write!(f, "Swift"),
            GcnSource::Fermi => write!(f, "Fermi"),
            GcnSource::Svom => write!(f, "SVOM"),
            GcnSource::EinsteinProbe => write!(f, "Einstein Probe"),
            GcnSource::IceCube => write!(f, "IceCube"),
            GcnSource::Custom => write!(f, "Custom"),
        }
    }
}

/// GCN event type classification
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum GcnEventType {
    /// Gravitational wave event (LVK)
    GravitationalWave,
    /// Gamma-ray burst
    GammaRayBurst,
    /// X-ray transient
    XRayTransient,
    /// High-energy neutrino event
    Neutrino,
    /// User-defined watchlist position
    Watchlist,
}

impl std::fmt::Display for GcnEventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GcnEventType::GravitationalWave => write!(f, "Gravitational Wave"),
            GcnEventType::GammaRayBurst => write!(f, "Gamma-Ray Burst"),
            GcnEventType::XRayTransient => write!(f, "X-Ray Transient"),
            GcnEventType::Neutrino => write!(f, "Neutrino"),
            GcnEventType::Watchlist => write!(f, "Watchlist"),
        }
    }
}

/// Spatial geometry for event localization.
///
/// **Invariant:** For `HealPixMap`, the `pixels` array is always sorted in
/// ascending order and `probabilities` is reordered to match. This is
/// enforced by the constructor, and by a custom `Deserialize` impl that
/// re-sorts after loading from JSON/BSON.
#[derive(Debug, Clone, Serialize, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EventGeometry {
    /// Simple circular region
    Circle {
        /// Right ascension of center (degrees)
        ra: f64,
        /// Declination of center (degrees)
        dec: f64,
        /// Error radius (degrees)
        error_radius: f64,
    },
    /// HEALPix skymap for complex localizations (GW events)
    HealPixMap {
        /// HEALPix nside parameter (determines resolution)
        nside: u32,
        /// Nested pixel indices within the credible region (sorted for binary search)
        pixels: Vec<u64>,
        /// Per-pixel probabilities (parallel to pixels before sorting; reordered to match)
        probabilities: Vec<f64>,
        /// Credible level (e.g., 0.90 for 90% contour)
        credible_level: f64,
    },
}

/// Raw mirror of `EventGeometry` used only for deserialization, so that we can
/// post-process (sort pixels) without writing a fully manual `Deserialize`.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EventGeometryRaw {
    Circle {
        ra: f64,
        dec: f64,
        error_radius: f64,
    },
    HealPixMap {
        nside: u32,
        pixels: Vec<u64>,
        probabilities: Vec<f64>,
        credible_level: f64,
    },
}

impl<'de> Deserialize<'de> for EventGeometry {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = EventGeometryRaw::deserialize(deserializer)?;
        Ok(match raw {
            EventGeometryRaw::Circle {
                ra,
                dec,
                error_radius,
            } => EventGeometry::Circle {
                ra,
                dec,
                error_radius,
            },
            EventGeometryRaw::HealPixMap {
                nside,
                pixels,
                probabilities,
                credible_level,
            } => {
                // Re-sort pixels to uphold the binary-search invariant even
                // when data comes from an external source (e.g. MongoDB).
                let mut pairs: Vec<(u64, f64)> =
                    pixels.into_iter().zip(probabilities).collect();
                pairs.sort_unstable_by_key(|(px, _)| *px);
                let (sorted_pixels, sorted_probs): (Vec<u64>, Vec<f64>) =
                    pairs.into_iter().unzip();
                EventGeometry::HealPixMap {
                    nside,
                    pixels: sorted_pixels,
                    probabilities: sorted_probs,
                    credible_level,
                }
            }
        })
    }
}

impl EventGeometry {
    /// Create a circular geometry
    pub fn circle(ra: f64, dec: f64, error_radius: f64) -> Self {
        EventGeometry::Circle {
            ra,
            dec,
            error_radius,
        }
    }

    /// Create a HEALPix skymap geometry.
    ///
    /// Pixels are sorted internally for O(log n) lookups. Returns an error
    /// if `nside` is not a power of 2 or if `pixels` and `probabilities`
    /// have different lengths.
    pub fn healpix(
        nside: u32,
        pixels: Vec<u64>,
        probabilities: Vec<f64>,
        credible_level: f64,
    ) -> Result<Self, WatchlistError> {
        if !nside.is_power_of_two() || nside == 0 {
            return Err(WatchlistError::InvalidGeometry(format!(
                "nside must be a power of 2, got {nside}"
            )));
        }
        if pixels.len() != probabilities.len() {
            return Err(WatchlistError::InvalidGeometry(format!(
                "pixels and probabilities must have the same length ({} != {})",
                pixels.len(),
                probabilities.len()
            )));
        }

        // Sort pixels (and probabilities in parallel) for O(log n) binary search
        let mut pairs: Vec<(u64, f64)> = pixels.into_iter().zip(probabilities).collect();
        pairs.sort_unstable_by_key(|(px, _)| *px);
        let (sorted_pixels, sorted_probs): (Vec<u64>, Vec<f64>) = pairs.into_iter().unzip();

        Ok(EventGeometry::HealPixMap {
            nside,
            pixels: sorted_pixels,
            probabilities: sorted_probs,
            credible_level,
        })
    }

    /// Get the center coordinates if this is a circular geometry
    pub fn center(&self) -> Option<(f64, f64)> {
        match self {
            EventGeometry::Circle { ra, dec, .. } => Some((*ra, *dec)),
            EventGeometry::HealPixMap { .. } => None,
        }
    }

    /// Check if a position (ra, dec in degrees) is within this geometry
    pub fn contains(&self, ra: f64, dec: f64) -> bool {
        match self {
            EventGeometry::Circle {
                ra: center_ra,
                dec: center_dec,
                error_radius,
            } => {
                let dist = great_circle_distance_deg(ra, dec, *center_ra, *center_dec);
                dist <= *error_radius
            }
            EventGeometry::HealPixMap { nside, pixels, .. } => {
                let pixel = ra_dec_to_healpix_nested(ra, dec, *nside);
                pixels.binary_search(&pixel).is_ok()
            }
        }
    }

    /// Get the probability at a position (for HEALPix maps)
    pub fn probability_at(&self, ra: f64, dec: f64) -> Option<f64> {
        match self {
            EventGeometry::Circle { .. } => None,
            EventGeometry::HealPixMap {
                nside,
                pixels,
                probabilities,
                ..
            } => {
                let pixel = ra_dec_to_healpix_nested(ra, dec, *nside);
                pixels
                    .binary_search(&pixel)
                    .ok()
                    .and_then(|idx| probabilities.get(idx).copied())
            }
        }
    }
}

/// Convert RA/Dec (degrees) to HEALPix nested pixel index
fn ra_dec_to_healpix_nested(ra: f64, dec: f64, nside: u32) -> u64 {
    use cdshealpix::nested;
    // Use integer math for exact depth from power-of-2 nside
    let depth = nside.trailing_zeros() as u8;
    let lon = ra.to_radians();
    let lat = dec.to_radians();
    nested::hash(depth, lon, lat)
}

/// Great circle distance between two points in degrees
fn great_circle_distance_deg(ra1: f64, dec1: f64, ra2: f64, dec2: f64) -> f64 {
    use flare::spatial::great_circle_distance;
    great_circle_distance(ra1, dec1, ra2, dec2)
}

/// Main GCN Event document stored in MongoDB
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GcnEvent {
    /// Unique event ID (e.g., "S240422ed" for GW, or UUID for watchlist)
    #[serde(rename = "_id")]
    pub id: String,

    /// Event source/mission
    pub source: GcnSource,

    /// Event type classification
    pub event_type: GcnEventType,

    /// Julian Date of event trigger
    pub trigger_time: f64,

    /// Spatial localization geometry
    pub geometry: EventGeometry,

    /// GeoJSON coordinates for MongoDB geospatial queries (for circular events)
    /// This is None for HEALPix-only events
    pub coordinates: Option<Coordinates>,

    /// Source-specific metadata (varies by mission)
    #[serde(default)]
    pub properties: HashMap<String, serde_json::Value>,

    /// Julian Date when this event expires (no longer active for matching)
    pub expires_at: f64,

    /// Whether this event is still active for matching
    pub is_active: bool,

    /// User ID if this is a watchlist entry (None for GCN events)
    pub user_id: Option<String>,

    /// Optional name for watchlist entries
    pub name: Option<String>,

    /// Optional description
    pub description: Option<String>,

    /// Julian Date when event was created
    pub created_at: f64,

    /// Julian Date when event was last updated
    pub updated_at: f64,

    /// ID of event this supersedes (for GCN event updates)
    pub supersedes: Option<String>,

    /// ID of event that superseded this one
    pub superseded_by: Option<String>,
}

impl GcnEvent {
    /// Check if a position and time match this event
    pub fn matches(&self, ra: f64, dec: f64, alert_jd: f64) -> bool {
        if !self.is_active {
            return false;
        }
        // Reject non-finite alert_jd — NaN would bypass the expiry comparison
        // (IEEE 754: NaN > x is always false) and Infinity is meaningless.
        if !alert_jd.is_finite() {
            return false;
        }
        if alert_jd > self.expires_at {
            return false;
        }
        self.geometry.contains(ra, dec)
    }
}

/// Record of a match between an event and an alert object
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventMatch {
    /// ID of the matched event
    pub event_id: String,

    /// Source of the event
    pub event_source: GcnSource,

    /// Angular separation in degrees (for circular events)
    pub distance_deg: Option<f64>,

    /// Probability at the alert position (for HEALPix events)
    pub probability: Option<f64>,

    /// Julian Date when this match was recorded
    pub matched_at: f64,

    /// Time offset: alert_jd - event_trigger_jd (days)
    pub trigger_offset_days: f64,
}

impl EventMatch {
    /// Create a new event match
    pub fn new(
        event: &GcnEvent,
        alert_ra: f64,
        alert_dec: f64,
        alert_jd: f64,
    ) -> Self {
        let now = flare::Time::now().to_jd();

        let (distance_deg, probability) = match &event.geometry {
            EventGeometry::Circle { ra, dec, .. } => {
                let dist = great_circle_distance_deg(alert_ra, alert_dec, *ra, *dec);
                (Some(dist), None)
            }
            EventGeometry::HealPixMap { .. } => {
                let prob = event.geometry.probability_at(alert_ra, alert_dec);
                (None, prob)
            }
        };

        EventMatch {
            event_id: event.id.clone(),
            event_source: event.source.clone(),
            distance_deg,
            probability,
            matched_at: now,
            trigger_offset_days: alert_jd - event.trigger_time,
        }
    }
}

/// Errors related to watchlist/event operations
#[derive(thiserror::Error, Debug)]
pub enum WatchlistError {
    #[error("event not found: {0}")]
    NotFound(String),

    #[error("unauthorized: user does not own this watchlist")]
    Unauthorized,

    #[error("invalid geometry: {0}")]
    InvalidGeometry(String),

    #[error("radius exceeds maximum allowed: {0} > {1}")]
    RadiusTooLarge(f64, f64),

    #[error("user has reached maximum watchlist limit: {0}")]
    LimitExceeded(usize),

    #[error("database error: {0}")]
    Database(#[from] mongodb::error::Error),
}
