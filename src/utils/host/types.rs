use serde::{Deserialize, Serialize};

/// A transient event to be associated with a host galaxy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transient {
    /// Right ascension in degrees
    pub ra: f64,
    /// Declination in degrees
    pub dec: f64,
    /// Positional uncertainty in RA (arcsec)
    pub ra_err: f64,
    /// Positional uncertainty in Dec (arcsec)
    pub dec_err: f64,
    /// Spectroscopic or photometric redshift (if known)
    pub redshift: Option<f64>,
    /// Uncertainty on redshift
    pub redshift_err: Option<f64>,
    /// Identifier
    pub name: Option<String>,
}

impl Transient {
    pub fn new(ra: f64, dec: f64) -> Self {
        Self {
            ra,
            dec,
            ra_err: 0.0,
            dec_err: 0.0,
            redshift: None,
            redshift_err: None,
            name: None,
        }
    }

    pub fn with_redshift(mut self, z: f64, z_err: f64) -> Self {
        self.redshift = Some(z);
        self.redshift_err = Some(z_err);
        self
    }

    pub fn with_position_err(mut self, ra_err: f64, dec_err: f64) -> Self {
        self.ra_err = ra_err;
        self.dec_err = dec_err;
        self
    }

    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }
}

/// A galaxy candidate with measured or image-derived properties.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GalaxyCandidate {
    /// Right ascension in degrees
    pub ra: f64,
    /// Declination in degrees
    pub dec: f64,
    /// Semi-major axis in arcsec
    pub a_arcsec: f64,
    /// Semi-minor axis in arcsec
    pub b_arcsec: f64,
    /// Position angle in degrees (N through E)
    pub pa_deg: f64,
    /// Photometric or spectroscopic redshift
    pub redshift: Option<f64>,
    /// Uncertainty on redshift
    pub redshift_err: Option<f64>,
    /// Adopted distance in Mpc. Only redshift-independent when
    /// `dist_mpc_method` says so; otherwise it is `z * c / H0`.
    pub dist_mpc: Option<f64>,
    /// NED-LVS: `"zIndependent"` or `"Redshift"`.
    pub dist_mpc_method: Option<String>,
    /// Apparent magnitude
    pub mag: Option<f64>,
    /// Uncertainty on apparent magnitude
    pub mag_err: Option<f64>,
    /// Morphological type or catalog classification
    pub objtype: Option<String>,
    /// Object identifier from source catalog
    pub objname: Option<String>,
    /// Source catalog name
    pub catalog: Option<String>,
    /// Whether shape was derived from image analysis (true) or catalog (false)
    pub shape_from_image: bool,
}

/// Result of host association for a single galaxy candidate.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostCandidate {
    /// Galaxy candidate properties
    pub galaxy: GalaxyCandidate,
    /// Angular separation from transient (arcsec)
    pub separation_arcsec: f64,
    /// Directional light radius
    pub dlr: f64,
    /// Fractional offset (separation / DLR)
    pub fractional_offset: f64,
    /// Rank by posterior (1 = best)
    pub dlr_rank: u32,
    /// Posterior probability of being the host
    pub posterior: f64,
    /// Offset posterior component
    pub posterior_offset: f64,
    /// Redshift posterior component
    pub posterior_redshift: f64,
    /// Absolute magnitude posterior component
    pub posterior_absmag: f64,
}
