use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Transient {
    /// Right ascension, degrees.
    pub ra: f64,
    /// Declination, degrees.
    pub dec: f64,
    pub redshift: Option<f64>,
    pub redshift_err: Option<f64>,
}

impl Transient {
    pub fn new(ra: f64, dec: f64) -> Self {
        Self {
            ra,
            dec,
            redshift: None,
            redshift_err: None,
        }
    }

    pub fn with_redshift(mut self, z: f64, z_err: f64) -> Self {
        self.redshift = Some(z);
        self.redshift_err = Some(z_err);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GalaxyCandidate {
    /// Right ascension, degrees.
    pub ra: f64,
    /// Declination, degrees.
    pub dec: f64,
    /// Semi-major axis, arcsec.
    pub a_arcsec: f64,
    /// Semi-minor axis, arcsec.
    pub b_arcsec: f64,
    /// Position angle, degrees east of north.
    pub pa_deg: f64,
    pub redshift: Option<f64>,
    pub redshift_err: Option<f64>,
    /// Adopted distance in Mpc, redshift-independent only when
    /// `dist_mpc_method` says so.
    pub dist_mpc: Option<f64>,
    /// NED-LVS: `"zIndependent"` or `"Redshift"`.
    pub dist_mpc_method: Option<String>,
    pub mag: Option<f64>,
    pub mag_err: Option<f64>,
    pub objtype: Option<String>,
    pub objname: Option<String>,
    pub catalog: Option<String>,
    /// Whether `a_arcsec` is a D25-equivalent isophotal size rather than a
    /// half-light radius, which undersizes the galaxy against catalogued
    /// diameters.
    pub size_is_isophotal: bool,
    /// Survey the catalogued diameter came from, where the catalog says.
    pub diam_survey: Option<String>,
    /// Whether the position angle is a catalogue default rather than measured.
    pub orientation_is_nominal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostCandidate {
    pub galaxy: GalaxyCandidate,
    /// Angular separation from the transient, arcsec.
    pub separation_arcsec: f64,
    /// Galaxy light radius toward the transient, arcsec.
    pub dlr: f64,
    /// Separation in units of `dlr`.
    pub fractional_offset: f64,
    /// Rank by `fractional_offset`, 1 = the galaxy the transient sits deepest in.
    pub dlr_rank: u32,
    pub posterior: f64,
    pub posterior_offset: f64,
    pub posterior_redshift: f64,
    pub posterior_absmag: f64,
}
