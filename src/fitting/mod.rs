pub mod common;
pub mod nonparametric;

pub use common::{photometry_to_mag_bands, BandData, LightcurveFittingResult};
pub use nonparametric::{fit_nonparametric, NonparametricBandResult};
