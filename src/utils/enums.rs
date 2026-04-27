use apache_avro_macros::serdavro;
use chrono::{Datelike, NaiveDate};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[serdavro]
#[derive(clap::ValueEnum, Clone, Debug, Serialize, Deserialize, ToSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "UPPERCASE")]
pub enum Survey {
    #[serde(alias = "ztf")]
    Ztf,
    #[serde(alias = "lsst")]
    Lsst,
    #[serde(alias = "decam")]
    Decam,
}

impl Survey {
    /// Observatory UTC offset in hours.
    ///
    /// - ZTF   (Palomar, CA, USA)       : UTC−7
    /// - LSST  (Cerro Pachón, CL, Chile): UTC−3
    /// - DECam (Cerro Tololo, CL, Chile): UTC−4
    pub fn observatory_utc_offset(&self) -> f64 {
        match self {
            Survey::Ztf => -7.0,
            Survey::Lsst => -3.0,
            Survey::Decam => -4.0,
        }
    }

    /// Convert a calendar date to Julian Date at **local noon** for the survey's
    /// observatory.
    ///
    /// An astronomical "night" for date D spans from JD(D, local noon) to
    /// JD(D+1, local noon).
    pub fn date_to_jd_local_noon(&self, date: &NaiveDate) -> f64 {
        let y = date.year() as f64;
        let m = date.month() as f64;
        let d = date.day() as f64;

        let (y_adj, m_adj) = if m <= 2.0 {
            (y - 1.0, m + 12.0)
        } else {
            (y, m)
        };

        let a = (y_adj / 100.0_f64).floor();
        let b = 2.0_f64 - a + (a / 4.0_f64).floor();

        // JD at 0h UT (midnight UTC)
        let jd_midnight =
            (365.25_f64 * (y_adj + 4716.0)).floor() + (30.6001_f64 * (m_adj + 1.0)).floor() + d + b
                - 1524.5;

        // Shift to local noon: local noon = (12 − utc_offset) hours UTC
        jd_midnight + (12.0 - self.observatory_utc_offset()) / 24.0
    }

    /// JD window `[start, end)` for the observing night labelled by `date`,
    /// running from local noon of `date` to local noon of `date + 1`.
    pub fn night_jd_window(&self, date: &NaiveDate) -> (f64, f64) {
        let start = self.date_to_jd_local_noon(date);
        let end = self.date_to_jd_local_noon(&(*date + chrono::Duration::days(1)));
        (start, end)
    }
}

impl std::fmt::Display for Survey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Survey::Ztf => write!(f, "ZTF"),
            Survey::Lsst => write!(f, "LSST"),
            Survey::Decam => write!(f, "DECAM"),
        }
    }
}

#[derive(clap::ValueEnum, Clone, Default, Debug, Serialize, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ProgramId {
    #[default]
    #[serde(alias = "1")]
    Public = 1,
    #[serde(alias = "2")]
    Partnership = 2, // ZTF-only
    #[serde(alias = "3")]
    Caltech = 3, // ZTF-only
}

impl std::fmt::Display for ProgramId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProgramId::Public => write!(f, "1"),
            ProgramId::Partnership => write!(f, "2"),
            ProgramId::Caltech => write!(f, "3"),
        }
    }
}
