use apache_avro_macros::serdavro;
use chrono::{Duration, NaiveDate, NaiveDateTime, NaiveTime, Offset, TimeZone};
use chrono_tz::Tz;
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
    #[serde(alias = "winter", alias = "wntr", alias = "WNTR")]
    Winter,
}

impl Survey {
    pub fn as_str(&self) -> &'static str {
        match self {
            Survey::Ztf => "ZTF",
            Survey::Lsst => "LSST",
            Survey::Decam => "DECAM",
            Survey::Winter => "WINTER",
        }
    }

    pub fn alert_input_queue(&self) -> String {
        format!("{}_alerts_packets_queue", self)
    }

    pub fn observatory_timezone(&self) -> Tz {
        match self {
            Survey::Ztf | Survey::Winter => Tz::America__Los_Angeles,
            Survey::Lsst | Survey::Decam => Tz::America__Santiago,
        }
    }

    pub fn date_to_jd_local_noon(&self, date: &NaiveDate) -> f64 {
        let tz = self.observatory_timezone();
        let offset = |utc: NaiveDateTime| tz.offset_from_utc_datetime(&utc).fix();
        let noon = date.and_time(NaiveTime::MIN) + Duration::hours(12);
        flare::Time::from_utc((noon - offset(noon - offset(noon))).and_utc()).to_jd()
    }

    /// JD window `[start, end)` for the observing night labelled by `date`,
    /// running from local noon of `date` to local noon of `date + 1`.
    pub fn night_jd_window(&self, date: &NaiveDate) -> (f64, f64) {
        let start = self.date_to_jd_local_noon(date);
        let end = self.date_to_jd_local_noon(&(*date + Duration::days(1)));
        (start, end)
    }
}

impl std::fmt::Display for Survey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_noon_follows_daylight_saving() {
        for (survey, (y, m, d), utc_hour) in [
            (Survey::Ztf, (2026, 1, 15), 20),
            (Survey::Ztf, (2026, 3, 8), 19),
            (Survey::Ztf, (2026, 7, 1), 19),
            (Survey::Winter, (2026, 11, 1), 20),
            (Survey::Lsst, (2026, 4, 5), 16),
            (Survey::Lsst, (2026, 7, 1), 16),
            (Survey::Decam, (2026, 9, 6), 15),
            (Survey::Decam, (2026, 12, 1), 15),
        ] {
            let date = NaiveDate::from_ymd_opt(y, m, d).unwrap();
            let expected = flare::Time::new(y, m, d, utc_hour, 0, 0).to_jd();
            assert!(
                (survey.date_to_jd_local_noon(&date) - expected).abs() < 1e-9,
                "{survey} on {date}"
            );
        }
    }
}
