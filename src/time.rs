use chrono::{DateTime, Datelike, Timelike, Utc};

pub fn utc_to_jd(utc: DateTime<Utc>) -> f64 {
    let year = utc.year() as f64;
    let month = utc.month() as f64;
    let day = utc.day() as f64;
    let hour = utc.hour() as f64;
    let minute = utc.minute() as f64;
    let second = utc.second() as f64;

    let jd = 367.0 * year - ((year + ((month + 9.0) / 12.0)).floor() * 7.0 / 4.0).floor()
        + ((275.0 * month) / 9.0).floor() + day + 1721013.5
        + ((hour + (minute / 60.0) + (second / 3600.0)) / 24.0);
    jd
}

pub fn jd_now() -> f64 {
    utc_to_jd(Utc::now())
}