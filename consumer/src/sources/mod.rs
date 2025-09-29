pub mod decam;
pub mod lsst;
pub mod ztf;

// Some common utilities for sources

use chrono::NaiveDate;

fn parse_date(s: &str) -> Result<NaiveDate, String> {
    let date = NaiveDate::parse_from_str(s, "%Y%m%d")
        .or_else(|_| NaiveDate::parse_from_str(s, "%Y-%m-%d"))
        .map_err(|_| "expected a date in YYYYMMDD or YYYY-MM-DD format")?;
    Ok(date)
}

fn yesterday() -> NaiveDate {
    chrono::Utc::now()
        .date_naive()
        .pred_opt()
        .expect("previous date is not representable")
}
