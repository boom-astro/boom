mod base;
mod lsst;
mod ztf;

pub use base::AlertConsumer;
pub use lsst::LsstAlertConsumer;
pub use ztf::{download_alerts_from_archive, produce_from_archive, ZtfAlertConsumer};
