pub mod cutouts;
pub mod data;
pub mod db;
pub mod derive_avro_schema;
pub mod enums;
pub mod fits;
pub mod gpu;
pub mod lightcurves;
pub mod mpcorb;
pub mod o11y;
pub mod outburst;
pub mod parser;
pub mod phase_curve;
pub mod retry;
pub mod spatial;
pub mod sso_geometry;
pub mod testing;
pub mod worker;

/// A BSON number as `f64`, whatever width it was written at.
///
/// Mongo stores an integer-valued double as an int, so a reader that accepts
/// only `Double` silently drops values that were written as floats.
pub fn bson_number(value: &mongodb::bson::Bson) -> Option<f64> {
    match value {
        mongodb::bson::Bson::Double(v) => Some(*v),
        mongodb::bson::Bson::Int32(v) => Some(*v as f64),
        mongodb::bson::Bson::Int64(v) => Some(*v as f64),
        _ => None,
    }
}
