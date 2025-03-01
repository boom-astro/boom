use boom::lsst_types::{Alert, LsstAlert};

const _MAGIC_BYTE: u8 = 0;
const _SCHEMA_REGISTRY_ID: u32 = 703;

fn main() {
    let schema = LsstAlert::alert_schema().unwrap();

    let avro_bytes = std::fs::read("alert.avro").unwrap();

    let start = std::time::Instant::now();

    let alert = LsstAlert::from_avro_bytes(avro_bytes, &schema).unwrap();

    // show the time taken
    println!("Time taken: {:?}", start.elapsed());
    // print the ra and dec
    println!("ra: {}, dec: {}", alert.candidate.ra, alert.candidate.dec);
}
