//! Integration test for Kafka external listener authentication.

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::metadata::Metadata;
use std::time::Duration;

fn get_password() -> String {
    // Load .env lazily (searches for a .env file up the directory tree); ignore errors.
    let _ = dotenvy::dotenv();
    std::env::var("KAFKA_READONLY_PASSWORD").expect("KAFKA_READONLY_PASSWORD must be set")
}

fn fetch_metadata(cfg: &ClientConfig) -> Result<Metadata, KafkaError> {
    let consumer: BaseConsumer = cfg
        .create()
        .map_err(|e| KafkaError::ClientCreation(e.to_string()))?;
    consumer.fetch_metadata(None, Duration::from_secs(1))
}

fn make_base_cfg() -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("group.id", "test-group");
    c.set("bootstrap.servers", "localhost:9093");
    c.set("enable.partition.eof", "false");
    c
}

#[test]
fn test_kafka_auth_enforcement() {
    let user = "readonly".to_string();
    let pass = get_password();

    println!("Using Kafka user '{user}' with password '{pass}'");

    // Ensure we can connect with no auth on localhost:9092 to ensure the
    // broker is up before testing auth on 9093
    let mut cfg = make_base_cfg();
    cfg.set("bootstrap.servers", "localhost:9092");
    let meta = fetch_metadata(&cfg).expect("Expected metadata fetch on PLAINTEXT to succeed");
    assert!(
        meta.topics().len() > 0,
        "Expected at least one topic in metadata"
    );

    // 1. No auth should fail
    let cfg_no_auth = make_base_cfg();
    let no_auth_result = fetch_metadata(&cfg_no_auth);
    assert!(
        no_auth_result.is_err(),
        "Expected unauthenticated metadata fetch to fail, but succeeded"
    );

    // 2. Wrong password should fail
    let mut cfg_bad = make_base_cfg();
    cfg_bad
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanism", "SCRAM-SHA-512")
        .set("sasl.username", &user)
        .set("sasl.password", "definitely-wrong-password");
    let bad_result = fetch_metadata(&cfg_bad);
    assert!(
        bad_result.is_err(),
        "Expected bad password to fail, but succeeded"
    );

    // 3. Correct password should succeed
    let mut cfg_good = make_base_cfg();
    cfg_good
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanism", "SCRAM-SHA-512")
        .set("sasl.username", &user)
        .set("sasl.password", &pass);
    let good_result = fetch_metadata(&cfg_good).unwrap();

    // Basic sanity: there should be at least one topic (internal topics count)
    assert!(
        good_result.topics().len() > 0,
        "Expected at least one topic in metadata"
    );
}
