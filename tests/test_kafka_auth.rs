//! Integration test for Kafka external listener authentication.

use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::metadata::Metadata;
use std::thread::sleep;
use std::time::Duration;

fn bootstrap() -> String {
    std::env::var("KAFKA_BOOTSTRAP").unwrap_or_else(|_| "localhost:9093".to_string())
}

fn bootstrap_host_port() -> (String, u16) {
    let b = bootstrap();
    // naive split host:port
    let mut parts = b.split(':');
    let host = parts.next().unwrap_or("localhost").to_string();
    let port = parts
        .next()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(9093);
    (host, port)
}

fn broker_reachable(timeout: Duration) -> bool {
    use std::net::{TcpStream, ToSocketAddrs};
    let (host, port) = bootstrap_host_port();
    let addr_iter = format!("{}:{}", host, port).to_socket_addrs();
    if let Ok(mut iter) = addr_iter {
        if let Some(sock) = iter.next() {
            let deadline = std::time::Instant::now() + timeout;
            while std::time::Instant::now() < deadline {
                match TcpStream::connect_timeout(&sock, Duration::from_millis(250)) {
                    Ok(_) => return true,
                    Err(_) => sleep(Duration::from_millis(100)),
                }
            }
        }
    }
    false
}

fn username() -> String {
    std::env::var("KAFKA_USERNAME").unwrap_or_else(|_| "readonly".to_string())
}

fn password() -> String {
    // Load .env lazily (searches for a .env file up the directory tree); ignore errors.
    let _ = dotenvy::dotenv();
    std::env::var("KAFKA_READONLY_PASSWORD")
        .or_else(|_| std::env::var("KAFKA_PASSWORD"))
        .expect("KAFKA_READONLY_PASSWORD (or legacy KAFKA_PASSWORD) must be set; define it in your environment or .env file")
}

fn fetch_metadata(cfg: &ClientConfig) -> Result<Metadata, KafkaError> {
    let consumer: BaseConsumer = cfg
        .create()
        .map_err(|e| KafkaError::ClientCreation(e.to_string()))?;
    consumer.fetch_metadata(None, Duration::from_secs(5))
}

fn fetch_metadata_with_retries(
    cfg: &ClientConfig,
    attempts: u32,
    delay: Duration,
) -> Result<Metadata, KafkaError> {
    let mut last_err: Option<KafkaError> = None;
    for _ in 0..attempts {
        match fetch_metadata(cfg) {
            Ok(m) => return Ok(m),
            Err(e) => {
                last_err = Some(e);
                sleep(delay);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| KafkaError::ClientCreation("Unknown error".into())))
}

fn base_cfg() -> ClientConfig {
    let mut c = ClientConfig::new();
    c.set("bootstrap.servers", &bootstrap());
    c.set("enable.partition.eof", "false");
    c
}

#[test]
fn kafka_auth_enforcement() {
    // Fast skip (treated as a pass) if broker not reachable; keeps unit test runs from failing
    // when integration environment (Kafka container) is not up.
    if !broker_reachable(Duration::from_secs(2)) {
        eprintln!(
            "Skipping kafka_auth_enforcement: Kafka bootstrap not reachable at {}",
            bootstrap()
        );
        return;
    }

    let user = username();
    let pass = password();

    // 1. No auth should fail
    let cfg_no_auth = base_cfg();
    let no_auth_result = fetch_metadata(&cfg_no_auth);
    assert!(
        no_auth_result.is_err(),
        "Expected unauthenticated metadata fetch to fail, but succeeded"
    );

    // 2. Wrong password should fail
    let mut cfg_bad = base_cfg();
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
    let mut cfg_good = base_cfg();
    cfg_good
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanism", "SCRAM-SHA-512")
        .set("sasl.username", &user)
        .set("sasl.password", &pass);
    let good_result = fetch_metadata_with_retries(&cfg_good, 5, Duration::from_millis(500))
        .expect("Expected successful metadata fetch with correct credentials");

    // Basic sanity: there should be at least one topic (internal topics count)
    assert!(
        good_result.topics().len() > 0,
        "Expected at least one topic in metadata"
    );
}
