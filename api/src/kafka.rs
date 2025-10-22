//! Functionality for working with Kafka in the API.

use rdkafka::admin::AdminClient;
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::error::KafkaError;

/// Create an AdminClient connected to the local broker using SCRAM auth.
/// Panics if `KAFKA_ADMIN_PASSWORD` is not set.
pub fn create_admin_client() -> Result<AdminClient<DefaultClientContext>, KafkaError> {
    let admin_client: AdminClient<_> = ClientConfig::new()
        .set("bootstrap.servers", "broker:29092")
        .set("security.protocol", "SASL_PLAINTEXT")
        .set("sasl.mechanism", "SCRAM-SHA-512")
        .set("sasl.username", "admin")
        .set(
            "sasl.password",
            &std::env::var("KAFKA_ADMIN_PASSWORD").expect("KAFKA_ADMIN_PASSWORD must be set"),
        )
        .create()?;

    Ok(admin_client)
}
