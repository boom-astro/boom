# Kafka Authentication & Authorization

This document describes how external (read-only) clients authenticate to the Boom Kafka broker and what guarantees / limitations apply.

## Goals

* Internal services (within the `boom` Docker network) continue to use unauthenticated plaintext (legacy) listeners.
* External clients authenticate over a dedicated SASL listener and have **read-only** access to topics.
* Admin operations restricted to an internal super user.
* Minimal change footprint; TLS can be added later.

## Overview

The broker now exposes four listeners:

| Listener Name | Address (container)    | Exposed Host Port | Security          | Purpose |
|---------------|------------------------|-------------------|-------------------|---------|
| PLAINTEXT     | broker:29092           | (not published)   | None              | Internal inter-service traffic, producers/consumers inside stack |
| PLAINTEXT_HOST| 0.0.0.0:9092           | localhost:9092    | None              | Local development on host |
| CONTROLLER    | broker:29093           | (internal)        | None              | KRaft quorum |
| EXTERNAL      | 0.0.0.0:9093           | 9093              | SASL/SCRAM-SHA-512| Authenticated external read-only clients |

### Traefik Routing

Kafka's EXTERNAL listener is exposed via Traefik as a raw TCP route (no TLS yet). Ensure Traefik static configuration defines an entrypoint:

```
--entrypoints.kafka.address=:9093
```

Compose labels added to the broker:

```
traefik.tcp.services.$STACK-kafka.loadbalancer.server.port=9093
traefik.tcp.routers.$STACK-kafka.rule=HostSNI(`*`)
traefik.tcp.routers.$STACK-kafka.entrypoints=kafka
```

Later (when enabling TLS passthrough or termination) you can uncomment / add:

```
traefik.tcp.routers.$STACK-kafka.tls.passthrough=true
```

Or for Traefik-terminated TLS (not passthrough) you would instead set certificate resolvers and remove passthrough while keeping SASL over plaintext inside the TLS tunnel.

## Authentication

Mechanism: `SASL_PLAINTEXT` with mechanism `SCRAM-SHA-512`.

Users created by init script `scripts/init_kafka_acls.sh`:

* `admin` – password from env `KAFKA_ADMIN_PASSWORD`; declared in `KAFKA_SUPER_USERS` giving full access.
* `readonly` – password from env `KAFKA_READONLY_PASSWORD`; ACL-limited to describing cluster and reading topics / committing offsets.

Environment variables must be provided (e.g. via `.env` or deployment secrets):

```
KAFKA_ADMIN_PASSWORD=change-me
KAFKA_READONLY_PASSWORD=change-me-too
# Optional host override (public DNS)
KAFKA_EXTERNAL_HOST=broker.example.org
```

## Authorization (ACLs)

The init script applies ACLs:

* Cluster: `DESCRIBE` for `readonly`
* Topics: `READ` + `DESCRIBE` on `*` (all existing and future topics)
* Consumer Groups: `READ` on `*` (allow offset commits). You may restrict to a prefix by editing the script.

Admin user is a super user; ACLs not required.

## Client Configuration Examples

### Kafka CLI (kafka-console-consumer)

```
bootstrap.servers=broker.example.org:9093
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required \
    username="readonly" \
    password="${KAFKA_READONLY_PASSWORD}";
```

Command example:

```
kafka-console-consumer \
  --bootstrap-server broker.example.org:9093 \
  --topic some_topic \
  --consumer.config client.properties \
  --group ro-myclient
```

### Rust (rdkafka)

```rust
let mut config = rdkafka::ClientConfig::new();
config
  .set("bootstrap.servers", "broker.example.org:9093")
  .set("security.protocol", "SASL_PLAINTEXT")
  .set("sasl.mechanism", "SCRAM-SHA-512")
  .set("sasl.username", "readonly")
  .set("sasl.password", std::env::var("KAFKA_READONLY_PASSWORD").unwrap());
```

### Python (confluent-kafka)

```python
from confluent_kafka import Consumer
c = Consumer({
  'bootstrap.servers': 'broker.example.org:9093',
  'security.protocol': 'SASL_PLAINTEXT',
  'sasl.mechanisms': 'SCRAM-SHA-512',
  'sasl.username': 'readonly',
  'sasl.password': os.environ['KAFKA_READONLY_PASSWORD'],
  'group.id': 'ro-sample',
  'auto.offset.reset': 'earliest'
})
```

## Operational Notes

* The script is idempotent; re-runs update passwords & reapply ACLs.
* If you change a password, restart the broker container to re-run script (or run commands manually).
* To restrict topic access, replace the `--topic '*'` ACL entry with specific topics or a prefixed pattern via `--topic 'astronomy-*'`.

## Future: TLS / mTLS

To prevent plaintext credential leakage, plan to migrate the EXTERNAL listener to `SASL_SSL`:

1. Generate broker cert & truststore.
2. Enable `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` entry `EXTERNAL:SASL_SSL`.
3. Provide `KAFKA_LISTENER_NAME_EXTERNAL_SSL_*` configuration (keystore, truststore).
4. Optionally enforce client cert auth and map DNs to principals.

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `SASL authentication failed` | Wrong password | Verify secrets & restart client |
| `TopicAuthorizationException` | Missing ACL for topic | Adjust script ACLs |
| Hang on connect | Port blocked / firewall | Verify 9093 reachable |
| Works internally but not externally | Hostname mismatch | Set `KAFKA_EXTERNAL_HOST` to public DNS |

## Security Considerations

* Passwords currently passed via environment variables (visible via `docker inspect`). Prefer Docker secrets or an external secrets manager in production.
* Without TLS, credentials move in cleartext on the network; limit exposure to internal secure networks or enable TLS soon.
* ACL wildcard grants broad read access; refine when multi-tenant requirements emerge.

---

Last updated: 2025-10-02
