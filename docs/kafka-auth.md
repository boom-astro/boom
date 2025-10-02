# Kafka authentication and authorization

This document describes how external (read-only) clients authenticate to the
BOOM Kafka broker and what guarantees/limitations apply.

## Goals

* Internal services (within the `boom` Docker network) continue to use
  unauthenticated plaintext (legacy) listeners.
* External clients authenticate over a dedicated SASL listener and have
  **read-only** access to topics.
* Admin operations restricted to an internal super user.
* TLS is currently not enabled, but can be added later.

## Overview

The broker now exposes four listeners:

| Listener name | Address (container)    | Exposed host port | Security          | Purpose |
|---------------|------------------------|-------------------|-------------------|---------|
| PLAINTEXT     | broker:29092           | (not published)   | None              | Internal inter-service traffic, producers/consumers inside stack |
| PLAINTEXT_HOST| 0.0.0.0:9092           | localhost:9092    | None              | Local development on host |
| CONTROLLER    | broker:29093           | (internal)        | None              | KRaft quorum |
| EXTERNAL      | 0.0.0.0:9093           | 9093              | SASL/SCRAM-SHA-512| Authenticated external read-only clients |

## Authentication

Mechanism: `SASL_PLAINTEXT` with mechanism `SCRAM-SHA-512`.

Users created by the init script `scripts/init_kafka_acls.sh`:

* `admin`: password from env `KAFKA_ADMIN_PASSWORD`; declared in `KAFKA_SUPER_USERS` giving full access.
* `readonly`: password from env `KAFKA_READONLY_PASSWORD`; ACL-limited to describing cluster and reading topics / committing offsets.

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

## Client configuration examples

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

## Operational notes

* The script is idempotent; re-runs update passwords & reapply ACLs.
* If you change a password,
  restart the broker container to re-run script (or run commands manually).
* To restrict topic access, replace the `--topic '*'` ACL entry with specific topics or a prefixed pattern via `--topic 'astronomy-*'`.
* ACL/user initialization now happens via a one-shot compose service `
  kafka-acl-init` (depends on broker health).
  Re-run with `docker compose run --rm kafka-acl-init`
  if you need to refresh credentials without recreating broker.
