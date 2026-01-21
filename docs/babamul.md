# Babamul

BOOM's Babamul feature provides public access to BOOM's
transient alert streams via Kafka.
Users sign up with an email address and receive credentials for both Kafka
stream access and optional API endpoints.

**Interactive documentation**: `/babamul/docs` (Swagger UI)

## Account separation

Babamul accounts are isolated from main BOOM API accounts:

- **Database**: Stored in separate `babamul_users` collection
- **JWT claims**: Subject contains `babamul:` prefix (e.g., `babamul:{user_id}`)
- **Access control**: Middleware rejects Babamul tokens on main API endpoints
- **Permissions**: Babamul users can only access `/babamul/*`
  endpoints and `babamul.*` Kafka topics

## Authentication flow

1. **Signup** (`POST /babamul/signup`): User provides email, system creates account with activation code
2. **Activation** (`POST /babamul/activate`): User submits activation code, receives 32-character password (shown once)
3. **Kafka access**: Use email + password with SCRAM-SHA-512 authentication
4. **API access** (`POST /babamul/auth`): Exchange email + password for JWT token

## Kafka access

After activation, connect to Kafka using:

- **Username**: Email address
- **Password**: Password from activation response
- **Mechanism**: SCRAM-SHA-512
- **Topics**: `babamul.*` (READ, DESCRIBE)
- **Consumer Groups**: `babamul-*` (READ)

### Example (Python)

This example uses the `confluent_kafka` package.

```python
from confluent_kafka import Consumer

# Subscribe to the babamul.none topic, which includes alerts that aren't
# stars, aren't galaxies, and have no cross-matches
consumer = Consumer(
    {
        "bootstrap.servers": "kafka.boom.example.com:9092",
        "security.protocol": "SASL_PLAINTEXT",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "user@example.com",
        "sasl.password": "your-password-here",
        "group.id": "babamul-myapp",
        "auto.offset.reset": "earliest"
    }
)

consumer.subscribe(["babamul.none"])

try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        print(msg.value())
finally:
    consumer.close()
```

## Object appearance in output topics

When an object is observed by multiple surveys,
alerts include survey match data in the
`survey_matches` field.
Topics follow the pattern: `babamul.{source_survey}.{other_survey}-match.*`.

On the first observation of a given object,
the alert has empty `survey_matches`.
When the object
is subsequently observed by another survey,
that alert includes information from
the other survey in its `survey_matches` field.
From that point forward, alerts on both streams include
`survey_matches` in their alerts.

### Multi-survey object appearance flow

```mermaid
sequenceDiagram
    participant LSST as LSST
    participant ZTF as ZTF
    participant Stream as Babamul topics

    Note over LSST,Stream: Day 1: First observation by LSST
    LSST->>Stream: Object discovered (stellar)
    rect rgb(45, 95, 63)
    Note over Stream: Topic: babamul.lsst.no-ztf-match.stellar<br/><br/>Survey matches: none
    end

    Note over ZTF,Stream: Day 3: ZTF observes same object
    ZTF->>Stream: Object observed (stellar)
    rect rgb(45, 63, 95)
    Note over Stream: Topic: babamul.ztf.lsst-match.stellar<br/><br/>Survey matches: lsst
    end

    Note over LSST,Stream: Day 5: LSST observes again
    LSST->>Stream: Object re-observed (stellar)
    rect rgb(45, 95, 63)
    Note over Stream: Topic: babamul.lsst.ztf-match.stellar<br/><br/>Survey matches: ztf
    end

    Note over ZTF,Stream: Day 7+: LSST and ZTF continue observing
    ZTF->>Stream: Subsequent observations
    rect rgb(45, 63, 95)
    Note over Stream: Topic: babamul.ztf.lsst-match.stellar<br/><br/>Survey matches: lsst
    end
    LSST->>Stream: Subsequent observations
    rect rgb(45, 95, 63)
    Note over Stream: Topic: babamul.lsst.ztf-match.stellar<br/><br/>Survey matches: ztf
    end
```

### Archival catalog cross-match flow

The classification suffix (stellar/hosted/hostless/unknown)
is determined by cross-matching against archival catalogs like LSPSC.
This happens independently of survey cross-matches.

Note: The `*` below represents survey match status, e.g., `ztf-match` or
`no-ztf-match`.

```mermaid
flowchart TD
    Alert[New LSST Alert] --> CheckStar{Already classified<br/>as stellar?}
    CheckStar -->|Yes| StellarTopic[Topic: babamul.lsst.*.stellar]
    CheckStar -->|No| CheckLSPSC{LSPSC<br/>cross-matches<br/>exist?}

    CheckLSPSC -->|No| CheckFootprint{In LSPSC<br/>footprint?}
    CheckFootprint -->|Yes| HostlessTopic[Topic: babamul.lsst.*.hostless]
    CheckFootprint -->|No| UnknownTopic[Topic: babamul.lsst.*.unknown]

    CheckLSPSC -->|Yes| EvalMatches{Check match<br/>distance & score}

    EvalMatches -->|Any match:<br/>distance ≤ 1.0″<br/>AND score > 0.5| StellarTopic
    EvalMatches -->|Any match:<br/>distance ≤ 30″<br/>AND score < 0.5| HostedTopic[Topic: babamul.lsst.*.hosted]
    EvalMatches -->|No qualifying<br/>matches| HostlessTopic

    style StellarTopic fill:#2d5f3f,color:#e0e0e0
    style HostedTopic fill:#5f2d2d,color:#e0e0e0
    style HostlessTopic fill:#2d3f5f,color:#e0e0e0
    style UnknownTopic fill:#3a3a3a,color:#e0e0e0
```
