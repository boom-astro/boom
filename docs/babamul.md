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

    Note over LSST,Stream: Day 1: Object discovered by LSST
    LSST->>Stream: Object discovered (stellar)
    rect rgb(95, 63, 45)
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

### Alert classification and topic assignment flow

#### LSST alerts

LSST alerts are first classified based on LSPSC catalog matches, then assigned to topics
based on their classification and whether they have a ZTF match.

```mermaid
flowchart TD
    LSST[New LSST Alert] --> CheckLSPSC{Has matches<br/>in LSPSC?}

    CheckLSPSC -->|No| CheckFootprint{In LSPSC<br/>footprint?}
    CheckFootprint -->|Yes| LSST_Hostless[LSST Hostless]
    CheckFootprint -->|No| LSST_Unknown[LSST Unknown]

    CheckLSPSC -->|Yes| CheckLSSTStellar{Any match with<br/>distance ≤ 1.0″<br/>and score > 0.5?}
    CheckLSSTStellar -->|Yes| LSST_Stellar[LSST Stellar]
    CheckLSSTStellar -->|No| CheckHosted{Any match with<br/>distance ≤ 30″<br/>and score < 0.5?}
    CheckHosted -->|Yes| LSST_Hosted[LSST Hosted]
    CheckHosted -->|No| LSST_Hostless

    LSST_Stellar --> CheckZTFMatch_Stellar{Has ZTF<br/>match?}
    LSST_Hosted --> CheckZTFMatch_Hosted{Has ZTF<br/>match?}
    LSST_Hostless --> CheckZTFMatch_Hostless{Has ZTF<br/>match?}
    LSST_Unknown --> CheckZTFMatch_Unknown{Has ZTF<br/>match?}

    CheckZTFMatch_Stellar -->|Yes| Topic1[babamul.lsst.ztf-match.stellar]
    CheckZTFMatch_Stellar -->|No| Topic5[babamul.lsst.no-ztf-match.stellar]
    CheckZTFMatch_Hosted -->|Yes| Topic2[babamul.lsst.ztf-match.hosted]
    CheckZTFMatch_Hosted -->|No| Topic6[babamul.lsst.no-ztf-match.hosted]
    CheckZTFMatch_Hostless -->|Yes| Topic3[babamul.lsst.ztf-match.hostless]
    CheckZTFMatch_Hostless -->|No| Topic7[babamul.lsst.no-ztf-match.hostless]
    CheckZTFMatch_Unknown -->|Yes| Topic4[babamul.lsst.ztf-match.unknown]
    CheckZTFMatch_Unknown -->|No| Topic8[babamul.lsst.no-ztf-match.unknown]

    style LSST_Stellar fill:#2d5f3f,color:#e0e0e0
    style LSST_Hosted fill:#5f2d2d,color:#e0e0e0
    style LSST_Hostless fill:#2d3f5f,color:#e0e0e0
    style LSST_Unknown fill:#3a3a3a,color:#e0e0e0
    style Topic1 fill:#2d5f3f,color:#e0e0e0
    style Topic2 fill:#5f2d2d,color:#e0e0e0
    style Topic3 fill:#2d3f5f,color:#e0e0e0
    style Topic4 fill:#3a3a3a,color:#e0e0e0
    style Topic5 fill:#2d5f3f,color:#e0e0e0
    style Topic6 fill:#5f2d2d,color:#e0e0e0
    style Topic7 fill:#2d3f5f,color:#e0e0e0
    style Topic8 fill:#3a3a3a,color:#e0e0e0
```

#### ZTF alerts

ZTF alerts are first classified based on stellar properties and star-galaxy scores, then assigned
to topics based on their classification and whether they have an LSST match.

```mermaid
flowchart TD
    ZTF[New ZTF Alert] --> CheckStellar{Stellar flag<br/>from enrichment<br/>worker?}

    CheckStellar -->|Yes| ZTF_Stellar[ZTF Stellar]
    CheckStellar -->|No| CheckSGScore{Any sgscore<br/>valid and < 0.5?}
    CheckSGScore -->|Yes| ZTF_Hosted[ZTF Hosted]
    CheckSGScore -->|No| ZTF_Hostless[ZTF Hostless]

    ZTF_Stellar --> CheckLSSTMatch_Stellar{Has LSST<br/>match?}
    ZTF_Hosted --> CheckLSSTMatch_Hosted{Has LSST<br/>match?}
    ZTF_Hostless --> CheckLSSTMatch_Hostless{Has LSST<br/>match?}

    CheckLSSTMatch_Stellar -->|Yes| Topic9[babamul.ztf.lsst-match.stellar]
    CheckLSSTMatch_Stellar -->|No| Topic12[babamul.ztf.no-lsst-match.stellar]
    CheckLSSTMatch_Hosted -->|Yes| Topic10[babamul.ztf.lsst-match.hosted]
    CheckLSSTMatch_Hosted -->|No| Topic13[babamul.ztf.no-lsst-match.hosted]
    CheckLSSTMatch_Hostless -->|Yes| Topic11[babamul.ztf.lsst-match.hostless]
    CheckLSSTMatch_Hostless -->|No| Topic14[babamul.ztf.no-lsst-match.hostless]

    style ZTF_Stellar fill:#2d5f3f,color:#e0e0e0
    style ZTF_Hosted fill:#5f2d2d,color:#e0e0e0
    style ZTF_Hostless fill:#2d3f5f,color:#e0e0e0
    style Topic9 fill:#2d5f3f,color:#e0e0e0
    style Topic10 fill:#5f2d2d,color:#e0e0e0
    style Topic11 fill:#2d3f5f,color:#e0e0e0
    style Topic12 fill:#2d5f3f,color:#e0e0e0
    style Topic13 fill:#5f2d2d,color:#e0e0e0
    style Topic14 fill:#2d3f5f,color:#e0e0e0
```
