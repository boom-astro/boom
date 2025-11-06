# Babamul API

Babamul provides public access to BOOM's transient alert streams via Kafka. Users sign up with an email address and receive credentials for both Kafka stream access and optional API endpoints.

**Interactive Documentation**: `/babamul/docs` (Swagger UI)

## Architecture

### Account Separation

Babamul accounts are isolated from main BOOM API accounts:

- **Database**: Stored in separate `babamul_users` collection
- **JWT Claims**: Subject contains `babamul:` prefix (e.g., `babamul:user-uuid`)
- **Access Control**: Middleware rejects Babamul tokens on main API endpoints
- **Permissions**: Babamul users can only access `/babamul/*` endpoints and Kafka topics

### Authentication Flow

1. **Signup** (`POST /babamul/signup`): User provides email, system creates account with activation code
2. **Activation** (`POST /babamul/activate`): User submits activation code, receives 32-character password (shown once)
3. **Kafka Access**: Use email + password with SCRAM-SHA-512 authentication
4. **API Access** (`POST /babamul/auth`): Exchange email + password for JWT token

### Database Schema

Collection: `babamul_users`

```javascript
{
  "_id": String,                      // UUID
  "email": String,                    // Unique indexed
  "password_hash": String,            // bcrypt hash
  "activation_code": Option<String>,  // UUID, cleared after activation
  "is_activated": bool,
  "created_at": i64                   // Unix timestamp
}
```

## API Endpoints

### POST /babamul/signup

Create new account with email-only registration.

**Request:**
```json
{ "email": "user@example.com" }
```

**Response (200):**
```json
{
  "message": "Signup successful. An activation code has been sent to user@example.com...",
  "activation_required": true
}
```

**Errors:** `400` (invalid email), `409` (duplicate), `500` (server error)

### POST /babamul/activate

Activate account and receive password.

**Request:**
```json
{
  "email": "user@example.com",
  "activation_code": "550e8400-e29b-41d4-a716-446655440000"
}
```

**Response (200):**
```json
{
  "message": "Account activated successfully. Save your password - it won't be shown again!",
  "activated": true,
  "email": "user@example.com",
  "password": "aB3cD4eF5gH6iJ7kL8mN9oP0qR1sT2uV"
}
```

**Side Effects:**
- Generates 32-character random password
- Creates Kafka SCRAM-SHA-512 user
- Adds Kafka ACLs for `babamul.*` topics and `babamul-*` consumer groups
- Clears activation code

**Errors:** `400` (invalid code), `404` (email not found), `500` (server error)

### POST /babamul/auth

Authenticate to receive JWT token for API access.

**Request:**
```json
{
  "email": "user@example.com",
  "password": "aB3cD4eF5gH6iJ7kL8mN9oP0qR1sT2uV"
}
```

**Response (200):**
```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "Bearer",
  "expires_in": 86400
}
```

**Errors:** `401` (invalid credentials or not activated), `500` (server error)

## Kafka Access

After activation, connect to Kafka using:

- **Username**: Email address
- **Password**: Password from activation response
- **Mechanism**: SCRAM-SHA-512
- **Topics**: `babamul.*` (READ, DESCRIBE)
- **Consumer Groups**: `babamul-*` (READ)

**Example (Python):**
```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'babamul.transients',
    bootstrap_servers='kafka.boom.example.com:9092',
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username='user@example.com',
    sasl_plain_password='aB3cD4eF5gH6iJ7kL8mN9oP0qR1sT2uV',
    group_id='babamul-myapp'
)

for message in consumer:
    print(message.value)
```

## Security

- **Password Storage**: bcrypt hashing with default cost
- **Email Validation**: Normalized (lowercase, trimmed) with unique constraint
- **JWT Security**: 24-hour expiration (configurable), `babamul:` prefix prevents privilege escalation
- **Password Disclosure**: Shown only once during activation
- **Activation Required**: Account must be activated before authentication succeeds

## Implementation

### Files

- **Routes**: `/src/api/routes/babamul.rs`
- **Middleware**: `/src/api/babamul_auth.rs`
- **Tests**: `/tests/api/test_babamul.rs`
- **Test Script**: `/scripts/test_babamul_signup.sh`
- **OpenAPI Docs**: `/src/api/docs.rs` (`BabamulApiDoc`)

### Kafka User Provisioning

During activation, the system executes:

```bash
# Create SCRAM user
kafka-configs.sh --alter \
  --add-config 'SCRAM-SHA-512=[password=...]' \
  --entity-type users \
  --entity-name user@example.com

# Add topic ACLs
kafka-acls.sh --add \
  --allow-principal User:user@example.com \
  --operation READ --operation DESCRIBE \
  --topic 'babamul.*'

# Add consumer group ACLs
kafka-acls.sh --add \
  --allow-principal User:user@example.com \
  --operation READ \
  --group 'babamul-*'
```

### Configuration

Required settings in `config.yaml`:

```yaml
# Enable/disable Babamul API endpoints
babamul:
  enabled: true  # Set to false to disable Babamul endpoints

api:
  auth:
    secret_key: "..."           # JWT signing secret
    token_expiration: 86400     # JWT TTL in seconds

database:
  name: "boom"
  host: "..."
  # ... connection details
```

Environment variables:
- `KAFKA_INTERNAL_BROKER`: Kafka broker address (default: `broker:29092`)

**Note**: When `babamul.enabled` is set to `false`, all Babamul endpoints (`/babamul/*`) and documentation (`/babamul/docs`) will be disabled, and the `babamul_users` collection will not be created.

## Development

### Testing

```bash
# Run test suite
cargo test test_babamul

# Manual end-to-end test
./scripts/test_babamul_signup.sh
```

### Database Inspection

```bash
mongosh boom

# List users
db.babamul_users.find().pretty()

# Find specific user
db.babamul_users.findOne({email: "test@example.com"})

# Delete user
db.babamul_users.deleteOne({email: "test@example.com"})
```

### Kafka ACL Verification

```bash
# Inside Kafka container
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server broker:29092 \
  --list | grep "User:test@example.com"
```

## Future Enhancements

- Email delivery of activation codes (currently stored in database only)
- Activation code expiration (e.g., 24-hour TTL)
- Rate limiting on signup endpoint
- Password reset functionality
- Account management endpoints (update email, deactivate account)
- Usage metrics and monitoring
