# Babamul Public API

This document describes the public-facing Babamul API endpoints that allow users to sign up for access to Babamul alert streams.

## Overview

Babamul is a public service that provides access to processed astronomical alert streams. Users can sign up with just an email address to receive:

1. A JWT token for accessing Babamul-specific API endpoints
2. Kafka credentials (using their email as username) for consuming alert streams

## User Account Separation

Babamul user accounts are **completely separate** from main BOOM API user accounts:

- **Babamul users**: Can access Babamul endpoints and read from `babamul.*` Kafka topics, but **cannot** access main API features like catalog queries
- **Main API users**: Can access all main API endpoints and Babamul endpoints, but use separate credentials

This separation is enforced via JWT token claims (Babamul users have a `babamul:` prefix in their subject claim) and authentication middleware.

## Endpoints

### POST /babamul/signup

**Public endpoint** - Create a new Babamul user account.

#### Request Body

```json
{
  "email": "user@example.com"
}
```

#### Response (200 OK)

```json
{
  "message": "Signup successful. Please check your email for activation instructions.",
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "expires_in": 86400,
  "activation_required": true
}
```

The `token` can be used immediately for API calls, but certain features may be restricted until the account is activated.

#### Error Responses

- **400 Bad Request**: Invalid email address
- **409 Conflict**: Email already registered
- **500 Internal Server Error**: Database or system error

### POST /babamul/activate

**Public endpoint** - Activate a Babamul user account using the activation code sent via email.

#### Request Body

```json
{
  "email": "user@example.com",
  "activation_code": "550e8400-e29b-41d4-a716-446655440000"
}
```

#### Response (200 OK)

```json
{
  "message": "Account activated successfully",
  "activated": true
}
```

#### Error Responses

- **400 Bad Request**: Invalid activation code
- **404 Not Found**: Email not found
- **500 Internal Server Error**: Database error

## Kafka Access

After signup, users can connect to Babamul Kafka streams using:

- **Username**: Their email address (as provided during signup)
- **Password**: The JWT token received during signup
- **Topics**: All topics matching the pattern `babamul.*`
- **Consumer Groups**: Groups matching the pattern `babamul-*`

### Kafka Configuration Example

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'babamul.transients',
    bootstrap_servers='kafka.boom.example.com:9092',
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username='user@example.com',
    sasl_plain_password='<JWT_TOKEN_FROM_SIGNUP>',
    group_id='babamul-myapp-consumer'
)

for message in consumer:
    print(message.value)
```

## Permissions

Babamul users have the following Kafka ACLs automatically created during signup:

- **READ** on topics with prefix `babamul.`
- **DESCRIBE** on topics with prefix `babamul.`
- **READ** on consumer groups with prefix `babamul-`

These ACLs are managed automatically and cannot be modified by users.

## Database Schema

Babamul users are stored in the `babamul_users` collection with the following schema:

```rust
{
  "_id": String,              // UUID
  "email": String,            // Unique, indexed
  "password_hash": String,    // bcrypt hash of the token (used for Kafka SCRAM)
  "activation_code": Option<String>,  // Cleared after activation
  "is_activated": bool,       // true after activation
  "created_at": i64          // Unix timestamp
}
```

## Implementation Details

### Authentication Flow

1. User signs up with email
2. System generates:
   - Unique user ID
   - Random token (used as Kafka password)
   - Activation code
3. Token is hashed and stored in database
4. JWT token is created with `sub: "babamul:<user_id>"`
5. Kafka SCRAM user is created with email as username
6. Kafka ACLs are added for `babamul.*` topics
7. JWT token and setup instructions are returned to user

### Security Considerations

- All passwords are hashed using bcrypt before storage
- JWT tokens include the `babamul:` prefix to prevent privilege escalation
- Authentication middleware checks for the prefix and restricts access accordingly
- Email addresses are normalized (lowercased and trimmed) before storage
- Unique index on email prevents duplicate accounts

### Future Enhancements

Planned features for the activation system:

- Email delivery of activation codes
- Activation code expiration (e.g., 24 hours)
- Resend activation code endpoint
- Password reset functionality (for Kafka access)
- Rate limiting on signup endpoint

## Testing

See `tests/api/test_babamul.rs` for comprehensive test coverage including:

- Successful signup flow
- Duplicate email rejection
- Invalid email validation
- Activation with correct/incorrect codes
- Already-activated account handling
