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
  "message": "Signup successful. An activation code has been sent to user@example.com. Use the /babamul/activate endpoint to activate your account and receive your password.",
  "activation_required": true
}
```

**Note:** No credentials are provided at signup. You must activate your account first to receive your password.

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
  "message": "Account activated successfully. Save your password - it won't be shown again!",
  "activated": true,
  "email": "user@example.com",
  "password": "aB3cD4eF5gH6iJ7kL8mN9oP0qR1sT2uV"
}
```

**Important:** The password is only shown once during activation. Save it securely! You'll need it for both Kafka and API authentication.

#### Error Responses

- **400 Bad Request**: Invalid activation code
- **404 Not Found**: Email not found
- **500 Internal Server Error**: Database error

### POST /babamul/auth

**Public endpoint** - Authenticate with email and password to get a JWT token for API access.

#### Request Body

```json
{
  "email": "user@example.com",
  "password": "aB3cD4eF5gH6iJ7kL8mN9oP0qR1sT2uV"
}
```

#### Response (200 OK)

```json
{
  "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "token_type": "Bearer",
  "expires_in": 86400
}
```

Use the `access_token` in the Authorization header for subsequent API requests:
```
Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...
```

#### Error Responses

- **401 Unauthorized**: Invalid credentials or account not activated
- **500 Internal Server Error**: Database or system error

## Kafka Access

After activation, use your email and password for Kafka authentication:

### Kafka Configuration Example

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'babamul.transients',
    bootstrap_servers='kafka.boom.example.com:9092',
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username='user@example.com',
    sasl_plain_password='aB3cD4eF5gH6iJ7kL8mN9oP0qR1sT2uV',  # Password from activation
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
  "_id": String,              // UUID (user ID)
  "email": String,            // Unique, indexed
  "password_hash": String,    // bcrypt hash of the password (32 chars)
  "activation_code": Option<String>,  // Cleared after activation
  "is_activated": bool,       // true after activation
  "created_at": i64          // Unix timestamp
}
```

The password is a 32-character random alphanumeric string, generated during activation and hashed for storage.

## Implementation Details

### Authentication Flow

1. User signs up with email
2. System generates:
   - Unique user ID (UUID)
   - Activation code (UUID)
3. User activates account with activation code
4. Upon activation, system generates:
   - Password (32 random alphanumeric characters)
   - Password is hashed (bcrypt) and stored
   - Kafka SCRAM user is created with email as username
   - Kafka ACLs are added for `babamul.*` topics
5. Password is returned to user (shown only once!)
6. For API access: POST to `/babamul/auth` with email + password → get JWT token
7. For Kafka access: Use email + password directly (SCRAM)

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
