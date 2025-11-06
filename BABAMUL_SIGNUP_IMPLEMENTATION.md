# Babamul Signup Implementation Summary

## Overview

This document summarizes the implementation of the Babamul public signup API endpoint at `/babamul/signup` and related functionality.

## Changes Made

### 1. New Files Created

#### `/src/api/routes/babamul.rs`
- **POST /babamul/signup**: Public endpoint for user registration
- **POST /babamul/activate**: Endpoint for account activation
- Data models: `BabamulUser`, `BabamulSignupPost`, `BabamulSignupResponse`, etc.
- Helper functions:
  - `create_kafka_user_and_acls()`: Creates Kafka SCRAM user and ACLs
  - `create_babamul_jwt()`: Generates JWT tokens with `babamul:` prefix

#### `/src/api/babamul_auth.rs`
- `babamul_auth_middleware()`: Middleware for authenticating Babamul users
- Validates JWT tokens with `babamul:` prefix
- Fetches BabamulUser from database and injects into request
- Checks activation status

#### `/tests/api/test_babamul.rs`
- Comprehensive test suite for signup and activation
- Tests for duplicate email handling, invalid emails, activation codes

#### `/docs/babamul-api.md`
- Complete documentation for the Babamul public API
- Usage examples for both API and Kafka access
- Security considerations and implementation details

### 2. Modified Files

#### `/src/api/mod.rs`
- Added `pub mod babamul_auth;`

#### `/src/api/routes/mod.rs`
- Added `pub mod babamul;`

#### `/src/api/auth.rs`
- Made `encoding_key` field public in `AuthProvider`
- Made `Claims` struct and its fields (`iat`, `exp`) public
- Updated `authenticate_user()` to reject Babamul users (with `babamul:` prefix)

#### `/src/api/db.rs`
- Added `"babamul_users"` to `PROTECTED_COLLECTION_NAMES`
- Created `babamul_users` collection with unique email index in `db_from_config()`

#### `/src/bin/api.rs`
- Registered `routes::babamul::post_babamul_signup`
- Registered `routes::babamul::post_babamul_activate`

#### `/src/api/docs.rs`
- Added both Babamul endpoints to OpenAPI documentation

#### `/tests/api/mod.rs`
- Added `pub mod test_babamul;`

## Key Features

### User Account Separation

- **Babamul users** are stored in a separate `babamul_users` collection
- JWT tokens have `sub: "babamul:<user_id>"` to distinguish them
- Auth middleware prevents Babamul users from accessing main API endpoints
- Main API users cannot use Babamul-specific endpoints (enforced by babamul_auth middleware)

### Signup Flow

1. User submits email to `/babamul/signup`
2. System validates email (basic format check)
3. Checks for duplicate email (unique constraint)
4. Generates:
   - User ID (UUID)
   - Token/password (UUID) for Kafka access
   - Activation code (UUID)
5. Stores user in database with hashed password
6. Creates Kafka SCRAM user (username = email, password = token)
7. Adds Kafka ACLs for `babamul.*` topics
8. Returns JWT token for API access

### Activation Flow

1. User receives activation code (in future: via email)
2. Submits email + code to `/babamul/activate`
3. System verifies code matches stored value
4. Sets `is_activated: true`, clears activation code
5. Account is now fully active

### Kafka Integration

Babamul users automatically get:
- **SCRAM-SHA-512 user** in Kafka (username = email)
- **READ** permission on `babamul.*` topics
- **DESCRIBE** permission on `babamul.*` topics
- **READ** permission on `babamul-*` consumer groups

This allows them to consume from any Babamul stream using their email and token.

## Security

- All passwords hashed with bcrypt
- JWT tokens contain `babamul:` prefix to prevent privilege escalation
- Email normalized (lowercase, trimmed) before storage
- Unique index prevents duplicate accounts
- Separate authentication middleware for Babamul endpoints
- Activation required before full access (configurable)

## Database Schema

### Collection: `babamul_users`

```javascript
{
  _id: String,              // UUID
  email: String,            // Unique index
  password_hash: String,    // bcrypt hash
  activation_code: String?, // Optional, cleared after activation
  is_activated: Boolean,    // Activation status
  created_at: i64          // Unix timestamp
}
```

### Indexes

- `email` (unique): Enforces one account per email

## API Endpoints

### Public Endpoints (No Auth Required)

- `POST /babamul/signup` - Create account
- `POST /babamul/activate` - Activate account

### Protected Babamul Endpoints (Babamul Auth Required)

None yet, but the middleware is in place for future endpoints like:
- `GET /babamul/streams` - List available streams
- `GET /babamul/profile` - Get user profile
- `PATCH /babamul/profile` - Update preferences

## Future Enhancements

1. **Email Integration**
   - Send activation codes via email
   - Welcome emails with setup instructions
   - Password reset functionality

2. **Rate Limiting**
   - Prevent signup spam
   - Limit activation attempts

3. **Expiration**
   - Activation codes expire after 24 hours
   - Option to resend activation code

4. **User Management**
   - Admin endpoints to manage Babamul users
   - User metrics and analytics
   - Quota management

5. **Additional Babamul Endpoints**
   - Profile management
   - Stream subscription preferences
   - Usage statistics

## Testing

Run tests with:
```bash
cargo test test_babamul
```

Tests cover:
- Successful signup
- Duplicate email rejection
- Invalid email validation
- Correct/incorrect activation codes
- Already-activated accounts
- Database state verification

## Dependencies

No new dependencies were added. Implementation uses existing:
- actix-web
- mongodb
- bcrypt
- jsonwebtoken
- uuid
- serde

## Deployment Considerations

1. **Environment Variables**
   - `KAFKA_INTERNAL_BROKER`: Override default broker address
   - Ensure Kafka CLI tools are available at `/opt/kafka/bin/`

2. **Database Migrations**
   - Index creation is automatic on first startup
   - No manual migration required

3. **Kafka Setup**
   - Ensure SCRAM-SHA-512 is enabled
   - Verify ACL authorizer is configured
   - Test topic prefix pattern matching

4. **Monitoring**
   - Log Kafka user/ACL creation failures
   - Track signup success/failure rates
   - Monitor activation rates
