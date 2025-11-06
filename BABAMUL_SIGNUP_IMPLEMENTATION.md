# Babamul Signup Implementation Summary

## Overview

Implemented a public signup API for Babamul users with email-based registration, activation workflow, and dual authentication (Kafka SCRAM + JWT API access).

## Authentication Flow

```
1. User signs up with email only (POST /babamul/signup)
   ↓
2. System creates user with activation code
   ↓
3. User receives activation code (currently in DB, future: via email)
   ↓
4. User activates account (POST /babamul/activate)
   ↓
5. System generates 32-char random password
   ↓
6. System creates Kafka SCRAM user + ACLs
   ↓
7. Password returned to user (shown ONCE)
   ↓
8. User authenticates (POST /babamul/auth) with email+password
   ↓
9. System returns JWT token for API access
```

## Key Design Decisions

### Password-Based Approach
- **Single password for both uses**: Simplified from initial dual-token design
- **32-character random string**: Secure, no special format needed
- **One-time reveal**: Password only shown during activation for security
- **Standard OAuth-like flow**: Separate authentication endpoint for JWT tokens

### Account Separation
- **JWT claim prefix**: Babamul users have `sub: "babamul:<user_id>"`
- **Middleware enforcement**: Main API endpoints reject Babamul users
- **Separate collection**: `babamul_users` isolated from main `users`
- **Future-proof**: Easy to add Babamul-specific endpoints

### Kafka Integration
- **SCRAM-SHA-512**: Industry-standard authentication
- **Username = email**: Natural user identifier
- **Password = activation password**: Same credential for simplicity
- **ACL pattern**: `babamul.*` topics, `babamul-*` consumer groups

## API Endpoints

### POST /babamul/signup
- **Public**: No authentication required
- **Input**: `{ "email": "user@example.com" }`
- **Output**: `{ "message": "...", "activation_required": true }`
- **Side effects**: Creates user in database with activation code

### POST /babamul/activate
- **Public**: No authentication required
- **Input**: `{ "email": "...", "activation_code": "..." }`
- **Output**: `{ "message": "...", "activated": true, "email": "...", "password": "..." }`
- **Side effects**:
  - Generates 32-char password
  - Creates Kafka SCRAM user
  - Adds Kafka ACLs
  - Marks user as activated
  - Clears activation code

### POST /babamul/auth
- **Public**: No authentication required
- **Input**: `{ "email": "...", "password": "..." }`
- **Output**: `{ "access_token": "JWT...", "token_type": "Bearer", "expires_in": 86400 }`
- **Validates**: Email, password (bcrypt), and activation status

## Database Schema

```rust
Collection: babamul_users
{
  "_id": String,              // UUID
  "email": String,            // Unique, indexed
  "password_hash": String,    // bcrypt hash
  "activation_code": Option<String>,  // UUID, cleared after activation
  "is_activated": bool,       // false until activated
  "created_at": i64          // Unix timestamp
}
```

**Indexes:**
- Unique index on `email`

## Kafka Configuration

**User Creation:**
```bash
kafka-configs.sh --alter \
  --add-config 'SCRAM-SHA-512=[password=...]' \
  --entity-type users \
  --entity-name user@example.com
```

**ACLs Added:**
- Topic: `babamul.*` - READ, DESCRIBE
- Group: `babamul-*` - READ

## Files Modified/Created

### Core Implementation
- `/src/api/routes/babamul.rs` - Signup, activation, and auth endpoints
- `/src/api/babamul_auth.rs` - Middleware for Babamul authentication
- `/src/api/mod.rs` - Module registration
- `/src/api/routes/mod.rs` - Route registration
- `/src/api/auth.rs` - Made Claims public, added Babamul rejection
- `/src/api/db.rs` - Database initialization with indexes
- `/src/bin/api.rs` - Endpoint registration

### Testing
- `/tests/api/test_babamul.rs` - Comprehensive test suite
- `/tests/api/mod.rs` - Module registration
- `/scripts/test_babamul_signup.sh` - Manual testing script

### Documentation
- `/docs/babamul-api.md` - Full API documentation
- `/docs/babamul-quick-reference.md` - Quick reference guide
- `/BABAMUL_SIGNUP_IMPLEMENTATION.md` - This file

## Security Features

1. **Password hashing**: bcrypt with DEFAULT_COST
2. **Email uniqueness**: Database constraint prevents duplicates
3. **Activation required**: Users must verify email before Kafka access
4. **One-time password reveal**: Password only shown during activation
5. **JWT expiration**: Tokens expire after 24 hours (configurable)
6. **Account isolation**: Middleware prevents cross-contamination

## Testing

```bash
# Run unit/integration tests (requires MongoDB)
cargo test test_babamul

# Manual end-to-end test
./scripts/test_babamul_signup.sh
```

## Future Enhancements

### Planned
1. **Email integration**: Send activation codes via email
2. **Code expiration**: Activation codes expire after N hours
3. **Rate limiting**: Prevent signup abuse
4. **Password reset**: Allow users to reset forgotten passwords
5. **Account management**: Update email, view Kafka topics, etc.

### Possible
- Token rotation/refresh tokens
- 2FA support
- Usage analytics
- Topic subscription management UI

## Configuration

Requires these config values:
- `api.auth.secret_key` - JWT signing secret
- `api.auth.token_expiration` - JWT TTL (seconds)
- Kafka broker access for SCRAM user creation
- MongoDB connection for user storage

## Migration Notes

**Evolution of Design:**
1. **Initial**: Dual tokens (JWT for API, bbml_* for Kafka)
2. **Iteration 1**: Single bbml_* token for both uses
3. **Final**: Pure password-based (activation reveals password, separate /auth endpoint)

**Rationale for final design:**
- Simpler user experience (one credential)
- Standard OAuth-like pattern (familiar to developers)
- Password-based Kafka auth is industry standard
- JWT tokens can be refreshed without changing Kafka password

## Production Checklist

Before deploying:
- [ ] Configure email sending for activation codes
- [ ] Set up rate limiting on signup endpoint
- [ ] Add activation code expiration
- [ ] Configure secure JWT secret
- [ ] Set up monitoring for signup/activation metrics
- [ ] Test Kafka broker connectivity from API container
- [ ] Verify MongoDB indexes are created
- [ ] Test complete flow in staging environment
- [ ] Document operational procedures (user deletion, password reset, etc.)

## Support

For questions or issues:
- See `/docs/babamul-api.md` for API documentation
- See `/docs/babamul-quick-reference.md` for quick examples
- Run `./scripts/test_babamul_signup.sh` for testing
- Check MongoDB `babamul_users` collection for user data
- Check Kafka ACLs with `kafka-acls.sh --list`
