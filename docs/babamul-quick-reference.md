# Babamul API Quick Reference

## Interactive Documentation

🚀 **Try it live**: Visit `/babamul/docs` on your BOOM API instance for interactive Swagger UI documentation where you can test all endpoints directly from your browser.

## Signup Flow

```bash
# 1. Sign up with email
curl -X POST https://api.boom.example.com/babamul/signup \
  -H "Content-Type: application/json" \
  -d '{"email": "user@example.com"}'

# Response:
# {
#   "message": "Signup successful. An activation code has been sent to user@example.com...",
#   "activation_required": true
# }

# 2. Activate account with code (check email or database)
curl -X POST https://api.boom.example.com/babamul/activate \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "activation_code": "550e8400-e29b-41d4-a716-446655440000"
  }'

# Response (SAVE THE PASSWORD!):
# {
#   "message": "Account activated successfully. Save your password - it won't be shown again!",
#   "activated": true,
#   "email": "user@example.com",
#   "password": "aB3cD4eF5gH6iJ7kL8mN9oP0qR1sT2uV"
# }

# 3. Get JWT token for API access
curl -X POST https://api.boom.example.com/babamul/auth \
  -H "Content-Type: application/json" \
  -d '{
    "email": "user@example.com",
    "password": "aB3cD4eF5gH6iJ7kL8mN9oP0qR1sT2uV"
  }'

# Response:
# {
#   "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
#   "token_type": "Bearer",
#   "expires_in": 86400
# }
```

## API Authentication

Use the JWT token from `/babamul/auth` in your requests:

```bash
curl https://api.boom.example.com/babamul/some-endpoint \
  -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
```

## Kafka Authentication

Use your email and password from activation:

- **Username**: `user@example.com`
- **Password**: `aB3cD4eF5gH6iJ7kL8mN9oP0qR1sT2uV` (from activation response)

### Connect to Kafka Streams
```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'babamul.transients',
    bootstrap_servers='kafka.boom.example.com:9092',
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username='user@example.com',  # Your email
    sasl_plain_password='aB3cD4eF5gH6iJ7kL8mN9oP0qR1sT2uV',  # Password from activation
    group_id='babamul-myapp'
)

for msg in consumer:
    print(msg.value)
```

## Key Concepts

- **One password, dual purpose**: The password from activation works for both Kafka (SCRAM) and API (via `/babamul/auth`)
- **Password shown once**: Save it during activation - it won't be displayed again
- **JWT tokens**: Get fresh JWT tokens via `/babamul/auth` endpoint as needed
- **Account separation**: Babamul users (prefix `babamul:`) are isolated from main API users

## For Developers

### File Locations
- **Routes**: `/src/api/routes/babamul.rs`
- **Middleware**: `/src/api/babamul_auth.rs`
- **Tests**: `/tests/api/test_babamul.rs`
- **Docs**: `/docs/babamul-api.md`

### Run Tests
```bash
cargo test test_babamul
```

### Check Database
```bash
# Connect to MongoDB
mongosh boom

# List all Babamul users
db.babamul_users.find().pretty()

# Find specific user
db.babamul_users.findOne({email: "test@example.com"})

# Delete test user
db.babamul_users.deleteOne({email: "test@example.com"})
```

### Check Kafka ACLs
```bash
# Inside Kafka container
/opt/kafka/bin/kafka-acls.sh \
  --bootstrap-server broker:29092 \
  --list \
  | grep "User:test@example.com"
```

## Architecture

### User Flow
```
1. POST /babamul/signup (email only)
   ↓
2. Create user in database with activation code
   ↓
3. User activates via POST /babamul/activate
   ↓
4. Generate 32-char random password
   ↓
5. Create Kafka SCRAM user with password
   ↓
6. Add Kafka ACLs (babamul.* topics)
   ↓
7. Return password to user (shown once!)
   ↓
8. User authenticates via POST /babamul/auth
   ↓
9. Receive JWT token for API access
```

### Authentication
- **Babamul users**: JWT with `sub: "babamul:<user_id>"`
- **Main users**: JWT with `sub: "<user_id>"`
- Middleware checks prefix to enforce separation

### Database Collections
- `users` - Main API users
- `babamul_users` - Babamul users (separate)
  - Fields: `id`, `email`, `password_hash`, `activation_code`, `is_activated`, `created_at`
  - Indexes: unique on `email`
- `filters` - Filter definitions

## Common Issues

### Issue: Kafka ACL creation fails
**Solution**: Ensure Kafka container has ACL authorizer enabled and the API has access to Kafka CLI tools at `/opt/kafka/bin/`

### Issue: Duplicate email error
**Solution**: Email is already registered. Use different email or delete existing user from database.

### Issue: Token validation fails
**Solution**: Check that JWT secret matches in config and that token hasn't expired.

### Issue: Can't connect to Kafka
**Solution**: Verify:
- Email and password are correct
- Kafka broker is accessible
- SCRAM-SHA-512 is enabled
- Topics exist with `babamul.*` prefix

## Configuration

### Environment Variables
- `KAFKA_INTERNAL_BROKER` - Override Kafka broker address (default: `broker:29092`)
- `MONGODB_URI` - MongoDB connection string

### Config File (config.yaml)
```yaml
api:
  auth:
    secret_key: "your-secret-key"
    token_expiration: 86400  # 24 hours in seconds
    admin_username: "admin"
    admin_password: "admin-password"
    admin_email: "admin@example.com"

database:
  name: "boom"
  host: "localhost"
  port: 27017
  username: "boom"
  password: "password"
  # ... other settings
```

## Next Steps

1. Implement email sending for activation codes
2. Add rate limiting to prevent signup spam
3. Add activation code expiration
4. Create additional Babamul endpoints (streams list, profile management)
5. Add monitoring and metrics for signups/activations
6. Implement password reset functionality
