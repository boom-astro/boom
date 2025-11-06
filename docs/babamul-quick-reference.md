# Babamul Signup Quick Reference

## For API Users

### Sign Up
```bash
curl -X POST http://localhost:4000/babamul/signup \
  -H "Content-Type: application/json" \
  -d '{"email": "your.email@example.com"}'
```

Response:
```json
{
  "message": "Signup successful. Please check your email for activation instructions.",
  "token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
  "expires_in": 86400,
  "activation_required": true
}
```

### Activate Account
```bash
curl -X POST http://localhost:4000/babamul/activate \
  -H "Content-Type: application/json" \
  -d '{
    "email": "your.email@example.com",
    "activation_code": "550e8400-e29b-41d4-a716-446655440000"
  }'
```

### Use Token for Protected Endpoints (Future)
```bash
curl -X GET http://localhost:4000/babamul/streams \
  -H "Authorization: Bearer <your-token>"
```

### Connect to Kafka Streams
```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'babamul.transients',
    bootstrap_servers='kafka.boom.example.com:9092',
    security_protocol='SASL_SSL',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username='your.email@example.com',  # Your email
    sasl_plain_password='<token-from-signup>',     # Token from signup
    group_id='babamul-myapp'
)

for msg in consumer:
    print(msg.value)
```

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
1. POST /babamul/signup
   ↓
2. Create user in database
   ↓
3. Create Kafka SCRAM user
   ↓
4. Add Kafka ACLs (babamul.* topics)
   ↓
5. Return JWT token
   ↓
6. POST /babamul/activate (with code)
   ↓
7. Account activated
```

### Authentication
- **Babamul users**: JWT with `sub: "babamul:<user_id>"`
- **Main users**: JWT with `sub: "<user_id>"`
- Middleware checks prefix to enforce separation

### Database Collections
- `users` - Main API users
- `babamul_users` - Babamul users (separate)
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
- Email and token are correct
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
3. Create additional Babamul endpoints (streams list, profile management)
4. Add monitoring and metrics for signups/activations
5. Implement activation code expiration
