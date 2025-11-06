#!/bin/bash

# Test script for Babamul signup flow
# Demonstrates the complete signup → activate → auth workflow

set -e

API_URL="${API_URL:-http://localhost:4000}"
TEST_EMAIL="${TEST_EMAIL:-test_$(date +%s)@example.com}"

echo "==================================="
echo "Babamul Signup Test"
echo "==================================="
echo "API URL: $API_URL"
echo "Test Email: $TEST_EMAIL"
echo ""

# Step 1: Signup
echo "Step 1: Signing up with email..."
SIGNUP_RESPONSE=$(curl -s -X POST "$API_URL/babamul/signup" \
  -H "Content-Type: application/json" \
  -d "{\"email\": \"$TEST_EMAIL\"}")

echo "Signup response:"
echo "$SIGNUP_RESPONSE" | jq .
echo ""

# Check if signup was successful
if ! echo "$SIGNUP_RESPONSE" | jq -e '.activation_required' > /dev/null; then
  echo "❌ Signup failed!"
  exit 1
fi

echo "✅ Signup successful!"
echo ""

# Step 2: Get activation code from database
echo "Step 2: Getting activation code from database..."
ACTIVATION_CODE=$(mongosh boom --quiet --eval "db.babamul_users.findOne({email: '$TEST_EMAIL'}).activation_code" | tail -1)

if [ -z "$ACTIVATION_CODE" ]; then
  echo "❌ Could not retrieve activation code from database!"
  exit 1
fi

echo "Activation code: $ACTIVATION_CODE"
echo ""

# Step 3: Activate account
echo "Step 3: Activating account..."
ACTIVATE_RESPONSE=$(curl -s -X POST "$API_URL/babamul/activate" \
  -H "Content-Type: application/json" \
  -d "{\"email\": \"$TEST_EMAIL\", \"activation_code\": \"$ACTIVATION_CODE\"}")

echo "Activation response:"
echo "$ACTIVATE_RESPONSE" | jq .
echo ""

# Extract password
PASSWORD=$(echo "$ACTIVATE_RESPONSE" | jq -r '.password')

if [ -z "$PASSWORD" ] || [ "$PASSWORD" == "null" ]; then
  echo "❌ Activation failed - no password returned!"
  exit 1
fi

echo "✅ Account activated!"
echo "Password: $PASSWORD"
echo ""

# Step 4: Authenticate to get JWT token
echo "Step 4: Authenticating to get JWT token..."
AUTH_RESPONSE=$(curl -s -X POST "$API_URL/babamul/auth" \
  -H "Content-Type: application/json" \
  -d "{\"email\": \"$TEST_EMAIL\", \"password\": \"$PASSWORD\"}")

echo "Auth response:"
echo "$AUTH_RESPONSE" | jq .
echo ""

# Extract JWT token
JWT_TOKEN=$(echo "$AUTH_RESPONSE" | jq -r '.access_token')

if [ -z "$JWT_TOKEN" ] || [ "$JWT_TOKEN" == "null" ]; then
  echo "❌ Authentication failed - no token returned!"
  exit 1
fi

echo "✅ Authentication successful!"
echo "JWT Token: ${JWT_TOKEN:0:50}..."
echo ""

# Summary
echo "==================================="
echo "✅ All tests passed!"
echo "==================================="
echo ""
echo "Summary:"
echo "  Email: $TEST_EMAIL"
echo "  Password: $PASSWORD"
echo "  JWT Token: ${JWT_TOKEN:0:50}..."
echo ""
echo "You can now use this account to:"
echo "  1. Authenticate to API endpoints using the JWT token"
echo "  2. Connect to Kafka using email + password"
echo ""
echo "Kafka example:"
echo "  Username: $TEST_EMAIL"
echo "  Password: $PASSWORD"
echo ""

# Optional: Test with Kafka (if kafka-console-consumer is available)
if command -v kafka-console-consumer &> /dev/null; then
  echo "To test Kafka connection:"
  echo "kafka-console-consumer \\"
  echo "  --bootstrap-server localhost:9092 \\"
  echo "  --topic babamul.test \\"
  echo "  --consumer-property security.protocol=SASL_SSL \\"
  echo "  --consumer-property sasl.mechanism=SCRAM-SHA-512 \\"
  echo "  --consumer-property sasl.jaas.config='org.apache.kafka.common.security.scram.ScramLoginModule required username=\"$TEST_EMAIL\" password=\"$PASSWORD\";'"
  echo ""
fi

# Cleanup prompt
echo "To clean up this test user from database:"
echo "mongosh boom --eval \"db.babamul_users.deleteOne({email: '$TEST_EMAIL'})\""
echo ""
