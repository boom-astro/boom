#!/bin/bash
# Test script for Babamul signup API endpoint

set -e

API_BASE="${API_BASE:-http://localhost:4000}"
TEST_EMAIL="${TEST_EMAIL:-test+$(date +%s)@babamul.example.com}"

echo "Testing Babamul Signup API"
echo "API Base: $API_BASE"
echo "Test Email: $TEST_EMAIL"
echo ""

# Test 1: Sign up
echo "=== Test 1: POST /babamul/signup ==="
SIGNUP_RESPONSE=$(curl -s -X POST "$API_BASE/babamul/signup" \
  -H "Content-Type: application/json" \
  -d "{\"email\": \"$TEST_EMAIL\"}")

echo "Response:"
echo "$SIGNUP_RESPONSE" | jq .

# Extract token
TOKEN=$(echo "$SIGNUP_RESPONSE" | jq -r '.token')
ACTIVATION_REQUIRED=$(echo "$SIGNUP_RESPONSE" | jq -r '.activation_required')

echo ""
echo "Token: $TOKEN"
echo "Activation Required: $ACTIVATION_REQUIRED"

# Test 2: Try to sign up again with same email (should fail)
echo ""
echo "=== Test 2: Duplicate signup (should fail with 409) ==="
DUPLICATE_RESPONSE=$(curl -s -w "\nHTTP_CODE:%{http_code}" -X POST "$API_BASE/babamul/signup" \
  -H "Content-Type: application/json" \
  -d "{\"email\": \"$TEST_EMAIL\"}")

echo "$DUPLICATE_RESPONSE"

# Test 3: Get activation code from database (for testing)
echo ""
echo "=== Test 3: Retrieve activation code from database ==="
echo "NOTE: In production, this would be sent via email"
echo "Connect to MongoDB and run:"
echo "  db.babamul_users.findOne({email: \"$TEST_EMAIL\"})"
echo ""
echo "For testing, you can extract it with:"
echo "  mongosh boom --eval 'db.babamul_users.findOne({email: \"$TEST_EMAIL\"})' | grep activation_code"

# Test 4: Activate account (requires manual step above)
echo ""
echo "=== Test 4: Activation ==="
echo "To activate, run:"
echo "  ACTIVATION_CODE=<code_from_db>"
echo "  curl -X POST \"$API_BASE/babamul/activate\" \\"
echo "    -H \"Content-Type: application/json\" \\"
echo "    -d '{\"email\": \"$TEST_EMAIL\", \"activation_code\": \"'\$ACTIVATION_CODE'\"}'"

# Test 5: Test with invalid email
echo ""
echo "=== Test 5: Invalid email (should fail with 400) ==="
INVALID_RESPONSE=$(curl -s -w "\nHTTP_CODE:%{http_code}" -X POST "$API_BASE/babamul/signup" \
  -H "Content-Type: application/json" \
  -d '{"email": "invalid-email"}')

echo "$INVALID_RESPONSE"

echo ""
echo "=== Tests Complete ==="
echo ""
echo "To clean up, connect to MongoDB and run:"
echo "  db.babamul_users.deleteOne({email: \"$TEST_EMAIL\"})"
