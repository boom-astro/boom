#!/usr/bin/env bash
set -euo pipefail

# This script runs inside the Kafka container (mounted read-only) on startup.
# It will:
#  1. Create SCRAM users (admin + readonly) if they don't exist
#  2. Create ACLs granting readonly user DESCRIBE / READ on all existing topics and consumer groups
#  3. (Idempotent) Skip creation if already present
#
# Requires env vars:
#   KAFKA_ADMIN_PASSWORD
#   KAFKA_READONLY_PASSWORD
# Uses 'admin' and 'readonly' usernames.
#
# NOTE: Broker requires no explicit JAAS file for SCRAM; credentials are stored in metadata log.

# Allow overriding the bootstrap address (useful in CI); default to internal broker listener
BROKER="${KAFKA_BOOTSTRAP_SERVER:-broker:29092}"  # Use internal PLAINTEXT inter-broker listener for administrative operations
ADMIN_USER="admin"
ADMIN_PWD="${KAFKA_ADMIN_PASSWORD}"
READ_USER="readonly"
READ_PWD="${KAFKA_READONLY_PASSWORD}"
TIMEOUT=300 # seconds (extended for CI environments)

# KAFKA_OPTS with JAAS is set at container level (docker-compose).

kafka_log() { echo "[init-kafka] $*"; }

# Wait for broker hostname to be resolvable and port to be reachable
# This handles Docker DNS/networking race conditions in CI
wait_for_broker_network() {
  local host="${BROKER%%:*}"  # Extract hostname from broker:port
  local port="${BROKER##*:}"  # Extract port
  local start=$(date +%s)
  local max_wait=$TIMEOUT

  kafka_log "Waiting for broker DNS resolution: $host"
  until getent hosts "$host" >/dev/null 2>&1; do
    if (( $(date +%s) - $start > $max_wait )); then
      kafka_log "DNS resolution failed for $host after ${max_wait}s"
      kafka_log "Trying nc/telnet as fallback..."
      break
    fi
    sleep 1
  done

  kafka_log "Waiting for broker port connectivity: $BROKER"
  start=$(date +%s)
  until nc -z "$host" "$port" 2>/dev/null || timeout 1 bash -c "echo > /dev/tcp/$host/$port" 2>/dev/null; do
    if (( $(date +%s) - $start > $max_wait )); then
      kafka_log "ERROR: Could not connect to $BROKER after ${max_wait} s"
      exit 1
    fi
    sleep 2
  done
  kafka_log "Network pre-check complete for $BROKER"
}

wait_for_kafka() {
  local start=$(date +%s)
  kafka_log "Waiting for Kafka at $BROKER (cluster-id)"
  until /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server "$BROKER" >/dev/null 2>&1; do
    if (( $(date +%s) - $start > $TIMEOUT )); then
      kafka_log "Timed out waiting for cluster-id from $BROKER"; exit 1
    fi
    sleep 3
  done

  local start=$(date +%s)
  kafka_log "Waiting for metadata quorum readiness"
  until /opt/kafka/bin/kafka-metadata-quorum.sh --bootstrap-server "$BROKER" describe --status >/dev/null 2>&1; do
    if (( $(date +%s) - $start > $TIMEOUT )); then
      kafka_log "Timed out waiting for metadata quorum describe --status"; exit 1
    fi
    sleep 3
  done

  local start=$(date +%s)
  kafka_log "Waiting for configs API (users --describe)"
  until /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BROKER" --entity-type users --describe >/dev/null 2>&1; do
    if (( $(date +%s) - $start > $TIMEOUT )); then
      kafka_log "Timed out waiting for configs API to respond"; exit 1
    fi
    sleep 3
  done

  local start=$(date +%s)
  kafka_log "Waiting for authorizer (acls --list)"
  until /opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BROKER" --list >/dev/null 2>&1; do
    if (( $(date +%s) - $start > $TIMEOUT )); then
      kafka_log "Timed out waiting for authorizer to respond"; exit 1
    fi
    sleep 3
  done

  kafka_log "Kafka is ready"
}

wait_for_broker_network
wait_for_kafka

# Helper to check if a SCRAM user exists
user_exists() {
  /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BROKER" \
    --entity-type users --entity-name "$1" --describe 2>/dev/null | grep -q "Configs for user-principal 'User:$1'"
}

create_or_update_user() {
  local user=$1
  local pwd=$2
  local attempt=0
  local max_attempts=5
  while (( attempt < max_attempts )); do
    if user_exists "$user"; then
      kafka_log "User $user exists; updating SCRAM credentials (attempt $((attempt+1)))"
    else
      kafka_log "Creating user $user (attempt $((attempt+1)))"
    fi
    if /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BROKER" \
        --alter --entity-type users --entity-name "$user" \
        --add-config "SCRAM-SHA-512=[password=$pwd]" >/dev/null 2>&1; then
        return 0
    fi
    attempt=$((attempt+1))
    sleep 4
  done
  kafka_log "Failed to create/update user $user after $max_attempts attempts" >&2
  exit 1
}

create_or_update_user "$ADMIN_USER" "$ADMIN_PWD"
create_or_update_user "$READ_USER" "$READ_PWD"

# ACLs: admin is super user (declared via KAFKA_SUPER_USERS) -> no ACL needed
# Readonly user: allow DESCRIBE & READ on topics; DESCRIBE on cluster; READ on consumer groups (so it can commit offsets)

# Add cluster describe for readonly (idempotent; duplicates ignored)
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BROKER" \
  --add --allow-principal "User:$READ_USER" --operation DESCRIBE --cluster >/dev/null 2>&1 || true

# Allow read on all topics present & future via pattern
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BROKER" \
  --add --allow-principal "User:$READ_USER" --operation READ --operation DESCRIBE --topic '*' >/dev/null 2>&1 || true

# Allow read (group membership) for any consumer group starting with ro- (convention) & explicit wildcard
/opt/kafka/bin/kafka-acls.sh --bootstrap-server "$BROKER" \
  --add --allow-principal "User:$READ_USER" --operation READ --group '*' >/dev/null 2>&1 || true

kafka_log "Initialization complete"
