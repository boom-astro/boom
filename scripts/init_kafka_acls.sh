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

BROKER="broker:9092"  # Use internal PLAINTEXT for administrative operations
ADMIN_USER="admin"
ADMIN_PWD="${KAFKA_ADMIN_PASSWORD}"
READ_USER="readonly"
READ_PWD="${KAFKA_READONLY_PASSWORD}"

# KAFKA_OPTS with JAAS is set at container level (docker-compose).

kafka_log() { echo "[init-kafka] $*"; }

wait_for_kafka() {
  local -r start=$(date +%s)
  until /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server "$BROKER" >/dev/null 2>&1; do
    if (( $(date +%s) - start > 60 )); then
      k kafka_log "Kafka did not become ready in time"; exit 1
    fi
    sleep 2
  done
}

wait_for_kafka

# Helper to check if a SCRAM user exists
user_exists() {
  /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BROKER" \
    --entity-type users --entity-name "$1" --describe 2>/dev/null | grep -q "Configs for user-principal 'User:$1'"
}

create_or_update_user() {
  local user=$1
  local pwd=$2
  if user_exists "$user"; then
    kafka_log "User $user exists; ensuring SCRAM credentials updated"
    /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BROKER" \
      --alter --entity-type users --entity-name "$user" \
      --add-config "SCRAM-SHA-512=[password=$pwd]" >/dev/null
  else
    kafka_log "Creating user $user"
    /opt/kafka/bin/kafka-configs.sh --bootstrap-server "$BROKER" \
      --alter --entity-type users --entity-name "$user" \
      --add-config "SCRAM-SHA-512=[password=$pwd]" >/dev/null
  fi
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
