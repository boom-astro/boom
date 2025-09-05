#!/usr/bin/env bash

BOOM_DIR="$HOME/boom"

LOGS_DIR=${1:-$BOOM_DIR/logs/boom}
CONFIG_FILE="$BOOM_DIR/config.yaml"

SCRIPTS_DIR="$BOOM_DIR/apptainer/scripts"
SIF_DIR="$BOOM_DIR/apptainer/sif"
PERSISTENT_DIR="$BOOM_DIR/apptainer/persistent"

mkdir -p "$PERSISTENT_DIR/mongodb"
mkdir -p "$PERSISTENT_DIR/valkey"
mkdir -p "$PERSISTENT_DIR/alerts"
mkdir -p "$PERSISTENT_DIR/kafka_data"

# Clear log files or create them if they don't exist
: > "$LOGS_DIR/scheduler.log"
mkdir -p "$LOGS_DIR/kafka"
mkdir -p "$LOGS_DIR/valkey"

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

echo "$(current_datetime) - Starting BOOM services with Apptainer"

# -----------------------------
# 1. MongoDB
# -----------------------------
echo "$(current_datetime) - Starting MongoDB"
apptainer instance start --bind "$PERSISTENT_DIR/mongodb:/data/db" "$SIF_DIR/mongo.sif" mongo
sleep 5
$SCRIPTS_DIR/mongodb-healthcheck.sh # Wait for MongoDB to be ready

# -----------------------------
# 2. Valkey
# -----------------------------
echo "$(current_datetime) - Starting Valkey"
apptainer instance start \
  --bind "$PERSISTENT_DIR/valkey:/data" \
  --bind "$LOGS_DIR/valkey:/valkey/logs" \
  "$SIF_DIR/valkey.sif" valkey
$SCRIPTS_DIR/valkey-healthcheck.sh # Wait for Valkey to be ready

# -----------------------------
# 3. Kafka broker
# -----------------------------
echo "$(current_datetime) - Starting Kafka broker"
if [ ! -f "/tmp/kraft-combined-logs/meta.properties" ]; then # Generate meta.properties if it doesn't exist
  echo "$(current_datetime) - Generating Kafka meta.properties file"
  apptainer exec "$SIF_DIR/kafka.sif" \
  /opt/kafka/bin/kafka-storage.sh format \
    --config /opt/kafka/config/server.properties \
    --cluster-id "$(uuidgen)" \
    --ignore-formatted \
    --standalone
fi
apptainer instance start \
    --bind "$LOGS_DIR/kafka:/opt/kafka/logs" \
    --bind "$PERSISTENT_DIR/kafka_data:/var/lib/kafka/data" \
    "$SIF_DIR/kafka.sif" broker
$SCRIPTS_DIR/kafka-healthcheck.sh # Wait for Kafka to be ready

echo "$(current_datetime) - BOOM services started successfully"