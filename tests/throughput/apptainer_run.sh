#!/usr/bin/env bash

BOOM_DIR="$HOME/boom"

LOGS_DIR=${1:-$BOOM_DIR/logs/boom}
SCRIPTS_DIR="$BOOM_DIR/apptainer/scripts"
DATA_DIR="$BOOM_DIR/data"
TESTS_DIR="$BOOM_DIR/tests"

SIF_DIR="$TESTS_DIR/apptainer/sif"
PERSISTENT_DIR="$TESTS_DIR/apptainer/persistent"
CONFIG_FILE="$TESTS_DIR/throughput/config.yaml"

EXPECTED_ALERTS=29142
N_FILTERS=25

mkdir -p "$PERSISTENT_DIR/mongodb"
mkdir -p "$PERSISTENT_DIR/valkey"
mkdir -p "$PERSISTENT_DIR/alerts"
mkdir -p "$PERSISTENT_DIR/kafka"

# Clear log files or create them if they don't exist
: > "$LOGS_DIR/producer.log"
: > "$LOGS_DIR/consumer.log"
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
$SCRIPTS_DIR/mongodb-healthcheck.sh # Wait for MongoDB to be ready

## Mongo-init
echo "$(current_datetime) - Running mongo-init"
apptainer exec \
    --bind "$DATA_DIR/alerts/kowalski.NED.json.gz:/kowalski.NED.json.gz" \
    --bind "$TESTS_DIR/throughput/apptainer_mongo-init.sh:/mongo-init.sh" \
    --bind "$TESTS_DIR/throughput/cats150.filter.json:/cats150.filter.json" \
    --env DB_NAME=boom-benchmarking \
    --env DB_ADD_URI= \
    "$SIF_DIR/mongo.sif" \
    /bin/bash /mongo-init.sh

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
    --bind "$PERSISTENT_DIR/kafka:/var/lib/kafka/data" \
    "$SIF_DIR/kafka.sif" broker
$SCRIPTS_DIR/kafka-healthcheck.sh # Wait for Kafka to be ready

# -----------------------------
# 4. Producer
# -----------------------------
echo "$(current_datetime) - Starting Producer"
apptainer run --bind "$PERSISTENT_DIR/alerts:/app/data/alerts" \
  "$SIF_DIR/boom-benchmarking.sif" \
  /app/kafka_producer ztf 20250311 public \
  > "$LOGS_DIR/producer.log" 2>&1
echo "$(current_datetime) - Producer finished sending alerts"

sleep 5

# -----------------------------
# 5. Consumer
# -----------------------------
echo "$(current_datetime) - Starting Consumer"
apptainer run "$SIF_DIR/boom-benchmarking.sif" \
    /bin/sh -c "/app/kafka_consumer ztf 20250311 public" \
    > "$LOGS_DIR/consumer.log" 2>&1 &
CONSUMER_PID=$! # Save the PID to kill it later

# -----------------------------
# 6. Scheduler
# -----------------------------
echo "$(current_datetime) - Starting Scheduler"
apptainer run --bind "$DATA_DIR/models:/app/data/models" \
    "apptainer/sif/boom-benchmarking.sif" \
    /app/scheduler ztf > "$LOGS_DIR/scheduler.log" 2>&1 &
SCHEDULER_PID=$! # Save the PID to kill it later

# -----------------------------
# 7. Wait for alerts ingestion
# -----------------------------
echo "$(current_datetime) - Waiting for all alerts to be ingested"
while [ $(apptainer exec instance://mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" --quiet --eval "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments()") -lt $EXPECTED_ALERTS ]; do
    sleep 1
done

echo "$(current_datetime) - Waiting for all alerts to be classified"
while [ $(apptainer exec instance://mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" --quiet --eval 'db.getSiblingDB("boom-benchmarking").ZTF_alerts.countDocuments({ classifications: { $exists: true } })') -lt $EXPECTED_ALERTS ]; do
    sleep 1
done

echo "$(current_datetime) - Waiting for filters to run on all alerts"
PASSED_ALERTS=0
while [ $PASSED_ALERTS -lt $EXPECTED_ALERTS ]; do
    PASSED_ALERTS=$(apptainer exec "$SIF_DIR/boom-benchmarking.sif" cat "$LOGS_DIR/scheduler.log" | grep "passed filter" | awk -F'/' '{sum += $NF} END {print sum}')
    PASSED_ALERTS=${PASSED_ALERTS:-0}
    PASSED_ALERTS=$((PASSED_ALERTS / N_FILTERS))
    sleep 1
done
kill $CONSUMER_PID $SCHEDULER_PID # Kill consumer and scheduler processes

# -----------------------------
# 8. Stop all instances
# -----------------------------
echo "$(current_datetime) - All tasks completed; shutting down BOOM services"
apptainer instance stop mongo
apptainer instance stop valkey
apptainer instance stop broker

exit 0