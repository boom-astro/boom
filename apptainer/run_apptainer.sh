#!/usr/bin/env bash

SIF_DIR="$HOME/boom/apptainer/sif"
PERSISTENT_DIR="$HOME/boom/apptainer/persistent"
DATA_DIR="$HOME/boom/data"
TESTS_DIR="$HOME/boom/tests/throughput"
CONFIG_FILE="$TESTS_DIR/config.yaml"
LOGS_DIR=${1:-$HOME/boom/logs/boom}

EXPECTED_ALERTS=29142
N_FILTERS=25

# Clean up old data
rm -rf data/valkey/*

mkdir -p "$PERSISTENT_DIR/mongodb"
mkdir -p "$PERSISTENT_DIR/valkey"

mkdir -p "$LOGS_DIR"
mkdir -p "$LOGS_DIR/kafka"
mkdir -p "$LOGS_DIR/valkey"

# Clear log files or create them if they don't exist
: > "$LOGS_DIR/producer.log"
: > "$LOGS_DIR/consumer.log"
: > "$LOGS_DIR/scheduler.log"

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

echo "$(current_datetime) - Starting BOOM services with Apptainer"

# -----------------------------
# 1. MongoDB
# -----------------------------
echo "$(current_datetime) - Starting MongoDB"
apptainer instance start --bind "$PERSISTENT_DIR/mongodb:/data/db" "$SIF_DIR/mongo.sif" mongo

# Wait for MongoDB to be healthy
echo "$(current_datetime) - Waiting for MongoDB to be ready"
until apptainer exec instance://mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" --eval "db.adminCommand('ping')" &>/dev/null; do
    echo "$(current_datetime) - MongoDB not ready yet..."
    sleep 1
done
echo "$(current_datetime) - MongoDB is ready"

## Mongo-init
echo "$(current_datetime) - Running mongo-init"
apptainer exec \
    --bind "$PERSISTENT_DIR/mongodb:/data/db" \
    --bind "$DATA_DIR/alerts/kowalski.NED.json.gz:/kowalski.NED.json.gz" \
    --bind "$TESTS_DIR/mongo-init-apptainer.sh:/mongo-init.sh" \
    --bind "$TESTS_DIR/cats150.filter.json:/cats150.filter.json" \
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

# -----------------------------
# 3. Kafka broker
# -----------------------------
echo "$(current_datetime) - Starting Kafka broker"

# Generate meta.properties file if it doesn't exist
if [ ! -f "/tmp/kraft-combined-logs/meta.properties" ]; then
  echo "$(current_datetime) - Generating Kafka meta.properties file"
  apptainer exec \
  --bind "$LOGS_DIR/kafka:/opt/kafka/logs" \
  apptainer/sif/kafka.sif \
  /opt/kafka/bin/kafka-storage.sh format \
    --config /opt/kafka/config/server.properties \
    --cluster-id "$(uuidgen)" \
    --ignore-formatted \
    --standalone
fi

# Start Kafka broker instance
apptainer instance start \
    --bind "$LOGS_DIR/kafka:/opt/kafka/logs" \
    "$SIF_DIR/kafka.sif" broker

cpt=0
while ! apptainer exec instance://broker /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    if [ $cpt -ge 5 ]; then
        echo "$(current_datetime) - Kafka broker still not ready after 15 seconds; exiting"
        exit 1
    fi
    sleep 3
    cpt=$((cpt + 1))
    echo "$(current_datetime) - Waiting for Kafka broker to be ready..."
done

echo "$(current_datetime) - Kafka broker is ready"

# -----------------------------
# 4. Producer
# -----------------------------
echo "$(current_datetime) - Starting Producer"
apptainer exec --pwd /app \
  --bind "$DATA_DIR/alerts:/app/data/alerts" \
  --bind "$CONFIG_FILE:/app/config.yaml" \
  "$SIF_DIR/boom-benchmarking.sif" \
  /app/kafka_producer ztf 20250311 public \
  > "$LOGS_DIR/producer.log" 2>&1 &

# -----------------------------
# 5. Consumer
# -----------------------------
echo "$(current_datetime) - Starting Consumer"
apptainer exec --pwd /app \
    --bind "$CONFIG_FILE:/app/config.yaml" \
    "$SIF_DIR/boom-benchmarking.sif" \
    /app/kafka_consumer ztf 20250311 public \
    > "$LOGS_DIR/consumer.log" 2>&1 &
CONSUMER_PID=$!

# -----------------------------
# 6. Scheduler
# -----------------------------
echo "$(current_datetime) - Starting Scheduler"
apptainer exec --pwd /app \
    --bind "$DATA_DIR/models:/app/models" \
    --bind "$CONFIG_FILE:/app/config.yaml" \
    --bind "$LOGS_DIR:/app/logs" \
    "apptainer/sif/boom-benchmarking.sif" \
    /app/scheduler ztf \
    > "$LOGS_DIR/scheduler.log" 2>&1 &
SCHEDULER_PID=$!

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
# Kill consumer and scheduler processes
kill $CONSUMER_PID $SCHEDULER_PID

echo "$(current_datetime) - All tasks completed; shutting down BOOM services"

# -----------------------------
# 8. Stop all instances
# -----------------------------
apptainer instance stop mongo
apptainer instance stop valkey
apptainer instance stop broker

exit 0