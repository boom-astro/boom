#!/usr/bin/env bash

# Script to benchmark BOOM throughput using Apptainer containers.
# $1 = boom directory
# $2 = log directory (optional; default: tests/apptainer/logs/boom)

BOOM_DIR="$1"
SCRIPTS_DIR="$BOOM_DIR/apptainer/scripts"
TESTS_DIR="$BOOM_DIR/tests"
SIF_DIR="$BOOM_DIR/apptainer/sif"

LOGS_DIR="${2:-$BOOM_DIR/tests/apptainer/logs/boom}"
PERSISTENT_DIR="$TESTS_DIR/apptainer/persistent"
CONFIG_FILE="$TESTS_DIR/throughput/config.yaml"

EXPECTED_ALERTS=29142
N_FILTERS=25
TIMEOUT_SECS=300 # 5 minutes

GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

mkdir -p "$LOGS_DIR"
mkdir -p "$PERSISTENT_DIR"

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

stop_all_instances() {
    apptainer instance stop boom
    apptainer instance stop kafka
    apptainer instance stop valkey
    apptainer instance stop mongo
}

# -----------------------------
# Load environment variables
# -----------------------------
set -a
source "$BOOM_DIR/.env"
set +a

# -----------------------------
# 1. MongoDB
# -----------------------------
echo && echo "$(current_datetime) - Starting MongoDB"
mkdir -p "$PERSISTENT_DIR/mongodb"
apptainer instance run \
  --env MONGO_INITDB_ROOT_USERNAME=${BOOM_DATABASE__USERNAME:-mongoadmin} \
  --env MONGO_INITDB_ROOT_PASSWORD=${BOOM_DATABASE__PASSWORD:?BOOM_DATABASE__PASSWORD must be set} \
  --bind "$PERSISTENT_DIR/mongodb:/data/db" "$SIF_DIR/mongo.sif" mongo
sleep 5
"$SCRIPTS_DIR/mongodb-healthcheck.sh"

echo "$(current_datetime) - Initializing MongoDB with test data"
apptainer exec \
    --bind "$TESTS_DIR/data/alerts/kowalski.NED.json.gz:/kowalski.NED.json.gz" \
    --bind "$TESTS_DIR/throughput/apptainer_mongo-init.sh:/mongo-init.sh" \
    --bind "$TESTS_DIR/throughput/cats150.filter.json:/cats150.filter.json" \
    --env DB_NAME=boom-benchmarking \
    --env DB_ADD_URI= \
    "$SIF_DIR/mongo.sif" \
    /bin/bash /mongo-init.sh

# -----------------------------
# 2. Valkey
# -----------------------------
echo && echo "$(current_datetime) - Starting Valkey"
mkdir -p "$PERSISTENT_DIR/valkey"
mkdir -p "$LOGS_DIR/valkey"
apptainer instance run \
  --bind "$PERSISTENT_DIR/valkey:/data" \
  --bind "$LOGS_DIR/valkey:/log" \
  "$SIF_DIR/valkey.sif" valkey
"$SCRIPTS_DIR/valkey-healthcheck.sh"

# -----------------------------
# 3. Kafka
# -----------------------------
echo && echo "$(current_datetime) - Starting Kafka"
mkdir -p "$PERSISTENT_DIR/kafka_data"
mkdir -p "$LOGS_DIR/kafka"
apptainer instance run \
    --bind "$BOOM_DIR/config/kafka_server_jaas.conf:/etc/kafka/kafka_server_jaas.conf:ro" \
    --bind "$PERSISTENT_DIR/kafka_data:/var/lib/kafka/data" \
    --bind "$PERSISTENT_DIR/kafka_data:/opt/kafka/config" \
    --bind "$LOGS_DIR/kafka:/opt/kafka/logs" \
    "$SIF_DIR/kafka.sif" kafka
"$SCRIPTS_DIR/kafka-healthcheck.sh"

# -----------------------------
# 4. Boom
# -----------------------------
echo && echo "$(current_datetime) - Starting BOOM instance"
mkdir -p "$PERSISTENT_DIR/alerts"
apptainer instance start \
  --env BOOM_DATABASE__PASSWORD=${BOOM_DATABASE__PASSWORD:?BOOM_DATABASE__PASSWORD must be set} \
  --env BOOM_DATABASE__USERNAME=${BOOM_DATABASE__USERNAME:-mongoadmin} \
  --env RUST_LOG=debug,ort=error \
  --bind "$CONFIG_FILE:/app/config.yaml" \
  --bind "$PERSISTENT_DIR/alerts:/app/data/alerts" \
  "$SIF_DIR/boom.sif" boom

sleep 3

# -----------------------------
# 5. Producer
# -----------------------------
echo && echo "$(current_datetime) - Starting Producer"
if pgrep -f "/app/kafka_producer" > /dev/null; then
  echo -e "${RED}Boom producer already running.${END}"
else
  apptainer exec --pwd /app \
    instance://boom /app/kafka_producer ztf 20250311 public --server-url localhost:29092 \
    > "$LOGS_DIR/producer.log" 2>&1
  echo -e "${GREEN}$(current_datetime) - Producer finished sending alerts${END}"
fi

# -----------------------------
# 6. Consumer
# -----------------------------
echo && echo "$(current_datetime) - Starting Consumer"
if pgrep -f "/app/kafka_consumer" > /dev/null; then
  echo -e "${RED}Boom consumer already running.${END}"
else
  apptainer exec --pwd /app \
    instance://boom /app/kafka_consumer ztf 20250311 public \
    > "$LOGS_DIR/consumer.log" 2>&1 &
  echo -e "${GREEN}Boom consumer started for survey ztf${END}"
fi

# -----------------------------
# 7. Scheduler
# -----------------------------
echo && echo "$(current_datetime) - Starting Scheduler"
if pgrep -f "/app/scheduler" > /dev/null; then
  echo -e "${RED}Boom scheduler already running.${END}"
else
  apptainer exec --pwd /app \
    instance://boom /app/scheduler ztf \
    > "$LOGS_DIR/scheduler.log" 2>&1 &
  echo -e "${GREEN}Boom scheduler started for survey ztf${END}"
fi

# -----------------------------
# 8. Wait for consumer to start expecting messages
# -----------------------------
echo "$(current_datetime) - Waiting for Kafka consumer to start"
START_TIME=$(date +%s)
while ! grep -q "Consumer received first message, continuing..." "$LOGS_DIR/consumer.log"; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo "$(current_datetime) - Timeout reached while waiting for Kafka consumer to start"
        stop_all_instances
        exit 1
    fi
    sleep 1
done

# -----------------------------
# 9. Wait for alerts ingestion
# -----------------------------
echo && echo "$(current_datetime) - Waiting for all alerts to be ingested"
START_TIME=$(date +%s)
while [ "$(apptainer exec instance://mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" --quiet --eval "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments()")" -lt $EXPECTED_ALERTS ]; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo "$(current_datetime) - Timeout reached while waiting for alerts to be ingested"
        stop_all_instances
        exit 1
    fi
    sleep 1
done

echo "$(current_datetime) - Waiting for all alerts to be classified"
START_TIME=$(date +%s)
while [ "$(apptainer exec instance://mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" --quiet --eval "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments({ classifications: { \$exists: true } })")" -lt $EXPECTED_ALERTS ]; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo "$(current_datetime) - Timeout reached while waiting for alerts to be classified"
        stop_all_instances
        exit 1
    fi
    sleep 1
done

echo "$(current_datetime) - Waiting for filters to run on all alerts"
START_TIME=$(date +%s)
PASSED_ALERTS=0
while [ $PASSED_ALERTS -lt $EXPECTED_ALERTS ]; do
    PASSED_ALERTS=$(cat "$LOGS_DIR/scheduler.log" | grep "passed filter" | awk -F'/' '{sum += $NF} END {print sum}')
    PASSED_ALERTS=${PASSED_ALERTS:-0}
    PASSED_ALERTS=$((PASSED_ALERTS / N_FILTERS))
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo "$(current_datetime) - Timeout reached while waiting for filters to run on all alerts"
        stop_all_instances
        exit 1
    fi
    sleep 1
done

# -----------------------------
# 10. Stop all instances
# -----------------------------
echo "$(current_datetime) - All tasks completed; shutting down BOOM services"
stop_all_instances

exit 0