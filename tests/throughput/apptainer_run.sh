#!/usr/bin/env bash

# Script to benchmark BOOM throughput using Apptainer containers.
# $1 = boom directory
# $2 = log directory name (e.g., benchmark_20240401)

BOOM_DIR="$1"
SCRIPTS_DIR="$BOOM_DIR/apptainer/scripts"
DATA_DIR="$BOOM_DIR/data"
TESTS_DIR="$BOOM_DIR/tests"
SIF_DIR="$BOOM_DIR/apptainer/sif"

LOGS_DIR="$TESTS_DIR/apptainer/${2:-logs/boom}"
PERSISTENT_DIR="$TESTS_DIR/apptainer/persistent"
CONFIG_FILE="$TESTS_DIR/throughput/config.yaml"

EXPECTED_ALERTS=29142
N_FILTERS=25

GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

mkdir -p "$LOGS_DIR"
mkdir -p "$PERSISTENT_DIR"

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

# -----------------------------
# 1. MongoDB
# -----------------------------
echo && echo "$(current_datetime) - Starting MongoDB"
mkdir -p "$PERSISTENT_DIR/mongodb"
apptainer instance run --bind "$PERSISTENT_DIR/mongodb:/data/db" "$SIF_DIR/mongo.sif" mongo
sleep 5
"$SCRIPTS_DIR/mongodb-healthcheck.sh"

echo "$(current_datetime) - Initializing MongoDB with test data"
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
echo && echo "$(current_datetime) - Starting Valkey"
mkdir -p "$PERSISTENT_DIR/valkey"
mkdir -p "$LOGS_DIR/valkey"
apptainer instance run \
  --bind "$PERSISTENT_DIR/valkey:/data" \
  --bind "$LOGS_DIR/valkey:/log" \
  "$SIF_DIR/valkey.sif" valkey
"$SCRIPTS_DIR/valkey-healthcheck.sh"

# -----------------------------
# 3. Kafka broker
# -----------------------------
echo && echo "$(current_datetime) - Starting Kafka broker"
mkdir -p "$PERSISTENT_DIR/kafka_data"
mkdir -p "$LOGS_DIR/kafka"
apptainer instance run \
    --bind "$PERSISTENT_DIR/kafka_data:/var/lib/kafka/data" \
    --bind "$PERSISTENT_DIR/kafka_data:/opt/kafka/config" \
    --bind "$LOGS_DIR/kafka:/opt/kafka/logs" \
    "$SIF_DIR/kafka.sif" broker
"$SCRIPTS_DIR/kafka-healthcheck.sh"

# -----------------------------
# 4. Boom
# -----------------------------
echo && echo "$(current_datetime) - Starting BOOM instance"
mkdir -p "$PERSISTENT_DIR/alerts"
apptainer instance start \
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
  apptainer exec \
    --bind "$PERSISTENT_DIR/alerts:/app/data/alerts" \
    instance://boom /app/kafka_producer "ztf 20250311 public" \
    > "$LOGS_DIR/producer.log" 2>&1
  echo "${GREEN}$(current_datetime) - Producer finished sending alerts${END}"
fi

# -----------------------------
# 6. Consumer
# -----------------------------
echo && echo "$(current_datetime) - Starting Consumer"
if pgrep -f "/app/kafka_consumer" > /dev/null; then
  echo -e "${RED}Boom consumer already running.${END}"
else
  apptainer exec \
    instance://boom /app/kafka_consumer "ztf 20250311 public" \
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
  apptainer exec \
    instance://boom /app/scheduler "ztf" \
    > "$LOGS_DIR/scheduler.log" 2>&1 &
  echo -e "${GREEN}Boom scheduler started for survey ztf${END}"
fi

# -----------------------------
# 8. Wait for alerts ingestion
# -----------------------------
echo && echo "$(current_datetime) - Waiting for all alerts to be ingested"
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
    PASSED_ALERTS=$(cat "$LOGS_DIR/scheduler.log" | grep "passed filter" | awk -F'/' '{sum += $NF} END {print sum}')
    PASSED_ALERTS=${PASSED_ALERTS:-0}
    PASSED_ALERTS=$((PASSED_ALERTS / N_FILTERS))
    sleep 1
done

# -----------------------------
# 8. Stop all instances
# -----------------------------
echo "$(current_datetime) - All tasks completed; shutting down BOOM services"
apptainer instance stop boom
apptainer instance stop broker
apptainer instance stop valkey
apptainer instance stop mongo

exit 0