#!/usr/bin/env bash

# Script to benchmark BOOM throughput using Docker or Apptainer containers.
# $1 = boom directory
# $2 = platform (docker|apptainer)
# $3 = log directory (optional; default: logs/boom)

GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

# Arguments
BOOM_DIR="$1"
PLATFORM="${2:-docker}"
LOGS_DIR="${3:-$BOOM_DIR/logs/boom}"
if [ "$PLATFORM" != "docker" ] && [ "$PLATFORM" != "apptainer" ]; then
    echo -e "${RED}Invalid platform specified. Use 'docker' or 'apptainer'.${END}"
    exit 1
fi

# Paths
CONFIG_FILE="$TESTS_DIR/throughput/config.yaml"
COMPOSE_CONFIG="tests/throughput/compose.yaml"
SCRIPTS_DIR="$BOOM_DIR/apptainer/scripts"
TESTS_DIR="$BOOM_DIR/tests"
SIF_DIR="$BOOM_DIR/apptainer/sif"
PERSISTENT_DIR="$TESTS_DIR/apptainer/persistent"

# A function that returns the current date and time
current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

stop_all_instances() {
  if [ "$PLATFORM" == "apptainer" ]; then
    apptainer instance stop boom
    apptainer instance stop kafka
    apptainer instance stop valkey
    apptainer instance stop mongo
  else
    docker compose -f $COMPOSE_CONFIG down
  fi
}

if [ "$PLATFORM" == "apptainer" ]; then
  # -----------------------------
  # Load environment variables
  # -----------------------------
  set -a
  source "$BOOM_DIR/.env"
  set +a

  # -----------------------------
  # Start MongoDB
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
  # Start Valkey
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
  # Start Kafka
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
  # Start Boom
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
  # Start Producer
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
  # Start Consumer
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
  # Start Scheduler
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

else # PLATFORM == "docker"

  # Remove any existing containers
  docker compose -f $COMPOSE_CONFIG down

  # Spin up BOOM services with Docker Compose
  docker compose -f $COMPOSE_CONFIG up --build -d

  # Send the logs to file so we can analyze later
  mkdir -p $LOGS_DIR
  docker compose -f $COMPOSE_CONFIG logs producer > $LOGS_DIR/producer.log &
  docker compose -f $COMPOSE_CONFIG logs consumer -f > $LOGS_DIR/consumer.log &
  docker compose -f $COMPOSE_CONFIG logs scheduler -f > $LOGS_DIR/scheduler.log &
  # Also log stats from containers for later analysis
  docker compose -f $COMPOSE_CONFIG stats consumer --format json > $LOGS_DIR/consumer.stats.log &
  docker compose -f $COMPOSE_CONFIG stats scheduler --format json > $LOGS_DIR/scheduler.stats.log &
fi

EXPECTED_ALERTS=29142
N_FILTERS=25
TIMEOUT_SECS=300 # 5 minutes

# -----------------------------
# Wait for the kafka consumer to start expecting messages (when it logs "Consumer received first message, continuing...")
# -----------------------------
echo && echo "$(current_datetime) - Waiting for Kafka consumer to start"
START_TIME=$(date +%s)
if [ "$PLATFORM" = "apptainer" ]; then
    CHECK_KAFKA_CMD="grep -q \"Consumer received first message, continuing...\" \"$LOGS_DIR/consumer.log\""
else
    CHECK_KAFKA_CMD="docker compose -f $COMPOSE_CONFIG logs consumer | grep -q \"Consumer received first message, continuing...\""
fi
while ! eval $CHECK_KAFKA_CMD; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo -e "$(current_datetime) - ${RED} Timeout reached while waiting for Kafka consumer to start${END}"
        stop_all_instances
        exit 1
    fi
    sleep 1
done

# -----------------------------
# Wait for alerts ingestion
# -----------------------------
echo "$(current_datetime) - Waiting for all alerts to be ingested"
START_TIME=$(date +%s)
if [ "$PLATFORM" = "apptainer" ]; then
    COUNT_INGESTION_CMD="apptainer exec instance://mongo mongosh \"mongodb://mongoadmin:mongoadminsecret@localhost:27017\" --quiet --eval \"db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments()\""
else
    COUNT_INGESTION_CMD="docker compose -f $COMPOSE_CONFIG exec mongo mongosh \"mongodb://mongoadmin:mongoadminsecret@localhost:27017\" --quiet --eval \"db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments()\""
fi
while [ "$($COUNT_INGESTION_CMD)" -lt $EXPECTED_ALERTS ]; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo -e "$(current_datetime) - ${RED}Timeout reached while waiting for alerts to be ingested${END}"
        stop_all_instances
        exit 1
    fi
    sleep 1
done

# -----------------------------
# Wait for alerts classification
# -----------------------------
echo "$(current_datetime) - Waiting for all alerts to be classified"
START_TIME=$(date +%s)
if [ "$PLATFORM" = "apptainer" ]; then
    COUNT_CLASSIFIED_CMD="apptainer exec instance://mongo mongosh \"mongodb://mongoadmin:mongoadminsecret@localhost:27017\" --quiet --eval \"db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments({ classifications: { \$exists: true } })\""
else
    COUNT_CLASSIFIED_CMD="docker compose -f $COMPOSE_CONFIG exec mongo mongosh \"mongodb://mongoadmin:mongoadminsecret@localhost:27017\" --quiet --eval \"db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments({ classifications: { \$exists: true } })\""
fi
while [ "$($COUNT_CLASSIFIED_CMD)" -lt $EXPECTED_ALERTS ]; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo -e "$(current_datetime) - ${RED}Timeout reached while waiting for alerts to be classified${END}"
        stop_all_instances
        exit 1
    fi
    sleep 1
done

# Wait until we've filtered all alerts
echo "$(current_datetime) - Waiting for filters to run on all alerts"
START_TIME=$(date +%s)
PASSED_ALERTS=0
while [ $PASSED_ALERTS -lt $EXPECTED_ALERTS ]; do
    if [ "$PLATFORM" = "apptainer" ]; then
      PASSED_ALERTS=$(cat "$LOGS_DIR/scheduler.log" | grep "passed filter" | awk -F'/' '{sum += $NF} END {print sum}')
    else
      PASSED_ALERTS=$(docker compose -f $COMPOSE_CONFIG logs scheduler | grep "passed filter" | awk -F'/' '{sum += $NF} END {print sum}')
    fi
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

echo "$(current_datetime) - All alerts ingested, classified, and filtered"
echo "$(current_datetime) - Reading from Kafka output topic"
uv run tests/throughput/read-kafka-output.py

# -----------------------------
# 10. Stop all instances
# -----------------------------
echo -e "$(current_datetime) - ${GREEN}All tasks completed; shutting down BOOM services${END}"
echo && stop_all_instances

exit 0