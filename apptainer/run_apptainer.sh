#!/usr/bin/env bash

SIF_DIR="$HOME/boom/apptainer/sif"
DATA_DIR="$HOME/boom/data"
TESTS_DIR="$HOME/boom/tests/throughput"
LOGS_DIR=${1:-$HOME/boom/logs/boom}

EXPECTED_ALERTS=29142
N_FILTERS=25
CONFIG_FILE="$TESTS_DIR/config.yaml"

mkdir -p "$LOGS_DIR"

# Clean up old data
rm -rf data/valkey/*

touch "$LOGS_DIR/producer.log"
touch "$LOGS_DIR/consumer.log"
touch "$LOGS_DIR/scheduler.log"

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

echo "$(current_datetime) - Starting BOOM services with Apptainer"

# -----------------------------
# 1. MongoDB
# -----------------------------
echo "$(current_datetime) - Starting MongoDB"
mkdir -p "$DATA_DIR/mongodb"
apptainer instance start --bind "$DATA_DIR/mongodb:/data/db" "$SIF_DIR/mongo.sif" mongo

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
    --bind "$DATA_DIR/mongodb:/data/db" \
    --bind "$DATA_DIR/alerts/kowalski.NED.json.gz:/kowalski.NED.json.gz" \
    --bind "$TESTS_DIR/mongo-init-apptainer.sh:/mongo-init.sh" \
    --bind "$TESTS_DIR/cats150.filter.json:/cats150.filter.json" \
    --env DB_NAME=boom-benchmarking \
    --env MONGO_INITDB_ROOT_USERNAME=mongoadmin \
    --env MONGO_INITDB_ROOT_PASSWORD=mongoadminsecret \
    "$SIF_DIR/mongo.sif" \
    /bin/bash /mongo-init.sh

# -----------------------------
# 2. Valkey
# -----------------------------
echo "$(current_datetime) - Starting Valkey"
mkdir -p "$DATA_DIR/valkey"
apptainer instance start \
  --bind "$DATA_DIR/valkey:/data" \
  "$SIF_DIR/valkey.sif" valkey


sleep 15

#apptainer exec --bind "data/valkey:/data" "apptainer/sif/valkey.sif" \
#  valkey-server --save "" --appendonly no --port 6379 --bind 0.0.0.0 --dbfilename ""

# -----------------------------
# 3. Kafka broker
# -----------------------------
echo "$(current_datetime) - Starting Kafka broker"
mkdir -p "$DATA_DIR/kafka/logs"

# Generate meta.properties file if it doesn't exist
if [ ! -f "/tmp/kraft-combined-logs/meta.properties" ]; then
  echo "$(current_datetime) - Generating Kafka meta.properties file"
  apptainer exec \
  --bind "$DATA_DIR/kafka/logs:/opt/kafka/logs" \
  apptainer/sif/kafka.sif \
  /opt/kafka/bin/kafka-storage.sh format \
    --config /opt/kafka/config/server.properties \
    --cluster-id "$(uuidgen)" \
    --ignore-formatted \
    --standalone
fi

sleep 5

# Start Kafka broker instance
apptainer instance start \
    --bind "$DATA_DIR/kafka/logs:/opt/kafka/logs" \
    "$SIF_DIR/kafka.sif" broker

cpt=0
while ! apptainer exec instance://broker /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    if [ $cpt -ge 5 ]; then
        echo "$(current_datetime) - Kafka broker still not ready after 15 seconds; exiting"
        exit 1
    fi
    sleep 3
    cpt=$((cpt + 1))
done

echo "$(current_datetime) - Kafka broker is ready"

# -----------------------------
# 4. Producer
# -----------------------------
echo "$(current_datetime) - Starting Producer"
#apptainer exec \
#    --bind "$DATA_DIR/alerts:/app/data/alerts" \
#    --bind "$TESTS_DIR/config.yaml:/app/config.yaml" \
#    "$SIF_DIR/boom-benchmarking.sif" \
#    /bin/sh -c "/app/kafka_producer ztf 20250311 public"

# Start producer instance
apptainer instance start \
  --bind "$DATA_DIR/alerts:/app/data/alerts" \
  --bind "$TESTS_DIR/config.yaml:/app/config.yaml" \
  "$SIF_DIR/boom-benchmarking.sif" producer \
  /bin/sh -c "sleep 0.1"

sleep 5

# Run producer command in the instance and redirect output to log file
apptainer exec instance://producer /app/kafka_producer ztf 20250311 public > "$LOGS_DIR/producer.log" 2>&1

# -----------------------------
# 5. Consumer
# -----------------------------
echo "$(current_datetime) - Starting Consumer"
apptainer exec \
    --bind "$TESTS_DIR/config.yaml:/app/config.yaml" \
    "$SIF_DIR/boom-benchmarking.sif" \
    /bin/sh -c "sleep 5 && /app/kafka_consumer ztf 20250311 public" \
    > "$LOGS_DIR/consumer.log" 2>&1 &

#
#apptainer exec \
#    --bind "tests/throughput/config.yaml:/app/config.yaml" \
#    "apptainer/sif/boom-benchmarking.sif" \
#    /bin/sh -c "/app/kafka_consumer ztf 20250311 public"

# -----------------------------
# 6. Scheduler
# -----------------------------
echo "$(current_datetime) - Starting Scheduler"
apptainer exec \
    --bind "$DATA_DIR/models:/app/models" \
    --bind "$TESTS_DIR/config.yaml:/app/config.yaml" \
    --env RUST_LOG=debug,ort=error \
    "$SIF_DIR/boom-benchmarking.sif" \
    /app/scheduler ztf \
    > "$LOGS_DIR/scheduler.log" 2>&1 &

# -----------------------------
# 7. Wait for alerts ingestion
# -----------------------------
echo "$(current_datetime) - Waiting for all alerts to be ingested"
while [ $(apptainer exec instance://mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" --quiet --eval "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments()") -lt $EXPECTED_ALERTS ]; do
    count=$(apptainer exec instance://mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" --quiet --eval "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments()")
    echo "$(current_datetime) - Alerts ingested so far: $count / $EXPECTED_ALERTS"
    sleep 1
done

echo "$(current_datetime) - Waiting for all alerts to be classified"
while [ $(apptainer exec "$SIF_DIR/mongo.sif" mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" --quiet --eval "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments({ classifications: { \$exists: true } })") -lt $EXPECTED_ALERTS ]; do
    sleep 1
done

echo "$(current_datetime) - Waiting for filters to run on all alerts"
while [ $(grep "passed filter $N_FILTERS" "$LOGS_DIR/scheduler.log" | awk -F'/' '{sum += $NF} END {print sum}') -lt $EXPECTED_ALERTS ]; do
    sleep 1
done

echo "$(current_datetime) - All tasks completed; shutting down BOOM services"

# -----------------------------
# 8. Stop all instances
# -----------------------------
apptainer instance stop mongo
apptainer instance stop valkey
apptainer instance stop broker

echo "$(current_datetime) - Done"
exit 0