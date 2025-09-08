#!/usr/bin/env bash

BOOM_DIR="$HOME/boom"

LOGS_DIR=${1:-$BOOM_DIR/logs/boom}
PERSISTENT_DIR="$BOOM_DIR/apptainer/persistent"
SCRIPTS_DIR="$BOOM_DIR/apptainer/scripts"
CONFIG_FILE="$BOOM_DIR/config.yaml"
SIF_DIR="$BOOM_DIR/apptainer/sif"

mkdir -p "$PERSISTENT_DIR/mongodb"
mkdir -p "$PERSISTENT_DIR/valkey"
mkdir -p "$PERSISTENT_DIR/alerts"
mkdir -p "$PERSISTENT_DIR/kafka_data"
mkdir -p "$PERSISTENT_DIR/uptime-kuma"

# Clear log files or create them if they don't exist
mkdir -p "$LOGS_DIR/kafka"
mkdir -p "$LOGS_DIR/valkey"
mkdir -p "$LOGS_DIR/monitoring"
: > "$LOGS_DIR/consumer.log"
: > "$LOGS_DIR/scheduler.log"
: > "$LOGS_DIR/monitoring/otel-collector.log"
: > "$LOGS_DIR/monitoring/prometheus.log"
: > "$LOGS_DIR/monitoring/uptime-kuma.log"

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

echo "$(current_datetime) - Starting BOOM services with Apptainer"

# -----------------------------
# 1. MongoDB
# -----------------------------
echo "$(current_datetime) - Starting MongoDB"
apptainer instance run --bind "$PERSISTENT_DIR/mongodb:/data/db" "$SIF_DIR/mongo.sif" mongo
sleep 5
$SCRIPTS_DIR/mongodb-healthcheck.sh # Wait for MongoDB to be ready

# -----------------------------
# 2. Valkey
# -----------------------------
echo "$(current_datetime) - Starting Valkey"
apptainer instance run "$SIF_DIR/valkey.sif" valkey
$SCRIPTS_DIR/valkey-healthcheck.sh # Wait for Valkey to be ready

# -----------------------------
# 3. Kafka broker
# -----------------------------
echo "$(current_datetime) - Starting Kafka broker"
apptainer instance run \
    --bind "$PERSISTENT_DIR/kafka_data:/var/lib/kafka/data" \
    --bind "$PERSISTENT_DIR/kafka_data:/opt/kafka/config" \
    --bind "$LOGS_DIR/kafka:/opt/kafka/logs" \
    "$SIF_DIR/kafka.sif" broker
$SCRIPTS_DIR/kafka-healthcheck.sh # Wait for Kafka to be ready

# -----------------------------
# 4. Boom
# -----------------------------
echo "$(current_datetime) - Starting BOOM instance"
apptainer instance start \
  --bind "$CONFIG_FILE:/app/config.yaml" \
  --bind "$PERSISTENT_DIR/alerts:/app/data/alerts" \
  "$SIF_DIR/boom.sif" boom

# -----------------------------
# 5. Monitoring services
# -----------------------------
echo "$(current_datetime) - Starting Prometheus"
apptainer exec \
  --bind "$BOOM_DIR/config/prometheus.yaml:/etc/prometheus/prometheus.yaml" \
  "$SIF_DIR/prometheus.sif" \
  /bin/prometheus --web.enable-otlp-receiver --config.file=/etc/prometheus/prometheus.yaml \
  > "$LOGS_DIR/monitoring/prometheus.log" 2>&1 &

echo "$(current_datetime) - Starting Otel Collector"
apptainer exec \
  --bind "$BOOM_DIR/config/apptainer-otel-collector-config.yaml:/etc/otelcol/config.yaml" \
  --bind "$LOGS_DIR/monitoring:/var/log/otel-collector" \
  "$SIF_DIR/otel-collector.sif" /otelcol --config /etc/otelcol/config.yaml \
  > "$LOGS_DIR/monitoring/otel-collector.log" 2>&1 &

apptainer instance start \
  --bind "$PERSISTENT_DIR/uptime-kuma:/app/data" \
  --bind "$LOGS_DIR/monitoring:/app/logs" \
  "$SIF_DIR/uptime-kuma.sif" kuma

echo "$(current_datetime) - BOOM services started successfully"