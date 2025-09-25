#!/usr/bin/env bash

# Script to start BOOM services using Apptainer containers. Arguments:
# $1 = boom directory
# $2 = service to start:
#      - boom        : starts both consumer and scheduler
#      - consumer    : starts the consumer process
#      - scheduler   : starts the scheduler process
#      - mongo       : starts the MongoDB instance
#      - broker      : starts the kafka broker instance
#      - valkey      : starts the Valkey instance
#      - prometheus  : starts the Prometheus instance
#      - otel        : starts OpenTelemetry Collector process
#      - listener    : starts Boom healthcheck listener process
#      - kuma        : starts the Kuma instance
#      If not specified or 'all', all services will be started.
#
# Additional arguments for 'boom', 'consumer', or 'scheduler':
# $3 = survey name (required for consumer/scheduler)
# $4 = date (optional, used by consumer)
# $5 = program ID (optional, used by consumer)
# $6 = scheduler config path (optional, used by scheduler)

BOOM_DIR="$1"
LOGS_DIR="$BOOM_DIR/logs/boom"
PERSISTENT_DIR="$BOOM_DIR/apptainer/persistent"
SCRIPTS_DIR="$BOOM_DIR/apptainer/scripts"
CONFIG_FILE="$BOOM_DIR/config.yaml"
SIF_DIR="$BOOM_DIR/apptainer/sif"

GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

mkdir -p "$LOGS_DIR"
mkdir -p "$PERSISTENT_DIR"

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

start_service() {
    local service="$1"
    local target="$2"
    if [[ -z "$target" || "$target" = "all" || "$target" = "$service" ]]; then
        return 0
    fi
    return 1
}

# -----------------------------
# 1. MongoDB
# -----------------------------
if start_service "mongo" "$2"; then
  echo && echo "$(current_datetime) - Starting MongoDB"
  mkdir -p "$PERSISTENT_DIR/mongodb"
  apptainer instance run --bind "$PERSISTENT_DIR/mongodb:/data/db" "$SIF_DIR/mongo.sif" mongo
  sleep 5
  "$SCRIPTS_DIR/mongodb-healthcheck.sh"
fi

# -----------------------------
# 2. Valkey
# -----------------------------
if start_service "valkey" "$2"; then
  echo && echo "$(current_datetime) - Starting Valkey"
  mkdir -p "$PERSISTENT_DIR/valkey"
  mkdir -p "$LOGS_DIR/valkey"
  apptainer instance run \
    --bind "$PERSISTENT_DIR/valkey:/data" \
    --bind "$LOGS_DIR/valkey:/log" \
    "$SIF_DIR/valkey.sif" valkey
  "$SCRIPTS_DIR/valkey-healthcheck.sh"
fi

# -----------------------------
# 3. Kafka broker
# -----------------------------
if start_service "broker" "$2"; then
  echo && echo "$(current_datetime) - Starting Kafka broker"
  mkdir -p "$PERSISTENT_DIR/kafka_data"
  mkdir -p "$LOGS_DIR/kafka"
  apptainer instance run \
      --bind "$PERSISTENT_DIR/kafka_data:/var/lib/kafka/data" \
      --bind "$PERSISTENT_DIR/kafka_data:/opt/kafka/config" \
      --bind "$LOGS_DIR/kafka:/opt/kafka/logs" \
      "$SIF_DIR/kafka.sif" broker
  "$SCRIPTS_DIR/kafka-healthcheck.sh"
fi

# -----------------------------
# 5. Prometheus
# -----------------------------
if start_service "prometheus" "$2"; then
  echo && echo "$(current_datetime) - Starting Prometheus instance"
  mkdir -p "$LOGS_DIR/prometheus"
  apptainer instance start \
    --bind "$BOOM_DIR/config/prometheus.yaml:/etc/prometheus/prometheus.yaml" \
    --bind "$LOGS_DIR/prometheus:/var/log" \
    "$SIF_DIR/prometheus.sif" prometheus
  "$SCRIPTS_DIR/prometheus-healthcheck.sh"
fi

# -----------------------------
# 6. OpenTelemetry Collector
# -----------------------------
if start_service "otel" "$2"; then
  echo && echo "$(current_datetime) - Starting Otel Collector"
  if pgrep -f "otelcol" > /dev/null; then
    echo "$(current_datetime) - Otel Collector already running"
  else
    mkdir -p "$LOGS_DIR/otel-collector"
    apptainer exec \
      --bind "$BOOM_DIR/config/apptainer-otel-collector-config.yaml:/etc/otelcol/config.yaml" \
      --bind "$LOGS_DIR/otel-collector:/var/log/otel-collector" \
      "$SIF_DIR/otel-collector.sif" /otelcol --config /etc/otelcol/config.yaml \
      > "$LOGS_DIR/otel-collector/otel-collector.log" 2>&1 &
  fi
  "$SCRIPTS_DIR/process-healthcheck.sh" "otelcol" otel-collector
fi

# -----------------------------
# 7. Healthcheck listener
# -----------------------------
if start_service "listener" "$2"; then
  echo && echo "$(current_datetime) - Starting Boom healthcheck listener"
  if pgrep -f "boom-healthcheck-listener.py" > /dev/null; then
    echo "$(current_datetime) - Boom healthcheck listener already running"
  else
    mkdir -p "$LOGS_DIR/listener"
    python "$SCRIPTS_DIR/boom-healthcheck-listener.py" > "$LOGS_DIR/listener/listener.log" 2>&1 &
  fi
  "$SCRIPTS_DIR/boom-listener-healthcheck.sh"
fi

# -----------------------------
# 4. Boom
# -----------------------------
if start_service "boom" "$2" || start_service "consumer" "$2" || start_service "scheduler" "$2"; then
  echo && echo "$(current_datetime) - Starting BOOM instance"
  mkdir -p "$PERSISTENT_DIR/alerts"
  apptainer instance start \
    --bind "$CONFIG_FILE:/app/config.yaml" \
    --bind "$PERSISTENT_DIR/alerts:/app/data/alerts" \
    "$SIF_DIR/boom.sif" boom

  sleep 3
  survey=$3
  if [ -z "$survey" ]; then
    echo -e "${RED}Missing required argument: survey name${END}"
    exit 1
  fi

  if start_service "boom" "$2" || start_service "consumer" "$2"; then
    if pgrep -f "/app/kafka_consumer" > /dev/null; then
      echo -e "${RED}Boom consumer already running.${END}"
    else
      ARGS=("$survey")
      [ -n "$4" ] && ARGS+=("$4") # $4=date
      [ -n "$5" ] && ARGS+=("$5") # $5=program ID
      apptainer exec --env-file env instance://boom /app/kafka_consumer "${ARGS[@]}" > "$LOGS_DIR/consumer.log" 2>&1 &
      echo -e "${GREEN}Boom consumer started for survey $survey${END}"
    fi
  fi

  if start_service "boom" "$2" || start_service "scheduler" "$2"; then
    if pgrep -f "/app/scheduler" > /dev/null; then
      echo -e "${RED}Boom scheduler already running.${END}"
    else
      ARGS=("$survey")
      [ -n "$6" ] && ARGS+=("$6") # $6=config path
      apptainer exec instance://boom /app/scheduler "${ARGS[@]}" > "$LOGS_DIR/scheduler.log" 2>&1 &
      echo -e "${GREEN}Boom scheduler started for survey $survey${END}"
    fi
  fi
fi

# -----------------------------
# 8. Uptime Kuma
# -----------------------------
if start_service "kuma" "$2"; then
  echo && echo "$(current_datetime) - Starting Uptime Kuma"
  mkdir -p "$PERSISTENT_DIR/kuma"
  mkdir -p "$LOGS_DIR/kuma"
  apptainer instance start \
    --bind "$PERSISTENT_DIR/kuma:/app/data" \
    --bind "$LOGS_DIR/kuma:/app/logs" \
    "$SIF_DIR/uptime-kuma.sif" kuma
  "$SCRIPTS_DIR/kuma-healthcheck.sh"
fi