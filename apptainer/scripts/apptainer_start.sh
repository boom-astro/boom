#!/usr/bin/env bash

# Script to start Boom services using Apptainer containers. Arguments:
# $1 = boom directory
# $2 = service to start:
#      - all         : starts all services
#      - boom        : starts both consumer and scheduler
#      - api         : starts the Boom API service
#      - consumer    : starts the consumer process
#      - scheduler   : starts the scheduler process
#      - mongo       : starts the MongoDB instance
#      - kafka       : starts the kafka instance
#      - valkey      : starts the Valkey instance
#      - prometheus  : starts the Prometheus instance
#      - otel        : starts OpenTelemetry Collector process
#      - listener    : starts Boom healthcheck listener process
#      - kuma        : starts the Kuma instance
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
HEALTHCHECK_DIR="$SCRIPTS_DIR/healthcheck"
CONFIG_FILE="$BOOM_DIR/config.yaml"
SIF_DIR="$BOOM_DIR/apptainer/sif"

YELLOW="\e[33m"
GREEN="\e[32m"
BLUE="\e[34m"
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

if [ "$2" != "all" ] && [ "$2" != "boom" ] && [ "$2" != "consumer" ] && [ "$2" != "scheduler" ] && [ "$2" != "api" ] \
  && [ "$2" != "mongo" ] && [ "$2" != "kafka" ] && [ "$2" != "valkey" ] && [ "$2" != "prometheus" ] \
  && [ "$2" != "otel" ] && [ "$2" != "listener" ] && [ "$2" != "kuma" ]; then
  echo -e "${RED}Error: Invalid service name '$2'.${END}"
  echo -e "  ${BLUE}<service>:${END} ${GREEN}boom | consumer | scheduler | api | mongo | kafka | valkey | prometheus | otel | listener | kuma | all${END}"
  exit 1
fi

# -----------------------------
# Load environment variables from .env file
# -----------------------------
set -a
source .env
set +a

# -----------------------------
# MongoDB
# -----------------------------
if start_service "mongo" "$2"; then
  if "$HEALTHCHECK_DIR/mongodb-healthcheck.sh" 0 > /dev/null 2>&1; then
    echo && echo -e "${YELLOW}$(current_datetime) - MongoDB is already running${END}"
  else
    echo && echo "$(current_datetime) - Starting MongoDB"
    mkdir -p "$PERSISTENT_DIR/mongodb"
    mkdir -p "$LOGS_DIR/mongodb"
    apptainer instance run --env-file .env \
      --bind "$PERSISTENT_DIR/mongodb:/data/db" \
      --bind "$LOGS_DIR/mongodb:/log" \
      "$SIF_DIR/mongo.sif" mongo
    sleep 5
    "$HEALTHCHECK_DIR/mongodb-healthcheck.sh"
  fi
fi

# -----------------------------
# Valkey
# -----------------------------
if start_service "valkey" "$2"; then
  if "$HEALTHCHECK_DIR/valkey-healthcheck.sh" 0 > /dev/null 2>&1; then
    echo && echo -e "${YELLOW}$(current_datetime) - Valkey is already running${END}"
  else
    echo && echo "$(current_datetime) - Starting Valkey"
    mkdir -p "$PERSISTENT_DIR/valkey"
    mkdir -p "$LOGS_DIR/valkey"
    apptainer instance run --env-file .env \
      --bind "$PERSISTENT_DIR/valkey:/data" \
      --bind "$LOGS_DIR/valkey:/log" \
      "$SIF_DIR/valkey.sif" valkey
    "$HEALTHCHECK_DIR/valkey-healthcheck.sh"
  fi
fi

# -----------------------------
# Kafka
# -----------------------------
if start_service "kafka" "$2"; then
  if "$HEALTHCHECK_DIR/kafka-healthcheck.sh" 0 > /dev/null 2>&1; then
    echo && echo -e "${YELLOW}$(current_datetime) - Kafka is already running${END}"
  else
    echo && echo "$(current_datetime) - Starting Kafka"
    mkdir -p "$PERSISTENT_DIR/kafka_data"
    mkdir -p "$LOGS_DIR/kafka"
    apptainer instance run --env-file .env \
      --bind "$BOOM_DIR/config/kafka_server_jaas.conf:/etc/kafka/kafka_server_jaas.conf:ro" \
      --bind "$PERSISTENT_DIR/kafka_data:/var/lib/kafka/data" \
      --bind "$PERSISTENT_DIR/kafka_data:/opt/kafka/config" \
      --bind "$LOGS_DIR/kafka:/opt/kafka/logs" \
      "$SIF_DIR/kafka.sif" kafka
    "$HEALTHCHECK_DIR/kafka-healthcheck.sh"

    echo "$(current_datetime) - Initializing Kafka ACLs"
    apptainer exec --bind "$BOOM_DIR/scripts/init_kafka_acls.sh:/init_kafka_acls.sh" \
      "$SIF_DIR/kafka.sif" /bin/bash /init_kafka_acls.sh apptainer
  fi
fi

# -----------------------------
# Prometheus
# -----------------------------
if start_service "prometheus" "$2"; then
  if "$HEALTHCHECK_DIR/prometheus-healthcheck.sh" 0 > /dev/null 2>&1; then
    echo && echo -e "${YELLOW}$(current_datetime) - Prometheus is already running${END}"
  else
    echo && echo "$(current_datetime) - Starting Prometheus instance"
    mkdir -p "$LOGS_DIR/prometheus"
    mkdir -p "$PERSISTENT_DIR/prometheus"
    apptainer instance start \
      --env-file .env \
      --bind "$BOOM_DIR/config/prometheus.yaml:/etc/prometheus/prometheus.yaml" \
      --bind "$PERSISTENT_DIR/prometheus:/prometheus/data" \
      --bind "$LOGS_DIR/prometheus:/var/log" \
      "$SIF_DIR/prometheus.sif" prometheus
    "$HEALTHCHECK_DIR/prometheus-healthcheck.sh"
  fi
fi

# -----------------------------
# OpenTelemetry Collector
# -----------------------------
if start_service "otel" "$2"; then
  if "$HEALTHCHECK_DIR/process-healthcheck.sh" "otelcol" otel-collector > /dev/null 2>&1; then
    echo && echo -e "${YELLOW}$(current_datetime) - Otel Collector is already running${END}"
  else
    echo && echo "$(current_datetime) - Starting Otel Collector"
    mkdir -p "$LOGS_DIR/otel"
    apptainer exec \
      --bind "$BOOM_DIR/config/apptainer-otel-collector-config.yaml:/etc/otelcol/config.yaml" \
      --bind "$LOGS_DIR/otel:/var/log/otel" \
      "$SIF_DIR/otel.sif" /otelcol --config /etc/otelcol/config.yaml \
      > "$LOGS_DIR/otel/otel.log" 2>&1 &
    sleep 1
    "$HEALTHCHECK_DIR/process-healthcheck.sh" "otelcol" otel-collector
  fi
fi

# -----------------------------
# Healthcheck listener
# -----------------------------
if start_service "listener" "$2"; then
  if "$HEALTHCHECK_DIR/boom-listener-healthcheck.sh" 0 > /dev/null 2>&1; then
    echo && echo -e "${YELLOW}$(current_datetime) - Boom healthcheck listener is already running${END}"
  else
    echo && echo "$(current_datetime) - Starting Boom healthcheck listener"
    mkdir -p "$LOGS_DIR/listener"
    python "$HEALTHCHECK_DIR/boom-healthcheck-listener.py" > "$LOGS_DIR/listener/listener.log" 2>&1 &
    "$HEALTHCHECK_DIR/boom-listener-healthcheck.sh"
  fi
fi

# -----------------------------
# Boom
# -----------------------------
if start_service "boom" "$2" || start_service "consumer" "$2" || start_service "scheduler" "$2"; then
  survey=$3
  if [ "$2" = "boom" ] && [ -z "$survey" ]; then
    if start_service "boom" "$2"; then
      echo && echo -e "${YELLOW}$(current_datetime) - Survey name not provided, consumer or scheduler cannot be started.${END}"
      if apptainer instance list | awk '{print $1}' | grep -xq "boom"; then
        echo && echo -e "${YELLOW}$(current_datetime) - Boom is already running${END}"
      else
        echo && echo "$(current_datetime) - Starting boom instance"
        apptainer instance start --env-file .env \
          --bind "$CONFIG_FILE:/app/config.yaml" \
          "$SIF_DIR/boom.sif" "boom"
        sleep 3
      fi
    else
      echo && echo -e "${RED}$(current_datetime) - Survey name not provided, consumer or scheduler cannot be started.${END}"
      echo -e "${BLUE}apptainer_start.sh start <service|all|'empty'> [survey_name] [date] [program_id] [scheduler_config_path]${END} ${YELLOW}('empty' will default to all}${END}"
      echo -e "  ${BLUE}<service>:${END} ${GREEN}boom | consumer | scheduler | mongo | kafka | valkey | prometheus | otel | listener | kuma | all${END}"
      echo -e "  ${YELLOW}The following arguments are only required if starting <all|boom|consumer|scheduler>${END}:"
      echo -e "  ${BLUE}[survey_name]:${END} ${GREEN}lsst | ztf | decam${END}"
      echo -e "  ${BLUE}[date]:${END} ${GREEN}YYYYMMDD${END} ${YELLOW}(optional for lsst)${END}"
      echo -e "  ${BLUE}[program_id]:${END} ${GREEN}public | partnership | caltech${END} ${YELLOW}(only for ztf)${END}"
    fi
  else
    if apptainer instance list | awk '{print $1}' | grep -xq "boom${survey:+_$survey}"; then
      echo && echo -e "${YELLOW}$(current_datetime) - Boom is already running${END}"
    else
      echo && echo "$(current_datetime) - Starting boom${survey:+_$survey} instance"
      apptainer instance start --env-file .env \
        --bind "$CONFIG_FILE:/app/config.yaml" \
        "$SIF_DIR/boom.sif" "boom${survey:+_$survey}"
      sleep 3
    fi

    # -----------------------------
    # Boom Consumer
    # -----------------------------
    if start_service "boom" "$2" || start_service "consumer" "$2"; then
      # If program ID is "all", consume for all programs
      if [ "$5" = "all" ]; then
        PROGRAMS=("public" "partnership" "caltech")
      elif [ -z "$5" ]; then
        PROGRAMS=("")
      elif [ "$5" = "public" ] || [ "$5" = "partnership" ] || [ "$5" = "caltech" ]; then
        PROGRAMS=("$5")
      else
        echo -e "${RED}Error: Invalid program ID '$5'.${END}"
        echo -e "  ${BLUE}[program_id]:${END} ${GREEN}public | partnership | caltech | all${END}"
        exit 1
      fi

      for prog in "${PROGRAMS[@]}"; do
        ARGS=("$survey")
        [ -n "$4" ] && ARGS+=("$4") # date
        [ -n "$prog" ] && ARGS+=("$prog") # program ID
        if pgrep -f "/app/kafka_consumer ${ARGS[*]}" > /dev/null; then
          echo -e "${YELLOW}Boom consumer already running for survey $survey${4:+ on date $4}${prog:+ for program $prog}.${END}"
        else
          apptainer exec --pwd /app \
            "instance://boom_$survey" /app/kafka_consumer "${ARGS[@]}" \
            > "$LOGS_DIR/${survey}${4:+_$4}${prog:+_$prog}_consumer.log" 2>&1 &
          echo -e "${GREEN}Boom consumer started for survey $survey${4:+ on date $4}${prog:+ for program $prog}${END}"
        fi
      done
    fi

    # -----------------------------
    # Boom Scheduler
    # -----------------------------
    if start_service "boom" "$2" || start_service "scheduler" "$2"; then
      ARGS=("$survey")
      [ -n "$6" ] && ARGS+=("$6") # $6=config path
      if pgrep -f "/app/scheduler ${ARGS[*]}" > /dev/null; then
        echo -e "${YELLOW}Boom scheduler already running.${END}"
      else
        apptainer exec --pwd /app "instance://boom_$survey" /app/scheduler \
          "${ARGS[@]}" > "$LOGS_DIR/${survey}_scheduler.log" 2>&1 &
        echo -e "${GREEN}Boom scheduler started for survey $survey${END}"
      fi
    fi
  fi
fi

# -----------------------------
# Api
# -----------------------------
if start_service "api" "$2"; then
  if apptainer instance list | awk '{print $1}' | grep -xq "boom_api"; then
    echo && echo -e "${YELLOW}$(current_datetime) - Boom_api instance is already running${END}"
  else
    echo && echo "$(current_datetime) - Starting Boom_api instance"
    apptainer instance start --env-file .env \
      --bind "$CONFIG_FILE:/app/config.yaml" \
      "$SIF_DIR/boom.sif" "boom_api"
    sleep 3
  fi

  if pgrep -f "/app/boom-api" > /dev/null; then
    echo -e "${YELLOW}Boom API already running.${END}"
  else
    apptainer exec --pwd /app "instance://boom_api" /app/boom-api \
      > "$LOGS_DIR/api.log" 2>&1 &
    echo -e "${GREEN}Boom API started.${END}"
  fi
fi

# -----------------------------
# Uptime Kuma
# -----------------------------
if start_service "kuma" "$2"; then
  if "$HEALTHCHECK_DIR/kuma-healthcheck.sh" 0 > /dev/null 2>&1; then
    echo && echo -e "${YELLOW}$(current_datetime) - Uptime Kuma is already running${END}"
  else
    echo && echo "$(current_datetime) - Starting Uptime Kuma"
    mkdir -p "$PERSISTENT_DIR/kuma"
    mkdir -p "$LOGS_DIR/kuma"
    apptainer instance start \
      --bind "$PERSISTENT_DIR/kuma:/app/data" \
      --bind "$LOGS_DIR/kuma:/app/logs" \
      "$SIF_DIR/kuma.sif" kuma
    "$HEALTHCHECK_DIR/kuma-healthcheck.sh"
  fi
fi