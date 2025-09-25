#!/bin/bash

# Script to manage Boom using Apptainer.
# $1 = action: build | start | stop | restart | health | benchmark | filters

BOOM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # Retrieves the boom directory
SCRIPTS_DIR="$BOOM_DIR/apptainer/scripts"

BLUE="\e[0;34m"
END="\e[0m"

if [ "$1" != "build" ] && [ "$1" != "start" ] && [ "$1" != "stop" ] && [ "$1" != "restart" ] \
  && [ "$1" != "health" ] && [ "$1" != "benchmark" ] && [ "$1" != "filters" ]; then
  echo "Usage: $0 {build|start|stop|restart|health|benchmark|filters}"
  exit 1
fi

kill_process() {
  local process="$1"
  local name="$2"
  if pgrep -f "$process" > /dev/null; then
    pkill -f "$process"
    echo -e "${BLUE}INFO${END}:    Stopping $name process"
  fi
}

stop_service() {
    local service="$1"
    local target="$2"
    if [[ -z "$target" || "$target" = "all" || "$target" = "$service" ]]; then
        return 0
    fi
    return 1
}

# -----------------------------
# 1. Build SIF files
# -----------------------------
if [ "$1" = "build" ]; then
  ./apptainer/def/build-sif.sh
  exit 0
fi

# -----------------------------
# 2. Start services
# -----------------------------
if [ "$1" == "start" ]; then
  ARGS=("$BOOM_DIR")
  [ -n "$2" ] && ARGS+=("$2") # $2=service to start
  [ -n "$3" ] && ARGS+=("$3") # $3=survey name
  [ -n "$5" ] && ARGS+=("$4") # $4=date
  [ -n "$6" ] && ARGS+=("$5") # $5=program ID
  [ -n "$7" ] && ARGS+=("$6") # $6=scheduler config path
  # See apptainer_start.sh for the full explanation of each argument
  "$SCRIPTS_DIR/apptainer_start.sh" "${ARGS[@]}"
fi

# -----------------------------
# 3. Stop services
# -----------------------------
# Arguments for 'stop':
# $2 = service to stop:
#      - boom        : stop the Boom instance
#      - consumer    : stop the consumer process
#      - scheduler   : stop the scheduler process
#      - mongo       : stop the MongoDB instance
#      - broker      : stop the kafka broker instance
#      - valkey      : stop the Valkey instance
#      - prometheus  : stop the Prometheus instance
#      - otel        : stop the OpenTelemetry Collector process
#      - listener    : stop the Boom healthcheck listener process
#      - kuma        : stop the Kuma service instance
#      If not specified or 'all', all services will be stopped.
if [ "$1" == "stop" ]; then
  target="$2" # $2=service to stop (all if empty or "all")

  if stop_service "kuma" "$target"; then
    apptainer instance stop kuma
  fi
  if stop_service "listener" "$target"; then
    kill_process "boom-healthcheck-listener.py" boom healthcheck listener
  fi
  if stop_service "otel" "$target"; then
    kill_process "otel-collector" Otel collector
  fi
  if stop_service "prometheus" "$target"; then
    apptainer instance stop prometheus
  fi
  if stop_service "boom" "$target"; then
    apptainer instance stop boom
  fi
  if stop_service "consumer" "$target"; then
    kill_process "/app/kafka_consumer" consumer
  fi
  if stop_service "scheduler" "$target"; then
    kill_process "/app/scheduler" scheduler
  fi
  if stop_service "valkey" "$target"; then
    apptainer instance stop valkey
  fi
  if stop_service "broker" "$target"; then
    apptainer instance stop broker
  fi
  if stop_service "mongo" "$target"; then
    apptainer instance stop mongo
  fi
fi

# -----------------------------
# 4. Restart services
# -----------------------------
if [ "$1" == "restart" ]; then
  "$0" stop "$2"
  "$0" start "$2" "$3" "$4" "$5" "$6"
fi

# -----------------------------
# 4. Health checks
# -----------------------------
if [ "$1" == "health" ]; then
  apptainer instance list && echo
  "$SCRIPTS_DIR/mongodb-healthcheck.sh" 0
  "$SCRIPTS_DIR/valkey-healthcheck.sh" 0
  "$SCRIPTS_DIR/kafka-healthcheck.sh" 0
  "$SCRIPTS_DIR/process-healthcheck.sh" "/app/kafka_consumer" consumer
  "$SCRIPTS_DIR/process-healthcheck.sh" "/app/scheduler" scheduler
  "$SCRIPTS_DIR/prometheus-healthcheck.sh" 0
  "$SCRIPTS_DIR/process-healthcheck.sh" "/otelcol" otel-collector
  "$SCRIPTS_DIR/boom-listener-healthcheck.sh" 0
  "$SCRIPTS_DIR/kuma-healthcheck.sh" 0
fi

# -----------------------------
# 5. Run benchmark
# -----------------------------
if [ "$1" == "benchmark" ]; then
  python3 "$BOOM_DIR/tests/throughput/apptainer_run.py"
fi

# -----------------------------
# 6. Add filters
# -----------------------------
# Arguments for 'filters':
# $2 = path to the file with the filters to add
if [ "$1" == "filters" ]; then
  "$SCRIPTS_DIR/add_filters.sh" "$2"
fi