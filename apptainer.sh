#!/bin/bash

# Script to manage Boom using Apptainer.
# $1 = action: build | start | stop | restart | health | benchmark | filters

BOOM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # Retrieves the boom directory
SCRIPTS_DIR="$BOOM_DIR/apptainer/scripts"

BLUE="\e[0;34m"
RED="\e[31m"
GREEN="\e[32m"
YELLOW="\e[33m"
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
  # See build-sif.sh for the full explanation of the argument
  ./apptainer/scripts/build-sif.sh "$2"
  exit 0
fi

# -----------------------------
# 2. Start services
# -----------------------------
if [ "$1" == "start" ]; then
  if [ -z "$2" ] || { { [ "$2" == "all" ] || [ "$2" == "boom" ] || [ "$2" == "consumer" ] || [ "$2" == "scheduler" ]; } && [ -z "$3" ]; }; then
    echo -e "${RED}Error: Missing required arguments.${END}"
    echo -e "Usage: ${BLUE}$0 start <service|all|'empty'> [survey_name] [date] [program_id] [scheduler_config_path]${END} ${YELLOW}('empty' will default to all}${END}"
    echo -e "  ${BLUE}<service>:${END} ${GREEN}boom | consumer | scheduler | mongo | broker | valkey | prometheus | otel | listener | kuma | all${END}"
    echo -e "  ${YELLOW}The following arguments are only required if starting <all|boom|consumer|scheduler>${END}:"
    echo -e "  ${BLUE}[survey_name]:${END} ${GREEN}lsst | ztf | decam${END}"
    echo -e "  ${BLUE}[date]:${END} ${GREEN}YYYYMMDD${END} ${YELLOW}(optional for lsst)${END}"
    echo -e "  ${BLUE}[program_id]:${END} ${GREEN}public | partnership | caltech${END} ${YELLOW}(only for ztf)${END}"
    exit 1
  fi

  ARGS=("$BOOM_DIR")
  # Check if $2 is a survey name
  if [[ "$2" == "lsst" || "$2" == "ztf" || "$2" == "decam" ]]; then
    ARGS+=("all") # service to start
  else
    [ -n "$2" ] && ARGS+=("$2") # service to start
    shift
  fi
  [ -n "$2" ] && ARGS+=("$2") # survey name
  [ -n "$3" ] && ARGS+=("$3") # date
  [ -n "$4" ] && ARGS+=("$4") # program ID
  [ -n "$5" ] && ARGS+=("$5") # scheduler config path
  # See apptainer_start.sh for the full explanation of each argument
  "$SCRIPTS_DIR/apptainer_start.sh" "${ARGS[@]}"
  exit 0
fi

# -----------------------------
# 3. Stop services
# -----------------------------
if [ "$1" == "stop" ]; then
  target="$2"
  if [ -n "$target" ] && [ "$target" != "all" ] && [ "$target" != "boom" ] && [ "$target" != "consumer" ] && [ "$target" != "scheduler" ] \
    && [ "$target" != "mongo" ] && [ "$target" != "broker" ] && [ "$target" != "valkey" ] && [ "$target" != "prometheus" ] \
    && [ "$target" != "otel" ] && [ "$target" != "listener" ] && [ "$target" != "kuma" ]; then
    echo -e "${RED}Error: Invalid service name '$target'.${END}"
    echo -e "Usage: ${BLUE}$0 stop [service|all|'empty']${END} ${YELLOW}('empty' will default to all)${END}"
    echo -e "  ${BLUE}[service]:${END} ${GREEN}boom | consumer | scheduler | mongo | broker | valkey | prometheus | otel | listener | kuma ${END}"
    exit 1
  fi

  if stop_service "kuma" "$target"; then
    apptainer instance stop kuma
  fi
  if stop_service "listener" "$target"; then
    kill_process "boom-healthcheck-listener.py" "boom healthcheck listener"
  fi
  if stop_service "otel" "$target"; then
    kill_process "/otelcol" "Otel collector"
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
  exit 0
fi

# -----------------------------
# 4. Restart services
# -----------------------------
if [ "$1" == "restart" ]; then
  "$0" stop "$2"
  "$0" start "$2" "$3" "$4" "$5" "$6"
  exit 0
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
  exit 0
fi

# -----------------------------
# 5. Run benchmark
# -----------------------------
if [ "$1" == "benchmark" ]; then
  pip install pandas pyyaml
  python3 "$BOOM_DIR/tests/throughput/apptainer_run.py"
  exit 0
fi

# -----------------------------
# 6. Add filters
# -----------------------------
# Arguments for 'filters':
# $2 = path to the file with the filters to add
if [ "$1" == "filters" ]; then
  "$SCRIPTS_DIR/add_filters.sh" "$2"
  exit 0
fi