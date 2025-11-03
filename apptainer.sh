#!/bin/bash

# Script to manage Boom using Apptainer.
# $1 = action: build | start | stop | restart | health | benchmark | filters | backup | restore | log

BOOM_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)" # Retrieves the boom directory
SCRIPTS_DIR="$BOOM_DIR/apptainer/scripts"
HEALTHCHECK_DIR="$SCRIPTS_DIR/healthcheck"
LOGS_DIR="$BOOM_DIR/logs/boom"

BLUE="\e[0;34m"
RED="\e[31m"
GREEN="\e[32m"
YELLOW="\e[33m"
END="\e[0m"

kill_process() {
  local process="$1"
  local name="$2"
  if pgrep -f "$process" > /dev/null; then
    pkill -f "$process"
    echo -e "${BLUE}INFO${END}:    Stopping $name process"
  else
    echo -e "${YELLOW}WARNING${END}: $name process is not running"
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

if [ "$1" != "build" ] && [ "$1" != "start" ] && [ "$1" != "stop" ] && [ "$1" != "restart" ] \
  && [ "$1" != "health" ] && [ "$1" != "benchmark" ] && [ "$1" != "filters" ] \
  && [ "$1" != "backup" ] && [ "$1" != "restore" ] && [ "$1" != "log" ]; then
  echo "Usage: $0 {build|start|stop|restart|health|benchmark|filters|backup|restore|log} [args...]"
  exit 1
fi

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
  ARGS=("$BOOM_DIR")
  # Check if $2 is a survey name
  if [ -z "$2" ] || [ "$2" = "lsst" ] || [ "$2" = "ztf" ] || [ "$2" = "decam" ]; then
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
  if [ -n "$target" ] && [ "$target" != "all" ] && [[ "$target" != boom* ]] && [ "$target" != "consumer" ] && [ "$target" != "scheduler" ] \
    && [ "$target" != "mongo" ] && [ "$target" != "kafka" ] && [ "$target" != "valkey" ] && [ "$target" != "prometheus" ] \
    && [ "$target" != "otel" ] && [ "$target" != "listener" ] && [ "$target" != "kuma" ]; then
    echo -e "${RED}Error: Invalid service name '$target'.${END}"
    echo -e "Usage: ${BLUE}$0 stop [service|all|'empty']${END} ${YELLOW}('empty' will default to all)${END}"
    echo -e "  ${BLUE}[service]:${END} ${GREEN}boom_<survey> | consumer | scheduler | mongo | kafka | valkey | prometheus | otel | listener | kuma ${END}"
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
    if [ "$target" = "boom" ] && [ -n "$3" ]; then
      apptainer instance stop "boom_$3"
      exit 0
    fi
    apptainer instance stop "boom_lsst"
    apptainer instance stop "boom_ztf"
    apptainer instance stop "boom_decam"
  if stop_service "consumer" "$target"; then
    ARGS=()
    [ -n "$3" ] && ARGS+=("$3") # survey, if not provided, all consumers are killed
    [ -n "$4" ] && ARGS+=("$4") # date, if not provided, all dates are killed
    [ -n "$5" ] && ARGS+=("$5") # program ID, if not provided, all program IDs are killed
    kill_process "/app/kafka_consumer ${ARGS[*]}" consumer
  elif stop_service "scheduler" "$target"; then
    survey=$3 # if no survey is provided, all schedulers are killed
    kill_process "/app/scheduler $survey" scheduler
  fi
  if stop_service "valkey" "$target"; then
    apptainer instance stop valkey
  fi
  if stop_service "kafka" "$target"; then
    apptainer instance stop kafka
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
  "$HEALTHCHECK_DIR/mongodb-healthcheck.sh" 0
  "$HEALTHCHECK_DIR/valkey-healthcheck.sh" 0
  "$HEALTHCHECK_DIR/kafka-healthcheck.sh" 0
  "$HEALTHCHECK_DIR/boom-healthcheck.sh"
  "$HEALTHCHECK_DIR/prometheus-healthcheck.sh" 0
  "$HEALTHCHECK_DIR/process-healthcheck.sh" "/otelcol" otel-collector
  "$HEALTHCHECK_DIR/boom-listener-healthcheck.sh" 0
  "$HEALTHCHECK_DIR/kuma-healthcheck.sh" 0
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
if [ "$1" == "filters" ]; then
  path_to_file="$2"
  "$SCRIPTS_DIR/add_filters.sh" "$path_to_file"
  exit 0
fi

# -----------------------------
# 7. Backup MongoDB
# -----------------------------
if [ "$1" == "backup" ]; then
  path_to_folder=${2:-/tmp/mongo_backups} # Folder to save the backup to
  mkdir -p "$path_to_folder"
  apptainer exec instance://mongo mongodump \
  --uri="mongodb://mongoadmin:mongoadminsecret@localhost:27017/boom?authSource=admin" \
  --archive="$path_to_folder/mongo_$(date +%Y-%m-%d).gz" \
  --gzip
  exit 0
fi

# -----------------------------
# 8. Restore MongoDB
# -----------------------------
if [ "$1" == "restore" ]; then
  path_to_file="$2" # Path to the backup file
  if [ -z "$path_to_file" ]; then
    echo -e "${RED}Error: Missing path to the backup file.${END}"
    echo -e "Usage: ${BLUE}$0 restore <path_to_backup_file>${END}"
    exit 1
  fi
  apptainer exec instance://mongo mongorestore \
  --uri="mongodb://mongoadmin:mongoadminsecret@localhost:27017/boom?authSource=admin" \
  --archive="$path_to_file" \
  --gzip \
  --drop
  exit 0
fi

# -----------------------------
# 8. Display log
# -----------------------------
if [ "$1" == "log" ]; then
  survey="${2:-lsst}"
  service="${3:-scheduler}"
  if { [ "$survey" != "lsst" ] && [ "$survey" != "ztf" ] && [ "$survey" != "decam" ]; } \
  || { [ "$service" != "scheduler" ] && [ "$service" != "s" ] && [ "$service" != "consumer" ] \
  && [ "$service" != "cons" ] && [ "$service" != "c" ]; }; then
    echo -e "${RED}Error: Invalid survey name '$survey'.${END}"
    echo -e "  ${BLUE}<survey>:${END} ${GREEN}lsst | ztf | decam${END} ${YELLOW}(optional, defaults to lsst)${END}"
    echo -e "  ${BLUE}<service>:${END} ${GREEN}scheduler | s | consumer | cons | c${END} ${YELLOW}(optional, defaults to scheduler)${END}"
    echo -e "  ${BLUE}<date>:${END} ${GREEN}YYYYMMDD${END} ${YELLOW}(optional, defaults to latest)${END}"
    echo -e "  ${BLUE}<program_id>:${END} ${GREEN}public | partnership (part) | caltech${END} ${YELLOW}(only for ztf)${END}"
    exit 1
  fi

  if [ "$service" = "scheduler" ] || [ "$service" = "s" ]; then
    echo -e "${BLUE}Displaying $survey scheduler log...${END}"
    tail -f "$LOGS_DIR/${survey}_scheduler.log"
    exit 0
  fi

  # --- Consumer ---
  shift 3
  args=("$@")

  date_arg=""
  program_arg=""
  logs_found=($(ls "$LOGS_DIR" | grep -E "^${survey}_[0-9]{8}_[a-z]+_consumer\.log$" | sort))

  for arg in "${args[@]}"; do
    case "$arg" in
      public|partnership|part|caltech)
        [ "$arg" = "part" ] && arg="partnership"
        program_arg="$arg"
        ;;
      last) # find the latest date available
        date_arg=$(echo "${logs_found[-1]}" | cut -d'_' -f2)
        ;;
      [0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9])
        date_arg="$arg"
        ;;
    esac
  done

  log_pattern="${survey}_${date_arg:-*}${program_arg:+_${program_arg}}*consumer.log"
  log_files=($LOGS_DIR/$log_pattern)

  if [ ${#log_files[@]} -eq 0 ]; then
    echo -e "${RED}No matching log files found for pattern: ${log_pattern}${END}"
    exit 1
  fi

  echo -e "${BLUE}Displaying ${log_pattern} file(s):${END}"
  tail -f "${log_files[@]}"
fi