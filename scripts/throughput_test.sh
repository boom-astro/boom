#!/usr/bin/env bash

declare -a PIDS
DEBUG=false

usage() {
  echo "Usage: $0 [OPTIONS] [--] <SURVEY> <DATE>"
}

help() {
  cat <<EOF
Usage: $(usage)

Runs a throughput test for the alert processing pipeline.

Requires kafka, valkey, and mongodb to be running via docker compose.

**Important:** the following will be dropped and overwritten:
- The kafka topic '<SURVEY>_<DATE>_programid1'
- The valkey queue '<SURVEY>_alerts_packets_queue' (with SURVEY in all caps)
- The mongodb database specified by --alert-db-name

Arguments:
  <SURVEY>  Survey name (e.g., ztf)
  <DATE>    Date string in YYYYMMDD format (e.g., 20240617)

Options:
      --config <PATH>         Path to the consumer/scheduler config file [default: ${1}]
      --producer <PATH>       Path to the producer binary [default: ${2}]
      --consumer <PATH>       Path to the consumer binary [default: ${3}]
      --scheduler <PATH>      Path to the scheduler binary [default: ${4}]
      --mongo-retries <N>     Number of times to attempt to ping mongodb [default: ${5}]
      --consumer-retries <N>  Number of times to restart the consumer when it fails to read from kafka [default: ${6}]
      --alert-db-name <NAME>  MongoDB database name [default: ${7}]
      --alert-check-interval <SEC>
                              How often, in seconds, to check the alert count [default: ${8}]
      --timeout <SEC>         Maximum test duration in seconds [default: ${9}]
      --debug                 Print DEBUG log messages from this program
  -h, --help                  Print help

Environment variables:
  MONGO_USERNAME  MongoDB username
  MONGO_PASSWORD  MongoDB password
EOF
}

# Clean up on exit
cleanup() {
  if [[ ${#PIDS[@]:-0} -gt 0 ]]; then
    for pid in "${PIDS[@]}"; do
      debug "Killing ${pid}"
      kill "${pid}" 2>/dev/null || true
    done
  fi
}

# Log DEBUG messages for this script
debug() {
  local message="$1"

  if [[ "$DEBUG" = true ]]; then
    echo "DEBUG: ${message}" >&2
  fi
}

# Log ERROR messages for this script
error() {
  local message="$1"

  echo "ERROR: ${message}" >&2
}

# Log ERROR specifically regarding CLI args
arg_error() {
  local message="$1"

  error "$message"
  echo >&2
  usage >&2
}

# Helper function to check option values
check_option() {
  local name="$1"
  local value="$2"

  if [[ -z $value ]]; then
    arg_error "no value provided for option '${name}'"
    return 1
  fi
  echo "$value"
}

# Helper function to check arguments
check_argument() {
  local name="$1"
  local value="$2"

  if [[ -z $value ]]; then
    arg_error "argument '<${name}>' not provided"
    return 1
  fi
  echo "$value"
}

# Helper function to check env vars
check_env_var() {
  local name="$1"

  if [[ -z ${!name} ]]; then
    error "required environment variable ${name} not set; see '--help'"
    return 1
  fi
}

get_mongo_container_id() {
  local container_name="$1"
  docker ps -q -f name="${container_name}"
}

wait_for_mongo() {
  local retries="$1"

  local wait_interval=2

  local attempt=0
  while true; do
    debug "Pinging mongo (attempt ${attempt})"
    if [[ $attempt -ge $retries ]]; then
      error "Could not ping mongodb, exiting"
      exit 1
    fi

    if docker exec boom-mongo-1 mongosh \
      --eval "db.adminCommand('ping')" \
      >/dev/null 2>&1
    then
      break
    fi

    ((attempt++))
    sleep "${wait_interval}"
  done
}

remove_alert_database() {
  local alert_db_name="$1"

  debug "Removing mongodb database ${alert_db_name}"
  docker exec boom-mongo-1 mongosh \
    --username "${MONGO_USERNAME}" \
    --password "${MONGO_PASSWORD}" \
    --authenticationDatabase admin \
    --eval "db.getSiblingDB('${alert_db_name}').dropDatabase()" >/dev/null
}

delete_alert_topic() {
  local survey="$1"
  local date="$2"

  local topic="${survey}_${date}_programid1"

  debug "Deleting kafka topic ${topic}"
  docker exec -it boom-broker-1 /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server broker:9092 \
    --delete \
    --if-exists \
    --topic "${topic}"
}

run_producer() {
  # Returns the expected alert count based on the producer output.

  local producer="$1"
  local survey="$2"
  local date="$3"

  debug "Running the producer"
  local output
  output="$("${producer}" "${survey}" "${date}" | tee /dev/stderr)"
  if [[ $output =~ Pushed\ ([0-9]+)\ alerts\ to\ the\ queue ]]; then
    local count="${BASH_REMATCH[1]}"
    debug "Expected alert count is ${count}"
    echo "${count}"
  else
    error "Could not determine the number of alerts produced"
    exit 1
  fi
}

start_consumer() {
  # Returns the consumer's start time and its PID, separated by a comma.

  local consumer="$1"
  local config="$2"
  local survey="$3"
  local date="$4"
  local retries="$5"

  local start
  local consumer_pid

  # TODO: Sometimes the consumer doesn't read from kafka and no amount of waiting
  # makes any difference. We need better logging to understand why this happens.
  # The workaround here is to keep restarting the consumer until we confirm it's
  # pushing alerts to the queue.
  local alert_queue_name
  alert_queue_name="$(echo "${survey}" | tr '[:lower:]' '[:upper:]')_alerts_packets_queue"
  local attempt=0
  while true; do
    debug "Starting the consumer (attempt ${attempt})"
    if [[ $attempt -ge $retries ]]; then
      error "Consumer not reading from kafka, exiting"
      exit 1
    fi

    start=$(date +%s)

    # Start the consumer
    "${consumer}" --config "${config}" --clear "${survey}" "${date}" >&2 &
    consumer_pid=$!

    sleep 1  # Short pause before checking the queue (slightly inflates execution time)
    local length
    length="$(docker exec boom-valkey-1 redis-cli LLEN "${alert_queue_name}")"
    if [[ $length -gt 0 ]]; then
      debug "Consumer started (pid ${consumer_pid})"
      break
    else
      debug "Killing ${consumer_pid}"
      kill "${consumer_pid}"
      wait "${consumer_pid}" || true
      ((attempt++))
    fi
  done

  echo "${start},${consumer_pid}"
}

start_scheduler() {
  # Returns the scheduler's PID

  local scheduler="$1"
  local config="$2"
  local survey="$3"

  local scheduler_pid
  debug "Starting the scheduler"
  "${scheduler}" --config "${config}" "${survey}" >&2 &
  scheduler_pid=$!
  debug "Scheduler started (pid ${scheduler_pid})"
  echo "${scheduler_pid}"
}

wait_for_scheduler() {
  # Returns the alert count and the elapsed time separated by a comma.

  local alert_db_name="$1"
  local survey="$2"
  local start="$3"
  local expected_count="$4"
  local check_interval="$5"
  local timeout="$6"

  local alert_collection_name
  alert_collection_name="$(echo "${survey}" | tr '[:lower:]' '[:upper:]')_alerts"

  local elapsed
  local count
  while true; do
    elapsed=$(($(date +%s) - start))
    if [[ $elapsed -gt $timeout ]]; then
      error "Timeout limit reached, exiting"
      exit 1
    fi

    count=$(docker exec boom-mongo-1 mongosh \
      --username "${MONGO_USERNAME}" \
      --password "${MONGO_PASSWORD}" \
      --authenticationDatabase admin \
      --quiet \
      --eval "db.getSiblingDB('${alert_db_name}').${alert_collection_name}.countDocuments()"
    )
    debug "${count} alerts processed in ${elapsed} seconds"
    if [[ "$count" -ge "$expected_count" ]]; then
      echo "${count},${elapsed}"
      break
    fi

    sleep "${check_interval}"
  done
}

main() {
  # TODO: number of iterations?

  # Defaults
  local config="./config.yaml"
  local producer="./target/release/kafka_producer"
  local consumer="./target/release/kafka_consumer"
  local scheduler="./target/release/scheduler"
  local mongo_retries=5
  local consumer_retries=5
  local alert_db_name="boom"
  local alert_check_interval=1
  local timeout=120

  # Options
  while :; do
    case $1 in
      -h|--help)
        help \
          "$config" \
          "$producer" \
          "$consumer" \
          "$scheduler" \
          "$mongo_retries" \
          "$consumer_retries" \
          "$alert_db_name" \
          "$alert_check_interval" \
          "$timeout"
        exit
        ;;
      --config)
        config="$(check_option "$1" "$2")" || exit $?
        shift
        ;;
      --producer)
        producer="$(check_option "$1" "$2")" || exit $?
        shift
        ;;
      --consumer)
        consumer="$(check_option "$1" "$2")" || exit $?
        shift
        ;;
      --scheduler)
        scheduler="$(check_option "$1" "$2")" || exit $?
        shift
        ;;
      --mongo-retries)
        mongo_retries="$(check_option "$1" "$2")" || exit $?
        shift
        ;;
      --consumer-retries)
        consumer_retries="$(check_option "$1" "$2")" || exit $?
        shift
        ;;
      --alert-db-name)
        alert_db_name="$(check_option "$1" "$2")" || exit $?
        shift
        ;;
      --alert-check-interval)
        alert_check_interval="$(check_option "$1" "$2")" || exit $?
        shift
        ;;
      --timeout)
        timeout="$(check_option "$1" "$2")" || exit $?
        shift
        ;;
      --debug)
        DEBUG=true
        ;;
      --)  # End of options
        shift
        break
        ;;
      *)
        break
        ;;
    esac
    shift
  done

  # Arguments
  local survey
  survey="$(check_argument "SURVEY" "$1")" || exit $?
  shift

  local date
  date="$(check_argument "DATE" "$1")" || exit $?
  shift

  [[ $# -eq 0 ]] || { arg_error "too many arguments provided"; exit 1; }

  # Required env vars
  check_env_var MONGO_USERNAME || exit $?
  check_env_var MONGO_PASSWORD || exit $?

  debug "
  survey:               ${survey}
  date:                 ${date}
  config:               ${config}
  producer:             ${producer}
  consumer:             ${consumer}
  scheduler:            ${scheduler}
  mongo_retries:        ${mongo_retries}
  consumer_retries:     ${consumer_retries}
  alert_db_name:        ${alert_db_name}
  alert_check_interval: ${alert_check_interval}
  timeout:              ${timeout}
  DEBUG:                ${DEBUG}"
  exit

  wait_for_mongo "${MONGO_RETRIES}"
  remove_alert_database "${ALERT_DB_NAME}"
  delete_alert_topic "${SURVEY}" "${DATE}"

  local expected_count
  expected_count="$(run_producer "${PRODUCER}" "${SURVEY}" "${DATE}")"

  local values
  local start
  local consumer_pid
  values="$(
    start_consumer \
      "${CONSUMER}" \
      "${CONFIG}" \
      "${SURVEY}" \
      "${DATE}" \
      "${CONSUMER_RETRIES}"
  )"
  start="${values%,*}"
  consumer_pid="${values#*,}"
  PIDS+=("${consumer_pid}")

  local scheduler_pid
  scheduler_pid="$(start_scheduler "${SCHEDULER}" "${CONFIG}" "${SURVEY}")"
  PIDS+=("${scheduler_pid}")

  local values
  local count
  local elapsed
  values="$(
    wait_for_scheduler \
      "${ALERT_DB_NAME}" \
      "${SURVEY}" \
      "${start}" \
      "${expected_count}" \
      "${ALERT_CHECK_INTERVAL}" \
      "${TIMEOUT}"
  )"

  count="${values%,*}"
  elapsed="${values#*,}"
  echo "Results:"
  echo "  number of alerts:        ${count}"
  echo "  processing time (sec):   ${elapsed}"
  echo "  throughput (alerts/sec): $(echo "scale=3; ${count} / ${elapsed}" | bc)"
}

trap cleanup EXIT
main "$@" || exit 1
