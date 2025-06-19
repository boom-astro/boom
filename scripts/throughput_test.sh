#!/bin/bash

set -euo pipefail

declare -a PIDS

# Defaults
CONFIG="./config.yaml"
PRODUCER="./target/release/kafka_producer"
CONSUMER="./target/release/kafka_consumer"
SCHEDULER="./target/release/scheduler"
MONGO_CONTAINER_NAME="boom-mongo-1"
MONGO_RETRIES=5
CONSUMER_RETRIES=5
ALERT_DB_NAME="boom"
ALERT_CHECK_INTERVAL=1
TIMEOUT=120
DEBUG=false

usage() {
  echo "Usage: $0 <SURVEY> <DATE> [OPTIONS]"
}

help() {
  usage
  echo
  echo "Runs a throughput test for the alert processing pipeline."
  echo
  echo "Requires kafka, valkey, and mongodb to be running via docker compose."
  echo
  echo "**Important:** the following will be dropped and overwritten:"
  echo "- The kafka topic '<SURVEY>_<DATE>_programid1'"
  echo "- The valkey queue '<SURVEY>_alerts_packets_queue' (with SURVEY in all caps)"
  echo "- The mongodb database specified by --alert-db-name"
  echo
  echo "Positional arguments:"
  echo "  SURVEY                Survey name (e.g., ztf)"
  echo "  DATE                  Date string in YYYYMMDD format (e.g., 20240617)"
  echo
  echo "Optional arguments:"
  echo "  -h, --help                    Show this help and exit"
  echo "  --config PATH                 Path to the consumer/scheduler config file"
  echo "                                (default: ${CONFIG})"
  echo "  --producer PATH               Path to the producer binary"
  echo "                                (default: ${PRODUCER})"
  echo "  --consumer PATH               Path to the consumer binary"
  echo "                                (default: ${CONSUMER})"
  echo "  --scheduler PATH              Path to the scheduler binary"
  echo "                                (default: ${SCHEDULER})"
  echo "  --mongo-container-name NAME   MongoDB container name (default: ${MONGO_CONTAINER_NAME})"
  echo "  --mongo-retries N             Number of times to attempt to ping mongodb"
  echo "                                (default: ${MONGO_RETRIES})"
  echo "  --consumer-retries N          Number of times to restart the consumer when it"
  echo "                                fails to read from kafka (default: ${CONSUMER_RETRIES})"
  echo "  --alert-db-name NAME          MongoDB database name (default: ${ALERT_DB_NAME})"
  echo "  --alert-check-interval SEC    How often, in seconds, to check the alert count"
  echo "                                (default: ${ALERT_CHECK_INTERVAL})"
  echo "  --timeout SEC                 Maximum test duration in seconds (default: ${TIMEOUT})"
  echo "  --debug                       Print DEBUG log messages from this program"
  echo
  echo "Environment variables:"
  echo "  MONGO_USERNAME             MongoDB username"
  echo "  MONGO_PASSWORD             MongoDB password"
}

# Clean up on exit
cleanup() {
  if [[ ${#PIDS[@]:-0} -gt 0 ]]; then
    for pid in "${PIDS[@]}"; do
      kill "${pid}" 2>/dev/null || true
    done
  fi
  exit
}

# Log DEBUG messages for this script
debug() {
  if [[ "$DEBUG" = true ]]; then
    echo "DEBUG: $1" >&2
  fi
}

# Log ERROR messages for this script
error() {
  echo "ERROR: $1" >&2
}

get_mongo_container_id() {
  local container_name="$1"
  docker ps -q -f name="${container_name}"
}

wait_for_mongo() {
  local container_id="$1"
  local retries="$2"

  local wait_interval=2

  local attempt=0
  while true; do
    debug "Pinging mongo (attempt ${attempt})"
    if [[ $attempt -ge $retries ]]; then
      error "Could not ping mongodb, exiting"
      exit 1
    fi

    if docker exec "${container_id}" mongosh \
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
  local container_id="$1"
  local alert_db_name="$2"

  debug "Removing mongodb database ${alert_db_name}"
  docker exec ${container_id} mongosh \
    --username ${MONGO_USERNAME} \
    --password ${MONGO_PASSWORD} \
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
  local output="$(${producer} ${survey} ${date} | tee /dev/stderr)"
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
  # Returns the start time of the consumer.
  # Has the side effect of adding the consumer's pid to the global PIDS array.

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
  local alert_queue_name="$(echo "${survey}" | tr '[:lower:]' '[:upper:]')_alerts_packets_queue"
  local attempt=0
  while true; do
    debug "Starting the consumer (attempt ${attempt})"
    if [[ $attempt -ge $retries ]]; then
      error "Consumer not reading from kafka, exiting"
      exit 1
    fi

    start=$(date +%s)

    # Start the consumer
    "${consumer}" --config "${config}" --clear true "${survey}" "${date}" >&2 &
    consumer_pid=$!

    sleep 1  # Short pause before checking the queue (slightly inflates execution time)
    local length="$(docker exec boom-valkey-1 redis-cli LLEN "${alert_queue_name}")"
    if [[ $length -gt 0 ]]; then
      PIDS+=($consumer_pid)
      break
    else
      debug "Killing ${consumer_pid}"
      kill ${consumer_pid}
      wait ${consumer_pid} || true
      ((attempt++))
    fi
  done

  echo "${start}"
}

start_scheduler() {
  # Has the side effect of adding the scheduler's pid to the global PIDS array.

  local scheduler="$1"
  local config="$2"
  local survey="$3"

  debug "Starting the scheduler"
  ${SCHEDULER} --config ${CONFIG} ${SURVEY} >&2 &
  PIDS+=($!)
}

wait_for_scheduler() {
  # Returns the alert count and the elapsed time separated by a comma.

  local mongo_container_id="$1"
  local alert_db_name="$2"
  local survey="$3"
  local start="$4"
  local expected_count="$5"
  local check_interval="$6"
  local timeout="$7"

  local alert_collection_name="$(echo "${survey}" | tr '[:lower:]' '[:upper:]')_alerts"

  local elapsed
  local count
  while true; do
    elapsed=$(($(date +%s) - start))
    if [[ $elapsed -gt $timeout ]]; then
      error "Timeout limit reached, exiting"
      exit 1
    fi

    count=$(docker exec ${mongo_container_id} mongosh \
      --username ${MONGO_USERNAME} \
      --password ${MONGO_PASSWORD} \
      --authenticationDatabase admin \
      --quiet \
      --eval "db.getSiblingDB('${alert_db_name}').${alert_collection_name}.countDocuments()"
    )
    debug "${count} alerts processed in ${elapsed} seconds"
    if [[ "$count" -ge "$expected_count" ]]; then
      echo "${count},${elapsed}"
      break
    fi

    sleep ${check_interval}
  done
}

main() {
  # TODO: number of iterations?

  # Check for -h or --help in the arguments
  for arg in "$@"; do
    if [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
      help
      exit
    fi
  done

  # Required positional args
  if [[ $# -lt 2 ]]; then
    error "Incorrect arguments"
    echo >&2
    usage >&2
    exit 1
  fi
  SURVEY="$1"
  DATE="$2"
  shift 2

  # Required env vars
  if [[ -z "${MONGO_USERNAME}" ]]; then
    error "The environment variable MONGO_USERNAME must be set to the MongoDB username"
    exit 1
  fi
  if [[ -z "${MONGO_PASSWORD}" ]]; then
    error "The environment variable MONGO_PASSWORD must be set to the MongoDB password"
    exit 1
  fi

  # Optional named args
  while [[ $# -gt 0 ]]; do
    case $1 in
      --config)
        CONFIG="$2"
        shift 2
        ;;
      --producer)
        PRODUCER="$2"
        shift 2
        ;;
      --consumer)
        CONSUMER="$2"
        shift 2
        ;;
      --scheduler)
        SCHEDULER="$2"
        shift 2
        ;;
      --mongo-container-name)
        MONGO_CONTAINER_NAME="$2"
        shift 2
        ;;
      --mongo-retries)
        MONGO_RETRIES="$2"
        shift 2
        ;;
      --consumer-retries)
        CONSUMER_RETRIES="$2"
        shift 2
        ;;
      --alert-db-name)
        ALERT_DB_NAME="$2"
        shift 2
        ;;
      --alert-check-interval)
        ALERT_CHECK_INTERVAL="$2"
        shift 2
        ;;
      --timeout)
        TIMEOUT="$2"
        shift 2
        ;;
      --debug)
        DEBUG=true
        shift 1
        ;;
      *)
        error "Unknown option: $1"
        echo >&2
        usage >&2
        exit 1
        ;;
    esac
  done

  local mongo_container_id="$(get_mongo_container_id "${MONGO_CONTAINER_NAME}")" || exit 1
  wait_for_mongo "${mongo_container_id}" "${MONGO_RETRIES}"
  remove_alert_database "${mongo_container_id}" "${ALERT_DB_NAME}"
  delete_alert_topic "${SURVEY}" "${DATE}"

  local expected_count="$(run_producer "${PRODUCER}" "${SURVEY}" "${DATE}")" || exit 1
  local start="$(start_consumer "${CONSUMER}" "${CONFIG}" "${SURVEY}" "${DATE}" "${CONSUMER_RETRIES}")" || exit 1
  start_scheduler "${CONSUMER}" "${CONFIG}" "${SURVEY}"
  local values="$(wait_for_scheduler "${mongo_container_id}" "${ALERT_DB_NAME}" "${SURVEY}" "${start}" "${expected_count}" "${ALERT_CHECK_INTERVAL}" "${TIMEOUT}")"

  local count="${values%,*}"
  local elapsed="${values#*,}"
  echo "Results:"
  echo "  number of alerts:        ${count}"
  echo "  processing time (sec):   ${elapsed}"
  echo "  throughput (alerts/sec): $(echo "scale=3; ${count} / ${elapsed}" | bc)"
}

trap cleanup EXIT INT TERM
main "$@"
