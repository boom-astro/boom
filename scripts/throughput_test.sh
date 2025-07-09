#!/usr/bin/env bash

declare -a PIDS

usage() {
  echo "Usage: ${0} [OPTIONS] [--] <SURVEY> <DATE>"
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
      --producer <PATH>       Path to the producer binary
                              [default: ${producer}]
      --consumer <PATH>       Path to the consumer binary
                              [default: ${consumer}]
      --scheduler <PATH>      Path to the scheduler binary
                              [default: ${scheduler}]
      --alert-check-interval <SEC>
                              How often, in seconds, to check the alert count
                              [default: ${alert_check_interval}]
      --timeout <SEC>         Maximum test duration in seconds [default: ${timeout}]
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
      info "killing ${pid}"
      kill "${pid}" 2>/dev/null || true
    done
  fi
}

# Helper function for logging
log() {
  local prefix="${1:?}"
  local message="${2:?}"

  echo "${prefix}${message}" >&2
}

# Log INFO messages for this script
info() {
  local message="${1:?}"

  log "INFO: " "${message}"
}

# Log ERROR messages for this script
error() {
  local message="${1:?}"

  log "ERROR: " "${message}"
}

# Log ERROR specifically regarding CLI args
arg_error() {
  local message="${1:?}"

  error "${message}"
  echo >&2
  usage >&2
}

# Helper function to check option values
check_option() {
  local name="${1:?}"
  local value="${2}"

  if [[ -z ${value} ]]; then
    arg_error "no value provided for option '${name}'"
    return 1
  fi
  echo "${value}"
}

# Helper function to check arguments
check_argument() {
  local name="${1:?}"
  local value="${2}"

  if [[ -z ${value} ]]; then
    arg_error "argument '<${name}>' not provided"
    return 1
  fi
  echo "${value}"
}

# Helper function to check env vars
check_env_var() {
  local name="${1:?}"

  if [[ -z ${!name} ]]; then
    error "required environment variable ${name} not set; see '--help'"
    return 1
  fi
}

start_containers() {
  local expected_service_count=3

  info "starting containers"
  docker compose up -d || return 1

  local healthy_count
  while true; do
    healthy_count="$(docker ps --quiet --filter health=healthy | wc -l)"
    if [[ ${healthy_count} -ge ${expected_service_count} ]]; then
      break
    fi
    info "waiting for containers to become healthy"
    sleep 5
  done
}

check_for_topic() {
  local topic="${1:?}"

  info "checking for topics"
  local topics
  topics="$(
    docker exec boom-broker-1 \
      /opt/kafka/bin/kafka-topics.sh \
      --bootstrap-server broker:9092 \
      --list
  )" || return 2
  echo "${topics}" 2>&1

  [[ ${topics} =~ ${topic} ]]
}

run_producer() {
  local producer="${1:?}"
  local survey="${2:?}"
  local date="${3:?}"

  info "running the producer"
  "${producer}" "${survey}" "${date}"
}

count_produced_alerts() {
  local topic="${1:?}"

  info "counting alerts in the topic"
  local output
  output="$(
    docker exec boom-broker-1 \
      /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server broker:9092 \
      --topic "${topic}" \
      --from-beginning \
      --timeout-ms 5000 \
      --property print.value=false 2>&1
  )" || {
    echo "${output}" >&2
    return 2
  }
  output="$(echo "${output}" | tail -n1 | tee /dev/stderr)"
  if [[ ${output} =~ Processed\ a\ total\ of\ ([0-9]+)\ messages ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    return 1
  fi
}

remove_alert_database() {
  debug "removing alert database"
  docker exec boom-mongo-1 mongosh \
    --username "${MONGO_USERNAME}" \
    --password "${MONGO_PASSWORD}" \
    --authenticationDatabase admin \
    --eval "db.getSiblingDB('boom').dropDatabase()" >/dev/null
}

start_consumer() {
  # Returns the consumer's start time and its PID, separated by a comma.

  local consumer="${1}"
  local config="${2}"
  local survey="${3}"
  local date="${4}"
  local retries="${5}"

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
    debug "starting the consumer (attempt ${attempt})"
    if [[ ${attempt} -ge ${retries} ]]; then
      error "consumer not reading from kafka, exiting"
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
      debug "consumer started (pid ${consumer_pid})"
      break
    else
      debug "killing ${consumer_pid}"
      kill "${consumer_pid}"
      wait "${consumer_pid}" || true
      ((attempt++))
    fi
  done

  echo "${start},${consumer_pid}"
}

start_scheduler() {
  # Returns the scheduler's PID

  local scheduler="${1}"
  local config="${2}"
  local survey="${3}"

  local scheduler_pid
  debug "starting the scheduler"
  "${scheduler}" --config "${config}" "${survey}" >&2 &
  scheduler_pid="$!"
  debug "scheduler started (pid ${scheduler_pid})"
  echo "${scheduler_pid}"
}

wait_for_scheduler() {
  # Returns the alert count and the elapsed time separated by a comma.

  local alert_db_name="${1}"
  local survey="${2}"
  local start="${3}"
  local expected_count="${4}"
  local check_interval="${5}"
  local timeout="${6}"

  local alert_collection_name
  alert_collection_name="$(echo "${survey}" | tr '[:lower:]' '[:upper:]')_alerts"

  local elapsed
  local count
  while true; do
    elapsed=$(($(date +%s) - start))
    if [[ ${elapsed} -gt ${timeout} ]]; then
      error "timeout limit reached, exiting"
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
    if [[ "${count}" -ge "${expected_count}" ]]; then
      echo "${count},${elapsed}"
      break
    fi

    sleep "${check_interval}"
  done
}

main() {
  # TODO: number of iterations?

  # Defaults
  local producer="./target/release/kafka_producer"
  local consumer="./target/release/kafka_consumer"
  local scheduler="./target/release/scheduler"
  local alert_check_interval=1
  local timeout=120

  # Options
  while :; do
    case ${1} in
      -h|--help)
        help
        exit
        ;;
      --producer)
        producer="$(check_option "${1}" "${2}")" || exit "$?"
        shift
        ;;
      --consumer)
        consumer="$(check_option "${1}" "${2}")" || exit "$?"
        shift
        ;;
      --scheduler)
        scheduler="$(check_option "${1}" "${2}")" || exit "$?"
        shift
        ;;
      --alert-check-interval)
        alert_check_interval="$(check_option "${1}" "${2}")" || exit "$?"
        shift
        ;;
      --timeout)
        timeout="$(check_option "${1}" "${2}")" || exit "$?"
        shift
        ;;
      --?*)
        arg_error "unrecognized option '${1}'"
        exit 1
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
  survey="$(check_argument "SURVEY" "${1}")" || exit "$?"
  shift

  local date
  date="$(check_argument "DATE" "${1}")" || exit "$?"
  shift

  [[ $# -eq 0 ]] || { arg_error "too many arguments provided"; exit 1; }

  # Required env vars
  check_env_var MONGO_USERNAME || exit "$?"
  check_env_var MONGO_PASSWORD || exit "$?"

  start_containers || {
    error "failed to start containers"
    exit 1
  }

  local topic="${survey}_${date}_programid1"
  check_for_topic "${topic}"
  case "$?" in
    0) info "topic exists";;
    1) run_producer "${producer}" "${survey}" "${date}" || {
        error "failed to run the producer"
        exit 1
      }
      ;;
    *)
      error "failed to check topics"
      exit 1
      ;;
  esac

  local expected_count
  expected_count="$(count_produced_alerts "${topic}")"
  case "$?" in
    0) info "expected alert count is ${expected_count}";;
    1)
      error "failed to match consumer output for counting"
      exit 1
      ;;
    *)
      error "failed to consume topic for counting"
      exit 1
      ;;
  esac
  info "done"
  exit

  # TODO: begin loop
  remove_alert_database || {
    error "failed to remove alert database"
    exit 1
  }

  info "done"
  exit

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
  echo "results:"
  echo "  number of alerts:        ${count}"
  echo "  processing time (sec):   ${elapsed}"
  echo "  throughput (alerts/sec): $(echo "scale=3; ${count} / ${elapsed}" | bc)"
}

trap cleanup EXIT
main "$@" || exit 1
