#!/usr/bin/env bash

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
  -h, --help                  Print help

Environment variables:
  MONGO_USERNAME  MongoDB username
  MONGO_PASSWORD  MongoDB password
EOF
}

CLEANUP_COMMAND=":"

# Clean up on exit
cleanup() {
  eval "$CLEANUP_COMMAND"
}

# Helper for stopping subprocesses
stop() {
  local name="${1:?}"
  local sigspec="${2:?}"
  local pid="${3:?}"

  if ps "${pid}" >/dev/null; then
    info "stopping ${name} (pid ${pid})"
    kill -s "${sigspec}" "${pid}"
    wait "${pid}"
  else
    info "${name} (pid ${pid}) already stopped"
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
  info "removing alert database"
  docker exec boom-mongo-1 mongosh \
    --username "${MONGO_USERNAME}" \
    --password "${MONGO_PASSWORD}" \
    --authenticationDatabase admin \
    --eval "db.getSiblingDB('boom').dropDatabase()"
}

start_consumer() {
  local consumer="${1:?}"
  local survey="${2:?}"
  local date="${3:?}"

  info "starting the consumer"
  "${consumer}" --clear "${survey}" "${date}" >&2 &
  CLEANUP_COMMAND+="; stop 'consumer' 'SIGTERM' $!"
}

start_scheduler() {
  local scheduler="${1:?}"
  local survey="${2:?}"

  info "starting the scheduler"
  "${scheduler}" "${survey}" >&2 &
  CLEANUP_COMMAND+="; stop 'scheduler' 'SIGINT' $!"
}

wait_for_scheduler() {
  local survey="${1:?}"
  local start="${2:?}"
  local expected_count="${3:?}"
  local check_interval="${4:?}"

  local alert_collection_name="$(echo "${survey}" | tr '[:lower:]' '[:upper:]')_alerts"

  local elapsed
  local count
  while true; do
    count=$(docker exec boom-mongo-1 mongosh \
      --username "${MONGO_USERNAME}" \
      --password "${MONGO_PASSWORD}" \
      --authenticationDatabase admin \
      --quiet \
      --eval "db.getSiblingDB('boom').${alert_collection_name}.countDocuments()"
    )
    elapsed=$(($(date +%s) - start))
    info "${count} alerts processed in ${elapsed} seconds"
    if [[ "${count}" -ge "${expected_count}" ]]; then
      echo "${count} ${elapsed}"
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

  # TODO: begin loop
  remove_alert_database || {
    error "failed to remove alert database"
    exit 1
  }
  local start=$(date +%s)
  start_consumer "${consumer}" "${survey}" "${date}"
  start_scheduler "${scheduler}" "${survey}"

  local values=($(
    wait_for_scheduler \
      "${survey}" \
      "${start}" \
      "${expected_count}" \
      "${alert_check_interval}"
  ))

  info "results:
  number of alerts:        ${values[0]}
  processing time (sec):   ${values[1]}
  throughput (alerts/sec): $(echo "scale=3; ${values[1]} / ${values[0]}" | bc)"
}

trap cleanup EXIT
main "$@" || exit 1
