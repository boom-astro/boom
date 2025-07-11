#!/usr/bin/env bash

usage() {
  echo "Usage: ${0} [OPTIONS] [--] <SURVEY> <DATE>"
}

help() {
  cat <<EOF
Usage: $(usage)

Perform a boom throughput test in a *non-production* environment.

This utility does the following:

1.  Start kafka, valkey, and mongodb in docker if they aren't already running.
2.  Run the producer to create the kafka topic for <SURVEY> and <DATE> if it
    does not yet exist.
3.  Count the total number of alerts in the topic.
4.  Repeat the following test <N> times (see '--iterations'):
    -   Drop the 'boom' database in mongodb.
    -   Start the consumer and scheduler in the background.
    -   Periodically count the number of alerts in 'boom' and stop the consumer
        and scheduler when the expected number is reached.
    -   Record the duration from when the consumer started to this point.

Arguments:
  <SURVEY>  Survey name (e.g., ztf)
  <DATE>    Date string in YYYYMMDD format (e.g., 20240617)

Options:
  -i, --iterations <N>    Repeat the test N > 0 times [default: ${iterations}]
      --producer <PATH>   Path to the producer binary [default: ${producer}]
      --consumer <PATH>   Path to the consumer binary [default: ${consumer}]
      --scheduler <PATH>  Path to the scheduler binary [default: ${scheduler}]
      --timeout <SEC>     Timeout for counting the alerts in the kafka topic
                          (may want to increase this for dates with a large
                          number of alerts) [default: ${timeout}]
  -o, --output <PATH>     Append test results to the given file.
  -h, --help              Print help

Environment variables:
  MONGO_USERNAME  MongoDB username
  MONGO_PASSWORD  MongoDB password
EOF
}

# Helper for stopping subprocesses
stop() {
  local name="${1:?}"
  local sigspec="${2:?}"
  local pid="${3:?}"

  if ps "${pid}" >/dev/null; then
    info "stopping ${name} (pid ${pid})"
    kill -s "${sigspec}" "${pid}"
    while ps -p "${pid}" >/dev/null; do
      sleep 1
    done
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

# Log WARN messages for this script
warn() {
  local message="${1:?}"

  log "WARN: " "${message}"
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
  docker compose up -d || {
    error "failed to start containers"
    return 1
  }

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
  )" || {
    error "failed to check topics"
    return 2
  }
  echo "${topics}" 2>&1

  [[ ${topics} =~ ${topic} ]]
}

run_producer() {
  local producer="${1:?}"
  local survey="${2:?}"
  local date="${3:?}"

  info "running the producer"
  "${producer}" "${survey}" "${date}" || {
    error "failed to run the producer"
    return 1
  }
}

count_produced_alerts() {
  local topic="${1:?}"
  local timeout="${2:?}"

  info "counting alerts in the topic"
  local output
  output="$(
    docker exec boom-broker-1 \
      /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server broker:9092 \
      --topic "${topic}" \
      --from-beginning \
      --timeout-ms "$((timeout * 1000))" \
      --property print.value=false 2>&1
  )" || {
    echo "${output}" >&2
    error "failed to consume topic for counting"
    return 2
  }
  output="$(echo "${output}" | tail -n1 | tee /dev/stderr)"
  if [[ ${output} =~ Processed\ a\ total\ of\ ([0-9]+)\ messages ]]; then
    echo "${BASH_REMATCH[1]}"
  else
    error "failed to match consumer output for counting"
    return 1
  fi
}

remove_alert_database() {
  info "removing alert database"
  docker exec boom-mongo-1 mongosh \
    --username "${MONGO_USERNAME}" \
    --password "${MONGO_PASSWORD}" \
    --authenticationDatabase admin \
    --eval "db.getSiblingDB('boom').dropDatabase()" >&2
}

start_consumer() {
  local consumer="${1:?}"
  local survey="${2:?}"
  local date="${3:?}"

  info "starting the consumer"
  "${consumer}" --clear "${survey}" "${date}" >&2 &
  echo "$!"
}

start_scheduler() {
  local scheduler="${1:?}"
  local survey="${2:?}"

  info "starting the scheduler"
  "${scheduler}" "${survey}" >&2 &
  echo "$!"
}

wait_for_scheduler() {
  local survey="${1:?}"
  local start="${2:?}"
  local expected_count="${3:?}"

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
    ) || return 1
    elapsed=$(($(date +%s) - start))
    info "${count} alerts processed in ${elapsed} seconds"
    if [[ "${count}" -ge "${expected_count}" ]]; then
      echo "${count} ${elapsed}"
      break
    fi

    sleep 1
  done
}

test() {
  local consumer="${1:?}"
  local scheduler="${2:?}"
  local survey="${3:?}"
  local date="${4:?}"
  local expected_count="${5:?}"

  remove_alert_database || {
    warn "failed to remove alert database"
    return 1
  }
  local start="$(date +%s)"
  local consumer_pid="$(start_consumer "${consumer}" "${survey}" "${date}")"
  local scheduler_pid="$(start_scheduler "${scheduler}" "${survey}")"

  local failed=false
  local results
  results="$(wait_for_scheduler "${survey}" "${start}" "${expected_count}")" || {
    warn "failed to poll mongodb"
    failed=true  # Don't return yet, need to clean up
  }

  stop 'consumer' 'SIGTERM' "${consumer_pid}"
  stop 'scheduler' 'SIGINT' "${scheduler_pid}"

  if "${failed}"; then
    return 1
  else
    echo "${results}"
  fi
}

main() {
  # TODO: number of iterations?

  # Defaults
  local iterations=1
  local producer="./target/release/kafka_producer"
  local consumer="./target/release/kafka_consumer"
  local scheduler="./target/release/scheduler"
  local timeout=5
  local output=

  # Options
  while :; do
    case ${1} in
      -h|--help)
        help
        exit
        ;;
      -i|--iterations)
        iterations="$(check_option "${1}" "${2}")" || exit "$?"
        shift
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
      --timeout)
        timeout="$(check_option "${1}" "${2}")" || exit "$?"
        shift
        ;;
      -o|--output)
        output="$(check_option "${1}" "${2}")" || exit "$?"
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

  if [[ -n ${output} ]]; then
    rm "${output}"
    touch "${output}"
  fi

  start_containers || exit 1

  local topic="${survey}_${date}_programid1"
  check_for_topic "${topic}"
  case "$?" in
    0) info "topic exists";;
    1) run_producer "${producer}" "${survey}" "${date}" || exit 1;;
    *) exit 1;;
  esac

  local expected_count
  expected_count="$(count_produced_alerts "${topic}" "${timeout}")" || exit 1
  info "expected alert count is ${expected_count}"

  local i=0
  local results
  local count
  local elapsed
  local rate
  while true; do
    i=$((i + 1))
    if ((i > iterations)); then
      break
    fi
    info "starting test ${i}"
    results="$(
      test \
        "${consumer}" \
        "${scheduler}" \
        "${survey}" \
        "${date}" \
        "${expected_count}"
    )" || continue
    count="${results% *}"
    elapsed="${results#* }"
    rate="$(echo "scale=6; ${count} / ${elapsed}" | bc)"

    info "test ${i} results:
    number of alerts:         ${count}
    processing time (sec):    ${elapsed}
    throughput (alerts/sec):  ${rate}"

    if [[ -n ${output} ]]; then
      echo "${rate}" >>"${output}"
    fi
  done
}

main "$@"
