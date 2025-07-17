#!/usr/bin/env bash

usage() {
  echo "Usage: ${0} [OPTIONS] [--] <SURVEY> <DATE>"
}

help() {
  cat <<EOF
Perform a boom throughput test in a *non-production* environment

$(usage)

Arguments:
  <SURVEY>  Survey name (e.g., ztf)
  <DATE>    Date string in YYYYMMDD format (e.g., 20240617)

Options:
  -p, --project <NAME>    Docker compose project name used to prefix all
                          containers and volumes; the project can stopped later
                          using 'docker compose -p <NAME> down' [default: ${project}]
  -i, --iterations <N>    Repeat the test N > 0 times [default: ${iterations}]
      --producer <CMD>    Base command for the producer [default: ${producer}]
      --consumer <CMD>    Base command for the consumer [default: ${consumer}]
      --scheduler <CMD>   Base command for the scheduler [default: ${scheduler}]
      --alert-db <NAME>   Name of the destination alert mongodb database.
                          IMPORTANT: this value *must* match the alert database
                          name specified in config.yaml, otherwise this utility
                          won't correctly [default: ${alert_db}]
      --timeout <SEC>     Timeout for counting the alerts in the kafka topic
                          (may want to increase this for dates with a large
                          number of alerts) [default: ${timeout}]
  -o, --output <PATH>     Write throughput values to the given file.
  -h, --help              Print help

Environment variables:
  MONGO_USERNAME  MongoDB username
  MONGO_PASSWORD  MongoDB password

Discussion:
    This utility does the following:

    1.  Start kafka, valkey, and mongodb in docker if they aren't already
        running.
    2.  Run the producer to create the kafka topic for <SURVEY> and <DATE> if
        it does not yet exist.
    3.  Count the total number of alerts in the topic (see '--timeout').
    4.  Repeat the following test <N> times (see '--iterations'):
        -   Drop the alert database in mongodb (see '--alert-db').
        -   Start the consumer and scheduler in the background.
        -   Periodically count the number of alerts in 'boom' and stop the
            consumer and scheduler when the expected number is reached.
        -   Record the time taken to process the alerts.

    Each test iteration concludes with a short report of results including,

    - The number of alerts (n)
    - The total processing time (t; from when the consumer started to when the
      last alert landed in the database)
    - The throughput rate (r = n / t)
    - The peak number of alerts that were in valkey queue (n_peak)
    - The time when the peak occurred (t_peak)

    The peak measurements are useful because they indicate how the throughput
    of the consumer (r_c) compares to the throughput of the scheduler (r_s).
    In essence, if n_peak and t_peak are unambiguously greater than zero and
    less than t, respectively, then the consumer was faster than the scheduler
    (r_c > r_s) and the overall throughput was limited by scheduler, i.e.,
    r = r_s. If instead n_peak is at or near zero and t_peak is approximately
    equal to t, then the consumer was as fast or slower than the scheduler
    (r_c < r_s) and the overall throughput was limited by the consumer, i.e.,
    r = r_c.
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
  local project="${1:?}"

  local expected_service_count=3

  info "starting containers"
  docker compose -p "${project}" up -d || {
    error "failed to start containers"
    return 1
  }

  local healthy_count
  while true; do
    healthy_count="$(docker compose -p "${project}" ps --format "table {{.Health}}" | grep "healthy" | wc -l)"
    if [[ ${healthy_count} -ge ${expected_service_count} ]]; then
      break
    fi
    info "waiting for containers to become healthy"
    sleep 5
  done
}

check_for_topic() {
  local project="${1:?}"
  local topic="${2:?}"

  info "checking for topics"
  local topics
  topics="$(
    docker exec "${project}-broker-1" \
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
  ${producer} "${survey}" "${date}" || {
    error "failed to run the producer"
    return 1
  }
}

count_produced_alerts() {
  local project="${1:?}"
  local topic="${2:?}"
  local timeout="${3:?}"

  info "counting alerts in the topic"
  local output
  output="$(
    docker exec "${project}-broker-1" \
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
  local project="${1:?}"
  local alert_db="${2:?}"

  info "removing alert database"
  docker exec "${project}-mongo-1" mongosh \
    --username "${MONGO_USERNAME}" \
    --password "${MONGO_PASSWORD}" \
    --authenticationDatabase admin \
    --eval "db.getSiblingDB('${alert_db}').dropDatabase()" >&2
}

start_consumer() {
  local consumer="${1:?}"
  local survey="${2:?}"
  local date="${3:?}"

  info "starting the consumer"
  eval "${consumer} --clear ${survey} ${date} >&2 &"
  echo "$!"
}

start_scheduler() {
  local scheduler="${1:?}"
  local survey="${2:?}"

  info "starting the scheduler"
  eval "${scheduler} ${survey} >&2 &"
  echo "$!"
}

wait_for_scheduler() {
  local project="${1:?}"
  local survey="${2:?}"
  local start="${3:?}"
  local expected_count="${4:?}"
  local alert_db="${5:?}"

  local alert_collection_name
  alert_collection_name="$(echo "${survey}" | tr '[:lower:]' '[:upper:]')_alerts"
  local alert_queue_name
  alert_queue_name="$(echo "${survey}" | tr '[:lower:]' '[:upper:]')_alerts_packets_queue"

  local elapsed
  local count
  local nprev=0  # Previous number of alerts in the queue
  local ncurr=0  # Current number of alerts in the queue
  local nmax=0  # Max number of alerts observed in the queue
  local tmax=0  # Time at nmax
  local decreasing=false
  while true; do
    # Count the number of alerts in the queue
    if ncurr="$(docker exec "${project}-valkey-1" redis-cli LLEN "${alert_queue_name}")"; then
      if ((ncurr > nprev)); then  # The queue is increasing
        if "${decreasing}"; then  # The queue just started increasing
          decreasing=false
        fi
      else  # The queue is decreasing
        if ! "${decreasing}"; then  # The queue just started decreasing
          decreasing=true
          nmax="${nprev}"
          tmax="$(date +%s)"
        fi
      fi
    else
      warn "failed to get the queue length"
    fi
    nprev="${ncurr}"

    # Count the number of alerts in mongodb
    count="$(docker exec "${project}-mongo-1" mongosh \
      --username "${MONGO_USERNAME}" \
      --password "${MONGO_PASSWORD}" \
      --authenticationDatabase admin \
      --quiet \
      --eval "db.getSiblingDB('${alert_db}').${alert_collection_name}.countDocuments()"
    )" || return 1
    elapsed=$(($(date +%s) - start))

    info "${count} alerts processed in ${elapsed} seconds"
    if [[ "${count}" -ge "${expected_count}" ]]; then
      echo "${count} ${elapsed} ${nmax} $((tmax - start))"
      break
    fi

    sleep 1
  done
}

test() {
  local project="${1:?}"
  local consumer="${2:?}"
  local scheduler="${3:?}"
  local survey="${4:?}"
  local date="${5:?}"
  local expected_count="${6:?}"
  local alert_db="${7:?}"

  remove_alert_database "${project}" "${alert_db}" || {
    warn "failed to remove alert database"
    return 1
  }
  local start
  start="$(date +%s)"
  local consumer_pid
  consumer_pid="$(start_consumer "${consumer}" "${survey}" "${date}")"
  local scheduler_pid
  scheduler_pid="$(start_scheduler "${scheduler}" "${survey}")"

  local failed=false
  local results
  results="$(wait_for_scheduler \
    "${project}" \
    "${survey}" \
    "${start}" \
    "${expected_count}" \
    "${alert_db}" \
  )" || {
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
  # Defaults
  local project="boom-throughput"
  local iterations=1
  local producer="./target/release/kafka_producer"
  local consumer="./target/release/kafka_consumer"
  local scheduler="./target/release/scheduler"
  local alert_db="boom"
  local timeout=5
  local output=

  # Options
  while :; do
    case ${1} in
      -h|--help)
        help
        exit
        ;;
      -p|--project)
        project="$(check_option "${1}" "${2}")" || exit "$?"
        shift
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
      --alert-db)
        alert_db="$(check_option "${1}" "${2}")" || exit "$?"
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
    rm "${output}" 2>/dev/null || true
    touch "${output}"
  fi

  start_containers "${project}" || exit 1

  local topic="${survey}_${date}_programid1"
  check_for_topic "${project}" "${topic}"
  case "$?" in
    0) info "topic exists";;
    1) run_producer "${producer}" "${survey}" "${date}" || exit 1;;
    *) exit 1;;
  esac

  local expected_count
  # Sometimes count_produced_alerts finds the topic to be empty, so retry.
  local i=0
  while true; do
    expected_count="$(count_produced_alerts "${project}" "${topic}" "${timeout}")" || exit 1
    if ((expected_count > 0)); then
      break
    fi
    ((x+=1))
    if ((i >= 3)); then
      error "failed to count any alerts in the topic"
      exit 1
    fi
    warn "topic appears to be empty; trying again"
    sleep 3
  done
  info "expected alert count is ${expected_count}"

  local i=0
  local results
  local count
  local elapsed
  local nmax
  local tmax
  local rate
  while true; do
    ((i+=1))
    if ((i > iterations)); then
      break
    fi
    info "starting test ${i}"
    results="$(
      test \
        "${project}" \
        "${consumer}" \
        "${scheduler}" \
        "${survey}" \
        "${date}" \
        "${expected_count}" \
        "${alert_db}"
    )" || continue
    read -r count elapsed nmax tmax <<<"${results}"
    rate="$(echo "scale=6; ${count} / ${elapsed}" | bc)"

    info "test ${i} results:
    number of alerts:         ${count}
    processing time (sec):    ${elapsed}
    throughput (alerts/sec):  ${rate}
    peak queue length:        ${nmax}
    time at peak (sec):       ${tmax}"

    if [[ -n ${output} ]]; then
      echo "${rate}" >>"${output}"
    fi
  done
}

main "$@"
