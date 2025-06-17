#!/bin/bash

set -e

# Defaults
PRODUCER="./target/release/kafka_producer"
CONSUMER="./target/release/kafka_consumer"
SCHEDULER="./target/release/scheduler"
TIMEOUT=300
MONGO_CONTAINER_NAME="boom-mongo-1"
ALERT_DB_NAME="boom"
ALERT_CHECK_INTERVAL=1

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
  echo "- The mongodb database specified by --alert-db-name"
  echo
  echo "Positional arguments:"
  echo "  SURVEY                Survey name (e.g., ztf)"
  echo "  DATE                  Date string in YYYYMMDD format (e.g., 20240617)"
  echo
  echo "Optional arguments:"
  echo "  -h, --help                    Show this help and exit"
  echo "  --producer PATH               Path to the producer binary"
  echo "                                (default: ${PRODUCER})"
  echo "  --consumer PATH               Path to the consumer binary"
  echo "                                (default: ${CONSUMER})"
  echo "  --scheduler PATH              Path to the scheduler binary"
  echo "                                (default: ${SCHEDULER})"
  echo "  --timeout SEC                 Maximum test duration in seconds (default: ${TIMEOUT})"
  echo "  --mongo-container-name NAME   MongoDB container name (default: ${MONGO_CONTAINER_NAME})"
  echo "  --alert-db-name NAME          MongoDB database name (default: ${ALERT_DB_NAME})"
  echo "  --alert-check-interval SEC    How often to check the alert count in seconds"
  echo "                                (default: ${ALERT_CHECK_INTERVAL})"
}

# Collection where alerts are stored
alert_collection_name() {
  echo "$(echo "${SURVEY}" | tr '[:lower:]' '[:upper:]')_alerts"
}

# Check for -h or --help in the arguments
for arg in "$@"; do
  if [[ "$arg" == "-h" || "$arg" == "--help" ]]; then
    help
    exit
  fi
done

# Required positional args
if [[ $# -ne 2 ]]; then
  echo "Error: incorrect arguments" >&2
  echo >&2
  usage >&2
  exit 1
fi
SURVEY="$1"
DATE="$2"
shift 2

# Optional named args
while [[ $# -gt 0 ]]; do
  case $1 in
    --timeout)
      TIMEOUT="$2"
      shift 2
      ;;
    --mongo-container-name)
      MONGO_CONTAINER_NAME="$2"
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
    *)
      echo "Error: Unknown option: $1" >&2
      echo >&2
      usage >&2
      exit 1
      ;;
  esac
done

# Clean up on exit
cleanup() {
  kill ${CONSUMER_PID} ${SCHEDULER_PID} 2>/dev/null || true
  exit
}
trap cleanup EXIT INT TERM

# Make sure mongodb is available and remove existing database
MONGO_CONTAINER_ID=$(docker ps -q -f name=${MONGO_CONTAINER_NAME})
# TODO: limit to a certain number of retries, then error
until docker exec ${MONGO_CONTAINER_ID} mongosh \
  --eval "db.adminCommand('ping')" \
  >/dev/null 2>&1
do
  sleep 2
done
# TODO: what's the best way to handle credentials? This is just a local test db...
docker exec ${MONGO_CONTAINER_ID} mongosh \
  --username ${MONGO_USERNAME} \
  --password ${MONGO_PASSWORD} \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('${ALERT_DB_NAME}').dropDatabase()" >/dev/null

# Run the producer, capture stdout
docker exec -it boom-broker-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:9092 \
  --delete \
  --if-exists \
  --topic ${SURVEY}_${DATE}_programid1
PRODUCER_OUTPUT=$(${PRODUCER} ${SURVEY} ${DATE} | tee /dev/tty)
if [[ $PRODUCER_OUTPUT =~ Pushed\ ([0-9]+)\ alerts\ to\ the\ queue ]]; then
  EXPECTED_COUNT="${BASH_REMATCH[1]}"
else
  echo "Error: Could not detect EXPECTED_COUNT from producer output" >&2
  exit 1
fi

START=$(date +%s)

# Start the consumer
${CONSUMER} ${SURVEY} ${DATE} &
CONSUMER_PID=$!

# TODO: would adding a short delay here make this work more consistently?
# Sometimes the scheduler prints a bunch of "queue is empty" messages, as if the consumer isn't feeding the queue.

# Start the scheduler
${SCHEDULER} ${SURVEY} &
SCHEDULER_PID=$!

# Wait for the expected number of alerts
ALERT_COLLECTION_NAME=$(alert_collection_name)
while true; do
  ELAPSED=$(($(date +%s) - START))
  if [ $ELAPSED -gt $TIMEOUT ]; then
    echo "WARNING: Timeout limit reached, exiting" >&2
    exit 1
  fi

  COUNT=$(docker exec ${MONGO_CONTAINER_ID} mongosh \
    --username ${MONGO_USERNAME} \
    --password ${MONGO_PASSWORD} \
    --authenticationDatabase admin \
    --quiet \
    --eval "db.getSiblingDB('${ALERT_DB_NAME}').${ALERT_COLLECTION_NAME}.countDocuments()"
  )
  if [ "$COUNT" -ge "$EXPECTED_COUNT" ]; then
    DURATION=$(($(date +%s) - START))
    echo "Results:"
    echo "  number of alerts:        ${COUNT}"
    echo "  processing time (sec):   ${DURATION}"
    echo "  throughput (alerts/sec): $(echo "scale=3; ${COUNT} / ${DURATION}" | bc)"
    break
  fi

  sleep ${ALERT_CHECK_INTERVAL}
done
