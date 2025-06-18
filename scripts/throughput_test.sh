#!/bin/bash

set -e

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
  echo
  echo "Environment variables:"
  echo "  MONGO_USERNAME             MongoDB username"
  echo "  MONGO_PASSWORD             MongoDB password"
}

# Name of the alert queue
alert_queue_name() {
  echo "$(echo "${SURVEY}" | tr '[:lower:]' '[:upper:]')_alerts_packets_queue"
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

# Required env vars
MONGO_USERNAME="${MONGO_USERNAME:? required environment variable is not set}"
MONGO_PASSWORD="${MONGO_PASSWORD:? required environment variable is not set}"

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

# Make sure mongodb is available
MONGO_CONTAINER_ID=$(docker ps -q -f name=${MONGO_CONTAINER_NAME})
ATTEMPT=0
while true; do
  if [ $ATTEMPT -ge $MONGO_RETRIES ]; then
    echo "ERROR: Could not ping mongodb, exiting" >&2
    exit 1
  fi

  if docker exec ${MONGO_CONTAINER_ID} mongosh \
    --eval "db.adminCommand('ping')" \
    >/dev/null 2>&1
  then
    break
  fi

  ((ATTEMPT++))
  sleep 2
done

# Remove existing database
docker exec ${MONGO_CONTAINER_ID} mongosh \
  --username ${MONGO_USERNAME} \
  --password ${MONGO_PASSWORD} \
  --authenticationDatabase admin \
  --eval "db.getSiblingDB('${ALERT_DB_NAME}').dropDatabase()" >/dev/null

# Delete the topic
docker exec -it boom-broker-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:9092 \
  --delete \
  --if-exists \
  --topic ${SURVEY}_${DATE}_programid1

# Run the producer, capture stdout
PRODUCER_OUTPUT=$(${PRODUCER} ${SURVEY} ${DATE} | tee /dev/tty)
if [[ $PRODUCER_OUTPUT =~ Pushed\ ([0-9]+)\ alerts\ to\ the\ queue ]]; then
  EXPECTED_COUNT="${BASH_REMATCH[1]}"
else
  echo "Error: Could not detect EXPECTED_COUNT from producer output" >&2
  exit 1
fi

# TODO: Sometimes the consumer doesn't read from kafka and no amount of waiting
# makes any difference. We need better logging to understand why this happens.
# The workaround here is to keep restarting the consumer until we confirm it's
# pushing alerts to the queue.
ALERTS_QUEUE_NAME=$(alert_queue_name)
ATTEMPT=0
while true; do
  if [ $ATTEMPT -ge $CONSUMER_RETRIES ]; then
    echo "ERROR: Consumer not reading from kafka, exiting" >&2
    exit 1
  fi

  START=$(date +%s)

  # Start the consumer
  ${CONSUMER} --config ${CONFIG} --clear true ${SURVEY} ${DATE} &
  CONSUMER_PID=$!

  sleep 1  # Short pause before checking the queue (slightly inflates execution time)
  ALERT_QUEUE_LENGTH=$(docker exec boom-valkey-1 redis-cli LLEN "${ALERTS_QUEUE_NAME}")
  if [ "$ALERT_QUEUE_LENGTH" -gt 0 ]; then
    break
  else
    kill ${CONSUMER_PID}
    wait ${CONSUMER_PID} || true
    ((ATTEMPT++))
  fi
done

# Start the scheduler
${SCHEDULER} --config ${CONFIG} ${SURVEY} &
SCHEDULER_PID=$!

# Wait for the expected number of alerts
ALERT_COLLECTION_NAME=$(alert_collection_name)
while true; do
  ELAPSED=$(($(date +%s) - START))
  if [ $ELAPSED -gt $TIMEOUT ]; then
    echo "ERROR: Timeout limit reached, exiting" >&2
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
