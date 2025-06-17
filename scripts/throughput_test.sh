#!/bin/bash

set -e

# Collection where alerts are stored
alert_collection_name() {
  echo "$(echo "${SURVEY}" | tr '[:lower:]' '[:upper:]')_alerts"
}

# TODO: add a usage/help message
# * Assumes binaries are already built and available in ./target/debug.
#   # TODO: ability to override this location
# * Assumes docker is already running
# * Explain that this script drops both the "boom" db and the "${SURVEY}_${DATE}_programid1" topic

# Required positional args
if [[ $# -lt 2 ]]; then
  # TODO: stderr
  echo "Usage: $0 <SURVEY> <DATE> [--timeout TIMEOUT]"
  exit 1
fi
SURVEY="$1"
DATE="$2"
shift 2

# Optional named args
TIMEOUT=300  # Default maximum test duration in seconds
MONGO_CONTAINER_NAME="boom-mongo-1"  # Mongodb container name
ALERT_DB_NAME="boom"                 # Database where alerts are stored
ALERT_CHECK_INTERVAL=1               # How often to check alert count (sec)
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
      echo "Unknown option: $1"  # TODO: stderr
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
  --eval "db.getSiblingDB('${ALERT_DB_NAME}').dropDatabase()"

# Run the producer, capture stdout
docker exec -it boom-broker-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server broker:9092 \
  --delete \
  --if-exists \
  --topic ${SURVEY}_${DATE}_programid1
PRODUCER_OUTPUT=$(./target/debug/kafka_producer ${SURVEY} ${DATE} | tee /dev/tty)
if [[ $PRODUCER_OUTPUT =~ Pushed\ ([0-9]+)\ alerts\ to\ the\ queue ]]; then
  EXPECTED_COUNT="${BASH_REMATCH[1]}"
else
  # TODO: stderr
  echo "Error: Could not detect EXPECTED_COUNT from producer output"
  exit 1
fi

START=$(date +%s)

# Start the consumer
./target/debug/kafka_consumer ${SURVEY} ${DATE} &
CONSUMER_PID=$!

# TODO: would adding a short delay here make this work more consistently?
# Sometimes the scheduler prints a bunch of "queue is empty" messages, as if the consumer isn't feeding the queue.

# Start the scheduler
./target/debug/scheduler ${SURVEY} &
SCHEDULER_PID=$!

# Wait for the expected number of alerts
ALERT_COLLECTION_NAME=$(alert_collection_name)
while true; do
  ELAPSED=$(($(date +%s) - START))
  if [ $ELAPSED -gt $TIMEOUT ]; then
    # TODO: stderr
    echo "WARNING: Timeout limit reached, exiting"
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
