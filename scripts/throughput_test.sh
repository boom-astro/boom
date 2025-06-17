#!/bin/bash

set -e

# Globals:

# Mongodb container name
MONGO_CONTAINER_NAME="boom-mongo-1"

# Database and collection where alerts are stored
DB="boom"
collection() {
  echo "$(echo "${SURVEY}" | tr '[:lower:]' '[:upper:]')_alerts"
}

# How often to check whether the expected number of alerts are in DB
POLL_INTERVAL=1

# Defaults:

# Default survey name and alert date
SURVEY="ztf"
DATE="20240617"

# Maximum test duration in seconds
TIMEOUT=300

# TODO: add a usage/help message
# * Assumes binaries are already built and available in ./target/debug.
#   # TODO: ability to override this location
# * Assumes docker is already running

while [[ $# -gt 0 ]]; do
  case $1 in
    --survey)
      SURVEY="$2"
      shift 2
      ;;
    --date)
      DATE="$2"
      shift 2
      ;;
    --timeout)
      TIMEOUT="$2"
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
  --eval "db.getSiblingDB('${DB}').dropDatabase()"

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

# Start the scheduler
./target/debug/scheduler ${SURVEY} &
SCHEDULER_PID=$!

# Wait for the expected number of alerts
COLLECTION=$(collection)
while true; do
  ELAPSED=$(($(date +%s) - START))
  if [ $ELAPSED -gt $TIMEOUT ]; then
    echo "Timeout limit reached, exiting"
    exit 1
  fi

  COUNT=$(docker exec ${MONGO_CONTAINER_ID} mongosh \
    --username ${MONGO_USERNAME} \
    --password ${MONGO_PASSWORD} \
    --authenticationDatabase admin \
    --quiet \
    --eval "db.getSiblingDB('${DB}').${COLLECTION}.countDocuments()"
  )
  if [ "$COUNT" -ge "$EXPECTED_COUNT" ]; then
    DURATION=$(($(date +%s) - START))
    echo "Results:"
    echo "  number of alerts:        ${COUNT}"
    echo "  processing time (sec):   ${DURATION}"
    echo "  throughput (alerts/sec): $(echo "scale=3; ${COUNT} / ${DURATION}" | bc)"
    break
  fi

  sleep ${POLL_INTERVAL}
done
