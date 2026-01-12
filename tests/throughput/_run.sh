#!/usr/bin/env bash

COMPOSE_CONFIG="tests/throughput/compose.yaml"

# Logs folder is the first argument to the script
LOGS_DIR=${1:-logs/boom}

# A function that returns the current date and time
current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

# Remove any existing containers
docker compose -f $COMPOSE_CONFIG down

# Spin up BOOM services with Docker Compose
docker compose -f $COMPOSE_CONFIG up --build -d

# Send the logs to file so we can analyze later
mkdir -p $LOGS_DIR
docker compose -f $COMPOSE_CONFIG logs producer > $LOGS_DIR/producer.log &
docker compose -f $COMPOSE_CONFIG logs consumer -f > $LOGS_DIR/consumer.log &
docker compose -f $COMPOSE_CONFIG logs scheduler -f > $LOGS_DIR/scheduler.log &
# Also log stats from containers for later analysis
docker compose -f $COMPOSE_CONFIG stats consumer --format json > $LOGS_DIR/consumer.stats.log &
docker compose -f $COMPOSE_CONFIG stats scheduler --format json > $LOGS_DIR/scheduler.stats.log &

EXPECTED_ALERTS=29142
N_FILTERS=25
TIMEOUT_SECS=300 # 5 minutes

# Wait for the kafka consumer to start expecting messages (when it logs "Consumer received first message, continuing...")
echo "$(current_datetime) - Waiting for Kafka consumer to start"
START_TIME=$(date +%s)
while ! docker compose -f $COMPOSE_CONFIG logs consumer | grep -q "Consumer received first message, continuing..."; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo "$(current_datetime) - Timeout reached while waiting for Kafka consumer to start"
        docker compose -f $COMPOSE_CONFIG down
        exit 1
    fi
    sleep 1
done
END_TIME=$(date +%s)
STARTUP_TIME=$((END_TIME - START_TIME))
echo "$(current_datetime) - Kafka consumer started in $STARTUP_TIME seconds"

# Wait until we see all alerts
echo "$(current_datetime) - Waiting for all alerts to be ingested"
START_TIME=$(date +%s)
while [ $(docker compose -f $COMPOSE_CONFIG exec mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" --quiet --eval "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments()") -lt $EXPECTED_ALERTS ]; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo "$(current_datetime) - Timeout reached while waiting for alerts to be ingested"
        docker compose -f $COMPOSE_CONFIG down
        exit 1
    fi
    sleep 1
done
END_TIME=$(date +%s)
INGESTION_TIME=$((END_TIME - START_TIME))
echo "$(current_datetime) - All $EXPECTED_ALERTS alerts ingested in $INGESTION_TIME seconds"

# Wait until we see all alerts with classifications
echo "$(current_datetime) - Waiting for all alerts to be classified"
START_TIME=$(date +%s)
while [ $(docker compose -f $COMPOSE_CONFIG exec mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" --quiet --eval "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments({ classifications: { \$exists: true } })") -lt $EXPECTED_ALERTS ]; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo "$(current_datetime) - Timeout reached while waiting for alerts to be classified"
        docker compose -f $COMPOSE_CONFIG down
        exit 1
    fi
    sleep 1
done
END_TIME=$(date +%s)
CLASSIFICATION_TIME=$((END_TIME - START_TIME))
echo "$(current_datetime) - All $EXPECTED_ALERTS alerts classified in $CLASSIFICATION_TIME seconds"

# Wait until we've filtered all alerts
echo "$(current_datetime) - Waiting for filters to run on all alerts"
START_TIME=$(date +%s)
PASSED_ALERTS=0
while [ $PASSED_ALERTS -lt $EXPECTED_ALERTS ]; do
    PASSED_ALERTS=$(docker compose -f $COMPOSE_CONFIG logs scheduler | grep "passed filter" | awk -F'/' '{sum += $NF} END {print sum}')
    PASSED_ALERTS=${PASSED_ALERTS:-0}
    PASSED_ALERTS=$((PASSED_ALERTS / N_FILTERS))
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo "$(current_datetime) - Timeout reached while waiting for filters to run on all alerts"
        docker compose -f $COMPOSE_CONFIG down
        exit 1
    fi
    sleep 1
done
END_TIME=$(date +%s)
FILTERING_TIME=$((END_TIME - START_TIME))
echo "$(current_datetime) - All $EXPECTED_ALERTS alerts filtered in $FILTERING_TIME seconds"

echo "$(current_datetime) - All alerts ingested, classified, and filtered"
echo "$(current_datetime) - Reading from Kafka output topic"
uv run tests/throughput/read-kafka-output.py

echo "$(current_datetime) - All tasks completed; shutting down BOOM services"

# Shut down the BOOM services
docker compose -f $COMPOSE_CONFIG down

exit 0
