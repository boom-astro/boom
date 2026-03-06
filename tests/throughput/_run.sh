#!/usr/bin/env bash

# Parse args
KEEP_UP=false
POSITIONAL_ARGS=()
while [ "$#" -gt 0 ]; do
    case "$1" in
        --keep-up)
            KEEP_UP=true
            shift
            ;;
        --*)
            echo "Unknown option: $1"
            echo "Usage: $0 [--keep-up] [logs_dir]"
            exit 1
            ;;
        *)
            POSITIONAL_ARGS+=("$1")
            shift
            ;;
    esac
done
if [ ${#POSITIONAL_ARGS[@]} -gt 1 ]; then
    echo "Usage: $0 [--keep-up] [logs_dir]"
    exit 1
fi
if [ -z "${BOOM_REPO_ROOT:-}" ]; then
    echo "Error: BOOM_REPO_ROOT is not set; set BOOM_REPO_ROOT environment variable"
    exit 1
fi

COMPOSE_CONFIG="$BOOM_REPO_ROOT/tests/throughput/compose.yaml"

# If LOW_STORAGE mode is enabled, use the override to prevent volume mounts
if [ "$LOW_STORAGE" = "true" ]; then
    COMPOSE_CONFIG="$COMPOSE_CONFIG -f $BOOM_REPO_ROOT/tests/throughput/compose.low-storage.yaml"
fi

# Logs folder is the optional positional argument to the script
LOGS_DIR=${POSITIONAL_ARGS[0]:-logs/boom}

# A function that returns the current date and time
current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

# Run a MongoDB count query and return a clean integer string.
mongo_count() {
    local query="$1"
    local raw
    raw=$(docker compose -f $COMPOSE_CONFIG exec -T mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" --quiet --eval "$query")
    raw=$(printf '%s\n' "$raw" | tail -n 1 | tr -d '\r')
    raw=$(printf '%s' "$raw" | tr -cd '0-9')
    echo "${raw:-0}"
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
TIMEOUT_SECS=${TIMEOUT_SECS:-300} # 5 minutes default

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

# IF we are in LOW_STORAGE mode, clean up the downloaded files (producer files are not mounted)
if [ "$LOW_STORAGE" = "true" ]; then
    echo "$(current_datetime) - LOW_STORAGE mode enabled; cleaning up downloaded files to save space"
    rm -rf ./data/alerts/kowalski.NED.json.gz || true
    rm -rf ./data/alerts/boom_throughput.ZTF_alerts_aux.dump.gz || true
fi

# Wait until we see all alerts
echo "$(current_datetime) - Waiting for all alerts to be ingested"
START_TIME=$(date +%s)
while [ "$(mongo_count "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments()")" -lt "$EXPECTED_ALERTS" ]; do
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
while [ "$(mongo_count "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments({ classifications: { \$exists: true } })")" -lt "$EXPECTED_ALERTS" ]; do
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
python $BOOM_REPO_ROOT/tests/throughput/read-kafka-output.py

# Shut down the BOOM services if --keep-up was not specified
if [ "$KEEP_UP" = false ]; then
    echo "$(current_datetime) - All tasks completed; shutting down BOOM services"
    docker compose -f $COMPOSE_CONFIG down
fi

# Check to see if any of our containers have exited with a non-zero status,
# which would indicate an error
EXIT_CODE=$(docker compose -f $COMPOSE_CONFIG ps -q | xargs docker inspect -f '{{.State.ExitCode}}' | grep -v '^0$' || true)
if [ -n "$EXIT_CODE" ]; then
    echo "$(current_datetime) - ERROR: One or more containers exited with a non-zero status"
    exit 1
fi

echo "$(current_datetime) - All tasks completed successfully"
