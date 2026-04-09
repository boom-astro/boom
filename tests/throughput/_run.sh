#!/usr/bin/env bash

set -euo pipefail

# A function that returns the current date and time
current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

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

COMPOSE_CONFIG=("-f" "$BOOM_REPO_ROOT/tests/throughput/compose.yaml")
BG_PIDS=()

PLATFORM=$(uname -s | tr '[:upper:]' '[:lower:]')

if [ "${BOOM_GPU__ENABLED:-false}" = "true" ] && [ "$PLATFORM" = "linux" ]; then
    echo "BOOM_GPU__ENABLED is true and platform is Linux; adding GPU override to Docker Compose configuration (CUDA support)"
    COMPOSE_CONFIG+=("-f" "$BOOM_REPO_ROOT/tests/throughput/compose.cuda.yaml")
fi

# If LOW_STORAGE mode is enabled, use the override to prevent volume mounts
if [ "${LOW_STORAGE:-}" = "true" ]; then
    COMPOSE_CONFIG+=("-f" "$BOOM_REPO_ROOT/tests/throughput/compose.low-storage.yaml")
fi

# Logs folder is the optional positional argument to the script
LOGS_DIR=${POSITIONAL_ARGS[0]:-logs/boom}

cleanup() {
    echo "$(current_datetime) - Cleaning up background processes"
    if [ ${#BG_PIDS[@]} -gt 0 ]; then
        kill "${BG_PIDS[@]}" 2>/dev/null || true
        wait "${BG_PIDS[@]}" 2>/dev/null || true
    fi
}

trap cleanup EXIT INT TERM

# Run a MongoDB count query and return a clean integer string.
mongo_count() {
    local query="$1"
    local raw
    raw=$(docker compose "${COMPOSE_CONFIG[@]}" exec -T mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017" --quiet --eval "$query")
    raw=$(printf '%s\n' "$raw" | tail -n 1 | tr -d '\r')
    raw=$(printf '%s' "$raw" | tr -cd '0-9')
    echo "${raw:-0}"
}

# Remove any existing containers
docker compose "${COMPOSE_CONFIG[@]}" down

# Spin up BOOM services with Docker Compose
if ! docker compose "${COMPOSE_CONFIG[@]}" up --build -d; then
    echo "$(current_datetime) - ERROR: Failed to start Docker Compose services"
    docker compose "${COMPOSE_CONFIG[@]}" logs mongo-init || true
    exit 1
fi

# Send the logs to file so we can analyze later
mkdir -p "$LOGS_DIR"
docker compose "${COMPOSE_CONFIG[@]}" logs -f producer > "$LOGS_DIR/producer.log" &
BG_PIDS+=($!)
docker compose "${COMPOSE_CONFIG[@]}" logs -f consumer > "$LOGS_DIR/consumer.log" &
BG_PIDS+=($!)
docker compose "${COMPOSE_CONFIG[@]}" logs -f scheduler > "$LOGS_DIR/scheduler.log" &
BG_PIDS+=($!)
docker compose "${COMPOSE_CONFIG[@]}" logs -f mongo-init > "$LOGS_DIR/mongo-init.log" &
BG_PIDS+=($!)
# Also log stats from containers for later analysis
docker compose "${COMPOSE_CONFIG[@]}" stats consumer --format json > "$LOGS_DIR/consumer.stats.log" &
BG_PIDS+=($!)
docker compose "${COMPOSE_CONFIG[@]}" stats scheduler --format json > "$LOGS_DIR/scheduler.stats.log" &
BG_PIDS+=($!)

EXPECTED_ALERTS=29142
N_FILTERS=25
TIMEOUT_SECS=${TIMEOUT_SECS:-300} # 5 minutes default

# Wait for the kafka consumer to start expecting messages (when it logs "Consumer received first message, continuing...")
echo "$(current_datetime) - Waiting for Kafka consumer to start"
START_TIME=$(date +%s)
while ! docker compose "${COMPOSE_CONFIG[@]}" logs consumer | grep -q "Consumer received first message, continuing..."; do
    MONGO_INIT_CONTAINER_ID=$(docker compose "${COMPOSE_CONFIG[@]}" ps -aq mongo-init | tail -n 1)
    if [ -n "$MONGO_INIT_CONTAINER_ID" ]; then
        MONGO_INIT_EXIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' "$MONGO_INIT_CONTAINER_ID" 2>/dev/null || true)
        if [[ "$MONGO_INIT_EXIT_CODE" =~ ^[0-9]+$ ]] && [ "$MONGO_INIT_EXIT_CODE" -ne 0 ]; then
            echo "$(current_datetime) - ERROR: mongo-init did not complete successfully (exit $MONGO_INIT_EXIT_CODE)"
            docker compose "${COMPOSE_CONFIG[@]}" logs mongo-init || true
            exit 1
        fi
    fi

    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo "$(current_datetime) - Timeout reached while waiting for Kafka consumer to start"
        exit 1
    fi
    sleep 1
done
END_TIME=$(date +%s)
STARTUP_TIME=$((END_TIME - START_TIME))
echo "$(current_datetime) - Kafka consumer started in $STARTUP_TIME seconds"

# IF we are in LOW_STORAGE mode, clean up the downloaded files (producer files are not mounted)
if [ "${LOW_STORAGE:-}" = "true" ]; then
    echo "$(current_datetime) - LOW_STORAGE mode enabled; cleaning up downloaded files to save space"
    rm -rf ./data/alerts/kowalski.NED.json.gz || true
    rm -rf ./data/alerts/boom_throughput.ZTF_alerts_aux.dump.gz || true
fi

# If GPU support is enabled, we wait until we have confirmed that GPU inference is working.
# On some architectures (recent GPUs, mostly) we may have to wait for CUDA to compile
# some kernels and populate the cache before we see successful GPU inference,
# so we wait until we see logs indicating that the ONNX CUDA warmup has completed.
if [ "${BOOM_GPU__ENABLED:-false}" = "true" ] && [ "$PLATFORM" = "linux" ]; then
    echo "$(current_datetime) - GPU support is enabled; waiting for GPUs to be inference-ready"
    START_TIME=$(date +%s)
    while ! grep -q "Confirmed GPU runtime preconditions and validated GPU inference successfully" < <(docker compose "${COMPOSE_CONFIG[@]}" logs scheduler); do
        CURRENT_TIME=$(date +%s)
        ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
        if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
            echo "$(current_datetime) - Timeout reached while waiting for GPU inference to be validated"
            exit 1
        fi
        sleep 1
    done
    END_TIME=$(date +%s)
    WARMUP_TIME=$((END_TIME - START_TIME))
    echo "$(current_datetime) - ONNX CUDA warmup completed in $WARMUP_TIME seconds"
fi

# Wait until we see all alerts
echo "$(current_datetime) - Waiting for all alerts to be ingested"
START_TIME=$(date +%s)
while [ "$(mongo_count "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments()")" -lt "$EXPECTED_ALERTS" ]; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo "$(current_datetime) - Timeout reached while waiting for alerts to be ingested"
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
    PASSED_ALERTS=$(docker compose "${COMPOSE_CONFIG[@]}" logs scheduler | grep "passed filter" | awk -F'/' '{sum += $NF} END {print sum}')
    PASSED_ALERTS=${PASSED_ALERTS:-0}
    PASSED_ALERTS=$((PASSED_ALERTS / N_FILTERS))
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo "$(current_datetime) - Timeout reached while waiting for filters to run on all alerts"
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

# Export collection stats to JSON for analysis
echo "$(current_datetime) - Collecting MongoDB collection stats"
MONGO_RESULT="$({
	docker compose "${COMPOSE_CONFIG[@]}" exec -T mongo \
		mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017/admin?authSource=admin" \
		--quiet \
		--eval '
const dbName = "boom-benchmarking";
const d = db.getSiblingDB(dbName);
function collectionStats(name) {
	const c = d.getCollection(name);
	const s = c.stats();
  return {
	collection: name,
	count: c.countDocuments(),
	data_size_bytes: s.size,
	storage_size_bytes: s.storageSize,
	total_index_size_bytes: s.totalIndexSize,
	total_size_bytes: s.totalSize
  };
}
const collectionNames = d
	.getCollectionInfos({ type: "collection" })
	.map((info) => info.name)
	.sort();
const out = {
  generated_at_utc: new Date().toISOString(),
  database: dbName,
  collections: collectionNames.map(collectionStats)
};
print(JSON.stringify(out));
'
} | tail -n 1)"

if [ -n "$MONGO_RESULT" ]; then
	mkdir -p "$LOGS_DIR"
	if command -v jq >/dev/null 2>&1; then
		printf '%s\n' "$MONGO_RESULT" | jq . > "$LOGS_DIR/collection_stats.json"
	else
		printf '%s\n' "$MONGO_RESULT" > "$LOGS_DIR/collection_stats.json"
	fi
	echo "$(current_datetime) - Wrote collection stats to $LOGS_DIR/collection_stats.json"
fi

# Check to see if any of our containers have exited with a non-zero status,
# which would indicate an error
EXIT_CODE=$(docker compose "${COMPOSE_CONFIG[@]}" ps -aq | xargs docker inspect -f '{{.State.ExitCode}}' | grep -v '^0$' || true)
if [ -n "$EXIT_CODE" ]; then
    echo "$(current_datetime) - ERROR: One or more containers exited with a non-zero status"
    exit 1
fi

# Shut down the BOOM services if --keep-up was not specified and no errors were detected
if [ "$KEEP_UP" = false ]; then
    echo "$(current_datetime) - All tasks completed; shutting down BOOM services"
    docker compose "${COMPOSE_CONFIG[@]}" down
fi

echo "$(current_datetime) - All tasks completed successfully"

exit 0
