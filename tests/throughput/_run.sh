#!/usr/bin/env bash

# Script to benchmark BOOM throughput using Docker or Apptainer containers.
# Usage: $0 [--keep-up] [--apptainer] [logs_dir]
#   --keep-up     Leave services running after the script finishes
#   --apptainer   Use Apptainer instead of Docker
#   logs_dir      Log directory (optional; default: $BOOM_REPO_ROOT/logs/boom)

set -euo pipefail

YELLOW="\e[33m"
GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

if [ -z "${BOOM_REPO_ROOT:-}" ]; then
    echo "Error: BOOM_REPO_ROOT is not set; set BOOM_REPO_ROOT environment variable"
    exit 1
fi

# Ports
MONGO_PORT=$BENCHMARK_MONGO_PORT
REDIS_PORT=$BENCHMARK_REDIS_PORT
KAFKA_PORT=$BENCHMARK_KAFKA_PORT

# Paths
TESTS_DIR="$BOOM_REPO_ROOT/tests"
CONFIG_FILE="$TESTS_DIR/throughput/config.yaml"
SCRIPTS_DIR="$BOOM_REPO_ROOT/apptainer/scripts"
BENCHMARK_SIF_DIR="$TESTS_DIR/apptainer/sif"
COMPOSE_CONFIG=("-f" "$TESTS_DIR/throughput/compose.yaml")
BG_PIDS=()

# If LOW_STORAGE mode is enabled, use the override to prevent volume mounts
if [ "${LOW_STORAGE:-}" = "true" ]; then
    COMPOSE_CONFIG+=("-f" "$TESTS_DIR/throughput/compose.low-storage.yaml")
fi

# Parse args
KEEP_UP=false
APPTAINER=false
POSITIONAL_ARGS=()
while [ "$#" -gt 0 ]; do
    case "$1" in
        --keep-up)
            KEEP_UP=true
            shift
            ;;
        --apptainer)
            APPTAINER=true
            shift
            ;;
        --*)
            echo "Unknown option: $1"
            echo "Usage: $0 [--keep-up] [--apptainer] [logs_dir]"
            exit 1
            ;;
        *)
            POSITIONAL_ARGS+=("$1")
            shift
            ;;
    esac
done
if [ ${#POSITIONAL_ARGS[@]} -gt 1 ]; then
    echo "Usage: $0 [--keep-up] [--apptainer] [logs_dir]"
    exit 1
fi

# Arguments
LOGS_DIR="${POSITIONAL_ARGS[0]:-$BOOM_REPO_ROOT/logs/boom}"

# A function that returns the current date and time
current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

cleanup() {
    echo "$(current_datetime) - Cleaning up background processes"
    if [ ${#BG_PIDS[@]} -gt 0 ]; then
        kill "${BG_PIDS[@]}" 2>/dev/null || true
        wait "${BG_PIDS[@]}" 2>/dev/null || true
    fi
}

trap cleanup EXIT INT TERM

stop_all_instances() {
  if [ "$KEEP_UP" = true ]; then
      echo -e "$(current_datetime) - ${YELLOW}--keep-up flag is set; leaving BOOM services running${END}"
      return
  fi
  echo -e "$(current_datetime) - ${GREEN}Shutting down BOOM services${END}"
  if [ "$APPTAINER" == "true" ]; then
    apptainer instance stop benchmark_boom
    apptainer instance stop benchmark_kafka
    apptainer instance stop benchmark_valkey
    apptainer instance stop benchmark_mongo
  else
    docker compose "${COMPOSE_CONFIG[@]}" down
  fi
}

run_mongo_query(){
    local query="$1"
    local as_admin="${2:-false}"
    local auth=""
    if [ "$as_admin" == "true" ]; then
        auth="/admin?authSource=admin"
    fi
    if [ "$APPTAINER" == "true" ]; then
      apptainer exec instance://benchmark_mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:${MONGO_PORT}${auth}" --quiet --eval "$query"
    else
      docker compose "${COMPOSE_CONFIG[@]}" exec -T mongo mongosh "mongodb://mongoadmin:mongoadminsecret@localhost:27017${auth}" --quiet --eval "$query"
    fi
}

mongo_count() {
    local query="$1"
    local raw
    raw=$(run_mongo_query "$query")
    raw=$(printf '%s\n' "$raw" | tail -n 1 | tr -d '\r')
    raw=$(printf '%s' "$raw" | tr -cd '0-9')
    echo "${raw:-0}"
}

if [ "$APPTAINER" == "true" ]; then
  # -----------------------------
  # Load environment variables
  # -----------------------------
  set -a
  source "$BOOM_REPO_ROOT/.env"
  set +a

  # -----------------------------
  # Start MongoDB
  # -----------------------------
  echo && echo "$(current_datetime) - Starting MongoDB"
  apptainer instance run --bind "$LOGS_DIR/mongodb:/log" "$BENCHMARK_SIF_DIR/mongo.sif" benchmark_mongo
  sleep 5
  "$SCRIPTS_DIR/mongodb-healthcheck.sh" --port "$MONGO_PORT" --instance benchmark_mongo

  echo "$(current_datetime) - Initializing MongoDB with test data"
  apptainer exec \
      --bind "$TESTS_DIR/throughput/apptainer_mongo-init.sh:/mongo-init.sh" \
      --bind "$TESTS_DIR/data/alerts/kowalski.NED.json.gz:/kowalski.NED.json.gz" \
      --bind "$TESTS_DIR/data/alerts/boom_throughput.ZTF_alerts_aux.dump.gz:/boom_throughput.ZTF_alerts_aux.dump.gz" \
      --bind "$TESTS_DIR/throughput/cats150.filter.json:/cats150.filter.json" \
      --env DB_NAME=boom-benchmarking \
      --env DB_ADD_URI= \
      "$BENCHMARK_SIF_DIR/mongo.sif" \
      /bin/bash /mongo-init.sh

  # -----------------------------
  # Start Valkey
  # -----------------------------
  echo && echo "$(current_datetime) - Starting Valkey"
  mkdir -p "$LOGS_DIR/valkey"
  apptainer instance run --bind "$LOGS_DIR/valkey:/log" "$BENCHMARK_SIF_DIR/valkey.sif" benchmark_valkey
  "$SCRIPTS_DIR/valkey-healthcheck.sh" --port "$REDIS_PORT" --instance benchmark_valkey

  # -----------------------------
  # Start Kafka
  # -----------------------------
  echo && echo "$(current_datetime) - Starting Kafka"
  mkdir -p "$LOGS_DIR/kafka"
  apptainer instance run \
      --bind "$BOOM_REPO_ROOT/config/kafka_server_jaas.conf:/etc/kafka/kafka_server_jaas.conf:ro" \
      --bind "$LOGS_DIR/kafka:/opt/kafka/logs" \
      "$BENCHMARK_SIF_DIR/kafka.sif" benchmark_kafka
  "$SCRIPTS_DIR/kafka-healthcheck.sh" --port "$KAFKA_PORT" --instance benchmark_kafka

  # -----------------------------
  # Start Boom
  # -----------------------------
  echo && echo "$(current_datetime) - Starting BOOM instance"
  apptainer instance start \
    --env RUST_LOG=debug,ort=error \
    --bind "$CONFIG_FILE:/app/config.yaml" \
    --bind "$TESTS_DIR/data/alerts:/app/data/alerts" \
    "$BOOM_REPO_ROOT/apptainer/sif/boom.sif" benchmark_boom
  sleep 3

  # -----------------------------
  # Start Producer
  # -----------------------------
  echo && echo "$(current_datetime) - Starting Producer"
  apptainer exec --pwd /app \
    instance://benchmark_boom /app/kafka_producer ztf 20250311 public --server-url localhost:"$KAFKA_PORT" \
    > "$LOGS_DIR/producer.log" 2>&1
  echo -e "${GREEN}$(current_datetime) - Producer finished sending alerts${END}"

  # -----------------------------
  # Start Consumer
  # -----------------------------
  echo && echo "$(current_datetime) - Starting Consumer"
  apptainer exec --pwd /app \
    instance://benchmark_boom /app/kafka_consumer ztf 20250311 --programids public \
    > "$LOGS_DIR/consumer.log" 2>&1 &
  echo -e "${GREEN}Boom consumer started for survey ztf${END}"

  # -----------------------------
  # Start Scheduler
  # -----------------------------
  echo && echo "$(current_datetime) - Starting Scheduler"
  apptainer exec --pwd /app instance://benchmark_boom /app/scheduler ztf \
    > "$LOGS_DIR/scheduler.log" 2>&1 &
  echo -e "${GREEN}Boom scheduler started for survey ztf${END}"

else
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
fi

EXPECTED_ALERTS=29142
N_FILTERS=25
TIMEOUT_SECS=${TIMEOUT_SECS:-300} # 5 minutes default

# -----------------------------
# Wait for the kafka consumer to start expecting messages (when it logs "Consumer received first message, continuing...")
# -----------------------------
echo && echo "$(current_datetime) - Waiting for Kafka consumer to start"
START_TIME=$(date +%s)
while true; do
    if [ "$APPTAINER" == "true" ]; then
        grep -q "Consumer received first message, continuing..." "$LOGS_DIR/consumer.log" && break
    else
        docker compose "${COMPOSE_CONFIG[@]}" logs consumer | grep -q "Consumer received first message, continuing..." && break
        MONGO_INIT_CONTAINER_ID=$(docker compose "${COMPOSE_CONFIG[@]}" ps -aq mongo-init | tail -n 1)
        if [ -n "$MONGO_INIT_CONTAINER_ID" ]; then
            MONGO_INIT_EXIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' "$MONGO_INIT_CONTAINER_ID" 2>/dev/null || true)
            if [[ "$MONGO_INIT_EXIT_CODE" =~ ^[0-9]+$ ]] && [ "$MONGO_INIT_EXIT_CODE" -ne 0 ]; then
                echo "$(current_datetime) - ERROR: mongo-init did not complete successfully (exit $MONGO_INIT_EXIT_CODE)"
                stop_all_instances
                exit 1
            fi
        fi
    fi

    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo -e "$(current_datetime) - ${RED} Timeout reached while waiting for Kafka consumer to start${END}"
        stop_all_instances
        exit 1
    fi
    sleep 1
done
END_TIME=$(date +%s)
STARTUP_TIME=$((END_TIME - START_TIME))
echo "$(current_datetime) - Kafka consumer started in $STARTUP_TIME seconds"

# If we are in LOW_STORAGE mode, clean up the downloaded files (producer files are not mounted)
if [ "${LOW_STORAGE:-}" = "true" ]; then
    echo "$(current_datetime) - LOW_STORAGE mode enabled; cleaning up downloaded files to save space"
    rm -rf ./data/alerts/kowalski.NED.json.gz || true
    rm -rf ./data/alerts/boom_throughput.ZTF_alerts_aux.dump.gz || true
fi

# -----------------------------
# Wait for alerts ingestion
# -----------------------------
echo "$(current_datetime) - Waiting for all alerts to be ingested"
START_TIME=$(date +%s)
while [ "$(mongo_count "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments()")" -lt "$EXPECTED_ALERTS" ]; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo -e "$(current_datetime) - ${RED}Timeout reached while waiting for alerts to be ingested${END}"
        stop_all_instances
        exit 1
    fi
    sleep 1
done
END_TIME=$(date +%s)
INGESTION_TIME=$((END_TIME - START_TIME))
echo "$(current_datetime) - All $EXPECTED_ALERTS alerts ingested in $INGESTION_TIME seconds"

# -----------------------------
# Wait for alerts classification
# -----------------------------
echo "$(current_datetime) - Waiting for all alerts to be classified"
START_TIME=$(date +%s)
while [ "$(mongo_count "db.getSiblingDB('boom-benchmarking').ZTF_alerts.countDocuments({ classifications: { \$exists: true } })")" -lt "$EXPECTED_ALERTS" ]; do
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo -e "$(current_datetime) - ${RED}Timeout reached while waiting for alerts to be classified${END}"
        stop_all_instances
        exit 1
    fi
    sleep 1
done
END_TIME=$(date +%s)
CLASSIFICATION_TIME=$((END_TIME - START_TIME))
echo "$(current_datetime) - All $EXPECTED_ALERTS alerts classified in $CLASSIFICATION_TIME seconds"

# -----------------------------
# Wait for all filters to run on all alerts
# -----------------------------
echo "$(current_datetime) - Waiting for filters to run on all alerts"
START_TIME=$(date +%s)
PASSED_ALERTS=0
while [ $PASSED_ALERTS -lt $EXPECTED_ALERTS ]; do
    if [ "$APPTAINER" == "true" ]; then
      PASSED_ALERTS=$(cat "$LOGS_DIR/scheduler.log" | grep "passed filter" | awk -F'/' '{sum += $NF} END {print sum}' || true)
    else
      PASSED_ALERTS=$(docker compose "${COMPOSE_CONFIG[@]}" logs scheduler | grep "passed filter" | awk -F'/' '{sum += $NF} END {print sum}' || true)
    fi
    PASSED_ALERTS=${PASSED_ALERTS:-0}
    PASSED_ALERTS=$((PASSED_ALERTS / N_FILTERS))
    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))
    if [ $ELAPSED_TIME -ge $TIMEOUT_SECS ]; then
        echo "$(current_datetime) - Timeout reached while waiting for filters to run on all alerts"
        stop_all_instances
        exit 1
    fi
    sleep 1
done
END_TIME=$(date +%s)
FILTERING_TIME=$((END_TIME - START_TIME))
echo "$(current_datetime) - All $EXPECTED_ALERTS alerts filtered in $FILTERING_TIME seconds"

echo "$(current_datetime) - All alerts ingested, classified, and filtered"
echo "$(current_datetime) - Reading from Kafka output topic"
if [ "$APPTAINER" == "true" ]; then
  python "$TESTS_DIR/throughput/read-kafka-output.py" --server "localhost:$KAFKA_PORT"
else
  python "$TESTS_DIR/throughput/read-kafka-output.py"
fi

# -----------------------------
# Export MongoDB collection stats to JSON for analysis
# -----------------------------
echo "$(current_datetime) - Collecting MongoDB collection stats"

MONGO_RESULT="$({ run_mongo_query '
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
' "true"; } | tail -n 1)"

if [ -n "$MONGO_RESULT" ]; then
	mkdir -p "$LOGS_DIR"
	if command -v jq >/dev/null 2>&1; then
		printf '%s\n' "$MONGO_RESULT" | jq . > "$LOGS_DIR/collection_stats.json"
	else
		printf '%s\n' "$MONGO_RESULT" > "$LOGS_DIR/collection_stats.json"
	fi
	echo "$(current_datetime) - Wrote collection stats to $LOGS_DIR/collection_stats.json"
fi

# -----------------------------
# Stop all instances
# -----------------------------
if [ "$APPTAINER" == "false" ]; then
  # Check to see if any of our containers have exited with a non-zero status, which would indicate an error
  EXIT_CODE=$(docker compose "${COMPOSE_CONFIG[@]}" ps -aq | xargs docker inspect -f '{{.State.ExitCode}}' | grep -v '^0$' || true)
  if [ -n "$EXIT_CODE" ]; then
      echo "$(current_datetime) - ERROR: One or more containers exited with a non-zero status"
      stop_all_instances
      exit 1
  fi
fi

echo -e "$(current_datetime) - ${GREEN}All tasks completed${END}"
echo && stop_all_instances

exit 0
