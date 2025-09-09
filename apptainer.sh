#!/bin/bash

# Script to manage Apptainer instances for MongoDB, Kafka, Valkey, and Boom.
# $1 = action: build | compose | start | health | stop
# $2-$6 = arguments used only with the 'start' action:
#   $2 = survey name for the consumer and scheduler
#   $3 = logs folder for consumer and scheduler (optional, default: $HOME/boom/logs/boom)
#   $4 = date for the consumer (optional)
#   $5 = program ID for the consumer (optional)
#   $6 = config path for the scheduler (optional)

SCRIPTS_DIR="$HOME/boom/apptainer/scripts"
GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

if [ "$1" != "build" ] && [ "$1" != "compose" ] && [ "$1" != "start" ] && [ "$1" != "health" ] && [ "$1" != "stop" ]; then
  echo "Usage: $0 {build|compose|start|health|stop}"
  exit 1
fi

if [ "$1" = "build" ]; then
  ./apptainer/def/build-sif.sh
  exit 0
fi

if [ "$1" = "compose" ]; then
  ./apptainer/scripts/apptainer_compose.sh
  exit 0
fi

if [ "$1" == "start" ]; then
  SURVEY=$2
  LOGS_FOLDER=${3:-"$HOME/boom/logs/boom"}
  if [ -z "$SURVEY" ]; then
    echo -e "${RED}Missing required argument: survey name${END}"
    exit 1
  fi

  if pgrep -f "/app/kafka_consumer" > /dev/null; then
    echo -e "${RED}Boom consumer already running.${END}"
  else
    ARGS=("$SURVEY")
    [ -n "$4" ] && ARGS+=("$4") # $4=date
    [ -n "$5" ] && ARGS+=("$5") # $5=program ID
    apptainer exec --env-file env instance://boom /app/kafka_consumer "${ARGS[@]}" > "$LOGS_FOLDER/consumer.log" 2>&1 &
    echo -e "${GREEN}Boom consumer started for survey $SURVEY${END}"
  fi

  if pgrep -f "/app/scheduler" > /dev/null; then
    echo -e "${RED}Boom scheduler already running.${END}"
  else
    ARGS=("$SURVEY")
    [ -n "$6" ] && ARGS+=("$6") # $6=config path
    apptainer exec instance://boom /app/scheduler "${ARGS[@]}" > "$LOGS_FOLDER/scheduler.log" 2>&1 &
    echo -e "${GREEN}Boom scheduler started for survey $SURVEY${END}"
  fi
fi

if [ "$1" == "stop" ]; then
  apptainer instance stop kuma
  if pgrep -f "boom-healthcheck-listener.py" > /dev/null; then
    pkill -f "boom-healthcheck-listener.py"
    echo "INFO:    Stopping boom healthcheck listener process"
  fi # Kill boom-healthcheck-listener process if running
  if pgrep -f "otel-collector" > /dev/null; then
    pkill -f "otel-collector"
    echo "INFO:    Stopping otel-collector process"
  fi # Kill otel-collector process if running
  apptainer instance stop prometheus
  apptainer instance stop boom
  apptainer instance stop valkey
  apptainer instance stop broker
  apptainer instance stop mongo
fi

if [ "$1" == "health" ]; then
  apptainer instance list
  echo
  $SCRIPTS_DIR/mongodb-healthcheck.sh 0
  $SCRIPTS_DIR/valkey-healthcheck.sh 0
  $SCRIPTS_DIR/kafka-healthcheck.sh 0
  $SCRIPTS_DIR/process-healthcheck.sh "/app/kafka_consumer" consumer
  $SCRIPTS_DIR/process-healthcheck.sh "/app/scheduler" scheduler
  $SCRIPTS_DIR/prometheus-healthcheck.sh 0
  $SCRIPTS_DIR/process-healthcheck.sh "/otelcol" otel-collector
  $SCRIPTS_DIR/boom-listener-healthcheck.sh 0
  $SCRIPTS_DIR/kuma-healthcheck.sh 0
fi