#!/bin/bash
# Description: Script to manage Apptainer instances for MongoDB, Kafka, Valkey, and Boom.
# Usage: ./apptainer.sh {compose|health|stop}

SCRIPTS_DIR="$HOME/boom/apptainer/scripts"

if [ "$1" != "stop" ] && [ "$1" != "compose" ] && [ "$1" != "health" ]; then
  echo "Usage: $0 {compose|health|stop}"
  exit 1
fi

if [ "$1" = "compose" ]; then
  ./apptainer/scripts/apptainer_compose.sh
  exit 0
fi

if [ "$1" == "stop" ]; then
  apptainer instance stop kuma
  if pgrep -f "otel-collector" > /dev/null; then
    pkill -f "otel-collector"
    echo "INFO:    Stopping otel-collector process"
  fi
  apptainer instance stop prometheus
  apptainer instance stop boom
  apptainer instance stop valkey
  apptainer instance stop broker
  apptainer instance stop mongo
fi

if [ "$1" == "health" ]; then
  apptainer instance list
  echo
  $SCRIPTS_DIR/mongodb-healthcheck.sh
  $SCRIPTS_DIR/valkey-healthcheck.sh
  $SCRIPTS_DIR/kafka-healthcheck.sh
  $SCRIPTS_DIR/process-healthcheck.sh "/bin/kafka_consumer" consumer
  $SCRIPTS_DIR/process-healthcheck.sh "/bin/scheduler" scheduler
  $SCRIPTS_DIR/prometheus-healthcheck.sh
  $SCRIPTS_DIR/process-healthcheck.sh "/otelcol" otel-collector
fi