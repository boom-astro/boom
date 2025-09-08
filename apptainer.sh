#!/bin/bash
# Description: Script to manage Apptainer instances for MongoDB, Kafka, Valkey, and Boom.
# Usage: ./apptainer.sh {compose|health|compose}

if [ "$1" != "stop" ] && [ "$1" != "compose" ] && [ "$1" != "health" ]; then
  echo "Usage: $0 {stop|compose|health}"
  exit 1
fi

if [ "$1" = "compose" ]; then
  ./apptainer/scripts/apptainer_compose.sh
  exit 0
fi

if [ "$1" == "stop" ]; then
  apptainer instance stop mongo
  apptainer instance stop broker
  apptainer instance stop valkey
  apptainer instance stop boom
  apptainer instance stop kuma
fi

if [ "$1" == "health" ]; then
  apptainer instance list
  SCRIPTS_DIR="$HOME/boom/apptainer/scripts"
  $SCRIPTS_DIR/mongodb-healthcheck.sh
  $SCRIPTS_DIR/valkey-healthcheck.sh
  $SCRIPTS_DIR/kafka-healthcheck.sh
fi