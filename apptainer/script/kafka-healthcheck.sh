#!/bin/bash

current_datetime() {
    date "+%Y-%m-%d %H:%M:%S"
}

INTERVAL=${INTERVAL:-10}   # interval between checks in seconds

while true; do
  if apptainer exec instance://broker /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9092 > /dev/null 2>&1; then
      echo "$(current_datetime) - Kafka is healthy"
      exit 0
  fi
  echo "$(current_datetime) - Kafka unhealthy"
  sleep $INTERVAL
done
