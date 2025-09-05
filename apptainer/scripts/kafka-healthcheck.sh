#!/bin/bash

current_datetime() {
    date "+%Y-%m-%d %H:%M:%S"
}

INTERVAL=${INTERVAL:-2}   # interval between checks in seconds

until apptainer exec instance://broker /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9092 > /dev/null 2>&1; do
  echo "$(current_datetime) - Kafka unhealthy"
  sleep $INTERVAL
done

echo "$(current_datetime) - Kafka is healthy"
exit 0
