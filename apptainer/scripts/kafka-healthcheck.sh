#!/bin/bash

current_datetime() {
    date "+%Y-%m-%d %H:%M:%S"
}

RED="\e[31m"
END="\e[0m"

INTERVAL=${INTERVAL:-2}   # interval between checks in seconds

until apptainer exec instance://broker /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9092 > /dev/null 2>&1; do
  echo -e "${RED}$(current_datetime) - Kafka unhealthy${END}"
  sleep $INTERVAL
done

echo "$(current_datetime) - Kafka is healthy"
exit 0
