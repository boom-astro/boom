#!/bin/bash

# Kafka health check
# $1 = NB_RETRIES max retries (empty = unlimited)

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

NB_RETRIES=${1:-}

cpt=0
until apptainer exec instance://broker /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:9092 > /dev/null 2>&1; do
    echo -e "${RED}$(current_datetime) - Kafka unhealthy${END}"
    if [ -n "$NB_RETRIES" ] && [ $cpt -ge $NB_RETRIES ]; then
      exit 1
    fi

    ((cpt++))
    sleep 2
done

echo -e "${GREEN}$(current_datetime) - Kafka is healthy${END}"
exit 0