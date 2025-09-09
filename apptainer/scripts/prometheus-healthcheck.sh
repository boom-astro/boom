#!/bin/bash

current_datetime() {
    date "+%Y-%m-%d %H:%M:%S"
}

RED="\e[31m"
END="\e[0m"

INTERVAL=${INTERVAL:-2}   # interval between checks in seconds
PROM_URL="http://localhost:9090/-/ready"

until curl -sf "$PROM_URL" > /dev/null; do
  echo -e "${RED}$(current_datetime) - Prometheus unhealthy${END}"
  sleep $INTERVAL
  exit 1
done

echo "$(current_datetime) - Prometheus is healthy"
