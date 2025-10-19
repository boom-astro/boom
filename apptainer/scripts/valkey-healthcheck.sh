#!/bin/bash

# Valkey health check
# $1 = NB_RETRIES max retries (empty = unlimited)

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

YELLOW="\e[33m"
GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

NB_RETRIES=${1:-}

cpt=0
while true; do
  output=$(timeout 3 bash -c "apptainer exec instance://valkey redis-cli ping" 2>&1)
  if echo "$output" | grep -q "PONG"; then
    echo -e "${GREEN}$(current_datetime) - valkey is healthy${END}"
    exit 0
  elif echo "$output" | grep -q "LOADING"; then
    echo -e "${YELLOW}$(current_datetime) - valkey is loading the dataset${END}"
    sleep 5
  else
    echo -e "${RED}$(current_datetime) - valkey unhealthy${END}"
    sleep 1
  fi

  if [ -n "$NB_RETRIES" ] && [ $cpt -ge $NB_RETRIES ]; then
    exit 1
  fi
  ((cpt++))
done