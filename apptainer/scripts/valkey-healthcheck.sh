#!/bin/bash

current_datetime() {
    date "+%Y-%m-%d %H:%M:%S"
}

RED="\e[31m"
END="\e[0m"

INTERVAL=${INTERVAL:-10}   # interval between checks in seconds
RETRIES=${RETRIES:-3}      # number of retries
TIMEOUT=${TIMEOUT:-5}      # timeout for each ping in seconds

attempt=0
until timeout $TIMEOUT apptainer exec instance://valkey redis-cli ping | grep -q PONG; do
    attempt=$((attempt+1))
    if [ "$attempt" -ge "$RETRIES" ]; then
        echo -e "${RED}$(current_datetime) - Valkey remains unhealthy after $RETRIES attempts${END}"
        exit 1
    fi
    echo -e "${RED}$(current_datetime) - Valkey unhealthy (attempt $attempt/$RETRIES)${END}"
    sleep $INTERVAL
done

echo "$(current_datetime) - Valkey is healthy"
exit 0