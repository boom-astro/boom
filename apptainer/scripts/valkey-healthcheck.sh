#!/bin/bash

# Valkey health check
# $1 = NB_RETRIES max retries (empty = unlimited)

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

NB_RETRIES=${1:-}

cpt=0
until timeout 3 apptainer exec instance://valkey redis-cli ping 2>/dev/null | grep -q PONG; do
    echo -e "${RED}$(current_datetime) - Valkey unhealthy${END}"
    if [ -n "$NB_RETRIES" ] && [ $cpt -ge $NB_RETRIES ]; then
      exit 1
    fi

    ((cpt++))
    sleep 1
done

echo -e "${GREEN}$(current_datetime) - Valkey is healthy${END}"
exit 0