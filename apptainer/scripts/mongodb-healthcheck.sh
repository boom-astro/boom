#!/bin/bash

# MongoDB health check
# $1 = NB_RETRIES max retries (empty = unlimited)

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

NB_RETRIES=${1:-}

cpt=0
until echo 'db.runCommand("ping").ok' | \
    apptainer exec instance://mongo mongosh localhost:27017/test --quiet 2>/dev/null | \
    grep -q 1; do
    echo -e "${RED}$(current_datetime) - MongoDB unhealthy${END}"
    if [ -n "$NB_RETRIES" ] && [ "$cpt" -ge "$NB_RETRIES" ]; then
        exit 1
    fi
    ((cpt++))
    sleep 5
done

echo -e "${GREEN}$(current_datetime) - MongoDB is healthy${END}"
exit 0