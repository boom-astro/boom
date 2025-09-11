#!/bin/bash

# Listener health check
# $1 = NB_RETRIES max retries (empty = unlimited)
# $2 = URL to check (optional, default: http://localhost:5555/health)

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

NB_RETRIES=${1:-}
URL=${2:-http://localhost:5555/health}

cpt=0
until timeout 3 curl -fs "$URL" >/dev/null 2>&1; do
    echo -e "${RED}$(current_datetime) - Listener unhealthy${END}"
    if [ -n "$NB_RETRIES" ] && [ $cpt -ge $NB_RETRIES ]; then
      exit 1
    fi

    ((cpt++))
    sleep 1
done

echo -e "${GREEN}$(current_datetime) - Listener is healthy${END}"
exit 0