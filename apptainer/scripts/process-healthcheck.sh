#!/bin/bash

# Process health check
# $1 = process name (required)
# $2 = name to print (optional, defaults to process name)

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

PROCESS=$1
NAME=${2:-$PROCESS}

if [ -z "$PROCESS" ]; then
    echo "Missing process name argument"
    exit 1
fi

if pgrep -af "$PROCESS" | grep -v "process-healthcheck.sh" > /dev/null; then
    echo -e "${GREEN}$(current_datetime) - $NAME is healthy${END}"
    exit 0
fi

echo -e "${RED}$(current_datetime) - $NAME unhealthy${END}"
exit 1