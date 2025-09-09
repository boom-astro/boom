#!/bin/bash

current_datetime() {
    date "+%Y-%m-%d %H:%M:%S"
}

RED="\e[31m"
END="\e[0m"

PROCESS=$1
NAME=${2:-$PROCESS}

if [ -z "$PROCESS" ]; then
    echo "Missing process name argument"
    exit 1
fi

if pgrep -af "$PROCESS" | grep -v "process-healthcheck.sh" > /dev/null; then
    echo "$(current_datetime) - $NAME is healthy"
    exit 0
fi

echo -e "${RED}$(current_datetime) - $NAME is unhealthy${END}"
exit 1