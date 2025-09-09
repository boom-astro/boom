#!/bin/bash

current_datetime() {
    date "+%Y-%m-%d %H:%M:%S"
}

RED="\e[31m"
END="\e[0m"

INTERVAL=${INTERVAL:-10}   # interval between checks in seconds

until echo 'db.runCommand("ping").ok' | apptainer exec instance://mongo mongosh localhost:27017/test --quiet | grep -q 1; do
    echo -e "${RED}${RED}$(current_datetime) - MongoDB unhealthy${END}"
    sleep $INTERVAL
done

echo "$(current_datetime) - MongoDB is healthy"
exit 0
