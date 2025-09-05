#!/bin/bash

current_datetime() {
    date "+%Y-%m-%d %H:%M:%S"
}

INTERVAL=${INTERVAL:-10}   # interval between checks in seconds
START_PERIOD=${START_PERIOD:-10}   # initial wait time before starting checks in seconds

sleep $START_PERIOD
until echo 'db.runCommand("ping").ok' | apptainer exec instance://mongo mongosh localhost:27017/test --quiet | grep -q 1; do
    echo "$(current_datetime) - MongoDB unhealthy"
    sleep $INTERVAL
done

echo "$(current_datetime) - MongoDB is healthy"
exit 0
