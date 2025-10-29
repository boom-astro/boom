#!/bin/bash

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

consumers=$(pgrep -f "/app/kafka_consumer")
if [ -n "$consumers" ]; then
  echo -e "${GREEN}$(current_datetime) - consumer is healthy${END}"
  # List each running consumer
  echo "$consumers" | while read -r pid; do
    cmd=$(ps -p "$pid" -o cmd --no-headers)
    echo "                      ${cmd/\/app\/kafka_consumer/consumer}"
  done
else
  echo -e "${RED}$(current_datetime) - consumer unhealthy${END}"
fi

schedulers=$(pgrep -f "/app/scheduler")
if [ -n "$schedulers" ]; then
  echo -e "${GREEN}$(current_datetime) - scheduler is healthy${END}"
  # List each running scheduler
  echo "$schedulers" | while read -r pid; do
    cmd=$(ps -p "$pid" -o cmd --no-headers)
    echo "                      ${cmd/\/app\/scheduler/scheduler}"
  done
else
  echo -e "${RED}$(current_datetime) - scheduler unhealthy${END}"
fi