#!/bin/bash

current_datetime() {
    TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

display_consumers_and_schedulers() {
  if [ "$1" == "api" ]; then
    pid=$(pgrep -f "/app/boom-api")
    if [ -n "$pid" ]; then
      listen=$(ss -tlnp 2>/dev/null | grep "pid=$pid" | awk '{print $4}' | head -1)
      echo "                      API running on ${listen:-?}"
    else
      echo -e "${RED}                      no API${END}"
    fi
    return
  fi

  local survey="$1"
  consumers=$(pgrep -f "/app/kafka_consumer ${survey}")
  if [ -n "$consumers" ]; then
    # List each running consumer
    echo "$consumers" | while read -r pid; do
      cmd=$(ps -p "$pid" -o cmd --no-headers)
      echo "                      ${cmd/\/app\/kafka_consumer/consumer}"
    done
  else
    echo -e "${RED}                      no consumer${END}"
  fi

  schedulers=$(pgrep -f "/app/scheduler ${survey}")
  if [ -n "$schedulers" ]; then
    # List each running scheduler
    echo "$schedulers" | while read -r pid; do
      cmd=$(ps -p "$pid" -o cmd --no-headers)
      echo "                      ${cmd/\/app\/scheduler/scheduler}"
    done
  else
    echo -e "${RED}                      no scheduler${END}"
  fi
}


if apptainer instance list | awk '{print $1}' | grep -q "^boom"; then
  for instance in $(apptainer instance list | awk '{print $1}' | grep "^boom"); do
    echo -e "${GREEN}$(current_datetime) - $instance is healthy${END}"
    display_consumers_and_schedulers "${instance#boom_}"
  done
else
  echo -e "${RED}$(current_datetime) - boom instance unhealthy${END}"
fi