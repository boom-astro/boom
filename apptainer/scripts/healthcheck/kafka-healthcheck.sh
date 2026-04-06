#!/bin/bash

# Kafka health check
#   nb_retries  max retries (empty = unlimited)
#  --port      port of the Kafka instance (default: 9092)
#  --instance  name of the Apptainer instance (default: kafka)

GREEN="\e[32m"
RED="\e[31m"
END="\e[0m"

current_datetime() {
  TZ=utc date "+%Y-%m-%d %H:%M:%S"
}

log() {
  msg="$1"
  color="$2"
  if [ -z "$color" ]; then
    echo "$(current_datetime) - ${msg}"
  else
    echo -e "${color}$(current_datetime) - ${msg}${END}"
  fi
}

usage() {
  log "Usage: $0 [nb_retries] [--port PORT] [--instance INSTANCE]" "$RED"
  exit 1
}

NB_RETRIES=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --port)
      PORT="$2"
      shift 2
      ;;
    --instance)
      INSTANCE="$2"
      shift 2
      ;;
    --*)
      log "Unknown option: $1" "$RED"
      usage
      ;;
    *)
      if [ -n "$NB_RETRIES" ]; then
        log "Too many arguments: $1" "$RED"
        usage
      fi
      NB_RETRIES="$1"
      shift
      ;;
  esac
done

cpt=0
until apptainer exec instance://"${INSTANCE:-kafka}" /opt/kafka/bin/kafka-cluster.sh cluster-id --bootstrap-server localhost:"${PORT:-9092}" > /dev/null 2>&1; do
  log "kafka unhealthy" "$RED"
  if [ -n "$NB_RETRIES" ] && [ $cpt -ge "$NB_RETRIES" ]; then
    exit 1
  fi
  ((cpt++))
  sleep 2
done

log "kafka is healthy" "$GREEN"
exit 0