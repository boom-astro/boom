#!/bin/bash

# Valkey health check
#  nb_retries  max retries (empty = unlimited)
#  --instance  name of the Apptainer instance (default: valkey)

YELLOW="\e[33m"
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
  log "Usage: $0 [nb_retries] [--instance INSTANCE]" "$RED"
  exit 1
}

NB_RETRIES=""
while [ "$#" -gt 0 ]; do
  case "$1" in
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
while true; do
  output=$(timeout 3 bash -c "apptainer exec instance://${INSTANCE:-valkey} redis-cli ping" 2>&1)
  if echo "$output" | grep -q "PONG"; then
    log "valkey is healthy" "$GREEN"
    exit 0
  elif echo "$output" | grep -q "LOADING"; then
    log "valkey is loading the dataset" "$YELLOW"
    sleep 5
  else
    log "valkey is unhealthy: $output" "$RED"
    sleep 1
  fi

  if [ -n "$NB_RETRIES" ] && [ $cpt -ge "$NB_RETRIES" ]; then
    exit 1
  fi
  ((cpt++))
done