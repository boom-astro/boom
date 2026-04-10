#!/bin/bash

# API health check
#   nb_retries  max retries (empty = unlimited)
#   --port      port of the API (default: from BOOM_API__PORT env or 4000)

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
  log "Usage: $0 [nb_retries] [--port PORT]" "$RED"
  exit 1
}

NB_RETRIES=""
while [ "$#" -gt 0 ]; do
  case "$1" in
    --port)
      PORT="$2"
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

PORT="${PORT:-${BOOM_API__PORT:-4000}}"

cpt=0
while true; do
  output=$(curl -sf --max-time 3 "http://localhost:${PORT}/" 2>&1)
  if echo "$output" | grep -q "BOOM"; then
    log "API is healthy" "$GREEN"
    echo "                      port ${PORT}"
    exit 0
  else
    log "API unhealthy in port ${PORT}" "$RED"
  fi

  if [ -n "$NB_RETRIES" ] && [ "$cpt" -ge "$NB_RETRIES" ]; then
    exit 1
  fi
  ((cpt++))
  sleep 2
done
