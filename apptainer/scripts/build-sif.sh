#!/bin/bash

# Script to build SIF files using Apptainer.
# $1 = service to build (optional):
#     - "all" (default): builds all services
#     - "benchmark"    : builds all services except monitoring services
#     - "mongo"        : builds MongoDB service
#     - "valkey"       : builds Valkey service
#     - "kafka"        : builds Kafka service
#     - "boom"         : builds BOOM service
#     - "prometheus"   : builds Prometheus service
#     - "otel"         : builds OpenTelemetry Collector service
#     - "kuma"         : builds Uptime Kuma service

mkdir -p apptainer/sif

start_service() {
    local service="$1"
    local target="$2"
    # Return 0 if target is empty, "all", "benchmark" or matches the service name
    [[ -z "$target" || "$target" = "all" || "$target" = "benchmark" || "$target" = "$service" ]]
}

build() {
    apptainer build --force apptainer/sif/"$1".sif "${2:-apptainer/def/$1.def}"
}

declare -a services=("mongo" "valkey" "kafka" "boom")
for service in "${services[@]}"; do
  if start_service "$service" "$1"; then
    build "$service"
  fi
done


declare -a monitoring_services=("prometheus" "otel docker://otel/opentelemetry-collector:0.131.1" "kuma")
if [ "$1" != "benchmark" ]; then
  for service in "${monitoring_services[@]}"; do
    read -r name def <<< "$service" # split service if a def is provided
    if start_service "$name" "$1"; then
      build "$name" "$def"
    fi
  done
fi
