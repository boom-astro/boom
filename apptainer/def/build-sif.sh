#!/bin/bash

# Script to build SIF files using Apptainer.
# $1 = service to build (optional):
#     - "all" (default): builds all services
#     - "test"         : builds all services except monitoring services
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
    # Start a service if target is empty, "all", or matches the service name
    if [[ -z "$target" || "$target" = "all" || "$target" = "$service" ]]; then
        return 0
    fi
    return 1
}

if start_service "mongo" "$1"; then
  apptainer build apptainer/sif/mongo.sif apptainer/def/mongo.def
fi

if start_service "valkey" "$1"; then
  apptainer build apptainer/sif/valkey.sif apptainer/def/valkey.def
fi

if start_service "kafka" "$1"; then
  apptainer build apptainer/sif/kafka.sif apptainer/def/kafka.def
fi

if start_service "boom" "$1"; then
  apptainer build apptainer/sif/boom.sif apptainer/def/boom.def
fi

# Skip building monitoring services if "test" is provided
if start_service "test" "$1"; then
  exit 0
fi

if start_service "prometheus" "$1"; then
  apptainer build apptainer/sif/prometheus.sif apptainer/def/prometheus.def
fi

if start_service "otel" "$1"; then
  apptainer build apptainer/sif/otel-collector.sif docker://otel/opentelemetry-collector:0.131.1
fi

if start_service "kuma" "$1"; then
  apptainer build apptainer/sif/uptime-kuma.sif apptainer/def/uptime-kuma.def
fi