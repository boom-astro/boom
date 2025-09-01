#!/bin/bash
set -e

# Dossiers sur ton host
DATA_DIR="$HOME/boom/data"
ALERTS_DIR="$DATA_DIR/alerts"
CONFIG_FILE="$HOME/boom/tests/throughput/config.yaml"
MONGO_INIT="$HOME/boom/tests/throughput/mongo-init.sh"

SIF_DIR="$HOME/boom/sif"

# 1️⃣ MongoDB
echo "Starting MongoDB..."
apptainer run \
  --bind "$DATA_DIR/mongodb:/data/db" \
  --env MONGO_INITDB_ROOT_USERNAME=mongoadmin \
  --env MONGO_INITDB_ROOT_PASSWORD=mongoadminsecret \
  "$SIF_DIR/mongo.sif" &
MONGO_PID=$!

# Wait for MongoDB to be ready
echo "Waiting for MongoDB to be healthy..."
sleep 10  # tu peux remplacer par check mongosh ping si tu veux

# 2️⃣ Mongo-init
echo "Running mongo-init..."
apptainer run \
  --bind "$DATA_DIR/mongodb:/data/db" \
  --bind "$ALERTS_DIR/kowalski.NED.json.gz:/kowalski.NED.json.gz" \
