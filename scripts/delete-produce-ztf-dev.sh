#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
ENV_FILE="${REPO_ROOT}/.env"

if [[ ! -f "${ENV_FILE}" ]]; then
  echo "ERROR: Missing ${ENV_FILE}. Refusing to run without an explicit local environment file." >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "${ENV_FILE}"
set +a

effective_domain="${DOMAIN:-${BOOM_API__DOMAIN:-}}"
if [[ "${effective_domain}" != "localhost" ]]; then
  echo "ERROR: Refusing to run. DOMAIN/BOOM_API__DOMAIN must resolve to localhost." >&2
  exit 1
fi

echo "Resetting ZTF Mongo collections in local database 'boom'"
docker compose exec -T mongo sh -lc '
  cat <<"JS" | mongosh \
    --quiet \
    --username "$MONGO_INITDB_ROOT_USERNAME" \
    --password "$MONGO_INITDB_ROOT_PASSWORD" \
    --authenticationDatabase admin
const dbName = "boom";
const collections = ["ZTF_alerts", "ZTF_alerts_aux", "ZTF_alerts_cutouts"];
const dbh = db.getSiblingDB(dbName);
for (const name of collections) {
  const result = dbh.getCollection(name).deleteMany({});
  print("Cleared " + name + ": deleted " + result.deletedCount + " documents");
}
JS
'

echo "Resetting Kafka topic and restarting consumer"
docker compose exec -T broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --delete --if-exists --topic ztf_20240617_programid1 >/dev/null || true
docker compose restart consumer-ztf

echo "Reproducing ZTF traffic"
cargo run --bin kafka_producer ztf 20240617 public --limit 500 --server-url localhost:9092
