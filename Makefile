.PHONY: dev
dev:
	docker compose --profile dev up

.PHONY: produce-ztf
produce-ztf:
	@$(MAKE) reset-ztf-state
	cargo run --bin kafka_producer ztf 20240617 public --limit 500 --server-url localhost:9092

.PHONY: reset-ztf-state
reset-ztf-state:
	@echo "Resetting ZTF dev pipeline state (Mongo, Valkey, Kafka topic)..."
	@docker compose exec -T mongo sh -lc 'mongosh "mongodb://$$MONGO_INITDB_ROOT_USERNAME:$$MONGO_INITDB_ROOT_PASSWORD@localhost:27017/boom?authSource=admin" --quiet --eval "db.ZTF_alerts.deleteMany({}); db.ZTF_alerts_aux.deleteMany({}); db.ZTF_alerts_cutouts.deleteMany({});"'
	@docker compose exec -T valkey redis-cli DEL ZTF_alerts_packets_queue ZTF_alerts_packets_queue_temp ZTF_alerts_enrichment_queue ZTF_alerts_filter_queue >/dev/null
	@docker compose exec -T broker /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker:29092 --delete --if-exists --topic ztf_20240617_programid1 >/dev/null || true
	@echo "ZTF state reset complete."

.PHONY: api-dev
api-dev:
	@echo "Starting API server and watching for changes"
	cargo watch --watch src -x "run --bin api"

.PHONY: format
format:
	@echo "Formatting code"
	pre-commit run --all

.PHONY: test-api
test-api:
	@echo "Running API tests"
	cargo test --test test_api
