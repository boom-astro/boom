.PHONY: dev
dev:
	docker compose --profile dev up

.PHONY: produce-ztf
produce-ztf:
	cargo run --bin kafka_producer ztf 20240617 public --limit 500 --server-url localhost:9092

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
