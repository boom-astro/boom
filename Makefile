# Cutout storage modes (choose one):
#   make dev                - shared MongoDB (same instance as alerts, simplest)
#   make dev-mongo          - dedicated MongoDB for cutouts (separate container, optional separate disk)
#   make dev-s3             - S3-compatible storage via local rustfs
#   make dev-s3-external    - external S3 bucket (AWS S3, Wasabi, …); requires BOOM_CUTOUTS_STORAGE__REGION/ACCESS_KEY/SECRET_KEY

.PHONY: dev
dev:
	docker compose -f docker-compose.yaml -f docker-compose.override.yaml --profile dev up

.PHONY: dev-mongo
dev-mongo:
	docker compose -f docker-compose.yaml -f docker-compose.cutouts-mongo.yaml -f docker-compose.override.yaml --profile dev up

.PHONY: dev-s3
dev-s3:
	docker compose -f docker-compose.yaml -f docker-compose.cutouts-s3.yaml -f docker-compose.override.yaml --profile dev up

.PHONY: dev-s3-external
dev-s3-external:
	docker compose -f docker-compose.yaml -f docker-compose.cutouts-s3-external.yaml -f docker-compose.override.yaml --profile dev up

.PHONY: delete-produce-ztf
delete-produce-ztf: # Delete Kafka topic, data, and re-produce ZTF traffic for testing
	@bash scripts/delete-produce-ztf-dev.sh

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
