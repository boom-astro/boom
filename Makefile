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

YQ_IMAGE := mikefarah/yq:4.47.1

.PHONY: configs
configs:
	@set -e; \
	for dir in config/prod/*/; do \
		dir=$${dir%/}; \
		[ -f "$$dir/overrides.yaml" ] || continue; \
		out="$$dir/config.yaml"; \
		printf '%s\n%s\n\n' \
			'# AUTO-GENERATED FILE. DO NOT EDIT DIRECTLY.' \
			"# Edit config.yaml or $$dir/overrides.yaml, then run 'make configs'." > "$$out"; \
		docker run --rm -v "$$PWD:/workdir" -w /workdir $(YQ_IMAGE) \
			eval-all '. as $$item ireduce ({}; . * $$item)' \
			config.yaml "$$dir/overrides.yaml" >> "$$out"; \
		echo "Generated $$out"; \
	done

.PHONY: check-configs
check-configs: configs
	@echo "Validating generated deployment configs"
	@set -e; \
	found=0; \
	for cfg in config/prod/*/config.yaml; do \
		[ -f "$$cfg" ] || continue; \
		found=1; \
		cargo run --bin check_config -- "$$cfg"; \
	done; \
	if [ "$$found" -eq 0 ]; then \
		echo "No generated configs found at config/prod/*/config.yaml"; \
		exit 1; \
	fi
