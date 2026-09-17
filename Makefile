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

# --- Milvus e2e testing ---------------------------------------------------
# Local, standalone Milvus (docker-compose.milvus.yaml) for running the
# end-to-end test without touching the shared NRP instance. The .env file keeps
# the NRP credentials as the default; the local overrides below are passed as
# shell env vars, which win over .env (dotenvy does not override already-set
# vars). So:
#   make test-milvus-local  -> runs the e2e test against local Milvus
#   make test-milvus-nrp    -> runs it against NRP (whatever .env points at)

# Local overrides: plaintext gRPC on :19530, auth disabled, `default` database.
# The standalone Milvus has authentication disabled, so it ignores the
# username/password entirely -- but config validation requires them to be
# non-empty when milvus.enabled, so we pass Milvus's own default placeholders.
MILVUS_LOCAL_ENV := \
	BOOM_MILVUS__ENABLED=true \
	BOOM_MILVUS__HOST=localhost \
	BOOM_MILVUS__PORT=19530 \
	BOOM_MILVUS__TLS=false \
	BOOM_MILVUS__USERNAME=root \
	BOOM_MILVUS__PASSWORD=Milvus \
	BOOM_MILVUS__DATABASE=default

.PHONY: milvus-up
milvus-up: # Start the local standalone Milvus stack (etcd + minio + standalone + attu)
	docker compose -p boom-milvus -f docker-compose.milvus.yaml up -d

.PHONY: milvus-down
milvus-down: # Stop the local Milvus stack, keeping its data volumes
	docker compose -p boom-milvus -f docker-compose.milvus.yaml down

.PHONY: milvus-clean
milvus-clean: # Stop the local Milvus stack AND wipe its data volumes
	docker compose -p boom-milvus -f docker-compose.milvus.yaml down -v

.PHONY: test-milvus-local
test-milvus-local: # Run the Milvus e2e test against the LOCAL standalone instance
	@echo "Running Milvus e2e test against local Milvus (localhost:19530)"
	$(MILVUS_LOCAL_ENV) cargo test --test test_milvus_e2e -- --nocapture

.PHONY: test-milvus-nrp
test-milvus-nrp: # Run the Milvus e2e test against NRP (uses .env as-is)
	@echo "Running Milvus e2e test against NRP (per .env)"
	cargo test --test test_milvus_e2e -- --nocapture
