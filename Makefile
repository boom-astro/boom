.PHONY: api-dev
api-dev:
	@echo "Starting API server and watching for changes"
	cargo watch --watch api -x "run --package boom-api"

.PHONY: format
format:
	@echo "Formatting code"
	pre-commit run --all

.PHONY: test-api
test-api:
	@echo "Running API tests"
	cargo test --package boom-api
