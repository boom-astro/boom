.PHONY: api-dev
api-dev:
	@echo "Starting API server and watching for changes"
	cargo watch --watch api -x "run --package boom-api"
