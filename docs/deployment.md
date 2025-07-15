# Deploying a BOOM system

## Option 1: Single node with Docker Compose and a GitHub Actions self-hosted runner

TODO: Point to the Caltech deployment repo as an example?
Will need to keep secrets out of committed text files.

1. Create a new GitHub repo to store your Docker Compose and BOOM configuration
   files.
   This should be private unless BOOM can be configured with secrets as
   environmental variables instead of in committed text files.
1. Copy in `docker-compose.default.yaml` and `config.default.yaml`, removing
   `.default` from the file names.
1. Change all important secrets in the configs to values that are unique and
   hard to guess.
   Change API token expiration time to be greater than zero, e.g., 3600.
   TODO: These should be set as secrets in the GitHub settings, not out
   in plain text in the repo files.
1. Add a BOOM API service to your Docker Compose config.
   TODO: Or we could deploy with `docker compose up --profile=prod`?
1. Add one BOOM Kafka consumer service to your Docker Compose config for each
   survey BOOM will subscribe to.
1. Add one BOOM scheduler service to your Docker Compose config for each survey
   BOOM will subscribe to.
1. TODO: Set up a Traefik reverse proxy config and run that in Docker Compose.
1. Configure your node as a GitHub Actions self-hosted runner.
1. Create a `deploy.yaml` GitHub Actions workflow that runs on each release.
   Ensure its `environment` is set to the environment that points to your
   GitHub Actions self-hosted runner.
   TODO: Deploy a staging instance?
