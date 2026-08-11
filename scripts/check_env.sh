#!/usr/bin/env bash
# Report .env variables that Compose requires but cannot resolve.
#
# Compose interpolates every service in every file it loads *before* it filters
# by profile, so `make dev` needs the ${VAR:?...} variables of prod-only
# services too. Left to Compose, a stale .env fails one variable at a time with
# an error naming a service that dev never runs (e.g. consumer-ztf-caltech),
# which reads like a broken compose file rather than an out-of-date .env.
#
# Usage: check_env.sh <compose-file>...
set -euo pipefail

cd "$(dirname "$0")/.."

if [ ! -f .env ]; then
    echo "error: no .env file" >&2
    echo "  cp .env.example .env" >&2
    exit 1
fi

# ${VAR:?message} -- the ':' form rejects empty values as well as unset ones.
required=$(
    grep -ohE '\$\{[A-Za-z_][A-Za-z0-9_]*:\?' "$@" |
        sed 's/^\${//; s/:?$//' |
        sort -u
)

missing=()
empty=()
for var in $required; do
    # A real environment variable wins over .env, so honour it here too.
    if [ -n "${!var-}" ]; then
        continue
    fi
    line=$(grep -E "^[[:space:]]*${var}=" .env | tail -n 1 || true)
    if [ -z "$line" ]; then
        missing+=("$var")
    elif [ -z "$(echo "${line#*=}" | sed 's/[[:space:]]*#.*$//; s/[[:space:]]*$//')" ]; then
        empty+=("$var")
    fi
done

if [ ${#missing[@]} -eq 0 ] && [ ${#empty[@]} -eq 0 ]; then
    exit 0
fi

echo "error: .env is missing values Docker Compose requires." >&2
echo >&2
for var in "${missing[@]-}"; do
    [ -n "$var" ] || continue
    default=$(grep -E "^${var}=" .env.example | tail -n 1 || true)
    if [ -n "$default" ]; then
        echo "  $default" >&2
    else
        echo "  $var=" >&2
    fi
done
for var in "${empty[@]-}"; do
    [ -n "$var" ] || continue
    echo "  $var= (set, but empty)" >&2
done
echo >&2
echo "Add the lines above to .env, or start over with 'cp .env.example .env'." >&2
echo "Variables belonging to services this target does not run are still" >&2
echo "required: Compose interpolates every file before it filters by profile." >&2
exit 1
