#!/usr/bin/env bash
# Report .env variables that Compose requires but cannot resolve.
#
# Compose interpolates every service in every file it loads *before* it filters
# by profile, so `make dev` needs the required variables of prod-only services
# too. Left to Compose, a stale .env fails one variable at a time with
# an error naming a service that dev never runs (e.g. consumer-ztf-caltech),
# which reads like a broken compose file rather than an out-of-date .env.
#
# Usage: check_env.sh <compose-file>...
set -euo pipefail

cd "$(dirname "$0")/.."

# Without this, grep below falls back to reading stdin and the script sits
# there waiting on the terminal instead of reporting anything.
if [ $# -eq 0 ]; then
    echo "usage: $(basename "$0") <compose-file>..." >&2
    echo "  e.g. $(basename "$0") docker-compose.yaml docker-compose.override.yaml" >&2
    echo "  or 'make check-env', which passes the files each dev target loads." >&2
    exit 2
fi

for file in "$@"; do
    if [ ! -f "$file" ]; then
        echo "error: no such compose file: $file" >&2
        exit 2
    fi
done

if [ ! -f .env ]; then
    echo "error: no .env file" >&2
    echo "  cp .env.example .env" >&2
    exit 1
fi

# Compose has two required-variable forms and they differ on empty values:
# ${VAR:?message} rejects empty as well as unset, ${VAR?message} only unset.
# The patterns are disjoint -- ':' is not in the name character class, so the
# second cannot match the first's ':?'.
#
# grep exits 1 when a form is absent from the files given, which under
# `set -e -o pipefail` would abort the whole check silently; `|| true` keeps a
# file that happens to use only one form from looking like a failure.
required_nonempty=$(
    { grep -ohE '\$\{[A-Za-z_][A-Za-z0-9_]*:\?' "$@" || true; } |
        sed 's/^\${//; s/:?$//' |
        sort -u
)
required_set=$(
    { grep -ohE '\$\{[A-Za-z_][A-Za-z0-9_]*\?' "$@" || true; } |
        sed 's/^\${//; s/?$//' |
        sort -u
)

# Value of VAR in .env, or the empty string if it has no entry there. Inline
# comments are stripped the way Compose strips them, so `VAR= # todo` counts as
# empty rather than as the text of the comment.
env_value() {
    local line
    line=$(grep -E "^[[:space:]]*${1}=" .env | tail -n 1 || true)
    [ -n "$line" ] || return 0
    echo "${line#*=}" | sed 's/[[:space:]]*#.*$//; s/[[:space:]]*$//'
}

has_entry() {
    grep -qE "^[[:space:]]*${1}=" .env
}

missing=()
empty=()
for var in $required_nonempty; do
    # A real environment variable wins over .env, so honour it here too.
    if [ -n "${!var-}" ]; then
        continue
    fi
    if ! has_entry "$var"; then
        missing+=("$var")
    elif [ -z "$(env_value "$var")" ]; then
        empty+=("$var")
    fi
done

for var in $required_set; do
    # Declared-but-empty satisfies this form, so only absence is a problem --
    # and an exported empty variable counts as declared.
    if [ -n "${!var+set}" ] || has_entry "$var"; then
        continue
    fi
    missing+=("$var")
done

if [ ${#missing[@]} -eq 0 ] && [ ${#empty[@]} -eq 0 ]; then
    exit 0
fi

echo "error: .env is missing values Docker Compose requires." >&2
echo >&2
# A variable required in both forms lands in `missing` twice; report it once.
for var in $(printf '%s\n' "${missing[@]-}" | sort -u); do
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
