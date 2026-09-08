# Working in this repository

Notes for anyone — human or agent — making changes here. This file covers the
things that are easy to get wrong because nothing fails loudly when you do.
For coding conventions (error handling, instrumentation, logging), see
[CONTRIBUTING.md](CONTRIBUTING.md).

## Production configs are generated — never edit them directly

`config/prod/*/config.yaml` are **build artifacts**. Each one is the base
`config.yaml` deep-merged with that deployment's `overrides.yaml`, and each
carries an `AUTO-GENERATED FILE. DO NOT EDIT DIRECTLY.` banner on line 1.

```text
config.yaml                          # base: every setting, with defaults
config/prod/caltech/overrides.yaml   # the ONLY file you hand-edit per deployment
config/prod/caltech/config.yaml      # generated; committed so PRs are reviewable
```

To change a deployment's configuration:

1. Add the setting (with its default) to the base `config.yaml`.
2. Put the deployment-specific value in that deployment's `overrides.yaml`.
3. Run `make configs` to regenerate, and commit the regenerated file too.
4. `make check-configs` regenerates *and* parses each result through
   `check_config`; use it before pushing.

Hand-editing a generated `config.yaml` looks like it works — the file parses,
the app starts, review passes — and then it silently reverts. The `Sync
configs` workflow (`.github/workflows/sync-configs.yaml`) runs
`make check-configs` on every pull request and **commits the regenerated files
back to your branch**, discarding anything that didn't come from the base
config or an `overrides.yaml`. On fork PRs it fails the build instead. Either
way the hand-edit does not survive, and the setting it was carrying quietly
reverts to the base default.

Hand-editing also tends to produce a file the generator never would: adding a
key to the deployment config but not to the base loses the sibling keys the
merge would have carried down.

The merge is `yq eval-all '. as $item ireduce ({}; . * $item)'`, a deep merge
that preserves comments. Comments in the base config propagate into every
generated config, and a comment in `overrides.yaml` *replaces* the base's for
that key — so leave a key's comment out of the override unless you mean to
shadow it.

## Secrets belong in the environment, not in YAML

Every `*.yaml` in this repo is committed, including the generated production
configs. Credentials therefore come from environment variables only, and the
YAML holds just the non-secret settings. Config is loaded from `config.yaml`
and then overlaid with `BOOM_`-prefixed env vars (`__` separates nesting
levels), so any field can be supplied that way:

```sh
BOOM_BABAMUL__OAUTH__GOOGLE__CLIENT_SECRET="…"   # babamul.oauth.google.client_secret
```

Prefer deriving "is this feature on?" from whether its credential is present,
rather than adding a separate `enabled:` flag in YAML. A flag that lives apart
from the secret can be switched on without one, which fails open at the worst
moment; presence-based enablement fails closed. `posthog.project_api_key` and
`babamul.oauth.*` both work this way.

Document every new variable in `.env.example`, and add it to the relevant
service in `docker-compose.yaml` — the compose files list env vars explicitly,
so an undeclared one silently never reaches the container.

An empty variable counts as *unset*, not as "set to empty": `load_raw_config`
passes `ignore_empty(true)` to the env source. This matters because a compose
line like `BOOM_BABAMUL__WEBAPP_URL: ${BOOM_BABAMUL__WEBAPP_URL:-}` renders as
`BOOM_BABAMUL__WEBAPP_URL=` for every deployment that doesn't set it — without
`ignore_empty` that blank would win over the value in
`config/prod/<deployment>/config.yaml`, and the setting would vanish with the
file, the container environment, and the deploy workflow all looking correct.
The flip side: a value cannot be *cleared* from the environment. To turn
something off, give it an empty value in `config.yaml` (which is what
`posthog.project_api_key` and the `babamul.oauth.*` credentials do) rather than
expecting `FOO=` to blank it.

## Tests need real services

The suite talks to actual infrastructure rather than mocks, so results depend
on what is running:

- **MongoDB** is required by most of `cargo test` (library tests under
  `src/alert`, `src/filter`, and the whole `tests/api` suite). Without it you
  will see a wall of failures that have nothing to do with your change.
- **Kafka** is additionally required by the Kafka-credential tests, which shell
  out to `kafka-configs`/`kafka-acls`.
- `tests/config.test.yaml` is the config used by tests; it points at
  `localhost:27017` with `mongoadmin` / `mongoadminsecret`.

`make dev` brings up the full dev stack. For a quick MongoDB-only run:

```sh
docker run -d --name boom-test-mongo -p 27017:27017 \
  -e MONGO_INITDB_ROOT_USERNAME=mongoadmin \
  -e MONGO_INITDB_ROOT_PASSWORD=mongoadminsecret mongo:8.2
```

Before claiming a pre-existing failure is your fault (or that it isn't), check
it against a clean tree — `git stash`, run the one test, `git stash pop`. A few
integration tests also flake under parallel access to a shared database; rerun
in isolation before chasing one.

## Prose conventions

These apply to comments, doc comments, log lines, and user-facing strings —
anywhere the repo writes English rather than code.

- **US spelling.** `normalized`, not `normalised`; `honored`, `canceled`,
  `catalog`, `center`, `labeled`. The `-ize`/`-or` forms throughout.
- **One space after a period.** Not two.
- **"Client", not "web app"**, for whatever is calling the API. The React app
  is one client; API tokens and the Kafka consumers are others, and a comment
  that says "web app" quietly excludes them. `babamul.webapp_url` keeps its
  name because it is a published config key and environment variable — the
  convention is about prose, not about renaming deployed settings.

Nothing enforces these, so they are worth a glance in review.

## Checks to run

```sh
cargo fmt --all                 # or `make format` for pre-commit on everything
cargo clippy --lib --bins --tests
make check-configs              # if you touched any config
cd frontend && npx tsc --noEmit -p tsconfig.app.json && npm run build
```

Clippy is noisy with pre-existing warnings across the tree; filter to the files
you touched (`--message-format=short | grep <your file>`) rather than trying to
read the whole output.

## API surface

New Babamul or BOOM API routes need to be added in three places, and missing
any one of them fails quietly:

1. `src/api/routes/**` — the handler, with its `#[utoipa::path(...)]` attribute.
2. `src/bin/api.rs` — `.service(...)` registration, inside the right scope.
3. `src/api/docs.rs` — the `paths(...)` list, or it won't appear in `/docs`.

Routes that must work without a token also need to be allowed in
`src/api/auth.rs`: `PUBLIC_ROUTES` / `BABAMUL_PUBLIC_ROUTES` match exact paths,
so anything with a path parameter needs a prefix check instead.
