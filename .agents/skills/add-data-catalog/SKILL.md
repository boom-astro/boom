---
name: add-data-catalog
description: Use when adding an archival catalog to BOOM — a new crossmatch target, or one held for direct querying — or when changing how an existing catalog is ingested, or refreshing one whose upstream data changed. Covers the boompy downloader, the Rust record type and CatalogDef, ingesting it, and only then adding it to crossmatch config. Also use when a catalog exists in the database but BOOM has no definition for it.
---

# Adding a data catalog

A catalog is declared in two halves, both in this repo:

| Where | Declares |
| --- | --- |
| `boompy/src/boompy/catalogs/<id>.py` | where the data lives and how to fetch one chunk of it |
| `src/catalogs/` — a record type plus a `CatalogDef` | what the columns mean and how they are stored |

Reference, with the reasoning: [`docs/catalogs.md`](../../../docs/catalogs.md).

## Order of operations

1. **Add the definition to the code** (both halves below).
2. **Ingest it** from the admin page. Every catalog in `CATALOGS` is listed
   there as soon as the release ships it; no config change makes it appear.
3. **Then** add it to `crossmatch.<survey>` in `config.yaml`.

Not the other way round. A crossmatch entry naming a collection that is missing
or empty does not error: `$geoWithin` returns nothing, the alert is written with
an empty match list, and that is indistinguishable from a genuine non-match. The
alert workers warn at startup about configured-but-empty catalogs, and the admin
page flags them, but no alert already written says it was under-matched.

## 1. The Python half

A module defining `ID`, `list_chunks()` and `fetch_chunk()`, registered in
`boompy/src/boompy/catalogs/__init__.py`. `base.CatalogModule` is a
`typing.Protocol`, so there is nothing to inherit.

- A **chunk** is one independently fetchable, independently ingestable piece —
  usually one published file or partition. It is the unit of resumability and of
  disk pressure: fetch, ingest, delete, record.
- **Chunk ids must be stable across runs.** A resumed run matches them against
  its already-done list; unstable ids re-ingest everything.
- **stdout is JSON, stderr is for humans.** The Rust side parses the first and
  forwards the second into the run's log live.
- BOOM reads **delimited text, JSONL and parquet only**. Convert anything else
  here — `astropy` for FITS (`_fits.fits_to_parquet`), `lsdb` for HATS — rather
  than teaching Rust a fourth format.

## 2. The Rust half

A record type implementing the trait for its format, plus `HasCoordinates`:

| Format | Trait |
| --- | --- |
| parquet | `FromRecordBatch` (`src/catalogs/arrow.rs`) |
| delimited or fixed-width text | `FromAsciiRow` (`src/catalogs/ascii.rs`) |
| CSV or JSONL | serde's `Deserialize` |

Then an entry in `CATALOGS` in `src/catalogs/mod.rs`: `id` (kebab-case slug),
`collection` (the MongoDB name crossmatch config uses), `title`, `description`,
`reader`, `source`, `aliases`.

- **`source: Source::Fetched`** deletes each chunk after ingesting it, which is
  what keeps peak disk at one chunk. **`Source::Staged`** never deletes: those
  files are the artifact, not a cache of it.
- **Give the record a deterministic `_id`** derived from a stable source
  identifier. It cannot be retrofitted: once a collection exists with generated
  ids, nothing can match new source records to the documents already there.
- **Field names on the record type are load-bearing.** The `crossmatch`
  projections in `config.yaml` are written against them.
- **`aliases`** recognize a collection ingested under an older name (a release
  stamped into it, say). A new ingest always writes to `collection`.

## 3. Verify the column names against the published table

Read the real schema — the published file, its documentation, a sample row.
Do not infer column names from the catalog's papers or from what would be
sensible.

Milliquas was ingested with `RXPCT`/`QPCT`, names that do not exist; the real
ones are `R` and `B`. It failed at ingest time, against a 42 MB download. The
fix was a test building a synthetic FITS table with *every* published column, so
the projection is proven to select rather than to accept whatever it is handed
(`boompy/tests/test_milliquas.py`). Write that test for a new catalog.

## 4. Ingest it

From the admin page: the catalog appears as `missing`, with an Ingest button. Or
`POST /tasks` with `catalog_ingest`. Useful parameters:

- `max_chunks: 1` — smoke-test the whole path end to end without ingesting
  hundreds of gigabytes.
- `num_workers` / `batch_size` — turn down when an ingest is competing with the
  live alert pipeline for write throughput.
- `drop_existing: true` — start over rather than resume. See the refresh note
  below before reaching for it.

Watch it from the admin page: progress, streamed logs, and cancel, which stops
at a chunk boundary and keeps the chunks already in.

## 5. Then wire up crossmatching

Add the entry to `crossmatch.<survey>` in the base `config.yaml`, put any
deployment-specific value in that deployment's `overrides.yaml`, and run
`make configs`. Config load **fails** if a crossmatch entry names a catalog this
release has no definition for, so a typo is caught at startup rather than
silently matching nothing.

Existing alerts are not re-crossmatched. `reprocess_crossmatch` with
`skip_existing: true` fills in only the records that have no key for the new
catalog, which is cheap and exact.

## Catalogs with no upstream bulk copy

Some catalogs are published only as a query service, so nobody can fetch them in
bulk — but BOOM already holds the rows. Export them with the `export_catalog`
task (gzipped JSONL plus a manifest), stage that as a `Source::Staged` catalog,
then publish the chunks somewhere durable and switch the `CatalogDef` to
`Source::Fetched`. `LSPSC` is the worked example; see **When BOOM is the
provenance** in [`docs/catalogs.md`](../../../docs/catalogs.md).

## Catalogs BOOM does not build

Some collections are produced outside BOOM (`LSPSC`, `TNS`, `LSDR10`). List them
in `WITHOUT_DEFINITIONS` in `src/catalogs/mod.rs` **with the reason and, if you
know it, the source**, so the next person is not left guessing. Config
validation accepts those names; the admin page reports them as having no
definition.

## Refreshing a catalog whose upstream data changed

Re-running the ingest does **not** refresh it. Completed chunks are skipped, and
within a chunk, inserts are `insert_many` with duplicate keys treated as already
written — the existing document wins. `drop_existing: true` does refresh, but
empties the collection for the hours or days the run takes, and crossmatches
against it return nothing meanwhile. On a live deployment, prefer ingesting the
new release under a versioned `collection` name, keeping the old one as an
`alias`, and switching crossmatch config when it completes.

## Checks before you push

```sh
cargo test --lib catalogs::
cd boompy && uv run pytest          # the downloader and its fixtures
make check-configs                  # if you touched any config
```

Never commit catalog data. `.gitignore` excludes bulk formats by extension, and
a 42 MB archive reached a commit once because `.zip` was not yet on that list —
stage explicit paths rather than `git add -A` while an ingest is running.
