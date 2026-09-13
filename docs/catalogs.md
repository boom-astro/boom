# Archival catalogs

BOOM cross-matches incoming survey alerts with archival catalogs so the
consumers of these new alerts have some context for whether or not they've
been observed by others in the past.

The cross-matching parameters are defined in the app config in the `crossmatch`
section.
These are defined for each catalog and for each survey, so ZTF might
cross-match against NED differently from how LSST cross-matches against NED.

Which catalogs an instance should hold is derived from the ones it actually
crossmatches against — the collection names under `crossmatch` — so there is one
place to declare a catalog rather than two lists to keep in step. The optional
`catalogs:` key adds any held for direct querying without being a crossmatch
target, as kebab-case slugs:

```yaml
catalogs: [gaia-dr3, 2mass] # BOOM_CATALOGS, comma-separated
```

If a declared catalog does not exist, a warning will be shown on the admin
page, and there will be a button to kick off and monitor an ingestion job
within the [task system](./task-system.md).

Every name under `crossmatch` must be either a catalog BOOM can build, a
watchlist, or listed in `WITHOUT_DEFINITIONS` in `src/catalogs/mod.rs` with the
reason it cannot be. Anything else fails config load, in the API at startup and
in `make check-configs`.

That strictness earns its keep because the failure it prevents is silent: the
pipeline reads these as collection names and queries them directly, so a
misspelled one does not error — it matches nothing, and every alert comes out
looking confidently unmatched.

A collection that is real but unbuildable still shows in the admin drift table
as an unknown slug, which is the honest answer: BOOM cannot tell whether it is
up to date.

## Ingesting a catalog

Ingestion runs as a task, kicked off from the admin page — there is deliberately
no binary to run by hand. Every catalog this release defines is listed there, so
ingesting one is a button click and needs no config change first. See
[The order of operations](#the-order-of-operations).

That is not just a nicer interface. A catalog ingest takes hours to days, so it
has to survive a deploy, report its logs while it runs, and be cancellable —
none of which a binary started over SSH can do. Running it as a task also
records who started it, with what parameters, under which release. See the
[task system](./task-system.md).

The ingest is **chunked**. A chunk is one independently fetchable,
independently ingestable piece of a catalog — usually one published file, or one
HEALPix partition. Each chunk is downloaded, ingested, deleted, and then
recorded as done in the `catalog_state` collection. Two things follow from that,
and both matter at this size:

- **Peak disk is one chunk, not one catalog.** AllWISE is hundreds of gigabytes
  in total and a few hundred megabytes per partition.
- **An interrupted run resumes.** A re-run skips the chunks already recorded and
  picks up at the first one that did not finish, so a deploy or a reboot costs
  one chunk rather than the whole ingest.
  Since every catalog derives its `_id` from a stable source identifier, a chunk
  that was interrupted mid-write re-ingests without producing duplicates.

The task takes a `drop_existing` parameter to start over instead of resuming,
and a `max_chunks` one that stops after N chunks, which is how to smoke-test a
new catalog end to end without ingesting all of it. The 2dsphere index is built
only once every chunk is in — an index maintained during the load roughly
doubles the time to ingest a large catalog — so a partially ingested catalog is
not yet queryable by position.

## Re-ingesting a catalog

Three different things get called "re-ingest", and they behave differently.

**Resuming an interrupted ingest.** Submit `catalog_ingest` again with the same
parameters. Chunks already recorded in `catalog_state` are skipped, and the run
reports how many (`92 chunks, 2 already done`). This is the case the chunking
exists for, and it needs nothing special.

**Refreshing a catalog whose upstream data changed** — a new Milliquas release,
corrected rows. **Re-running the task does not do this.** Two things stop it:
completed chunks are skipped outright, and within a chunk that does run, inserts
are `insert_many` with duplicate keys treated as already-written, so the
document already in the collection wins and the incoming one is discarded. A
plain re-run is "fill in what is missing", never "replace what is there".

To actually refresh, submit with **`drop_existing: true`**. That drops the
collection and its ingest state and starts from chunk zero.

Know what that costs before clicking it:

- **The catalog is empty for the duration of the re-ingest** — hours to days.
  Crossmatches against it during that window return zero matches on every alert,
  and nothing distinguishes that from a genuine non-match. Alerts ingested in
  that window are silently under-matched.
- The 2dsphere index is rebuilt at the end, so the catalog is not queryable by
  position until the whole run completes, even for the chunks already in.
- Existing alerts keep the crossmatches they already have. Refreshing catalog
  data does not mark them stale — see
  [What is not invalidated](#what-is-not-invalidated).

If the alert pipeline is live and the catalog is in crossmatch config, the safer
sequence is to ingest the new release under a **new collection name** (give the
`CatalogDef` a versioned `collection`, keeping the old one as an alias), then
switch crossmatch config over once it is complete. That is what the `aliases`
field on `CatalogDef` is for: `milliquas_v8` is the collection, `milliquas_v6`
and `milliquas_v7` are aliases, so a deployment that ingested an earlier release
still resolves to the same catalog definition.

## Available catalogs

| Slug | Collection | Format | Chunks | What it is |
| --- | --- | --- | --- | --- |
| `2mass` | `2MASS_PSC` | pipe-delimited text | ~92 files | Near-infrared JHKs photometry for 471 million point sources. |
| `ned-lvs` | `NED` | parquet, from FITS | 1 | Redshifts, distances, stellar masses and angular diameters for nearby galaxies. Always the current release. |
| `allwise` | `AllWISE` | parquet | ~1000 HEALPix partitions | Mid-infrared W1–W4 photometry and proper motions for 748 million sources, from the LSDB HATS mirror. |
| `milliquas` | `milliquas_v8` | parquet, from FITS | 1 | Quasars and candidates with redshifts and radio/X-ray associations. |
| `desi-dr1` | `DESI_DR1` | parquet, from FITS | 1 | Spectroscopic redshifts from the iron zcatalog, filtered to the primary spectrum per science target. |
| `catwise2020` | `CatWISE2020` | parquet, from IPAC tables | ~700 files | Mid-infrared W1/W2 photometry and proper motions. |
| `gaia-dr3` | `Gaia_DR3` | gzipped CSV | ~3400 files | Astrometry, parallaxes, proper motions and G/BP/RP photometry for 1.8 billion sources. |
| `galex` | `GALEX` | gzipped CSV | per release | Ultraviolet FUV/NUV photometry from the All-Sky Imaging Survey. |
| `vsx` | `VSX` | fixed-width text | 1 | Variability types, magnitudes, epochs and periods for known and suspected variable stars. |
| `panstarrs` | `PS1_DR1` | parquet | HEALPix partitions | Mean PSF magnitudes in grizy, from the HATS mirror of the otmo table. Requester-pays S3. |
| `ls-dr10-photoz` | `LS_DR10_PHOTOZ` | parquet, **staged** | one per hive partition | Tractor positions joined to photo-z on `lsid`. Built offline — see below. |

### Staged catalogs

`ls-dr10-photoz` is the one catalog BOOM ingests but does not fetch. Its table is
a LEFT join of the minified LS DR10 tractor sweeps (~101 GB) onto the photo-z
catalog (~34 GB) on `lsid`, done as a single out-of-core DuckDB hash join. That
cannot be chunked — the inputs are partitioned differently and the join key is
unique per source — so it runs offline, and BOOM reads the result.

Stage the built dataset (the directory holding the `ra_deg=NN/` subdirectories)
at `$BOOM_LS_DR10_PHOTOZ_DIR`, or at `<catalog data path>/ls-dr10-photoz`. Each
parquet file is a chunk, so the ingest is still resumable and still bounded.

A definition declares `source: Source::Staged`, and **BOOM never deletes a
staged file** — for a fetched catalog the chunk is a cache and deleting it is
what keeps peak disk at one chunk; for a staged one the files *are* the artifact,
and rebuilding one is hours of work over hundreds of gigabytes.

Two crossmatch targets have no definition, and are listed in
`WITHOUT_DEFINITIONS` in `src/catalogs/mod.rs`. Config load rejects any name
that is neither defined nor listed there:

- **`LSPSC`** — no downloader or record type exists in either repo. The
  collection is created empty by the test workflow and where its data comes from
  is not recorded.
- **`TNS`** — a live, credentialed feed rather than an archival download,
  populated outside the catalog ingest path.

## The order of operations

Adding a catalog to a deployment goes in this order, and the order matters:

1. **Add the definition to the code** — a boompy module and a `CatalogDef` in
   `CATALOGS`. See [Adding a catalog](#adding-a-catalog) below.
2. **Ingest it from the admin page.** Every catalog in `CATALOGS` is listed
   there as soon as the release ships it, whether or not anything crossmatches
   against it. No config change is needed to make it appear or to ingest it.
3. **Add it to `crossmatch` config** once the ingest reports complete.

Step 2 does not depend on step 3 on purpose. The catalogs table is built from
`CATALOGS` in the code, not from crossmatch config, so you never have to
configure the pipeline to use a catalog in order to be allowed to ingest it.

### What happens if you configure a catalog that is not there

This is worth being precise about, because the failure is quiet rather than
loud.

**The alert worker does not die.** A missing collection is not an error in
MongoDB: the `$geoWithin` match and the `$unionWith` legs both return zero rows,
the aggregation succeeds, and the worker carries on. The same is true of a
collection that exists but is empty, or one that is only partly ingested.

That is worse than dying, not better. The alert is written with an empty match
list for that catalog, which is exactly what a genuine non-match looks like.
Nothing on the document, then or later, says the catalog was not there.

Three things push back on it, none of which is a hard stop:

- **Config load fails** if crossmatch names a catalog this release has no
  definition for — a typo, most often. That is `validate_crossmatch`, and it is
  fatal, because a name that can never resolve is never going to start working.
- **The alert worker warns at startup** for each configured crossmatch
  collection that holds no documents, naming it and saying every alert will be
  written with zero matches for it. A warning rather than a failure: an ingest
  takes days, and refusing to process alerts until it finishes is a worse outage
  than degraded crossmatches.
- **The admin page** marks the catalog `missing` or `partial` with an `in use`
  badge, and counts it on the badge over the admin link. Only catalogs crossmatch
  config actually references are counted — one nobody queries is simply
  available.

`partial` deserves its own mention: the collection exists and is queryable, so
the crossmatch succeeds and returns whatever fraction of the catalog made it in.
Absent is at least uniformly absent; partial is wrong in a way that varies by
position on the sky.

## What is not invalidated

Crossmatches are computed **once**, when an alert is first ingested. Nothing
revisits them. So a change on the catalog side leaves existing alerts holding
matches computed against something that no longer exists — the same problem the
[enrichment set](./alert-processing.md#re-enriching-alerts-after-a-change)
solves for models and derivations, and it is *not* solved here.

**What is handled: adding a new catalog.** `xmatch` writes a key for every
configured catalog, including an empty array when nothing matched, so an alert
whose `cross_matches` has no key for a catalog provably predates that catalog
being configured. `reprocess_crossmatch` with `skip_existing: true` selects
exactly those (`cross_matches.<catalog>: { $exists: false }`), so filling in a
newly added catalog is already selective and already cheap.

**What is not handled:**

| Change | Why nothing catches it |
| --- | --- |
| Radius, `use_distance`, `max_results`, or the projection changes for a catalog already in use | The key exists, so `skip_existing` skips it. Nothing records which radius produced the stored matches. |
| The catalog is re-ingested with new data | Same: the key exists. Nothing records which version of the catalog contents the matches came from. |
| A catalog is removed from config and later re-added with different settings | The stale key from the first configuration survives. |

For those, the only correct move today is `reprocess_crossmatch` **without**
`skip_existing`, which recomputes every record for the selected catalogs. It is
correct and it is expensive — a full pass over `alerts_aux`.

### What fixing it would take

The instinct to hash the crossmatch config into the ledger is right, but a
single hash over the whole `crossmatch` block is too coarse: adding NED would
invalidate every Gaia match too, and the reprocessing run is exactly the thing
worth keeping small.

The shape that fits is the one the enrichment set already uses — intern per
component, diff to see what moved — applied per catalog rather than per survey:

- Version each catalog's crossmatch settings (`radius`, `use_distance`,
  `max_results`, `projection`) *together with* an identity for the catalog
  contents. The ingest already writes a `catalog_state` document per collection;
  a content marker on it, bumped by every completed ingest, is the missing half.
- Store that version next to the matches — `cross_matches_meta.<catalog>` as an
  integer beside `cross_matches.<catalog>` — so "stale for NED" is
  `cross_matches_meta.NED: { $ne: <current> }`: one indexed, exact query, the
  same shape as `enrichment_set`.
- `reprocess_crossmatch` then gains a `stale` selection alongside
  `skip_existing`, and the admin page can report crossmatch drift per catalog
  the way it reports enrichment drift per survey.

The cost is a small integer per catalog per **object** — `alerts_aux` is
per-object, not per-alert — which is a much cheaper place to put it than the
alert stream. None of this is built.

## Adding a catalog

A catalog is declared in two halves, and both are in this repo:

| Where | What it declares |
| --- | --- |
| `boompy/src/boompy/catalogs/<id>.py` | where the data lives and how to fetch a chunk of it |
| `src/catalogs/` — a record type plus a `CatalogDef` | what the columns mean and how they are stored |

The Python side is a module defining `ID`, `list_chunks()` and `fetch_chunk()`
(see [`boompy/README.md`](../boompy/README.md)). It is Python because the
archives are: LSDB reads HATS partitioning, `astroquery` speaks to the archives
directly, and reimplementing either in Rust to avoid a subprocess would be a bad
trade. Everything after the file lands on disk is Rust.

The Rust side needs a record type implementing the trait for its format —
`FromRecordBatch` for parquet, `FromAsciiRow` for delimited text, or serde's
`Deserialize` for CSV — plus `HasCoordinates`, and an entry in `CATALOGS` in
`src/catalogs/mod.rs`. Field names on the record type are load-bearing: the
`crossmatch` projections in `config.yaml` are written against them.

**BOOM reads two formats: delimited text and parquet.** Anything else is
converted to parquet by boompy, where the library that reads it already lives —
`astropy` for FITS, `lsdb` for HATS. That is why there is no FITS reader in the
Rust tree: adding one meant linking cfitsio into every BOOM binary to answer
"give me this column as f64". Column reads coerce across widths (f32/f64,
integer types, `Utf8`/`Utf8View`), so ingest does not depend on which tool wrote
the file, and a column the projection stopped emitting fails with its own name
rather than ingesting nulls.

Give the record a **deterministic `_id`** derived from a stable source
identifier. This is what stops a re-ingest producing a second copy of every
row, and it cannot be retrofitted — once a collection exists with generated ids
there is no way to match new source records to the documents already there.

Note what it is *not*: inserts are `insert_many` with duplicate-key errors
treated as "already written", so a record whose `_id` is already present is
**kept as it is** and the incoming version discarded. Deterministic ids make a
re-ingest safe to repeat; they do not make it a way to refresh data. See
[Re-ingesting a catalog](#re-ingesting-a-catalog).

