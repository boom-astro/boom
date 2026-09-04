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

A crossmatch target with no ingest definition — `TNS`, a hand-imported
collection, anything built by `prepare_catalog` — is reported as an unknown slug
rather than treated as an error. That is the honest answer: BOOM has no way to
tell whether it is up to date. Watchlists are excluded entirely, being
user-managed rather than archival.

## Ingesting a catalog

Ingestion runs as a task, kicked off from the admin page — there is deliberately
no binary to run by hand. Declare the catalog in `catalogs:`, then use the
button on the admin page next to the catalog reported as missing.

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
  that was interrupted mid-write re-ingests as an upsert rather than as
  duplicates.

The task takes a `drop_existing` parameter to start over instead of resuming,
and a `max_chunks` one that stops after N chunks, which is how to smoke-test a
new catalog end to end without ingesting all of it. The 2dsphere index is built
only once every chunk is in — an index maintained during the load roughly
doubles the time to ingest a large catalog — so a partially ingested catalog is
not yet queryable by position.

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

Two crossmatch targets still have no definition, and are listed as exceptions in
`WITHOUT_DEFINITIONS` in `src/catalogs/mod.rs`:

- **`LS_DR10_PHOTOZ`** — the stored table is a join of the LS DR10 tractor
  sweeps with the photo-z catalog, produced by the pandas minifiers in
  [boom-catalogs](https://github.com/boom-astro/boom-catalogs). Porting it means
  porting that pipeline, not just a download.
- **`LSPSC`** — no downloader or record type exists in either repo. The
  collection is created empty by the test workflow and where its data comes from
  is not recorded.

`VSX` also remains unported: its `vsx.dat` needs the exact fixed-column parser
from boom-catalogs, which was not carried over.

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
identifier. This is what makes re-ingest an upsert rather than a duplicate
factory, and it cannot be retrofitted — once a collection exists with generated
ids there is no way to match new source records to the documents already there.

