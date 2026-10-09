# boompy

**Internal package. Not a client library.**

If you are looking for a Python package to *use* BOOM's data from the outside,
you want [babamul](https://github.com/boom-astro/babamul) instead. `boompy` runs
inside the BOOM cluster, is invoked by the Rust services as a subprocess, and
has no stable public API — it changes with the BOOM release it ships in.

It exists for the work where Python's astronomy stack is simply better equipped
than Rust: talking to survey archives, LSDB/HATS partitioning, and the
occasional format that only `astropy` reads. Everything else stays in Rust.

## Catalog sourcing

`boompy.catalog` is how BOOM gets archival catalog files onto disk. Rust drives
the loop; this package only knows how to enumerate and fetch.

```sh
uv run --project boompy python -m boompy.catalog list-chunks 2mass
uv run --project boompy python -m boompy.catalog fetch-chunk 2mass --chunk psc_aaa --dest /data/catalogs/2mass
```

Both print one JSON object to **stdout**; everything human-readable goes to
**stderr**, where the Rust side forwards it into the log as it arrives.

A **chunk** is one independently fetchable, independently ingestable piece of a
catalog — usually one published file. It is the unit of resumability and of disk
pressure: BOOM fetches a chunk, ingests it, deletes it, and records it as done,
so peak disk is one chunk rather than one catalog, and an interrupted run
resumes rather than restarting. Chunk ids must be **stable across runs**, since
that is what a resumed run matches against its already-done list.

Adding a catalog means dropping a module that defines `ID`, `list_chunks()` and
`fetch_chunk()` into `boompy/src/boompy/catalog/catalogs/`, and adding a record
type and a `CatalogDef` on the Rust side in `src/catalogs/`. Nothing registers
it: `catalog/catalogs/` holds one module per catalog and nothing else, and
`registry` builds the index by listing it, so the file being there is the
registration. Shared machinery -- the HTTP helpers, the FITS conversion, the
chunk interface, the CLI -- lives one level up in `boompy.catalog`, which is
what keeps that listing meaningful.

A catalog is a module rather than a class -- there is only ever one of each and
they hold no state; `base.CatalogModule` is a `typing.Protocol` describing the
shape, so there is nothing to inherit from and a type checker still verifies it.

## Development

```sh
cd boompy
uv sync --extra dev
uv run pytest
```

The tests cover chunk enumeration and the fetch protocol against recorded
fixtures; none of them touch the network.
