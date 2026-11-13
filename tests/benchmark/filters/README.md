# Benchmark filters

`run.py --filters DIR` loads every `*.json` in `DIR` and keeps the filters for
the surveys being replayed. A file holds one filter or a list, in either form:

- **Short form**, as in `ztf_cats150.json`:
  `{"survey": "ZTF", "name": "...", "pipeline": [ ...aggregation stages... ]}`.
- **Exported** from a deployment's `filters` collection, unchanged. Every
  exported filter is marked active for the run.

The only filter tracked here is the CATS150 pipeline the old throughput test
used. One filter tells you little about the filter stage. A realistic run uses
the filters people actually run, exported from a deployment:

```sh
mongoexport --uri "$PROD_URI" --db boom --collection filters \
  --query '{"active": true}' --jsonArray --out ~/boom-bench/filters-prod/filters.json
```

Keep those files out of the repository. They belong to their owners, and some
of them encode unpublished science cases.

`--n-filters N` takes the first N filters by name. If N is larger than the
number available, the extra filters are copies. Copies share query plans and
cached pages, so they cost less than the same number of distinct filters. The
run records a warning when it uses copies.
