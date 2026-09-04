# Archival catalogs

BOOM cross-matches incoming survey alerts with archival catalogs so the
consumers of these new alerts have some context for whether or not they've
been observed by others in the past.

The cross-matching parameters are defined in the app config in the `crossmatch`
section.
These are defined for each catalog and for each survey, so ZTF might
cross-match against NED differently from how LSST cross-matches against NED.

For a given instance, the desired catalogs are defined as a list in the
config as kebab-case slugs, e.g.:

```yaml
catalogs:
  - ned
  - desi-dr1
  - ls-dr10-photoz
  - gaia-dr3
  - milliquas-v8
  - 2mass
```

If a declared catalog does not exist, a warning will be shown on the admin
page, and there will be a button to kick off and monitor an ingestion job
within the [task system](./task-system.md).

## Available catalogs

TODO: List all of these, describe them, and define their slugs that can be
added to the BOOM config.
