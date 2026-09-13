# Adding a model to the enrichment worker

This page lists what needs to change in BOOM to run a new classifier on ZTF alerts. It uses FLARE as the example, which reads the light curve and the crossmatches and writes a structured result. Models that only need the cutouts should implement the `Model` trait in `src/enrichment/models/` instead, like ACAI and BTSbot.

## Where the model runs

The enrichment worker (`src/enrichment/ztf.rs`) reads alerts from the database in batches, computes properties and ML scores, and writes them back with a `$set` update. The alert is fetched with the aggregation pipeline in `create_ztf_alert_pipeline`, which joins the aux document. Add any field you need to the projection there (FLARE added `cross_matches`).

What is available per alert:

- `candidate`: the current detection
- `prv_candidates`: previous alert detections
- `fp_hists`: forced photometry
- `cross_matches`: catalog name to list of matches, each with `distance_arcsec`
- properties BOOM already computed, e.g. the peak magnitude

Which catalogs exist depends on the deployment. The base `config.yaml` has Gaia DR3, milliquas, NED, DESI DR1 and the Legacy Surveys photo-z. Each site adds its own in `config/prod/<site>/overrides.yaml`. Treat a missing catalog as NaN rather than failing.

## Files to change

- `Cargo.toml`: the model crate as an optional dependency and a feature that enables it, so the default build does not change.
- `src/enrichment/<model>.rs`: load the ONNX files with `load_model`, build the inputs from the alert, run inference, return a serializable struct.
- `src/enrichment/mod.rs`: `#[cfg(feature = "...")] pub mod <model>;`
- `src/enrichment/base.rs`: a variant in `EnrichmentWorkerError` for the model's error type.
- `src/enrichment/ztf.rs`: keep the loaded model on the worker, call it per alert, `$set` the result.
- `src/conf.rs` and `config.yaml`: a config section with `enabled: false` by default. Document the env variable override (`BOOM_<SECTION>__ENABLED`).
- `data/models/`: the ONNX files, tracked with Git LFS (see `.gitattributes`).
- `frontend/src/components/ClassificationsV2.tsx`: `mapAlertClassifications` only shows fields it knows about. Extend `AlertLike`, add a tile family and append it to `FAMILY_ORDER`.

If the result is more than one number per alert, write it as its own top-level field (FLARE uses `flare`, villar-pso uses `villar_fit`). `ZtfAlertClassifications` is a flat struct of `f32` with an Avro schema used by Babamul, so do not add structured data there.

## ONNX files

Put everything the model needs into the ONNX metadata (`metadata_props`): the input column order, and any calibration the model ships with. `ort` reads them with `session.metadata()?.custom("key")`. This keeps the model to a few files under `data/models/` with nothing else to deploy.

For tree models, check that the ONNX output matches the original framework before relying on it. `onnxmltools` stores LightGBM thresholds as float32 and applies a softmax, which changes the raw scores. FLARE exports its own `TreeEnsembleRegressor` with double thresholds and no post transform, and does the softmax and calibration in Rust.

## Errors and skipped alerts

Model errors on one alert should log a warning and move on, not stop the batch. If the model has a quality cut (FLARE needs at least 8 detections), return `None` for alerts below it and log at trace level, since most young alerts will not pass.

## Build and deploy

- CI runs `cargo test --release` without features. Add a `cargo check --features <yours>` step to `.github/workflows/test.yaml` so the code keeps compiling.
- `Dockerfile` and `Dockerfile.gpu` run `cargo build --release` without features. Add the feature and any system packages the crate needs (FLARE's `ceres` needs `cmake`).
- Run `make configs` after changing `config.yaml` or the overrides and commit the generated files.

## Testing

- `cargo test --features <yours> --lib enrichment::<model>` for unit tests that do not need a database. Test the input mapping on hand-written documents.
- Mark tests that load the ONNX files as `#[ignore]` when they need a runtime that CI does not have, and say so in the ignore message.
- Compare the Rust output against the original implementation on a sample of objects before opening the PR, and give the numbers in the PR description.
