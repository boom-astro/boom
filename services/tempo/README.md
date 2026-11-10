# TEMPO inference service

HTTP sidecar the API calls for `POST /babamul/surveys/ztf/objects/{id}/classify`
with `model: tempo_evidential`. It runs as the `tempo-inference` compose service.

## Model bundle

The bundle is committed in Git LFS under `data/models/tempo/` and copied into
the image at `/models/tempo`, so the compose build context is the repo root:

- `best_evidential.pt`: bare `EvidentialClassifier` state_dict, or `{"model": ...}`
- `config.json`: training hyperparameters, including `stats_file`
- the feature stats `.npz` that `stats_file` names

To ship a new model, replace those files and bump `TEMPO_HYRAX_REF` in the
Dockerfile if it was trained on newer code. If the bundle fails to load (for
example a checkout without `git lfs pull`), the service still starts, reports
unhealthy, and the API answers classify requests with 503.

## Environment

| Variable | Where | Default |
| --- | --- | --- |
| `TEMPO_BUNDLE_DIR` | this service | `/models/tempo` |
| `TEMPO_SERVICE_URL` | `api` service | `http://tempo-inference:8500` |
| `TEMPO_MODEL_ID` | this service | `tempo_evidential` |
| `TEMPO_LOG_LEVEL` | this service | `INFO` |

## Running outside compose

```sh
pip install --index-url https://download.pytorch.org/whl/cpu torch
pip install -r requirements.txt
pip install --no-deps https://github.com/frenbox/hyrax_tempo/archive/509931b05b3678fac7d712a9f78ed3440f08cfc5.tar.gz
TEMPO_BUNDLE_DIR=../../data/models/tempo uvicorn app:app --port 8500
```

Then point the API at it with `TEMPO_SERVICE_URL=http://localhost:8500`.
