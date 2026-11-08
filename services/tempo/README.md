# TEMPO inference service

HTTP sidecar the API calls for `POST /babamul/surveys/ztf/objects/{id}/classify`
with `model: tempo_evidential`. It runs as the `tempo-inference` compose service.

## Model bundle

The image holds no weights. Mount a bundle directory containing:

- `best_evidential.pt`: bare `EvidentialClassifier` state_dict, or `{"model": ...}`
- `config.json`: training hyperparameters, including `stats_file`
- the feature stats `.npz` that `stats_file` names

Compose mounts `${TEMPO_BUNDLE_HOST_DIR:-./data/tempo}` read-only at
`/models/tempo`. Without a bundle the service still starts, reports unhealthy, and
the API answers classify requests with 503.

## Environment

| Variable | Where | Default |
| --- | --- | --- |
| `TEMPO_BUNDLE_HOST_DIR` | compose host | `./data/tempo` |
| `TEMPO_SERVICE_URL` | `api` service | `http://tempo-inference:8500` |
| `TEMPO_MODEL_ID` | this service | `tempo_evidential` |
| `TEMPO_LOG_LEVEL` | this service | `INFO` |

## Running outside compose

```sh
pip install --index-url https://download.pytorch.org/whl/cpu torch
pip install -r requirements.txt
pip install --no-deps https://github.com/frenbox/hyrax_tempo/archive/509931b05b3678fac7d712a9f78ed3440f08cfc5.tar.gz
TEMPO_BUNDLE_DIR=/path/to/bundle uvicorn app:app --port 8500
```

Then point the API at it with `TEMPO_SERVICE_URL=http://localhost:8500`.
