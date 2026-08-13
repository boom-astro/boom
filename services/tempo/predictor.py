"""Load a trained TEMPO evidential checkpoint and classify raw ZTF photometry.

This mirrors the feature pipeline in `tempo_hyrax.predict.TempoPredictor`
(horizon truncation -> max_len cap -> stat normalization -> evidential head), but
deliberately does NOT use that class, for two reasons:

1. `TempoPredictor` builds a `tempo_hyrax.model.TempoEvidential`, which holds the
   classifier at `self.model` and so expects every state_dict key to be prefixed
   `model.`. The deployed bundles are bare `EvidentialClassifier` state_dicts
   (`encoder.*`, `global_mlp.*`, `head.*`), so loading one through
   `TempoPredictor` fails on all-missing/all-unexpected keys.
2. `TempoEvidential` is decorated with `@hyrax_model`, so importing it drags in
   the whole hyrax training stack (pytorch-ignite, tensorboard, mlflow,
   chromadb, ...) and a ~2 minute import, none of which inference needs.

Everything the pipeline actually needs -- `tempo_model`, `raw_photometry`,
`uncertainty`, `taxonomy` -- imports only torch/numpy/pandas, so this service
installs `tempo-hyrax` with `--no-deps` and reuses those modules verbatim rather
than re-deriving the preprocessing. Keep this file in step with `predict.py`.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
import torch
from torch.nn.utils.rnn import pad_sequence

from tempo_hyrax.raw_photometry import raw_photometry_to_event_array
from tempo_hyrax.taxonomy import get_taxonomy
from tempo_hyrax.tempo_model import (
    FeatureStats,
    _global_features_from_sequence,
    build_event_tensor,
    build_tempo_model,
)
from tempo_hyrax.uncertainty import (
    alpha_from_evidence,
    dirichlet_mean,
    predictive_entropy,
    vacuity,
)

log = logging.getLogger(__name__)

# Hyperparameters that must match the trained checkpoint, with the same defaults
# `tempo_hyrax.model.TempoEvidential` applies when a config omits them.
_MODEL_DEFAULTS = {
    "d_model": 128,
    "n_heads": 8,
    "n_layers": 4,
    "dropout": 0.4,
    "pool": "attn",
    "band_mode": "embed",
    "band_embed_dim": 8,
    "time_encoding": "dt_both",
    "use_global_features": True,
    "global_feature_set": "basic",
    "global_hidden_dim": 64,
    "taxonomy_preset": "scheme_a_5c",
    "horizon_days": 100.0,
    "max_len": 257,
}


class TempoBundle:
    """A model bundle directory: `best_evidential.pt` + `config.json` + stats npz."""

    def __init__(self, bundle_dir: str | Path, device: str | None = None):
        self.bundle_dir = Path(bundle_dir)
        config_path = self.bundle_dir / "config.json"
        if not config_path.is_file():
            raise FileNotFoundError(f"bundle is missing config.json: {config_path}")
        raw_config = json.loads(config_path.read_text())
        cfg = {**_MODEL_DEFAULTS, **raw_config}

        self.band_mode = str(cfg["band_mode"])
        self.global_feature_set = str(cfg["global_feature_set"])
        self.horizon_days = float(cfg["horizon_days"])
        self.max_len = int(cfg["max_len"])

        # `stats_file` in a bundle config is usually a bare filename relative to
        # the bundle, but absolute paths from the training machine also appear.
        stats_file = Path(str(cfg["stats_file"]))
        if not stats_file.is_file():
            stats_file = self.bundle_dir / stats_file.name
        if not stats_file.is_file():
            raise FileNotFoundError(f"bundle is missing its feature stats file: {stats_file}")
        self.stats = FeatureStats.load(stats_file)
        self._mean = self.stats.mean.view(1, 1, 4)
        self._std = self.stats.std.view(1, 1, 4)

        taxonomy = get_taxonomy(str(cfg["taxonomy_preset"]))
        self.class_names = list(taxonomy.broad_classes)
        self.taxonomy_preset = str(cfg["taxonomy_preset"])

        self.device = torch.device(device or ("cuda" if torch.cuda.is_available() else "cpu"))
        # build_tempo_model duck-types its config, so a namespace of the resolved
        # hyperparameters is enough and keeps model construction in the upstream repo.
        self.model = build_tempo_model(SimpleNamespace(**cfg), taxonomy, self.device)

        checkpoint_path = self.bundle_dir / "best_evidential.pt"
        if not checkpoint_path.is_file():
            raise FileNotFoundError(f"bundle is missing best_evidential.pt: {checkpoint_path}")
        state = _load_state_dict(checkpoint_path)
        # Strict: a silently partial load produces confident nonsense rather than
        # an error, which is far worse than refusing to start.
        self.model.load_state_dict(state)
        self.model.eval()
        log.info(
            "loaded TEMPO bundle %s (%s, %d classes, device=%s)",
            self.bundle_dir,
            self.taxonomy_preset,
            len(self.class_names),
            self.device,
        )

    # ---- feature pipeline (mirrors predict.py's _event_tensor / _collate) ----
    def _event_tensor(self, raw):
        if self.horizon_days is not None and raw.shape[0] > 0:
            import numpy as np

            end = int(np.searchsorted(raw[:, 0], float(self.horizon_days), side="right"))
            raw = raw[: max(1, end)]
        if raw.shape[0] > self.max_len - 1:
            raw = raw[-(self.max_len - 1) :]
        return build_event_tensor(raw, band_mode=self.band_mode)

    def _prepare_one(self, photometry: pd.DataFrame, object_id: str) -> dict | None:
        raw = raw_photometry_to_event_array(photometry, obj_id=object_id)
        if raw.shape[0] == 0:
            return None
        events = self._event_tensor(raw)
        globs = _global_features_from_sequence(
            events, band_mode=self.band_mode, feature_set=self.global_feature_set
        )
        return {
            "object_id": object_id,
            "events": events,
            "globals": globs,
            "n_events_used": int(events.size(0)),
        }

    def _collate(self, prepared: list[dict]):
        seqs = [p["events"] for p in prepared]
        lens = torch.tensor([s.size(0) for s in seqs], dtype=torch.long)
        padded = pad_sequence(seqs, batch_first=True)
        batch, length, _ = padded.shape
        pad_mask = torch.arange(length).unsqueeze(0).expand(batch, length) >= lens.unsqueeze(1)

        cont = (padded[..., :4].float() - self._mean) / (self._std + 1e-8)
        if self.band_mode == "embed":
            events = torch.cat([cont, padded[..., 4:5].float()], dim=-1)
        else:
            events = torch.cat([cont, padded[..., 4:].float()], dim=-1)
        globs = torch.stack([p["globals"] for p in prepared], dim=0)
        return events.to(self.device), pad_mask.to(self.device), globs.to(self.device)

    @torch.no_grad()
    def classify(self, photometry: pd.DataFrame, object_id: str) -> dict | None:
        """Classify one object. Returns None when its photometry yields no events."""
        prepared = self._prepare_one(photometry, object_id)
        if prepared is None:
            return None

        events, pad_mask, globs = self._collate([prepared])
        evidence = self.model(events, pad_mask, globs)
        alpha = alpha_from_evidence(evidence)
        probs = dirichlet_mean(alpha).cpu()
        vac = vacuity(alpha).cpu()
        entropy = predictive_entropy(probs)

        row = probs[0]
        best = int(row.argmax())
        return {
            "object_id": object_id,
            "classes": {name: float(row[i]) for i, name in enumerate(self.class_names)},
            "pred_class": self.class_names[best],
            "pred_confidence": float(row[best]),
            "vacuity": float(vac[0]),
            "predictive_entropy": float(entropy[0]),
            "n_events_used": prepared["n_events_used"],
        }


def _load_state_dict(path: Path):
    """Read a checkpoint, tolerating both bare state_dicts and {"model": ...} wrappers."""
    try:
        checkpoint = torch.load(path, map_location="cpu", weights_only=True)
    except Exception:
        # Older bundles pickle more than tensors; they are operator-supplied files,
        # not user input, so falling back to a full unpickle is acceptable here.
        log.warning("%s could not be loaded with weights_only=True; retrying", path)
        checkpoint = torch.load(path, map_location="cpu", weights_only=False)
    if isinstance(checkpoint, dict) and "model" in checkpoint:
        return checkpoint["model"]
    return checkpoint
