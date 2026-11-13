"""When each packet is sent during a replay.

The replay, the fixture slicer and the analysis all call `plan_replay` with the
same arguments, so they agree on which packets a run sends and therefore on the
moment the database snapshot must represent.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from .bundle import Bundle

CLOCKS = ("exposure", "kafka", "none")
ALIGNMENTS = ("start", "absolute")


def _absolute_seconds(index: pd.DataFrame, clock: str) -> pd.Series:
    if clock == "exposure":
        return index["t_obs_jd"].astype(float) * 86400.0
    if clock == "kafka":
        if index["t_kafka_ms"].isna().any():
            raise ValueError("clock=kafka needs t_kafka_ms on every row; captured bundles have it, archive bundles do not")
        return index["t_kafka_ms"].astype(float) / 1000.0
    return pd.Series(0.0, index=index.index)


def _window_counts(seconds: np.ndarray, width: float) -> tuple[np.ndarray, np.ndarray]:
    """Sorted times, and how many packets fall in the `width` seconds starting at each."""
    t = np.sort(np.asarray(seconds, dtype=float))
    return t, np.searchsorted(t, t + width, side="left") - np.arange(t.size)


def busiest_window_start(seconds: np.ndarray, width: float) -> float:
    """Start of the `width`-second window holding the most packets."""
    t, counts = _window_counts(seconds, width)
    return float(t[int(np.argmax(counts))]) if t.size else 0.0


def peak_rate(seconds: np.ndarray, width: float = 60.0) -> float:
    """Packets per second in the busiest `width` seconds, wherever they start."""
    t, counts = _window_counts(seconds, width)
    return float(counts.max()) / width if t.size else 0.0


def plan_replay(
    bundles: list[Bundle],
    clock: str = "exposure",
    align: str = "start",
    window_seconds: float | None = None,
    window_start: str | float = "auto",
) -> pd.DataFrame:
    """Rows to send, sorted by `t_event` (seconds after the replay starts).

    `align=start` shifts each bundle to begin at zero, which suits surveys
    replayed from different nights. `align=absolute` keeps their relative
    timing, for bundles from the same night. A window keeps `window_seconds` of
    the night, starting at `window_start` seconds or at the busiest stretch.
    Packets of one exposure share a `t_event` and are sent as one burst.
    """
    if clock not in CLOCKS:
        raise ValueError(f"clock must be one of {CLOCKS}")
    if align not in ALIGNMENTS:
        raise ValueError(f"align must be one of {ALIGNMENTS}")

    frames = []
    for i, b in enumerate(bundles):
        df = b.index.copy()
        df["bundle"] = i
        df["row"] = np.arange(len(df))
        df["survey"] = b.survey
        df["abs_s"] = _absolute_seconds(df, clock).to_numpy()
        if align == "start" and len(df):
            df["abs_s"] -= df["abs_s"].min()
        frames.append(df)
    plan = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if plan.empty:
        return plan

    def windowed(df: pd.DataFrame) -> pd.DataFrame:
        if window_seconds is None or clock == "none":
            df = df.copy()
            df["t_event"] = df["abs_s"] - df["abs_s"].min()
            return df
        t = df["abs_s"].to_numpy()
        if window_start == "auto":
            start = busiest_window_start(t, window_seconds)
        else:
            start = float(window_start) + t.min()
        df = df[(t >= start) & (t < start + window_seconds)].copy()
        df["t_event"] = df["abs_s"] - start
        return df

    if align == "start" and window_seconds is not None:
        plan = pd.concat([windowed(g) for _, g in plan.groupby("bundle", sort=True)], ignore_index=True)
    else:
        plan = windowed(plan)

    plan["t_event"] = plan["t_event"] - plan["t_event"].min() if len(plan) else plan["t_event"]
    return plan.sort_values(["t_event", "bundle", "row"], kind="stable").reset_index(drop=True)


def cut_jd_per_survey(plan: pd.DataFrame) -> dict[str, float]:
    """Earliest exposure each survey replays: the snapshot must predate it."""
    return {s: float(g["t_obs_jd"].min()) for s, g in plan.groupby("survey")}


def cut_jd_per_survey(plan: pd.DataFrame) -> dict[str, float]:
    """Earliest exposure each survey replays: the snapshot must predate it."""
    return {s: float(g["t_obs_jd"].min()) for s, g in plan.groupby("survey")}


def rate_profile(plan: pd.DataFrame, bin_seconds: float = 60.0) -> pd.Series:
    """Packets sent per second, averaged over `bin_seconds` bins, at speed 1."""
    if plan.empty:
        return pd.Series(dtype=float)
    bins = np.floor(plan["t_event"].to_numpy() / bin_seconds).astype(int)
    counts = pd.Series(bins).value_counts().sort_index()
    full = counts.reindex(range(int(bins.max()) + 1), fill_value=0)
    return full / bin_seconds
