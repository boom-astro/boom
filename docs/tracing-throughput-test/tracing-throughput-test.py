# altair[all]>=5.5.0
# pandas>=2.3.1
# vl-convert-python>=1.8.0

import altair as alt
import pandas as pd


def load(filename):
    with open(filename) as file:
        rates = pd.Series([float(s) for s in file.readlines()], name="Rate")
    return rates


def hist(data):
    return alt.Chart(data).mark_bar().encode(
        alt.X("Rate:Q").bin(extent=(8, 32), step=2),
        alt.Y("count()").stack(None),
    )


data = pd.concat([
    # no compile-time filters, RUST_LOG=info,ort=off [21 lines]
    # Baseline case.
    load("../../env/runs/throughput-20250714-20240617-none-info.dat").to_frame().assign(Experiment="none_info"),

    # no compile-time filters, RUST_LOG=debug,ort=off [29 lines]
    # More events => should be slower. BUT, only 8 more lines.
    # (Supposed to show the cost of subscribing to more events.)
    (load("../../env/runs/throughput-20250714-20240617-none-debug.dat").to_frame().assign(Experiment="none_debug")
        # Laptop went to sleep and inflated the time of one run:
        .loc[lambda df: df["Rate"] > df["Rate"].min()]
    ),

    # no compile-time filters, RUST_LOG=debug [667 lines]
    # Repeat of `none_debug` but with events from `ort` for a more extreme scenario.
    load("../../env/runs/throughput-20250714-20240617-none-debug2.dat").to_frame().assign(Experiment="none_debug2"),

    # only compile up to INFO, RUST_LOG=debug [652 lines]
    # Blocks out all DEBUG and TRACE, but there's only 17 DEBUG messages! Most of ort's events are INFO.
    # (Supposed to show cost is reduced when fewer levels are enabled, not sure it will be measurable in this case.)
    load("../../env/runs/throughput-20250714-20240617-info-debug2.dat").to_frame().assign(Experiment="info_debug2"),

    # don't compile any levels, RUST_LOG=debug [4 lines]
    # No spans or events => fastest possible rate.
    # (Supposed to show what rate is possible without any logging.)
    load("../../env/runs/throughput-20250714-20240617-off-debug2.dat").to_frame().assign(Experiment="off_debug2"),
])[["Experiment", "Rate"]]


for name in ["none_info", "none_debug", "none_debug2", "info_debug2", "off_debug2"]:
    df = data.loc[lambda df: df["Experiment"].eq(name)]
    chart = hist(df)
    chart.save(f"{name}.svg")
    print(name)
    print(df.describe())
    print()
