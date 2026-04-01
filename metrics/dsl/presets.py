# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Optional higher-level panel helpers built on the primitive DSL."""

from __future__ import annotations

from collections.abc import Sequence

from .api import graph, heatmap
from .promql import (
    LabelMatcher,
    heatmap_query,
    histogram_average_rate,
    histogram_quantile_rate,
    legend_for,
    series_query,
    sum_rate,
)
from .specs import GraphPanelSpec, HeatmapPanelSpec, ScalarOrNone


def histogram_heatmap_panel(
    title: str,
    *,
    metric: str,
    matchers: Sequence[LabelMatcher] = (),
    unit: str = "short",
    width: int = 12,
    height: int = 7,
    description: str | None = None,
    window: str = "1m",
) -> HeatmapPanelSpec:
    """Build a heatmap panel from a histogram metric."""

    return heatmap(
        title,
        targets=[
            heatmap_query(
                sum_rate(
                    metric if metric.endswith("_bucket") else f"{metric}_bucket",
                    matchers=matchers,
                    by=("le",),
                    window=window,
                )
            )
        ],
        unit=unit,
        width=width,
        height=height,
        description=description,
    )


def histogram_quantile_graph(
    title: str,
    *,
    metric: str,
    matchers: Sequence[LabelMatcher] = (),
    by: Sequence[str] = (),
    unit: str = "short",
    width: int = 12,
    height: int = 7,
    x: int | None = None,
    description: str | None = None,
    min: ScalarOrNone = None,
    max: ScalarOrNone = None,
    decimals: int | None = None,
    format: str | None = None,
    window: str = "1m",
    quantile: float = 0.99,
    quantile_prefix: str = "p99",
    average_prefix: str = "avg",
    quantile_legend: str | None = None,
    average_legend: str | None = None,
) -> GraphPanelSpec:
    """Build a TiKV-style graph panel with quantile and average series."""

    return graph(
        title,
        targets=[
            series_query(
                histogram_quantile_rate(
                    metric,
                    quantile=quantile,
                    matchers=matchers,
                    by=by,
                    window=window,
                ),
                legend=quantile_legend or legend_for(*by, prefix=quantile_prefix),
                format=format,
            ),
            series_query(
                histogram_average_rate(
                    metric,
                    matchers=matchers,
                    by=by,
                    window=window,
                ),
                legend=average_legend or legend_for(*by, prefix=average_prefix),
                ref="B",
                format=format,
            )
        ],
        unit=unit,
        width=width,
        height=height,
        x=x,
        description=description,
        min=min,
        max=max,
        decimals=decimals,
    )


def histogram_panel_pair(
    *,
    heatmap_title: str,
    graph_title: str,
    metric: str,
    matchers: Sequence[LabelMatcher] = (),
    by: Sequence[str] = (),
    unit: str = "short",
    width: int = 12,
    height: int = 7,
    heatmap_description: str | None = None,
    graph_description: str | None = None,
    window: str = "1m",
    quantile: float = 0.99,
    quantile_legend: str | None = None,
    average_legend: str | None = None,
) -> list[HeatmapPanelSpec | GraphPanelSpec]:
    """Build a TiKV-style histogram heatmap + quantile graph pair."""

    return [
        histogram_heatmap_panel(
            heatmap_title,
            metric=metric,
            matchers=matchers,
            unit=unit,
            width=width,
            height=height,
            description=heatmap_description,
            window=window,
        ),
        histogram_quantile_graph(
            graph_title,
            metric=metric,
            matchers=matchers,
            by=by,
            unit=unit,
            width=width,
            height=height,
            description=graph_description,
            window=window,
            quantile=quantile,
            quantile_legend=quantile_legend,
            average_legend=average_legend,
        ),
    ]
