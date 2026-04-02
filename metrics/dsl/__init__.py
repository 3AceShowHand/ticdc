# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Public entry points for the primitive dashboard DSL."""

from .api import (
    custom_var,
    dashboard,
    graph,
    heatmap,
    query,
    query_var,
    row,
    table,
    target,
    transformation,
)
from .presets import (
    histogram_heatmap_panel,
    histogram_panel_pair,
    histogram_quantile_graph,
)
from .promql import (
    LabelMatcher,
    avg_by,
    delta,
    eq,
    heatmap_query,
    histogram_average_rate,
    histogram_quantile_rate,
    increase,
    instant_query,
    legend_for,
    max_by,
    min_by,
    neq,
    not_regex,
    promql,
    rate,
    regex,
    selector,
    series_query,
    sum_by,
    sum_rate,
)
from .render import render_dashboard

__all__ = [
    "LabelMatcher",
    "avg_by",
    "custom_var",
    "dashboard",
    "delta",
    "eq",
    "graph",
    "heatmap",
    "heatmap_query",
    "histogram_heatmap_panel",
    "histogram_average_rate",
    "histogram_panel_pair",
    "histogram_quantile_rate",
    "histogram_quantile_graph",
    "increase",
    "instant_query",
    "legend_for",
    "max_by",
    "min_by",
    "neq",
    "not_regex",
    "promql",
    "query",
    "query_var",
    "rate",
    "regex",
    "render_dashboard",
    "row",
    "selector",
    "series_query",
    "sum_by",
    "sum_rate",
    "table",
    "target",
    "transformation",
]
