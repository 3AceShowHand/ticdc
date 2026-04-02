# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Thin, author-friendly helpers for PromQL and common target presets.

This layer is intentionally optional. Dashboard authors can keep using the
primitive `query()` / `graph()` APIs directly when that is clearer.
"""

from __future__ import annotations

import textwrap
from collections.abc import Sequence
from dataclasses import dataclass

from .api import query
from .specs import TargetSpec


@dataclass(frozen=True, slots=True)
class LabelMatcher:
    """One Prometheus label matcher."""

    name: str
    op: str
    value: str

    def render(self) -> str:
        return f'{self.name}{self.op}"{self.value}"'


def promql(expr: str) -> str:
    """Normalize a multiline PromQL string for readable authoring."""

    return textwrap.dedent(expr).strip()


def eq(name: str, value: str) -> LabelMatcher:
    return LabelMatcher(name=name, op="=", value=value)


def regex(name: str, value: str) -> LabelMatcher:
    return LabelMatcher(name=name, op="=~", value=value)


def neq(name: str, value: str) -> LabelMatcher:
    return LabelMatcher(name=name, op="!=", value=value)


def not_regex(name: str, value: str) -> LabelMatcher:
    return LabelMatcher(name=name, op="!~", value=value)


def selector(metric: str, *matchers: LabelMatcher) -> str:
    """Render `metric{...}` using normalized matcher formatting."""

    if not matchers:
        return metric
    rendered = ", ".join(matcher.render() for matcher in matchers)
    return f"{metric}{{{rendered}}}"


def legend_for(
    *labels: str,
    prefix: str | None = None,
    suffix: str | None = None,
    separator: str = "-",
) -> str:
    """Build a Grafana legend from grouped labels."""

    parts: list[str] = []
    if prefix:
        parts.append(prefix)
    parts.extend(f"{{{{{label}}}}}" for label in labels)
    if suffix:
        parts.append(suffix)
    return separator.join(parts)


def _by_clause(labels: Sequence[str]) -> str:
    if not labels:
        return ""
    return f" by ({', '.join(labels)})"


def aggregate(op: str, expr: str, *labels: str) -> str:
    return f"{op}({expr}){_by_clause(labels)}"


def sum_by(expr: str, *labels: str) -> str:
    return aggregate("sum", expr, *labels)


def avg_by(expr: str, *labels: str) -> str:
    return aggregate("avg", expr, *labels)


def max_by(expr: str, *labels: str) -> str:
    return aggregate("max", expr, *labels)


def min_by(expr: str, *labels: str) -> str:
    return aggregate("min", expr, *labels)


def rate(expr: str, window: str) -> str:
    return f"rate({expr}[{window}])"


def delta(expr: str, window: str) -> str:
    return f"delta({expr}[{window}])"


def increase(expr: str, window: str) -> str:
    return f"increase({expr}[{window}])"


def sum_rate(
    metric: str,
    *,
    matchers: Sequence[LabelMatcher] = (),
    by: Sequence[str] = (),
    window: str = "1m",
) -> str:
    return sum_by(rate(selector(metric, *matchers), window), *by)


def histogram_quantile_rate(
    metric: str,
    *,
    quantile: float | str,
    matchers: Sequence[LabelMatcher] = (),
    by: Sequence[str] = (),
    window: str = "1m",
) -> str:
    """Build a TiKV-style histogram quantile expression from a base metric."""

    bucket_metric = metric if metric.endswith("_bucket") else f"{metric}_bucket"
    bucket_by = ("le", *by)
    bucket_rate = sum_rate(
        bucket_metric,
        matchers=matchers,
        by=bucket_by,
        window=window,
    )
    return f"histogram_quantile({quantile}, {bucket_rate})"


def histogram_average_rate(
    metric: str,
    *,
    matchers: Sequence[LabelMatcher] = (),
    by: Sequence[str] = (),
    window: str = "1m",
) -> str:
    """Build the standard histogram average rate: `sum(rate(sum))/sum(rate(count))`."""

    sum_expr = sum_rate(f"{metric}_sum", matchers=matchers, by=by, window=window)
    count_expr = sum_rate(f"{metric}_count", matchers=matchers, by=by, window=window)
    return f"{sum_expr} / {count_expr}"


def series_query(
    expr: str,
    legend: str | None = None,
    *,
    ref: str = "A",
    hide: bool = False,
    format: str | None = "time_series",
    instant: bool | None = None,
) -> TargetSpec:
    """Build a target for normal time-series panels."""

    return query(
        promql(expr),
        legend=legend,
        ref=ref,
        hide=hide,
        format=format,
        instant=instant,
    )


def instant_query(
    expr: str,
    *,
    legend: str | None = None,
    ref: str = "A",
    hide: bool = False,
    format: str = "time_series",
) -> TargetSpec:
    """Build a query that should be evaluated as an instant vector."""

    return query(
        promql(expr),
        legend=legend,
        ref=ref,
        hide=hide,
        format=format,
        instant=True,
    )


def heatmap_query(
    expr: str,
    legend: str = "{{le}}",
    *,
    ref: str = "A",
    hide: bool = False,
) -> TargetSpec:
    """Build a heatmap target with Grafana's expected defaults."""

    return query(
        promql(expr),
        legend=legend,
        ref=ref,
        hide=hide,
        format="heatmap",
        instant=False,
    )
