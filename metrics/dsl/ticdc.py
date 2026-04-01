# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""TiCDC-oriented dashboard helpers built on top of the generic primitive DSL."""

from __future__ import annotations

from collections.abc import Sequence

from .api import graph
from .promql import LabelMatcher, delta, eq, increase, rate, regex, selector, series_query
from .specs import GraphPanelSpec, ScalarOrNone


def changefeed_scope(
    *,
    instance: str | None = "$ticdc_instance",
    namespace: str | None = "$namespace",
    changefeed: str | None = "$changefeed",
    extra: Sequence[LabelMatcher] = (),
) -> list[LabelMatcher]:
    """Common TiCDC changefeed-level selector set."""

    matchers = [
        eq("k8s_cluster", "$k8s_cluster"),
        eq("tidb_cluster", "$tidb_cluster"),
    ]
    if instance is not None:
        matchers.append(regex("instance", instance))
    if namespace is not None:
        matchers.append(regex("namespace", namespace))
    if changefeed is not None:
        matchers.append(regex("changefeed", changefeed))
    matchers.extend(extra)
    return matchers


def instance_scope(
    *,
    instance: str | None = "$ticdc_instance",
    extra: Sequence[LabelMatcher] = (),
) -> list[LabelMatcher]:
    """Common TiCDC instance-level selector set."""

    matchers = [
        eq("k8s_cluster", "$k8s_cluster"),
        eq("tidb_cluster", "$tidb_cluster"),
    ]
    if instance is not None:
        matchers.append(regex("instance", instance))
    matchers.extend(extra)
    return matchers


def _apply_transform(expr: str, *, transform: str | None, window: str) -> str:
    if transform is None:
        return expr
    if transform == "rate":
        return rate(expr, window)
    if transform == "delta":
        return delta(expr, window)
    if transform == "increase":
        return increase(expr, window)
    raise ValueError(f"unsupported transform: {transform}")


def _apply_aggregate(expr: str, *, aggregate: str | None, by: Sequence[str]) -> str:
    if aggregate is None:
        if by:
            raise ValueError("by labels require an aggregate operation")
        return expr
    if aggregate not in {"sum", "avg", "max", "min"}:
        raise ValueError(f"unsupported aggregate: {aggregate}")
    by_clause = f" by ({', '.join(by)})" if by else ""
    return f"{aggregate}({expr}){by_clause}"


def metric_graph(
    title: str,
    *,
    metric: str,
    scope: Sequence[LabelMatcher],
    aggregate: str | None = None,
    transform: str | None = None,
    window: str = "1m",
    by: Sequence[str] = (),
    legend: str | None = None,
    ref: str = "A",
    hide: bool = False,
    format: str | None = None,
    instant: bool | None = None,
    description: str | None = None,
    unit: str = "short",
    min: ScalarOrNone = None,
    max: ScalarOrNone = None,
    decimals: int | None = None,
    width: int = 12,
    height: int = 7,
    x: int | None = None,
) -> GraphPanelSpec:
    """Build a graph panel from metric semantics instead of raw PromQL."""

    expr = selector(metric, *scope)
    expr = _apply_transform(expr, transform=transform, window=window)
    expr = _apply_aggregate(expr, aggregate=aggregate, by=by)

    return graph(
        title,
        targets=[
            series_query(
                expr,
                legend=legend,
                ref=ref,
                hide=hide,
                format=format,
                instant=instant,
            )
        ],
        description=description,
        unit=unit,
        min=min,
        max=max,
        decimals=decimals,
        width=width,
        height=height,
        x=x,
    )


def changefeed_graph(
    title: str,
    *,
    metric: str,
    aggregate: str | None = None,
    transform: str | None = None,
    window: str = "1m",
    by: Sequence[str] = (),
    legend: str | None = None,
    description: str | None = None,
    unit: str = "short",
    min: ScalarOrNone = None,
    max: ScalarOrNone = None,
    decimals: int | None = None,
    width: int = 12,
    height: int = 7,
    x: int | None = None,
    instance: str | None = "$ticdc_instance",
    namespace: str | None = "$namespace",
    changefeed: str | None = "$changefeed",
    extra_scope: Sequence[LabelMatcher] = (),
    format: str | None = None,
    instant: bool | None = None,
) -> GraphPanelSpec:
    """Common one-line helper for changefeed-scoped graph panels."""

    return metric_graph(
        title,
        metric=metric,
        scope=changefeed_scope(
            instance=instance,
            namespace=namespace,
            changefeed=changefeed,
            extra=extra_scope,
        ),
        aggregate=aggregate,
        transform=transform,
        window=window,
        by=by,
        legend=legend,
        description=description,
        unit=unit,
        min=min,
        max=max,
        decimals=decimals,
        width=width,
        height=height,
        x=x,
        format=format,
        instant=instant,
    )


def instance_graph(
    title: str,
    *,
    metric: str,
    aggregate: str | None = None,
    transform: str | None = None,
    window: str = "1m",
    by: Sequence[str] = (),
    legend: str | None = None,
    description: str | None = None,
    unit: str = "short",
    min: ScalarOrNone = None,
    max: ScalarOrNone = None,
    decimals: int | None = None,
    width: int = 12,
    height: int = 7,
    x: int | None = None,
    instance: str | None = "$ticdc_instance",
    extra_scope: Sequence[LabelMatcher] = (),
    format: str | None = None,
    instant: bool | None = None,
) -> GraphPanelSpec:
    """Common one-line helper for instance-scoped graph panels."""

    return metric_graph(
        title,
        metric=metric,
        scope=instance_scope(instance=instance, extra=extra_scope),
        aggregate=aggregate,
        transform=transform,
        window=window,
        by=by,
        legend=legend,
        description=description,
        unit=unit,
        min=min,
        max=max,
        decimals=decimals,
        width=width,
        height=height,
        x=x,
        format=format,
        instant=instant,
    )
