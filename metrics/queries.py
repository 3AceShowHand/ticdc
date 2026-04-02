# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""TiKV-style PromQL and panel helper facade for TiCDC dashboards."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Literal

from metrics.dsl.api import (
    graph as _build_graph_panel,
)
from metrics.dsl.api import (
    heatmap as _build_heatmap_panel,
)
from metrics.dsl.api import (
    table as _build_table_panel,
)
from metrics.dsl.api import (
    target as build_target,
)
from metrics.dsl.api import (
    transformation as transformation,
)
from metrics.dsl.promql import (
    LabelMatcher,
    legend_for,
    promql,
)
from metrics.dsl.promql import (
    eq as eq,
)
from metrics.dsl.promql import (
    neq as neq,
)
from metrics.dsl.promql import (
    not_regex as not_regex,
)
from metrics.dsl.promql import (
    regex as regex,
)
from metrics.dsl.specs import (
    GraphPanelSpec,
    HeatmapPanelSpec,
    TablePanelSpec,
    TargetSpec,
    TransformationSpec,
)

type SelectorLike = LabelMatcher | str
type SelectorSeq = Sequence[SelectorLike]
type LabelSeq = Sequence[str]
type ScopeName = Literal[
    "instance",
    "changefeed",
    "cluster",
    "tikv_instance",
    "runtime_instance",
    "none",
]


@dataclass(frozen=True, slots=True)
class Expr:
    """Explicit PromQL expression object with optional group-by metadata."""

    text: str
    by_labels: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "text", promql(self.text))
        object.__setattr__(self, "by_labels", tuple(self.by_labels))

    def __str__(self) -> str:
        return self.text

    def range(self, selector: str) -> Expr:
        return Expr(f"{self.text}[{selector}]", by_labels=self.by_labels)

    def call(self, func: str, *, range_selector: str | None = None) -> Expr:
        inner = self.range(range_selector) if range_selector is not None else self
        return Expr(f"{func}({inner})", by_labels=self.by_labels)

    def op(self, operator: str, rhs: Expr | str) -> Expr:
        return Expr(f"{self} {operator} {rhs}")


def _render_selector(selector_value: SelectorLike) -> str:
    if isinstance(selector_value, LabelMatcher):
        return selector_value.render()
    return promql(selector_value)


def _scope_selectors(scope: ScopeName) -> list[str]:
    if scope == "none":
        return []
    base = [
        'k8s_cluster="$k8s_cluster"',
        'tidb_cluster="$tidb_cluster"',
    ]
    if scope == "cluster":
        return base
    if scope == "instance":
        return [*base, 'instance=~"$ticdc_instance"']
    if scope == "changefeed":
        return [
            *base,
            'instance=~"$ticdc_instance"',
            'namespace=~"$namespace"',
            'changefeed=~"$changefeed"',
        ]
    if scope == "tikv_instance":
        return [*base, 'instance=~"$tikv_instance"']
    if scope == "runtime_instance":
        return [*base, 'instance=~"$runtime_instance"']
    raise ValueError(f"unsupported scope: {scope}")


def _looks_like_promql(value: str) -> bool:
    return any(
        token in value for token in ("{", "}", "(", ")", "[", "]", " ", "\n", "/", "+", "*", "-")
    )


def _resolve_scope(scope: ScopeName | None) -> ScopeName:
    if scope is not None:
        return scope
    return "instance"


def _resolve_by_labels(
    by_labels: LabelSeq | None,
) -> tuple[str, ...]:
    if by_labels is not None:
        return tuple(by_labels)
    return ()


def expr_simple(
    value: str | Expr,
    *,
    scope: ScopeName | None = None,
    selectors: SelectorSeq = (),
) -> Expr:
    if isinstance(value, Expr):
        if scope not in (None, "none") or selectors:
            raise ValueError("prebuilt expressions cannot receive scope or selectors")
        return value

    resolved_scope = _resolve_scope(scope)

    if resolved_scope == "none" and not selectors and _looks_like_promql(value):
        return Expr(value)

    rendered_selectors = [
        *_scope_selectors(resolved_scope),
        *(_render_selector(item) for item in selectors),
    ]
    if not rendered_selectors:
        return Expr(value)
    return Expr(f"{value}{{{', '.join(rendered_selectors)}}}")


def _by_clause(by_labels: LabelSeq) -> str:
    if not by_labels:
        return ""
    return f" by ({', '.join(by_labels)})"


def _aggregate(
    op: str,
    value: str | Expr,
    *,
    by_labels: LabelSeq | None = None,
) -> Expr:
    resolved_by_labels = _resolve_by_labels(by_labels)
    return Expr(
        f"{op}({expr_simple(value, scope='none')}){_by_clause(resolved_by_labels)}",
        by_labels=resolved_by_labels,
    )


def _legend_from_by_labels(by_labels: LabelSeq) -> str | None:
    if not by_labels:
        return None
    return legend_for(*by_labels)


def _transform(
    func: str,
    value: str | Expr,
    *,
    scope: ScopeName = "instance",
    selectors: SelectorSeq = (),
    window: str = "1m",
) -> Expr:
    return expr_simple(
        value,
        scope=scope,
        selectors=selectors,
    ).call(func, range_selector=window)


def expr_sum(
    value: str | Expr,
    *,
    by_labels: LabelSeq | None = None,
    scope: ScopeName | None = None,
    selectors: SelectorSeq = (),
) -> Expr:
    return _aggregate(
        "sum",
        expr_simple(value, scope=scope, selectors=selectors),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_avg(
    value: str | Expr,
    *,
    by_labels: LabelSeq | None = None,
    scope: ScopeName | None = None,
    selectors: SelectorSeq = (),
) -> Expr:
    return _aggregate(
        "avg",
        expr_simple(value, scope=scope, selectors=selectors),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_max(
    value: str | Expr,
    *,
    by_labels: LabelSeq | None = None,
    scope: ScopeName | None = None,
    selectors: SelectorSeq = (),
) -> Expr:
    return _aggregate(
        "max",
        expr_simple(value, scope=scope, selectors=selectors),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_min(
    value: str | Expr,
    *,
    by_labels: LabelSeq | None = None,
    scope: ScopeName | None = None,
    selectors: SelectorSeq = (),
) -> Expr:
    return _aggregate(
        "min",
        expr_simple(value, scope=scope, selectors=selectors),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_count(
    value: str | Expr,
    *,
    by_labels: LabelSeq | None = None,
    scope: ScopeName | None = None,
    selectors: SelectorSeq = (),
) -> Expr:
    return _aggregate(
        "count",
        expr_simple(value, scope=scope, selectors=selectors),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_rate(
    value: str | Expr,
    *,
    scope: ScopeName = "instance",
    selectors: SelectorSeq = (),
    window: str = "1m",
) -> Expr:
    return _transform("rate", value, scope=scope, selectors=selectors, window=window)


def expr_delta(
    value: str | Expr,
    *,
    scope: ScopeName = "instance",
    selectors: SelectorSeq = (),
    window: str = "1m",
) -> Expr:
    return _transform("delta", value, scope=scope, selectors=selectors, window=window)


def expr_increase(
    value: str | Expr,
    *,
    scope: ScopeName = "instance",
    selectors: SelectorSeq = (),
    window: str = "1m",
) -> Expr:
    return _transform(
        "increase",
        value,
        scope=scope,
        selectors=selectors,
        window=window,
    )


def expr_sum_rate(
    value: str | Expr,
    *,
    by_labels: LabelSeq | None = None,
    scope: ScopeName | None = None,
    selectors: SelectorSeq = (),
    window: str = "1m",
) -> Expr:
    resolved_scope = _resolve_scope(scope)
    return _aggregate(
        "sum",
        expr_rate(value, scope=resolved_scope, selectors=selectors, window=window),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_max_rate(
    value: str | Expr,
    *,
    by_labels: LabelSeq | None = None,
    scope: ScopeName | None = None,
    selectors: SelectorSeq = (),
    window: str = "1m",
) -> Expr:
    resolved_scope = _resolve_scope(scope)
    return _aggregate(
        "max",
        expr_rate(value, scope=resolved_scope, selectors=selectors, window=window),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_count_rate(
    value: str | Expr,
    *,
    by_labels: LabelSeq | None = None,
    scope: ScopeName | None = None,
    selectors: SelectorSeq = (),
    window: str = "1m",
) -> Expr:
    resolved_scope = _resolve_scope(scope)
    return _aggregate(
        "count",
        expr_rate(value, scope=resolved_scope, selectors=selectors, window=window),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_sum_delta(
    value: str | Expr,
    *,
    by_labels: LabelSeq | None = None,
    scope: ScopeName | None = None,
    selectors: SelectorSeq = (),
    window: str = "1m",
) -> Expr:
    resolved_scope = _resolve_scope(scope)
    return _aggregate(
        "sum",
        expr_delta(value, scope=resolved_scope, selectors=selectors, window=window),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_sum_increase(
    value: str | Expr,
    *,
    by_labels: LabelSeq | None = None,
    scope: ScopeName | None = None,
    selectors: SelectorSeq = (),
    window: str = "1m",
) -> Expr:
    resolved_scope = _resolve_scope(scope)
    return _aggregate(
        "sum",
        expr_increase(
            value,
            scope=resolved_scope,
            selectors=selectors,
            window=window,
        ),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_over_time(
    func: str,
    value: str | Expr,
    *,
    scope: ScopeName = "none",
    selectors: SelectorSeq = (),
    window: str,
) -> Expr:
    return expr_simple(
        value,
        scope=scope,
        selectors=selectors,
    ).call(func, range_selector=window)


def expr_operator(lhs: str | Expr, operator: str, rhs: str | Expr) -> Expr:
    lhs_expr = expr_simple(lhs, scope="none")
    rhs_expr = expr_simple(rhs, scope="none")
    by_labels: tuple[str, ...] = ()
    if lhs_expr.by_labels == rhs_expr.by_labels:
        by_labels = lhs_expr.by_labels
    return Expr(f"{lhs_expr} {operator} {rhs_expr}", by_labels=by_labels)


def expr_histogram_quantile(
    quantile: float | str,
    metric: str,
    *,
    by_labels: LabelSeq = (),
    scope: ScopeName = "instance",
    selectors: SelectorSeq = (),
    window: str = "1m",
) -> Expr:
    bucket_metric = metric if metric.endswith("_bucket") else f"{metric}_bucket"
    bucket_labels = ("le", *by_labels)
    bucket_rate = expr_sum_rate(
        bucket_metric,
        by_labels=bucket_labels,
        scope=scope,
        selectors=selectors,
        window=window,
    )
    return Expr(f"histogram_quantile({quantile}, {bucket_rate})", by_labels=tuple(by_labels))


def expr_histogram_avg(
    metric: str,
    *,
    by_labels: LabelSeq = (),
    scope: ScopeName = "instance",
    selectors: SelectorSeq = (),
    window: str = "1m",
) -> Expr:
    sum_rate_expr = expr_sum_rate(
        f"{metric}_sum",
        by_labels=by_labels,
        scope=scope,
        selectors=selectors,
        window=window,
    )
    count_rate_expr = expr_sum_rate(
        f"{metric}_count",
        by_labels=by_labels,
        scope=scope,
        selectors=selectors,
        window=window,
    )
    return expr_operator(
        sum_rate_expr,
        "/",
        count_rate_expr,
    )


def heatmap_expr(
    metric: str,
    *,
    by_labels: LabelSeq = (),
    scope: ScopeName = "instance",
    selectors: SelectorSeq = (),
    window: str = "1m",
) -> Expr:
    bucket_metric = metric if metric.endswith("_bucket") else f"{metric}_bucket"
    return expr_sum_rate(
        bucket_metric,
        by_labels=("le", *by_labels),
        scope=scope,
        selectors=selectors,
        window=window,
    )


def target(
    expr: str | Expr,
    legend_format: str | None = None,
    *,
    legend: str | None = None,
    ref: str = "A",
    hide: bool = False,
    format: str | None = "time_series",
    instant: bool | None = None,
) -> TargetSpec:
    if legend_format is not None and legend is not None and legend_format != legend:
        raise ValueError("legend and legend_format must match when both are set")
    resolved_legend = legend_format or legend
    if resolved_legend is None and isinstance(expr, Expr):
        resolved_legend = _legend_from_by_labels(expr.by_labels)
    return build_target(
        str(expr_simple(expr, scope="none")),
        legend=resolved_legend,
        ref=ref,
        hide=hide,
        format=format,
        instant=instant,
    )


def heatmap_target(
    expr: str | Expr,
    legend_format: str = "{{le}}",
    *,
    legend: str | None = None,
    ref: str = "A",
    hide: bool = False,
) -> TargetSpec:
    return target(
        expr=expr,
        legend_format=legend_format if legend is None else legend,
        ref=ref,
        hide=hide,
        format="heatmap",
        instant=False,
    )


def graph_panel(
    title: str,
    *,
    targets: list[TargetSpec],
    key: str | None = None,
    description: str | None = None,
    unit: str = "short",
    min: str | int | float | None = None,
    max: str | int | float | None = None,
    decimals: int | None = None,
    span: int | None = None,
    width: int | None = None,
    height: int = 7,
    x: int | None = None,
) -> GraphPanelSpec:
    return _build_graph_panel(
        title,
        targets=targets,
        key=key,
        description=description,
        unit=unit,
        min=min,
        max=max,
        decimals=decimals,
        span=span,
        width=width,
        height=height,
        x=x,
    )


def heatmap_panel(
    title: str,
    *,
    targets: list[TargetSpec],
    key: str | None = None,
    description: str | None = None,
    unit: str = "short",
    span: int | None = None,
    width: int | None = None,
    height: int = 7,
    x: int | None = None,
) -> HeatmapPanelSpec:
    return _build_heatmap_panel(
        title,
        targets=targets,
        key=key,
        description=description,
        unit=unit,
        span=span,
        width=width,
        height=height,
        x=x,
    )


def table_panel(
    title: str,
    *,
    targets: list[TargetSpec],
    key: str | None = None,
    description: str | None = None,
    span: int | None = None,
    width: int | None = None,
    height: int = 7,
    x: int | None = None,
    transformations: Sequence[TransformationSpec] | None = None,
) -> TablePanelSpec:
    return _build_table_panel(
        title,
        targets=targets,
        key=key,
        description=description,
        span=span,
        width=width,
        height=height,
        x=x,
        transformations=transformations,
    )


def histogram_heatmap_panel(
    title: str,
    *,
    metric: str,
    by_labels: LabelSeq = (),
    by: LabelSeq | None = None,
    scope: ScopeName = "instance",
    selectors: SelectorSeq = (),
    matchers: SelectorSeq | None = None,
    description: str | None = None,
    unit: str = "short",
    span: int | None = None,
    width: int = 12,
    height: int = 7,
    window: str = "1m",
) -> HeatmapPanelSpec:
    if by is not None:
        by_labels = by
    if matchers is not None:
        selectors = matchers
        if scope == "instance":
            scope = "none"
    return heatmap_panel(
        title=title,
        targets=[
            heatmap_target(
                expr=heatmap_expr(
                    metric,
                    by_labels=by_labels,
                    scope=scope,
                    selectors=selectors,
                    window=window,
                )
            )
        ],
        description=description,
        unit=unit,
        span=span,
        width=width,
        height=height,
    )


def histogram_quantile_graph_panel(
    title: str,
    *,
    metric: str,
    by_labels: LabelSeq = (),
    by: LabelSeq | None = None,
    scope: ScopeName = "instance",
    selectors: SelectorSeq = (),
    matchers: SelectorSeq | None = None,
    description: str | None = None,
    unit: str = "short",
    min: str | int | float | None = None,
    max: str | int | float | None = None,
    decimals: int | None = None,
    span: int | None = None,
    width: int = 12,
    height: int = 7,
    x: int | None = None,
    window: str = "1m",
    quantile: float = 0.99,
    quantile_legend: str | None = None,
    average_legend: str | None = None,
    format: str | None = None,
) -> GraphPanelSpec:
    if by is not None:
        by_labels = by
    if matchers is not None:
        selectors = matchers
        if scope == "instance":
            scope = "none"
    quantile_prefix = str(quantile).replace(".", "")
    quantile_legend_format = quantile_legend or legend_for(
        *by_labels,
        prefix=f"p{quantile_prefix}",
    )
    return graph_panel(
        title=title,
        targets=[
            target(
                expr=expr_histogram_quantile(
                    quantile,
                    metric,
                    by_labels=by_labels,
                    scope=scope,
                    selectors=selectors,
                    window=window,
                ),
                legend_format=quantile_legend_format,
                format=format,
            ),
            target(
                expr=expr_histogram_avg(
                    metric,
                    by_labels=by_labels,
                    scope=scope,
                    selectors=selectors,
                    window=window,
                ),
                legend_format=average_legend or legend_for(*by_labels, prefix="avg"),
                ref="B",
                format=format,
            ),
        ],
        description=description,
        unit=unit,
        min=min,
        max=max,
        decimals=decimals,
        span=span,
        width=width,
        height=height,
        x=x,
    )


def histogram_panel_pair(
    *,
    heatmap_title: str,
    graph_title: str,
    metric: str,
    by_labels: LabelSeq = (),
    by: LabelSeq | None = None,
    scope: ScopeName = "instance",
    selectors: SelectorSeq = (),
    matchers: SelectorSeq | None = None,
    unit: str = "short",
    span: int | None = None,
    width: int = 12,
    height: int = 7,
    heatmap_description: str | None = None,
    graph_description: str | None = None,
    window: str = "1m",
    quantile: float = 0.99,
    quantile_legend: str | None = None,
    average_legend: str | None = None,
    format: str | None = None,
) -> list[HeatmapPanelSpec | GraphPanelSpec]:
    if by is not None:
        by_labels = by
    if matchers is not None:
        selectors = matchers
        if scope == "instance":
            scope = "none"
    return [
        histogram_heatmap_panel(
            title=heatmap_title,
            metric=metric,
            by_labels=by_labels,
            scope=scope,
            selectors=selectors,
            description=heatmap_description,
            unit=unit,
            span=span,
            width=width,
            height=height,
            window=window,
        ),
        histogram_quantile_graph_panel(
            title=graph_title,
            metric=metric,
            by_labels=by_labels,
            scope=scope,
            selectors=selectors,
            description=graph_description,
            unit=unit,
            span=span,
            width=width,
            height=height,
            window=window,
            quantile=quantile,
            quantile_legend=quantile_legend,
            average_legend=average_legend,
            format=format,
        ),
    ]


__all__ = [
    "Expr",
    "eq",
    "expr_avg",
    "expr_count",
    "expr_count_rate",
    "expr_delta",
    "expr_histogram_avg",
    "expr_histogram_quantile",
    "expr_increase",
    "expr_max",
    "expr_max_rate",
    "expr_min",
    "expr_operator",
    "expr_over_time",
    "expr_rate",
    "expr_simple",
    "expr_sum",
    "expr_sum_delta",
    "expr_sum_increase",
    "expr_sum_rate",
    "graph_panel",
    "heatmap_expr",
    "heatmap_panel",
    "heatmap_target",
    "histogram_heatmap_panel",
    "histogram_panel_pair",
    "histogram_quantile_graph_panel",
    "legend_for",
    "neq",
    "not_regex",
    "regex",
    "table_panel",
    "target",
    "transformation",
]
