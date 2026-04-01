# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""TiKV-style authoring facade for TiCDC Grafana dashboards.

Dashboard authors should mainly import this module. It exposes a small set of
functions that read like metric intent instead of low-level JSON assembly, while
the generic renderer stays in `metrics.dsl`.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Final, Literal, TypeAlias

from metrics.dsl.api import (
    custom_var,
    graph,
    heatmap,
    query_var,
    row,
    table,
    target as build_target,
    timeseries,
    transformation,
)
from metrics.dsl.promql import LabelMatcher, eq, legend_for, neq, not_regex, promql, regex
from metrics.dsl.specs import (
    GraphPanelSpec,
    HeatmapPanelSpec,
    PanelSpecLike,
    RowSpec,
    TablePanelSpec,
    TargetSpec,
    TimeSeriesPanelSpec,
)

BASE_DASHBOARD_TITLE: Final = "test-cluster-TiCDC-New-Arch"
BASE_DASHBOARD_UID: Final = "YiGL8hBZ0aac"
DATASOURCE_INPUT_NAME: Final = "DS_TEST-CLUSTER"
DATASOURCE: Final = f"${{{DATASOURCE_INPUT_NAME}}}"
DATASOURCE_INPUT: Final[dict[str, object]] = {
    "name": DATASOURCE_INPUT_NAME,
    "label": DATASOURCE,
    "type": "datasource",
    "pluginId": "prometheus",
    "pluginName": "Prometheus",
}
EXPECTED_TEMPLATE_NAMES: Final[list[str]] = [
    "k8s_cluster",
    "tidb_cluster",
    "namespace",
    "changefeed",
    "ticdc_instance",
    "tikv_instance",
    "spike_threshold",
    "runtime_instance",
]
EXPECTED_ROW_TITLES: Final[list[str]] = [
    "Summary",
    "Lag Summary",
    "Dataflow",
    "Server",
    "Changefeed",
    "Lag analyze",
    "Coordinator",
    "Maintainer",
    "Log Puller",
    "Event Store",
    "Schema Store",
    "Event Service",
    "Message Center",
    "Dispatcher",
    "Dynamic Stream",
    "Sink - General",
    "Sink - Transaction Sink",
    "Sink - MQ Sink",
    "Sink - Cloud Storage Sink",
    "Scheduler",
    "TiKV",
    "Active Active",
    "Redo",
    "Runtime $runtime_instance",
    "DDL",
    "Pulsar Sink",
]

SelectorLike: TypeAlias = LabelMatcher | str
ScopeName: TypeAlias = Literal[
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


def validate_dashboard_identity(
    dashboard: dict[str, object],
    *,
    expected_row_titles: list[str] | tuple[str, ...] = EXPECTED_ROW_TITLES,
) -> None:
    assert dashboard["title"] == BASE_DASHBOARD_TITLE
    assert dashboard["uid"] == BASE_DASHBOARD_UID
    assert dashboard["__inputs"] == [DATASOURCE_INPUT]
    assert [item["name"] for item in dashboard["templating"]["list"]] == EXPECTED_TEMPLATE_NAMES
    assert [panel["title"] for panel in dashboard["panels"]] == list(expected_row_titles)


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
    return any(token in value for token in ("{", "}", "(", ")", "[", "]", " ", "\n", "/", "+", "*", "-"))


def _resolve_scope(scope: ScopeName | None) -> ScopeName:
    if scope is not None:
        return scope
    return "instance"


def _resolve_by_labels(by_labels: list[str] | tuple[str, ...] | None) -> tuple[str, ...]:
    if by_labels is not None:
        return tuple(by_labels)
    return ()


def expr_simple(
    value: str | Expr,
    *,
    scope: ScopeName | None = None,
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
) -> Expr:
    if isinstance(value, Expr):
        if scope not in (None, "none") or selectors:
            raise ValueError("prebuilt expressions cannot receive scope or selectors")
        return value

    resolved_scope = _resolve_scope(scope)

    if resolved_scope == "none" and not selectors and _looks_like_promql(value):
        return Expr(value)

    rendered_selectors = [*_scope_selectors(resolved_scope), *(_render_selector(item) for item in selectors)]
    if not rendered_selectors:
        return Expr(value)
    return Expr(f'{value}{{{", ".join(rendered_selectors)}}}')


def _by_clause(by_labels: list[str] | tuple[str, ...]) -> str:
    if not by_labels:
        return ""
    return f' by ({", ".join(by_labels)})'


def _aggregate(
    op: str,
    value: str | Expr,
    *,
    by_labels: list[str] | tuple[str, ...] | None = None,
) -> Expr:
    resolved_by_labels = _resolve_by_labels(by_labels)
    return Expr(
        f"{op}({expr_simple(value, scope='none')}){_by_clause(resolved_by_labels)}",
        by_labels=resolved_by_labels,
    )


def _legend_from_by_labels(by_labels: list[str] | tuple[str, ...]) -> str | None:
    if not by_labels:
        return None
    return legend_for(*by_labels)


def _transform(
    func: str,
    value: str | Expr,
    *,
    scope: ScopeName = "instance",
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    window: str = "1m",
) -> Expr:
    return expr_simple(value, scope=scope, selectors=selectors).call(func, range_selector=window)


def expr_sum(
    value: str | Expr,
    *,
    by_labels: list[str] | tuple[str, ...] | None = None,
    scope: ScopeName | None = None,
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
) -> Expr:
    return _aggregate(
        "sum",
        expr_simple(value, scope=scope, selectors=selectors),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_avg(
    value: str | Expr,
    *,
    by_labels: list[str] | tuple[str, ...] | None = None,
    scope: ScopeName | None = None,
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
) -> Expr:
    return _aggregate(
        "avg",
        expr_simple(value, scope=scope, selectors=selectors),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_max(
    value: str | Expr,
    *,
    by_labels: list[str] | tuple[str, ...] | None = None,
    scope: ScopeName | None = None,
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
) -> Expr:
    return _aggregate(
        "max",
        expr_simple(value, scope=scope, selectors=selectors),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_min(
    value: str | Expr,
    *,
    by_labels: list[str] | tuple[str, ...] | None = None,
    scope: ScopeName | None = None,
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
) -> Expr:
    return _aggregate(
        "min",
        expr_simple(value, scope=scope, selectors=selectors),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_count(
    value: str | Expr,
    *,
    by_labels: list[str] | tuple[str, ...] | None = None,
    scope: ScopeName | None = None,
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
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
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    window: str = "1m",
) -> Expr:
    return _transform("rate", value, scope=scope, selectors=selectors, window=window)


def expr_delta(
    value: str | Expr,
    *,
    scope: ScopeName = "instance",
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    window: str = "1m",
) -> Expr:
    return _transform("delta", value, scope=scope, selectors=selectors, window=window)


def expr_increase(
    value: str | Expr,
    *,
    scope: ScopeName = "instance",
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    window: str = "1m",
) -> Expr:
    return _transform("increase", value, scope=scope, selectors=selectors, window=window)


def expr_sum_rate(
    value: str | Expr,
    *,
    by_labels: list[str] | tuple[str, ...] | None = None,
    scope: ScopeName | None = None,
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    window: str = "1m",
) -> Expr:
    return _aggregate(
        "sum",
        expr_rate(value, scope=scope, selectors=selectors, window=window),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_max_rate(
    value: str | Expr,
    *,
    by_labels: list[str] | tuple[str, ...] | None = None,
    scope: ScopeName | None = None,
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    window: str = "1m",
) -> Expr:
    return _aggregate(
        "max",
        expr_rate(value, scope=scope, selectors=selectors, window=window),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_count_rate(
    value: str | Expr,
    *,
    by_labels: list[str] | tuple[str, ...] | None = None,
    scope: ScopeName | None = None,
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    window: str = "1m",
) -> Expr:
    return _aggregate(
        "count",
        expr_rate(value, scope=scope, selectors=selectors, window=window),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_sum_delta(
    value: str | Expr,
    *,
    by_labels: list[str] | tuple[str, ...] | None = None,
    scope: ScopeName | None = None,
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    window: str = "1m",
) -> Expr:
    return _aggregate(
        "sum",
        expr_delta(value, scope=scope, selectors=selectors, window=window),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_sum_increase(
    value: str | Expr,
    *,
    by_labels: list[str] | tuple[str, ...] | None = None,
    scope: ScopeName | None = None,
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    window: str = "1m",
) -> Expr:
    return _aggregate(
        "sum",
        expr_increase(value, scope=scope, selectors=selectors, window=window),
        by_labels=_resolve_by_labels(by_labels),
    )


def expr_over_time(
    func: str,
    value: str | Expr,
    *,
    scope: ScopeName = "none",
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    window: str,
) -> Expr:
    return expr_simple(value, scope=scope, selectors=selectors).call(func, range_selector=window)


def expr_operator(lhs: str | Expr, operator: str, rhs: str | Expr) -> Expr:
    return Expr(f"{expr_simple(lhs, scope='none')} {operator} {expr_simple(rhs, scope='none')}")


def expr_histogram_quantile(
    quantile: float | str,
    metric: str,
    *,
    by_labels: list[str] | tuple[str, ...] = (),
    scope: ScopeName = "instance",
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    window: str = "1m",
) -> Expr:
    bucket_metric = metric if metric.endswith("_bucket") else f"{metric}_bucket"
    bucket_labels = ("le", *by_labels)
    return Expr(
        f"histogram_quantile({quantile}, {expr_sum_rate(bucket_metric, by_labels=bucket_labels, scope=scope, selectors=selectors, window=window)})"
    )


def expr_histogram_avg(
    metric: str,
    *,
    by_labels: list[str] | tuple[str, ...] = (),
    scope: ScopeName = "instance",
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    window: str = "1m",
) -> Expr:
    return expr_operator(
        expr_sum_rate(f"{metric}_sum", by_labels=by_labels, scope=scope, selectors=selectors, window=window),
        "/",
        expr_sum_rate(f"{metric}_count", by_labels=by_labels, scope=scope, selectors=selectors, window=window),
    )


def heatmap_expr(
    metric: str,
    *,
    by_labels: list[str] | tuple[str, ...] = (),
    scope: ScopeName = "instance",
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
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
    return graph(
        title,
        targets=targets,
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


def timeseries_panel(
    title: str,
    *,
    targets: list[TargetSpec],
    description: str | None = None,
    unit: str = "short",
    min: str | int | float | None = None,
    max: str | int | float | None = None,
    decimals: int | None = None,
    span: int | None = None,
    width: int | None = None,
    height: int = 7,
    x: int | None = None,
) -> TimeSeriesPanelSpec:
    return timeseries(
        title,
        targets=targets,
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
    description: str | None = None,
    unit: str = "short",
    span: int | None = None,
    width: int | None = None,
    height: int = 7,
    x: int | None = None,
) -> HeatmapPanelSpec:
    return heatmap(
        title,
        targets=targets,
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
    description: str | None = None,
    span: int | None = None,
    width: int | None = None,
    height: int = 7,
    x: int | None = None,
    transformations: list[object] | None = None,
) -> TablePanelSpec:
    return table(
        title,
        targets=targets,
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
    by_labels: list[str] | tuple[str, ...] = (),
    by: list[str] | tuple[str, ...] | None = None,
    scope: ScopeName = "instance",
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    matchers: list[SelectorLike] | tuple[SelectorLike, ...] | None = None,
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
    by_labels: list[str] | tuple[str, ...] = (),
    by: list[str] | tuple[str, ...] | None = None,
    scope: ScopeName = "instance",
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    matchers: list[SelectorLike] | tuple[SelectorLike, ...] | None = None,
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
                legend_format=quantile_legend or legend_for(*by_labels, prefix=f"p{quantile_prefix}"),
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
    by_labels: list[str] | tuple[str, ...] = (),
    by: list[str] | tuple[str, ...] | None = None,
    scope: ScopeName = "instance",
    selectors: list[SelectorLike] | tuple[SelectorLike, ...] = (),
    matchers: list[SelectorLike] | tuple[SelectorLike, ...] | None = None,
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


class Layout:
    """TiKV-style row builder that evenly distributes panel widths."""

    row_panel: RowSpec

    def __init__(
        self,
        title: str,
        *,
        collapsed: bool = True,
        repeat: str | None = None,
        panel_height: int = 7,
    ) -> None:
        self._title = title
        self._collapsed = collapsed
        self._repeat = repeat
        self._panel_height = panel_height
        self._panels: list[PanelSpecLike] = []
        self.row_panel = row(title, [], collapsed=collapsed, repeat=repeat)

    def row(self, panels: list[PanelSpecLike], *, width: int = 24) -> list[PanelSpecLike]:
        if not panels:
            return panels

        panel_width = width // len(panels)
        remainder = width % len(panels)
        adjusted = [
            replace(
                panel,
                span=panel_width + (remainder if index == len(panels) - 1 else 0),
                height=self._panel_height,
            )
            for index, panel in enumerate(panels)
        ]
        self._panels.extend(adjusted)
        self.row_panel = row(
            self._title,
            self._panels,
            collapsed=self._collapsed,
            repeat=self._repeat,
        )
        return adjusted

    def half_row(self, panels: list[PanelSpecLike]) -> list[PanelSpecLike]:
        return self.row(panels, width=12)


__all__ = [
    "BASE_DASHBOARD_TITLE",
    "BASE_DASHBOARD_UID",
    "DATASOURCE",
    "DATASOURCE_INPUT",
    "DATASOURCE_INPUT_NAME",
    "EXPECTED_ROW_TITLES",
    "EXPECTED_TEMPLATE_NAMES",
    "Expr",
    "Layout",
    "custom_var",
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
    "query_var",
    "regex",
    "row",
    "table_panel",
    "target",
    "timeseries_panel",
    "transformation",
    "validate_dashboard_identity",
]
