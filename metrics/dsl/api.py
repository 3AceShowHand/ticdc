# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""Author-facing constructor helpers for the primitive Grafana DSL.

This module keeps dashboard authoring intentionally boring:

- small immutable spec objects
- keyword-heavy constructors
- readable aliases over Grafana-specific jargon when helpful
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import TypeVar

from .specs import (
    Annotation,
    AxisSpec,
    CustomVarSpec,
    DashboardSpec,
    GraphPanelSpec,
    HeatmapPanelSpec,
    LegendSpec,
    PanelSpecLike,
    QueryVarSpec,
    RowSpec,
    ScalarOrNone,
    SeriesOverrideSpec,
    TablePanelSpec,
    TargetSpec,
    ThresholdSpec,
    TransformationSpec,
    VariableSpecLike,
)

DEFAULT_PANEL_WIDTH = 12
ItemT = TypeVar("ItemT")


def _copy_items(items: Sequence[ItemT] | None) -> list[ItemT]:
    if items is None:
        return []
    return list(items)


def _resolve_span(*, span: int | None, width: int | None) -> int:
    if span is not None and width is not None and span != width:
        raise ValueError("span and width must match when both are set")
    if width is not None:
        return width
    if span is not None:
        return span
    return DEFAULT_PANEL_WIDTH


def dashboard(
    *,
    title: str,
    uid: str,
    variables: Sequence[VariableSpecLike],
    rows: Sequence[RowSpec],
    version: int = 1,
    annotations: Sequence[Annotation] | None = None,
    refresh: str = "10s",
) -> DashboardSpec:
    """Build a dashboard spec from variables, rows, and annotations."""

    return DashboardSpec(
        title=title,
        uid=uid,
        variables=list(variables),
        rows=list(rows),
        version=version,
        annotations=_copy_items(annotations),
        refresh=refresh,
    )


def row(
    title: str,
    panels: Sequence[PanelSpecLike],
    collapsed: bool = True,
    repeat: str | None = None,
) -> RowSpec:
    """Group panels into one Grafana row."""

    return RowSpec(title=title, panels=list(panels), collapsed=collapsed, repeat=repeat)


def graph(
    title: str,
    *,
    targets: Sequence[TargetSpec],
    key: str | None = None,
    unit: str = "short",
    span: int | None = None,
    width: int | None = None,
    height: int = 7,
    x: int | None = None,
    description: str | None = None,
    min: ScalarOrNone = None,
    max: ScalarOrNone = None,
    decimals: int | None = None,
    stack: bool = False,
    fill: int = 1,
    legend: LegendSpec | None = None,
    axis: AxisSpec | None = None,
    thresholds: Sequence[ThresholdSpec] | None = None,
    overrides: Sequence[SeriesOverrideSpec] | None = None,
) -> GraphPanelSpec:
    """Build a classic Grafana graph panel.

    `width` is a readable alias for Grafana's historical `span` terminology.
    Existing code may keep using `span`; new code can prefer `width`.
    """

    return GraphPanelSpec(
        title=title,
        key=title if key is None else key,
        targets=list(targets),
        span=_resolve_span(span=span, width=width),
        height=height,
        x=x,
        description=description,
        unit=unit,
        min=min,
        max=max,
        decimals=decimals,
        stack=stack,
        fill=fill,
        legend=legend,
        axis=axis,
        thresholds=_copy_items(thresholds),
        overrides=_copy_items(overrides),
    )


def heatmap(
    title: str,
    *,
    targets: Sequence[TargetSpec],
    key: str | None = None,
    unit: str = "short",
    span: int | None = None,
    width: int | None = None,
    height: int = 7,
    x: int | None = None,
    description: str | None = None,
) -> HeatmapPanelSpec:
    """Build a Grafana heatmap panel."""

    return HeatmapPanelSpec(
        title=title,
        key=title if key is None else key,
        targets=list(targets),
        span=_resolve_span(span=span, width=width),
        height=height,
        x=x,
        description=description,
        unit=unit,
    )


def table(
    title: str,
    *,
    targets: Sequence[TargetSpec],
    key: str | None = None,
    span: int | None = None,
    width: int | None = None,
    height: int = 7,
    x: int | None = None,
    description: str | None = None,
    transformations: Sequence[TransformationSpec] | None = None,
) -> TablePanelSpec:
    """Build a Grafana table panel."""

    return TablePanelSpec(
        title=title,
        key=title if key is None else key,
        targets=list(targets),
        span=_resolve_span(span=span, width=width),
        height=height,
        x=x,
        description=description,
        transformations=_copy_items(transformations),
    )


def query_var(
    name: str,
    *,
    query: str,
    label: str | None = None,
    multi: bool = False,
    include_all: bool = False,
    all_value: str | None = None,
    hide: int = 0,
    regex: str = "",
    sort: int = 0,
) -> QueryVarSpec:
    """Build a query-backed template variable."""

    return QueryVarSpec(
        name=name,
        query=query,
        label=label,
        multi=multi,
        include_all=include_all,
        all_value=all_value,
        hide=hide,
        regex=regex,
        sort=sort,
    )


def custom_var(
    name: str,
    *,
    options: Sequence[str],
    label: str | None = None,
    include_all: bool = False,
    all_value: str | None = None,
    hide: int = 0,
) -> CustomVarSpec:
    """Build a fixed-option template variable."""

    return CustomVarSpec(
        name=name,
        options=list(options),
        label=label,
        include_all=include_all,
        all_value=all_value,
        hide=hide,
    )


def target(
    expr: str,
    *,
    legend: str | None = None,
    ref: str = "A",
    hide: bool = False,
    format: str | None = None,
    instant: bool | None = None,
) -> TargetSpec:
    """Build a panel query target."""

    return TargetSpec(
        expr=expr,
        legend=legend,
        ref=ref,
        hide=hide,
        format=format,
        instant=instant,
    )


def query(
    expr: str,
    *,
    legend: str | None = None,
    ref: str = "A",
    hide: bool = False,
    format: str | None = None,
    instant: bool | None = None,
) -> TargetSpec:
    """Readable alias for `target()` when authoring PromQL."""

    return target(
        expr,
        legend=legend,
        ref=ref,
        hide=hide,
        format=format,
        instant=instant,
    )


def transformation(
    kind: str,
    options: Mapping[str, object] | None = None,
) -> TransformationSpec:
    """Build a Grafana table transformation."""

    return TransformationSpec(
        id=kind,
        options={} if options is None else dict(options),
    )
