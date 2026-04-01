# Copyright 2026 PingCAP, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Generic, Self, TypeVar

from metrics.dsl.api import dashboard as build_dashboard_spec
from metrics.dsl.api import row as build_row_spec
from metrics.dsl.specs import (
    Annotation,
    PanelSpecLike,
    RowSpec,
    TargetSpec,
    VariableSpecLike,
)
from metrics.grafana.common import (
    Expr,
    graph_panel as build_graph_panel,
    heatmap_panel as build_heatmap_panel,
    table_panel as build_table_panel,
    target as build_target,
    timeseries_panel as build_timeseries_panel,
)

_UNSET = object()


def _next_ref(index: int) -> str:
    if index < 0:
        raise ValueError("query index must be non-negative")

    ref = []
    value = index + 1
    while value > 0:
        value, remainder = divmod(value - 1, 26)
        ref.append(chr(ord("A") + remainder))
    return "".join(reversed(ref))


def _next_available_ref(targets: list[TargetSpec]) -> str:
    used = {target.ref for target in targets}
    index = 0
    while True:
        candidate = _next_ref(index)
        if candidate not in used:
            return candidate
        index += 1


def _resolve_explicit_span(
    *,
    span: int | None,
    width: int | None,
    default_span: int | None,
) -> tuple[int | None, int | None]:
    if span is not None or width is not None:
        return span, width
    if default_span is None:
        return None, None
    return None, default_span


@dataclass(slots=True)
class DashboardBuilder:
    title: str
    uid: str
    variables: list[VariableSpecLike]
    annotations: list[Annotation] = field(default_factory=list)
    refresh: str = "10s"
    _rows: list[RowBuilder | RowSpec] = field(default_factory=list)

    def add_row(self, row_spec: RowBuilder | RowSpec) -> Self:
        self._rows.append(row_spec)
        return self

    def build(self) -> object:
        return build_dashboard_spec(
            title=self.title,
            uid=self.uid,
            variables=self.variables,
            annotations=self.annotations,
            rows=[
                row_spec.build() if isinstance(row_spec, RowBuilder) else row_spec
                for row_spec in self._rows
            ],
            refresh=self.refresh,
        )


PanelSpecT = TypeVar("PanelSpecT", bound=PanelSpecLike)


@dataclass(slots=True)
class BasePanelBuilder(Generic[PanelSpecT]):
    title: str
    description: str | None = None
    span: int | None = None
    width: int | None = None
    height: int | None = None
    x: int | None = None
    _targets: list[TargetSpec] = field(default_factory=list)

    def _default_format(self) -> str | None:
        return "time_series"

    def _default_instant(self) -> bool | None:
        return None

    def add_query(
        self,
        expr: str | Expr,
        *,
        legend: str | None = None,
        legend_format: str | None = None,
        ref: str | None = None,
        hide: bool = False,
        format: str | None | object = _UNSET,
        instant: bool | None | object = _UNSET,
    ) -> Self:
        self._targets.append(
            build_target(
                expr,
                legend=legend,
                legend_format=legend_format,
                ref=ref or _next_available_ref(self._targets),
                hide=hide,
                format=self._default_format() if format is _UNSET else format,
                instant=self._default_instant() if instant is _UNSET else instant,
            )
        )
        return self

    def _resolve_dimensions(
        self,
        *,
        default_span: int | None,
        default_height: int | None,
    ) -> tuple[int | None, int | None, int | None]:
        span, width = _resolve_explicit_span(
            span=self.span,
            width=self.width,
            default_span=default_span,
        )
        height = default_height if self.height is None else self.height
        return span, width, height

    def build(
        self,
        *,
        default_span: int | None = None,
        default_height: int | None = None,
    ) -> PanelSpecT:
        raise NotImplementedError


@dataclass(slots=True)
class GraphPanelBuilder(BasePanelBuilder):
    unit: str = "short"
    min: str | int | float | None = None
    max: str | int | float | None = None
    decimals: int | None = None

    def build(
        self,
        *,
        default_span: int | None = None,
        default_height: int | None = None,
    ):
        span, width, height = self._resolve_dimensions(
            default_span=default_span,
            default_height=default_height,
        )
        return build_graph_panel(
            title=self.title,
            targets=self._targets,
            description=self.description,
            unit=self.unit,
            min=self.min,
            max=self.max,
            decimals=self.decimals,
            span=span,
            width=width,
            height=7 if height is None else height,
            x=self.x,
        )


@dataclass(slots=True)
class TimeSeriesPanelBuilder(BasePanelBuilder):
    unit: str = "short"
    min: str | int | float | None = None
    max: str | int | float | None = None
    decimals: int | None = None

    def build(
        self,
        *,
        default_span: int | None = None,
        default_height: int | None = None,
    ):
        span, width, height = self._resolve_dimensions(
            default_span=default_span,
            default_height=default_height,
        )
        return build_timeseries_panel(
            title=self.title,
            targets=self._targets,
            description=self.description,
            unit=self.unit,
            min=self.min,
            max=self.max,
            decimals=self.decimals,
            span=span,
            width=width,
            height=7 if height is None else height,
            x=self.x,
        )


@dataclass(slots=True)
class HeatmapPanelBuilder(BasePanelBuilder):
    unit: str = "short"

    def _default_format(self) -> str | None:
        return "heatmap"

    def _default_instant(self) -> bool | None:
        return None

    def add_query(
        self,
        expr: str | Expr,
        *,
        legend: str | None = None,
        legend_format: str | None = None,
        ref: str | None = None,
        hide: bool = False,
        format: str | None | object = _UNSET,
        instant: bool | None | object = _UNSET,
    ) -> Self:
        if legend is None and legend_format is None:
            legend_format = "{{le}}"
        super(HeatmapPanelBuilder, self).add_query(
            expr,
            legend=legend,
            legend_format=legend_format,
            ref=ref,
            hide=hide,
            format=format,
            instant=instant,
        )
        return self

    def build(
        self,
        *,
        default_span: int | None = None,
        default_height: int | None = None,
    ):
        span, width, height = self._resolve_dimensions(
            default_span=default_span,
            default_height=default_height,
        )
        return build_heatmap_panel(
            title=self.title,
            targets=self._targets,
            description=self.description,
            unit=self.unit,
            span=span,
            width=width,
            height=7 if height is None else height,
            x=self.x,
        )


@dataclass(slots=True)
class TablePanelBuilder(BasePanelBuilder):
    transformations: list[object] = field(default_factory=list)

    def _default_format(self) -> str | None:
        return None

    def add_transformation(self, transformation_spec: object) -> Self:
        self.transformations.append(transformation_spec)
        return self

    def build(
        self,
        *,
        default_span: int | None = None,
        default_height: int | None = None,
    ):
        span, width, height = self._resolve_dimensions(
            default_span=default_span,
            default_height=default_height,
        )
        return build_table_panel(
            title=self.title,
            targets=self._targets,
            description=self.description,
            span=span,
            width=width,
            height=7 if height is None else height,
            x=self.x,
            transformations=self.transformations,
        )


@dataclass(slots=True)
class RowBuilder:
    title: str
    collapsed: bool = True
    repeat: str | None = None
    default_height: int | None = None
    default_span: int | None = None
    _panels: list[BasePanelBuilder[PanelSpecLike] | PanelSpecLike] = field(
        default_factory=list
    )

    def add_graph(self, panel: GraphPanelBuilder | PanelSpecLike) -> Self:
        self._panels.append(panel)
        return self

    def add_timeseries(self, panel: TimeSeriesPanelBuilder | PanelSpecLike) -> Self:
        self._panels.append(panel)
        return self

    def add_heatmap(self, panel: HeatmapPanelBuilder | PanelSpecLike) -> Self:
        self._panels.append(panel)
        return self

    def add_table(self, panel: TablePanelBuilder | PanelSpecLike) -> Self:
        self._panels.append(panel)
        return self

    def build(self) -> RowSpec:
        return build_row_spec(
            self.title,
            [
                panel.build(default_span=self.default_span, default_height=self.default_height)
                if isinstance(panel, BasePanelBuilder)
                else panel
                for panel in self._panels
            ],
            collapsed=self.collapsed,
            repeat=self.repeat,
        )


def dashboard(
    *,
    title: str,
    uid: str,
    variables: list[VariableSpecLike],
    annotations: list[Annotation] | None = None,
    refresh: str = "10s",
) -> DashboardBuilder:
    return DashboardBuilder(
        title=title,
        uid=uid,
        variables=list(variables),
        annotations=[] if annotations is None else list(annotations),
        refresh=refresh,
    )


def row(
    title: str,
    *,
    collapsed: bool = True,
    repeat: str | None = None,
    default_height: int | None = None,
    default_span: int | None = None,
) -> RowBuilder:
    return RowBuilder(
        title=title,
        collapsed=collapsed,
        repeat=repeat,
        default_height=default_height,
        default_span=default_span,
    )


def graph(
    title: str,
    *,
    description: str | None = None,
    unit: str = "short",
    min: str | int | float | None = None,
    max: str | int | float | None = None,
    decimals: int | None = None,
    span: int | None = None,
    width: int | None = None,
    height: int | None = None,
    x: int | None = None,
) -> GraphPanelBuilder:
    return GraphPanelBuilder(
        title=title,
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


def timeseries(
    title: str,
    *,
    description: str | None = None,
    unit: str = "short",
    min: str | int | float | None = None,
    max: str | int | float | None = None,
    decimals: int | None = None,
    span: int | None = None,
    width: int | None = None,
    height: int | None = None,
    x: int | None = None,
) -> TimeSeriesPanelBuilder:
    return TimeSeriesPanelBuilder(
        title=title,
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


def heatmap(
    title: str,
    *,
    description: str | None = None,
    unit: str = "short",
    span: int | None = None,
    width: int | None = None,
    height: int | None = None,
    x: int | None = None,
) -> HeatmapPanelBuilder:
    return HeatmapPanelBuilder(
        title=title,
        description=description,
        unit=unit,
        span=span,
        width=width,
        height=height,
        x=x,
    )


def table(
    title: str,
    *,
    description: str | None = None,
    span: int | None = None,
    width: int | None = None,
    height: int | None = None,
    x: int | None = None,
) -> TablePanelBuilder:
    return TablePanelBuilder(
        title=title,
        description=description,
        span=span,
        width=width,
        height=height,
        x=x,
    )


__all__ = [
    "DashboardBuilder",
    "GraphPanelBuilder",
    "HeatmapPanelBuilder",
    "RowBuilder",
    "TablePanelBuilder",
    "TimeSeriesPanelBuilder",
    "dashboard",
    "graph",
    "heatmap",
    "row",
    "table",
    "timeseries",
]
